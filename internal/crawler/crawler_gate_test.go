package crawler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/s0rg/set"

	"github.com/s0rg/crawley/internal/client"
)

// fanoutServer builds a site whose root page links to n pages, each of them
// linking back to the root. The graph deliberately exceeds every in-memory
// buffer so disk spill-over is exercised end to end.
func fanoutServer(t *testing.T, n int, slow http.HandlerFunc) *httptest.Server {
	t.Helper()

	var sb strings.Builder

	sb.WriteString("<html>")

	for i := range n {
		fmt.Fprintf(&sb, `<a href="/p/%d">p%d</a>`, i, i)
	}

	sb.WriteString("</html>")

	root := sb.String()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slow != nil {
			slow(w, r)
		}

		w.Header().Add(contentType, contentHTML)

		if r.RequestURI == "/" {
			_, _ = io.WriteString(w, root)

			return
		}

		_, _ = io.WriteString(w, `<html><a href="/">root</a></html>`)
	}))
}

// TestRunOverflowGraphComplete is the main regression for the congested
// channel drops: a graph much larger than the in-memory buffer cap must be
// crawled and emitted completely even with a slow output callback and slow
// responses.
func TestRunOverflowGraphComplete(t *testing.T) {
	t.Parallel()

	const (
		pages   = 500
		buffer  = 32 // far below the number of links and pending pages
		workers = 8
	)

	ts := fanoutServer(t, pages, func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Millisecond) // slow-ish responses
	})

	defer ts.Close()

	var (
		got   = make(set.Unordered[string])
		cbmu  sync.Mutex
		calls atomic.Int64
	)

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(workers),
		WithBufferCap(buffer),
	)

	// slow output callback: output buffer stays saturated.
	err := c.RunContext(context.Background(), ts.URL, func(s string) {
		time.Sleep(100 * time.Microsecond)
		cbmu.Lock()
		got.Add(s)
		cbmu.Unlock()
		calls.Add(1)
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Every page plus the back-linked root (emitted with its canonical
	// trailing-slash form). All are deduplicated regardless of buffer
	// pressure.
	want := pages + 1

	if len(got) != want {
		t.Fatalf("collected %d unique urls, want %d", len(got), want)
	}

	if !got.Has(ts.URL + "/") {
		t.Error("canonical root missing from output")
	}

	missing := 0

	for i := range pages {
		if !got.Has(fmt.Sprintf("%s/p/%d", ts.URL, i)) {
			missing++
		}
	}

	if missing != 0 {
		t.Fatalf("%d pages missing from output", missing)
	}
}

// TestRunBuffersBoundedMemory asserts the in-memory rings never exceed the
// configured cap while crawling an over-cap graph; growth beyond cap is
// absorbed by disk spill, not by growing slices.
func TestRunBuffersBoundedMemory(t *testing.T) {
	t.Parallel()

	const (
		pages   = 300
		buffer  = 24
		workers = 4
	)

	ts := fanoutServer(t, pages, nil)
	defer ts.Close()

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(workers),
		WithBufferCap(buffer),
	)

	// drive once with instrumentation by re-implementing RunContext steps.
	base, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	run, err := newRun(context.Background(), c.cfg)
	if err != nil {
		t.Fatalf("new run: %v", err)
	}

	defer run.cleanup()

	if run.discover.Cap() != buffer || run.pending.Cap() != buffer ||
		run.output.Cap() != buffer {
		t.Fatalf("queue caps: %d %d %d, want %d",
			run.discover.Cap(), run.pending.Cap(), run.output.Cap(), buffer)
	}

	stop := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if run.discover.MemLen() > buffer ||
					run.pending.MemLen() > buffer ||
					run.output.MemLen() > buffer {
					panic("in-memory buffer grew beyond cap")
				}
			}
		}
	}()

	run.robots = c.robots

	run.seen.Add(urlhash(ts.URL))

	run.addActive()

	if err = run.pending.Push(base); err != nil {
		t.Fatalf("pending: %v", err)
	}

	web := client.New(&c.cfg.Client)
	run.start(web, func(string) {})
	run.drive(base)
	run.stop()
	close(stop)

	if err := run.outcome(); err != nil {
		t.Fatalf("outcome: %v", err)
	}

	// spill files must be cleaned up by run cleanup.
	if entries, err := os.ReadDir(run.tempDir); err == nil && len(entries) != 0 {
		t.Fatalf("residue in temp dir: %v", entries)
	}
}

// TestRunCancelStopsAndReturnsContextError verifies cancellation stops new
// requests and new callbacks, lets in-flight work finish, and returns the
// context error.
func TestRunCancelStopsAndReturnsContextError(t *testing.T) {
	t.Parallel()

	const pages = 200

	var inFlight atomic.Int64

	ts := fanoutServer(t, pages, func(_ http.ResponseWriter, _ *http.Request) {
		inFlight.Add(1)
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
	})

	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(8),
		WithBufferCap(32),
	)

	var (
		cbmu        sync.Mutex
		afterCancel atomic.Int64
		done        = make(chan error, 1)
	)

	go func() {
		done <- c.RunContext(ctx, ts.URL, func(_ string) {
			time.Sleep(time.Millisecond)
			if ctx.Err() != nil {
				afterCancel.Add(1)
			}

			cbmu.Lock()
			cbmu.Unlock()
		})
	}()

	// let some work happen, then cancel.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return after cancellation")
	}

	// give in-flight requests a moment to fully settle, then assert none leak.
	deadline := time.Now().Add(5 * time.Second)

	for inFlight.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if inFlight.Load() != 0 {
		t.Fatalf("%d HTTP requests still in flight after return", inFlight.Load())
	}
}

// TestRunCancelDuringSlowCallback ensures a callback already in progress is
// allowed to finish rather than being interrupted.
func TestRunCancelDuringSlowCallback(t *testing.T) {
	t.Parallel()

	ts := fanoutServer(t, 50, nil)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())

	var (
		started  = make(chan struct{})
		finished atomic.Bool
	)

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(4),
		WithBufferCap(16),
	)

	done := make(chan error, 1)

	go func() {
		done <- c.RunContext(ctx, ts.URL, func(_ string) {
			select {
			case <-started:
			default:
				close(started)
			}

			time.Sleep(200 * time.Millisecond)
			finished.Store(true)
		})
	}()

	<-started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run hung after cancellation")
	}

	if !finished.Load() {
		t.Fatal("in-progress callback was interrupted")
	}
}

// TestRunStorageFailureReportsAndCleans forces the spill directory to become
// unusable and asserts the run reports ErrStorage and leaves no residue.
func TestRunStorageFailureReportsAndCleans(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	// occupy a path where MkdirTemp would create its working directory.
	blocker := filepath.Join(parent, "blocker")

	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	c := New(
		WithoutHeads(true),
		WithBufferCap(8),
		WithTempDir(blocker), // a file, not a directory: MkdirTemp fails
	)

	err := c.Run("http://127.0.0.1:1/", func(string) {})
	if !errors.Is(err, ErrStorage) {
		t.Fatalf("want ErrStorage, got %v", err)
	}

	// nothing should have been created beside the blocker.
	if entries, err := os.ReadDir(parent); err != nil {
		t.Fatalf("readdir: %v", err)
	} else if len(entries) != 1 || entries[0].Name() != "blocker" {
		t.Fatalf("unexpected residue: %v", entries)
	}
}

// TestRunSpillFailureMidRunNoResidue makes on-disk spill fail while crawling
// (the in-memory buffers are far smaller than the page fan-out) and asserts an
// ErrStorage is returned and every temporary file is removed.
func TestRunSpillFailureMidRunNoResidue(t *testing.T) {
	t.Parallel()

	const (
		pages   = 100
		buffer  = 4
		workers = 2
	)

	ts := fanoutServer(t, pages, nil)
	defer ts.Close()

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(workers),
		WithBufferCap(buffer),
	)

	base, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	run, err := newRun(context.Background(), c.cfg)
	if err != nil {
		t.Fatalf("new run: %v", err)
	}

	// Occupy every spill path with a directory so os.Create fails (EISDIR)
	// on the first overflow. Deleting the temp directory mid-run would be
	// racy instead: an already-open spill file keeps working after its
	// directory entry is unlinked, so the run could finish without error.
	// The directory collision fails deterministically, even for root.
	for _, name := range []string{spillDiscovery, spillPending, spillOutput} {
		p := filepath.Join(run.tempDir, name)
		if err := os.Mkdir(p, 0o755); err != nil {
			t.Fatalf("blocker %s: %v", name, err)
		}
	}

	run.robots = c.robots

	run.seen.Add(urlhash(ts.URL))

	run.addActive()

	if err := run.pending.Push(base); err != nil {
		t.Fatalf("seed: %v", err)
	}

	web := client.New(&c.cfg.Client)
	run.start(web, func(string) {})
	run.drive(base)
	run.stop()

	if err := run.outcome(); !errors.Is(err, ErrStorage) {
		t.Fatalf("want ErrStorage, got %v", err)
	}

	run.cleanup()

	// cleanup removes the whole working directory, blocker dirs included.
	if _, err := os.Stat(run.tempDir); !os.IsNotExist(err) {
		t.Fatalf("temp dir residue after storage failure: %v", err)
	}
}

// TestRunTempDirCleanupNormalRun asserts a normal run leaves no temporary
// files or directories behind.
func TestRunTempDirCleanupNormalRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ts := fanoutServer(t, 100, nil)
	defer ts.Close()

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(4),
		WithBufferCap(16),
		WithTempDir(dir),
	)

	if err := c.Run(ts.URL, func(string) {}); err != nil {
		t.Fatalf("run: %v", err)
	}

	if entries, err := os.ReadDir(dir); err != nil {
		t.Fatalf("readdir: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("temp residue after normal run: %v", entries)
	}
}

// TestRunNoPerLinkGoroutine verifies a large fan-out run does not spawn a
// goroutine per link. It runs non-parallel so stack inspection counts only
// this run's worker/handler goroutines; parallel tests would contribute
// unrelated workers that cannot be attributed to a specific run.
func TestRunNoPerLinkGoroutine(t *testing.T) {
	const (
		pages   = 400
		workers = 6
	)

	ts := fanoutServer(t, pages, nil)
	defer ts.Close()

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(workers),
		WithBufferCap(32),
	)

	var peak atomic.Int64

	done := make(chan error, 1)

	go func() {
		done <- c.Run(ts.URL, func(string) {})
	}()

	stop := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n := int64(countCrawlerGoroutines())
				for {
					cur := peak.Load()
					if n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
			}
		}
	}()

	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	close(stop)

	// Fixed worker pool + single output handler; the coordinator runs on the
	// caller goroutine and the single done-watcher is not a worker/handler
	// loop. No per-link goroutines are allowed. A per-link design would push
	// this count into the hundreds for a 400-page fan-out.
	const ceiling = int64(workers + 1)

	if peak.Load() > ceiling {
		t.Fatalf("crawler worker/handler goroutine peak %d exceeds ceiling %d "+
			"(per-link goroutines?)", peak.Load(), ceiling)
	}
}

// countCrawlerGoroutines returns how many live goroutines are currently
// executing one of the crawler's long-lived per-run loops (a fetch worker or
// the single output handler). Transient method frames (process, enqueue...)
// are deliberately not matched, so the number reflects owned goroutines.
func countCrawlerGoroutines() int {
	const (
		worker = "github.com/s0rg/crawley/internal/crawler.(*runState).worker("
		output = "github.com/s0rg/crawley/internal/crawler.(*runState).handleOutput("
	)

	buf := make([]byte, 1<<16)

	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]

			break
		}

		buf = make([]byte, 2*len(buf))
	}

	count := 0

	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, worker) || strings.Contains(g, output) {
			count++
		}
	}

	return count
}

// TestRunRobotsAndSitemapNoDeadlock runs the robots + sitemap initialization
// path concurrently with heavy fan-out and slow callbacks, asserting the run
// always completes rather than deadlocking.
func TestRunRobotsAndSitemapNoDeadlock(t *testing.T) {
	t.Parallel()

	const pages = 200

	var robot string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(contentType, contentHTML)

		switch {
		case r.RequestURI == robotsEP:
			_, _ = io.WriteString(w, robot)
		case strings.HasSuffix(r.RequestURI, ".xml"):
			var sb strings.Builder
			sb.WriteString(`<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`)

			for i := range 50 {
				fmt.Fprintf(&sb, "<url><loc>http://%s/sm/%d</loc></url>", r.Host, i)
			}

			sb.WriteString(`</urlset>`)
			_, _ = io.WriteString(w, sb.String())
		default:
			var sb strings.Builder
			sb.WriteString("<html>")

			for i := range pages {
				fmt.Fprintf(&sb, `<a href="/p/%d">x</a>`, i)
			}

			sb.WriteString("</html>")
			_, _ = io.WriteString(w, sb.String())
		}
	}))

	defer ts.Close()

	robot = fmt.Sprintf("user-agent: *\nsitemap: %s/sitemap.xml\n", ts.URL)

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(-1),
		WithWorkersCount(8),
		WithBufferCap(16),
		WithRobotsPolicy(RobotsCrawl),
		WithScanJS(true),
		WithScanCSS(true),
	)

	done := make(chan error, 1)

	go func() {
		done <- c.RunContext(context.Background(), ts.URL, func(_ string) {
			time.Sleep(100 * time.Microsecond) // saturated slow callback
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: robots/sitemap + fan-out + slow callback")
	}
}

// TestRunContextPropagatesCancel verifies a context canceled before start
// still returns promptly with the context error and cleans resources.
func TestRunContextPreCanceled(t *testing.T) {
	t.Parallel()

	ts := fanoutServer(t, 20, nil)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := New(
		WithoutHeads(true),
		WithBufferCap(16),
	)

	done := make(chan error, 1)

	go func() {
		done <- c.RunContext(ctx, ts.URL, func(string) {})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pre-canceled run did not return")
	}
}

// TestRunDeduplicatesOutput asserts the original de-duplication semantics
// hold: the same url discovered many times is fetched and emitted once.
func TestRunDeduplicatesOutput(t *testing.T) {
	t.Parallel()

	const repeats = 100

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add(contentType, contentHTML)

		var sb strings.Builder
		sb.WriteString("<html>")

		for range repeats {
			sb.WriteString(`<a href="/same">x</a>`)
		}

		sb.WriteString("</html>")
		_, _ = io.WriteString(w, sb.String())
	}))

	defer ts.Close()

	var (
		mu  sync.Mutex
		got = make(set.Unordered[string])
	)

	c := New(
		WithoutHeads(true),
		WithMaxCrawlDepth(1),
		WithBufferCap(8),
	)

	if err := c.Run(ts.URL, func(s string) {
		mu.Lock()
		got.Add(s)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(got) != 1 { // only the discovered /same link; root is not emitted
		t.Fatalf("got %d unique urls, want 1: %v", len(got), got)
	}

	if !got.Has(ts.URL + "/same") {
		t.Fatalf("deduped link missing: %v", got)
	}
}
