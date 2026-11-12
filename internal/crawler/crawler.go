package crawler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/s0rg/set"
	"golang.org/x/net/html/atom"

	"github.com/s0rg/crawley/internal/client"
	"github.com/s0rg/crawley/internal/links"
	"github.com/s0rg/crawley/internal/queue"
	"github.com/s0rg/crawley/internal/robots"
)

type crawlClient interface {
	Get(context.Context, string) (io.ReadCloser, http.Header, error)
	Head(context.Context, string) (http.Header, error)
}

const (
	doubleDash = "//"
	// spill file names for the three bounded buffers.
	spillDiscovery = "crawley-discovery.json"
	spillPending   = "crawley-pending.json"
	spillOutput    = "crawley-output.json"
)

// ErrStorage is returned when the temporary on-disk buffer storage fails.
var ErrStorage = errors.New("crawler: temporary storage failure")

type taskFlag byte

const (
	// TaskDefault marks result for printing only.
	TaskDefault taskFlag = iota
	// TaskCrawl marks result as to-be-crawled.
	TaskCrawl
	// TaskDone marks result as final - crawling ends here.
	TaskDone
)

type crawlResult struct {
	URI  string
	Hash uint64
	Flag taskFlag
}

// Crawler holds crawling process config and state.
type Crawler struct {
	cfg    *config
	robots *robots.TXT
	filter links.TokenFilter
}

// New creates Crawler instance.
func New(opts ...Option) (c *Crawler) {
	cfg := &config{}

	for _, o := range opts {
		o(cfg)
	}

	cfg.validate()

	return &Crawler{
		cfg:    cfg,
		robots: robots.AllowALL(),
		filter: prepareFilter(cfg.AlowedTags),
	}
}

// Run starts crawling process for given base uri.
func (c *Crawler) Run(uri string, urlcb func(string)) error {
	return c.RunContext(context.Background(), uri, urlcb)
}

// RunContext starts crawling process for given base uri, honoring the given
// context.
//
// On context cancellation no new HTTP requests and no new user callbacks are
// started; in-flight HTTP requests and internal tasks run to completion, then
// the context error is returned. A user callback that already started is
// never interrupted.
//
// Every URL accepted by the configured filters is fetched and emitted at
// least once: back-pressure from slow consumers or a slow network never
// causes a link to be dropped. Pending work, discoveries and pending output
// are bounded in memory (see WithBufferCap) and spill to a temporary
// directory instead of growing without limit.
func (c *Crawler) RunContext(ctx context.Context, uri string, urlcb func(string)) (err error) {
	var base *url.URL

	if base, err = url.Parse(uri); err != nil {
		return fmt.Errorf("parse url: %w", err)
	}

	run, err := newRun(ctx, c.cfg)
	if err != nil {
		return err
	}

	defer run.cleanup()

	web := client.New(&c.cfg.Client)

	run.robots = c.robots

	c.initRobots(base, web, run)

	run.seen.Add(urlhash(uri))

	// The base url itself is crawled but not emitted: it is the entry point,
	// not a discovered link, matching the historical output semantics.
	run.addActive()

	if err = run.pending.Push(base); err != nil {
		return run.wrapStorage(err)
	}

	run.start(web, urlcb)
	run.drive(base)
	run.stop()

	return run.outcome()
}

// DumpConfig returns internal config representation.
func (c *Crawler) DumpConfig() string {
	return c.cfg.String()
}

// initRobots fetches and applies robots.txt state to the given run,
// mirroring the resulting policy back onto the crawler for callers (tests)
// that initialize robots without a full run.
func (c *Crawler) initRobots(host *url.URL, web crawlClient, r *runState) {
	r.initRobots(host, web)
	c.robots = r.robots
}

// runState holds the mutable state of a single RunContext invocation. It owns
// exactly three bounded buffers - discovery, pending and output - each backed
// by a fixed-size in-memory ring with transparent disk spill-over, so memory
// consumption stays bounded regardless of the graph size. The de-duplication
// set and the HTML/CSS/JS parsers are intentionally not part of these
// buffers.
type runState struct {
	cfg     *config
	robots  *robots.TXT
	filter  links.TokenFilter
	tempDir string

	rootCtx context.Context
	ctx     context.Context
	cancel  context.CancelFunc

	// driveCtx is canceled when the run finishes (doneCh) or is canceled,
	// letting the coordinator's blocking queue Pop return.
	driveCtx    context.Context
	driveCancel context.CancelFunc

	seen     set.Unordered[uint64]
	discover *queue.Queue[crawlResult]
	pending  *queue.Queue[*url.URL]
	output   *queue.Queue[string]

	// active counts urls that are fetchable but not yet fully processed:
	// items in the pending buffer plus urls currently held by workers. It is
	// incremented only by the coordinator and decremented by workers.
	active atomic.Int64

	// queued counts discovery results sitting in the discovery buffer that
	// the coordinator has not consumed yet. Producers increment on push, the
	// coordinator decrements on pop.
	queued atomic.Int64

	doneCh   chan struct{}
	doneOnce sync.Once

	workers  sync.WaitGroup
	handlers sync.WaitGroup

	storeErr error
	errMu    sync.Mutex
}

func newRun(ctx context.Context, cfg *config) (r *runState, err error) {
	cap := cfg.BufferCap
	if cap == 0 {
		cap = defaultBufferCap
	}

	dir, err := os.MkdirTemp(cfg.TempDir, "crawley-*")
	if err != nil {
		return nil, fmt.Errorf("%w: create temp dir: %v", ErrStorage, err)
	}

	child, cancel := context.WithCancel(ctx)

	// driveCtx is canceled either by the parent context or by the done
	// channel closing (all work finished), so the coordinator can block on a
	// queue Pop and still wake up when the counters reach zero.
	driveCtx, driveCancel := context.WithCancel(child)

	r = &runState{
		cfg:         cfg,
		robots:      robots.AllowALL(),
		filter:      prepareFilter(cfg.AlowedTags),
		tempDir:     dir,
		rootCtx:     ctx,
		ctx:         child,
		cancel:      cancel,
		driveCtx:    driveCtx,
		driveCancel: driveCancel,
		seen:        make(set.Unordered[uint64]),
		discover:    queue.New[crawlResult](dir, spillDiscovery, cap),
		pending:     queue.New[*url.URL](dir, spillPending, cap),
		output:      queue.New[string](dir, spillOutput, cap),
		doneCh:      make(chan struct{}),
	}

	// One goroutine per run (never per link): wakes the coordinator when all
	// active work and buffered discoveries are exhausted.
	go func() {
		select {
		case <-r.doneCh:
			driveCancel()
		case <-driveCtx.Done():
		}
	}()

	return r, nil
}

// addActive marks one more url as pending/in-flight.
func (r *runState) addActive() {
	r.active.Add(1)
}

// finishActive marks one in-flight url as fully processed. A worker pushes
// every discovered link (incrementing queued) before calling this, so the
// counters can reach zero here only when genuinely no work remains.
func (r *runState) finishActive() {
	r.active.Add(-1)
	r.checkDone()
}

// addQueued marks one discovery result as buffered.
func (r *runState) addQueued() {
	r.queued.Add(1)
}

// takeQueued consumes one buffered discovery result. It never terminates the
// run on its own: the coordinator calls checkDone only after the consumed
// item has been fully handled, so a pending emit cannot be cut short.
func (r *runState) takeQueued() {
	r.queued.Add(-1)
}

// checkDone closes the done channel once no url is active and no discovery
// result remains buffered.
func (r *runState) checkDone() {
	if r.active.Load() == 0 && r.queued.Load() == 0 {
		r.doneOnce.Do(func() { close(r.doneCh) })
	}
}

// failStorage records the first storage error and cancels the run so no new
// work starts.
func (r *runState) failStorage(err error) {
	r.errMu.Lock()

	if r.storeErr == nil {
		r.storeErr = fmt.Errorf("%w: %v", ErrStorage, err)
	}

	r.errMu.Unlock()

	r.cancel()
}

func (r *runState) wrapStorage(err error) error {
	r.failStorage(err)

	return fmt.Errorf("%w: %v", ErrStorage, err)
}

// emit enqueues an url for the user callback, applying the directory output
// policy. It applies back-pressure (spilling to disk) instead of dropping
// the url.
func (r *runState) emit(u string) error {
	if !r.shallHandle(u) {
		return nil
	}

	if err := r.output.Push(u); err != nil {
		return r.wrapStorage(err)
	}

	return nil
}

func (r *runState) shallHandle(u string) (show bool) {
	show = true

	idx := strings.LastIndexByte(u, '/')
	if idx == -1 {
		return false
	}

	switch r.cfg.Dirs {
	case DirsHide:
		show = isResorce(u[idx:])
	case DirsOnly:
		show = !isResorce(u[idx:])
	}

	return show
}

// enqueue applies crawl filters and, when the url is crawlable, puts it into
// the pending buffer. Rejected urls are simply not fetched.
func (r *runState) enqueue(base *url.URL, t *crawlResult) (yes bool) {
	if t.Flag != TaskCrawl {
		return false
	}

	u, err := url.Parse(t.URI)
	if err != nil {
		return false
	}

	if !canCrawl(base, u, r.cfg.Depth, r.cfg.Subdomains) ||
		r.robots.Forbidden(u.Path) ||
		(r.cfg.Dirs == DirsOnly && isResorce(u.Path)) {
		return false
	}

	if err := r.pending.Push(u); err != nil {
		r.failStorage(err)

		return false
	}

	return true
}

// start launches the fixed pool of fetch workers and the single output
// handler goroutine. Goroutines are created once per run, never per link.
func (r *runState) start(web crawlClient, urlcb func(string)) {
	workers := r.cfg.Client.Workers

	r.workers.Add(workers)

	for i := 0; i < workers; i++ {
		go r.worker(web)
	}

	r.handlers.Add(1)

	go r.handleOutput(urlcb)
}

// drive is the single coordinator goroutine. It de-duplicates every
// discovered link, schedules fetchable urls and emits output.
//
// Two counters make termination exact: active counts urls being fetched or
// waiting in the pending buffer, queued counts discoveries not yet consumed.
// A run finishes only when both are zero, which cannot happen while a worker
// is still parsing a large fan-out page, nor while its results are buffered.
func (r *runState) drive(base *url.URL) {
	for {
		select {
		case <-r.doneCh:
			return
		case <-r.ctx.Done():
			return
		default:
		}

		t, ok, err := r.discover.Pop(r.driveCtx)
		if err != nil || !ok {
			return
		}

		if t.Flag == TaskDone {
			continue
		}

		r.takeQueued()

		if !r.seen.Add(t.Hash) {
			r.checkDone()

			continue // duplicate: no fetch, no output
		}

		if r.enqueue(base, &t) {
			r.addActive()
		}

		if err := r.emit(t.URI); err != nil {
			return
		}

		r.checkDone()
	}
}

// stop waits for all in-flight HTTP requests and internal tasks, then drains
// and stops the output handler. The pending buffer is closed before waiting
// so workers blocked on it wake up and exit once the run is canceled.
// Buffers are closed only after every producer has exited, so no
// send-on-closed-channel style failures can happen.
func (r *runState) stop() {
	r.cancel()
	r.driveCancel()
	_ = r.pending.Close()
	r.workers.Wait()

	// Normal completion must wait for output to be exhausted; on
	// cancellation the output handler keeps draining without starting new
	// callbacks (see handleOutput), while any callback already in progress
	// is allowed to finish. Waiting without a timeout honors that contract.
	r.output.WaitEmpty(context.Background())

	_ = r.discover.Close()
	_ = r.output.Close()
	r.handlers.Wait()
}

// cleanup releases every resource owned by the run, including spill files.
// It is safe to call multiple times.
func (r *runState) cleanup() {
	r.cancel()
	r.driveCancel()

	_ = r.discover.Close()
	_ = r.pending.Close()
	_ = r.output.Close()

	if r.tempDir != "" {
		if err := os.RemoveAll(r.tempDir); err != nil {
			log.Printf("[-] remove temp dir %s: %v", r.tempDir, err)
		}
	}
}

func (r *runState) outcome() error {
	r.errMu.Lock()
	err := r.storeErr
	r.errMu.Unlock()

	if err != nil {
		return err
	}

	if err := r.rootCtx.Err(); err != nil {
		return err
	}

	return nil
}

func (r *runState) initRobots(host *url.URL, web crawlClient) {
	if r.cfg.Robots == RobotsIgnore {
		return
	}

	// Honor cancellation while keeping the configured request timeout bound.
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Client.Timeout)
	defer cancel()

	body, _, err := web.Get(ctx, robots.URL(host))
	if err != nil {
		var herr client.HTTPError

		if !errors.As(err, &herr) {
			log.Println("[-] GET /robots.txt:", err)

			return
		}

		if herr.Code() == http.StatusInternalServerError {
			r.robots = robots.DenyALL()
		}

		return
	}

	defer body.Close()

	rbt, err := robots.FromReader(r.cfg.Client.UserAgent, body)
	if err != nil {
		log.Println("[-] parse robots.txt:", err)

		return
	}

	r.robots = rbt

	r.crawlRobots(host)
}

func (r *runState) crawlRobots(host *url.URL) {
	base := *host
	base.Fragment = ""
	base.RawQuery = ""

	for _, u := range r.robots.Links() {
		t := base
		t.Path = u

		r.linkHandler(atom.A, t.String())
	}

	for _, u := range r.robots.Sitemaps() {
		if _, e := url.Parse(u); e == nil {
			r.crawlHandler(u)
		}
	}
}

func (r *runState) isIgnored(v string) (yes bool) {
	if len(r.cfg.Ignored) == 0 {
		return
	}

	return slices.ContainsFunc(r.cfg.Ignored, func(s string) bool {
		return strings.Contains(v, s)
	})
}

// linkHandler is the HTML link callback invoked by parsers inside worker
// goroutines. Every result is handed to the bounded discovery buffer;
// back-pressure spills to disk rather than timing out and dropping links.
func (r *runState) linkHandler(a atom.Atom, s string) {
	if s == "" {
		// mirrors the parser's handleNotEmpty: empty links never existed.
		return
	}

	res := crawlResult{
		URI:  s,
		Hash: urlhash(s),
	}

	fetch := (a == atom.A || a == atom.Iframe) ||
		(r.cfg.ScanJS && a == atom.Script) ||
		(r.cfg.ScanCSS && a == atom.Link)

	if fetch && !r.isIgnored(s) {
		res.Flag = TaskCrawl
	}

	// Count the buffered discovery before it becomes visible, so the run
	// cannot terminate while a discovered link is still queued.
	r.addQueued()

	if err := r.discover.Push(res); err != nil {
		r.takeQueued()

		if !errors.Is(err, queue.ErrClosed) {
			r.failStorage(err)
		}
	}
}

func (r *runState) staticHandler(s string) {
	r.linkHandler(atom.Link, s)
}

func (r *runState) crawlHandler(s string) {
	r.linkHandler(atom.A, s)
}

func (r *runState) process(
	web crawlClient,
	base *url.URL,
	uri string,
) {
	// Derive from the run context so cancellation interrupts an in-flight
	// request instead of waiting out the full timeout; the timeout still
	// bounds the request when the context is not canceled.
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Client.Timeout)
	defer cancel()

	body, hdrs, err := web.Get(ctx, uri)
	if err != nil {
		var herr client.HTTPError

		// ignore any http errors, just parse body (if any)
		if !errors.As(err, &herr) {
			log.Printf("[-] GET %s: %v", uri, err)

			return
		}
	}

	handleStatic := func(s string) {
		var ok bool

		switch {
		case strings.HasPrefix(s, doubleDash):
			s, ok = base.Scheme+s, true
		case strings.Contains(s, doubleDash):
			ok = true
		default:
			s, ok = resolveRef(uri, s)
		}

		if ok {
			r.staticHandler(s)
		}
	}

	content := hdrs.Get(contentType)

	switch {
	case isHTML(content):
		links.ExtractHTML(body, base, links.HTMLParams{
			Brute:        r.cfg.Brute,
			ScanJS:       r.cfg.ScanJS,
			ScanCSS:      r.cfg.ScanCSS,
			Filter:       r.filter,
			HandleHTML:   r.linkHandler,
			HandleStatic: handleStatic,
		})
	case isSitemap(uri):
		links.ExtractSitemap(body, base, r.crawlHandler)
	case r.cfg.ScanJS && isJS(content, uri):
		links.ExtractJS(body, handleStatic)
	case r.cfg.ScanCSS && isCSS(content, uri):
		links.ExtractCSS(body, handleStatic)
	}

	client.Discard(body)
}

// worker runs the fetch loop for one of the fixed worker goroutines. On
// cancellation it stops starting new requests, but any url already picked up
// is fully processed (in-flight HTTP and parsing) and then marked complete.
func (r *runState) worker(web crawlClient) {
	defer r.workers.Done()

	for {
		uri, ok, err := r.pending.Pop(r.ctx)
		if err != nil || !ok {
			return
		}

		if r.cfg.Delay > 0 {
			select {
			case <-time.After(r.cfg.Delay):
			case <-r.ctx.Done():
			}
		}

		if r.ctx.Err() == nil {
			r.processURL(web, uri)
		}

		// All links discovered while processing this url are already queued
		// (and counted) before the active slot is released, so a worker that
		// produced links never drives the counters to zero.
		r.finishActive()
	}
}

func (r *runState) processURL(web crawlClient, uri *url.URL) {
	us := uri.String()

	if r.cfg.NoHEAD {
		if canParse(uri.Path) {
			r.process(web, uri, us)
		}

		return
	}

	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Client.Timeout)
	defer cancel()

	hdrs, err := web.Head(ctx, us)
	if err != nil {
		log.Printf("[-] HEAD %s: %v", us, err)

		return
	}

	ct := hdrs.Get(contentType)

	canProcess := isHTML(ct) ||
		isSitemap(us) ||
		(r.cfg.ScanJS && isJS(ct, us)) ||
		(r.cfg.ScanCSS && isCSS(ct, us))

	if canProcess {
		r.process(web, uri, us)
	}
}

// handleOutput is the single output goroutine. It drains the bounded output
// buffer completely. After cancellation no new callbacks start, while a
// callback already in progress runs to completion. A nil callback means the
// caller only wants the crawl side effects, so output is still drained.
func (r *runState) handleOutput(urlcb func(string)) {
	defer r.handlers.Done()

	for {
		s, ok, err := r.output.Pop(context.Background())
		if err != nil || !ok {
			return
		}

		if urlcb == nil || r.rootCtx.Err() != nil {
			continue // no callback, or canceled: drain without invoking it
		}

		urlcb(s)
	}
}
