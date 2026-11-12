package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newTestQueue(t *testing.T, cap int) *Queue[string] {
	t.Helper()

	dir := t.TempDir()

	return New[string](dir, t.Name()+".spill", cap)
}

func TestQueueInMemoryFIFO(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t, 4)

	for i := range 4 {
		if err := q.Push("v" + strconv.Itoa(i)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	if q.Len() != 4 {
		t.Fatalf("len = %d, want 4", q.Len())
	}

	if q.spilling {
		t.Fatal("unexpected spill")
	}

	for i := range 4 {
		v, ok, err := q.Pop(context.Background())
		if err != nil || !ok || v != "v"+strconv.Itoa(i) {
			t.Fatalf("pop %d: v=%q ok=%v err=%v", i, v, ok, err)
		}
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestQueueSpillFIFO(t *testing.T) {
	t.Parallel()

	const (
		cap = 2
		n   = 64
	)

	dir := t.TempDir()
	name := "spill.json"
	q := New[string](dir, name, cap)

	for i := range n {
		if err := q.Push("v" + strconv.Itoa(i)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	if !q.spilling {
		t.Fatal("spill not activated")
	}

	if len(q.ring) != cap {
		t.Fatalf("ring grew: %d, want %d", len(q.ring), cap)
	}

	if q.Len() != n {
		t.Fatalf("len = %d, want %d", q.Len(), n)
	}

	spillPath := q.spillPath

	for i := range n {
		v, ok, err := q.Pop(context.Background())
		if err != nil || !ok {
			t.Fatalf("pop %d: ok=%v err=%v", i, ok, err)
		}

		if want := "v" + strconv.Itoa(i); v != want {
			t.Fatalf("pop %d: v=%q want %q", i, v, want)
		}
	}

	if q.spilling {
		t.Fatal("spill not finalized after drain")
	}

	if _, err := os.Stat(spillPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill file residue: %v", err)
	}

	if q.Len() != 0 {
		t.Fatalf("len after drain = %d", q.Len())
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestQueueSpillThenMemoryRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	q := New[int](dir, "spill.json", 2)

	for i := range 8 {
		if err := q.Push(i); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	for i := range 8 {
		v, ok, err := q.Pop(context.Background())
		if err != nil || !ok || v != i {
			t.Fatalf("pop %d: v=%d ok=%v err=%v", i, v, ok, err)
		}
	}

	// spill must be gone, queue reusable in-memory again.
	if q.spilling {
		t.Fatal("spill still active")
	}

	for i := 100; i < 102; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("repush: %v", err)
		}
	}

	for i := 100; i < 102; i++ {
		v, ok, err := q.Pop(context.Background())
		if err != nil || !ok || v != i {
			t.Fatalf("repop: v=%d ok=%v err=%v", v, ok, err)
		}
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestQueuePopBlockingAndCancel(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t, 2)

	errCh := make(chan error, 1)

	go func() {
		_, _, err := q.Pop(context.Background())
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("pop returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := q.Pop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	go func() {
		_ = q.Push("late")
	}()

	if err := <-errCh; err != nil {
		t.Fatalf("blocked pop: %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, ok, err := q.Pop(context.Background()); ok || err != nil {
		t.Fatalf("pop on closed: ok=%v err=%v", ok, err)
	}

	if err := q.Push("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("push on closed: %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestQueueCloseUnblocksWaiters(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t, 1)

	errCh := make(chan error, 1)

	go func() {
		_, ok, err := q.Pop(context.Background())
		if err == nil && ok {
			err = errors.New("unexpected value")
		}

		errCh <- err
	}()

	runtime.Gosched()

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock waiter")
	}
}

func TestQueueConcurrent(t *testing.T) {
	t.Parallel()

	const (
		producers   = 8
		perProducer = 200
		consumers   = 4
		total       = producers * perProducer
	)

	dir := t.TempDir()
	q := New[int](dir, "spill.json", 7)

	var (
		wgP sync.WaitGroup
		wgC sync.WaitGroup
		mu  sync.Mutex
		got = make(map[int]struct{}, total)
	)

	for p := range producers {
		wgP.Add(1)

		go func(p int) {
			defer wgP.Done()

			for i := range perProducer {
				if err := q.Push(p*perProducer + i); err != nil {
					t.Errorf("push: %v", err)

					return
				}
			}
		}(p)
	}

	for range consumers {
		wgC.Add(1)

		go func() {
			defer wgC.Done()

			for {
				v, ok, err := q.Pop(context.Background())
				if err != nil {
					t.Errorf("pop: %v", err)

					return
				}

				if !ok {
					return
				}

				mu.Lock()
				got[v] = struct{}{}
				mu.Unlock()
			}
		}()
	}

	wgP.Wait()

	for q.Len() > 0 {
		time.Sleep(time.Millisecond)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wgC.Wait()

	if len(got) != total {
		t.Fatalf("got %d items, want %d", len(got), total)
	}
}

func TestQueueSpillCreateFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	q := New[int](dir, "spill.json", 1)
	q.makeSpill = func(_, _ string) (*os.File, error) {
		return nil, errors.New("disk offline")
	}

	if err := q.Push(1); err != nil {
		t.Fatalf("first push: %v", err)
	}

	err := q.Push(2)

	var se *SpillError
	if !errors.As(err, &se) || se.Op != "create" {
		t.Fatalf("want create SpillError, got %v", err)
	}

	if _, _, err := q.Pop(context.Background()); err == nil {
		t.Fatal("pop after storage failure must error")
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestQueueSpillWriteFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "spill.json")

	if f, err := os.Create(path); err != nil {
		t.Fatalf("seed: %v", err)
	} else {
		_ = f.Close()
	}

	// unrelated marker that cleanup must never touch.
	marker := filepath.Join(dir, "marker.txt")

	if f, err := os.Create(marker); err != nil {
		t.Fatalf("marker: %v", err)
	} else {
		_ = f.Close()
	}

	q := New[int](dir, "spill.json", 1)
	q.makeSpill = func(_, _ string) (*os.File, error) {
		return os.Open(path) // read-only file handle: writes fail
	}

	_ = q.Push(1)

	err := q.Push(2)

	var se *SpillError
	if !errors.As(err, &se) || se.Op != "write" {
		t.Fatalf("want write SpillError, got %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// all spill state must be gone after close, unrelated files untouched.
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker lost: %v", err)
	}
}

func TestQueueSpillReadFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "spill.json")
	q := New[int](dir, "spill.json", 1)

	q.makeSpill = func(_, _ string) (*os.File, error) {
		return os.Create(path)
	}

	for i := range 4 {
		if err := q.Push(i); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	if !q.spilling {
		t.Fatal("expected spill")
	}

	// Remove the spill file before the first pop opens it for reading.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}

	if _, _, err := q.Pop(context.Background()); err == nil {
		t.Fatal("want read error")
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("spill residue after failure: %v %v", entries, err)
	}
}

func TestQueueCloseRemovesSpill(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "spill.json")
	q := New[int](dir, "spill.json", 1)

	for i := range 10 {
		if err := q.Push(i); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("spill not created: %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill residue after close: %v", err)
	}

	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("dir not clean: %v %v", entries, err)
	}
}

func TestQueueMinCap(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t, 0)

	if q.Cap() != 1 {
		t.Fatalf("cap = %d, want 1", q.Cap())
	}

	if err := q.Push("a"); err != nil {
		t.Fatalf("push: %v", err)
	}

	if q.spilling {
		t.Fatal("cap-1 queue must hold one item in memory")
	}

	if err := q.Push("b"); err != nil {
		t.Fatalf("spill push: %v", err)
	}

	if !q.spilling {
		t.Fatal("second item must spill")
	}

	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
