// Package queue provides a bounded FIFO queue with transparent temporary
// disk spill-over.
//
// The in-memory part of the queue never holds more than the configured
// capacity: once it is full, every subsequent Push is appended to a temporary
// spill file inside the given directory, so the amount of memory used by the
// queue stays bounded regardless of the number of queued items. Spill files
// are removed automatically when the queue is closed, leaving no residue.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrClosed is returned by Push and Pop when called on a closed queue.
var ErrClosed = errors.New("queue: closed")

// SpillError wraps a failure of the temporary on-disk storage.
type SpillError struct {
	// Op is the failed operation: "create", "write", "read" or "remove".
	Op string
	// Err is the underlying error.
	Err error
}

// Error implements error interface.
func (e *SpillError) Error() string {
	return fmt.Sprintf("queue: spill %s: %v", e.Op, e.Err)
}

// Unwrap returns the wrapped error.
func (e *SpillError) Unwrap() error {
	return e.Err
}

// spillMaker creates a new spill file inside the given directory.
type spillMaker func(dir, name string) (*os.File, error)

func defaultSpillMaker(dir, name string) (*os.File, error) {
	return os.Create(filepath.Join(dir, name))
}

// T is a bounded FIFO queue. The zero value is not usable, create instances
// with New. A queue is safe for concurrent use: any number of producers and
// consumers is allowed.
type Queue[T any] struct {
	dir  string
	name string
	cap  int

	mu     sync.Mutex
	ring   []T
	head   int
	size   int
	closed bool
	err    error

	notify chan struct{}

	spilling  bool
	spillPath string
	spill     *os.File
	readFile  *os.File
	enc       *json.Encoder
	dec       *json.Decoder
	written   int
	read      int

	makeSpill spillMaker
}

// New creates a queue whose in-memory buffer holds at most cap items. Spill
// files are created inside dir using the given file name.
func New[T any](dir, name string, cap int) *Queue[T] {
	if cap < 1 {
		cap = 1
	}

	return &Queue[T]{
		dir:       dir,
		name:      name,
		cap:       cap,
		ring:      make([]T, cap),
		notify:    make(chan struct{}, 1),
		makeSpill: defaultSpillMaker,
	}
}

// Push appends v to the tail of the queue. It never blocks waiting for a
// consumer: when the in-memory buffer is full the item is stored on disk. A
// non-nil error means the temporary storage failed and the queue is unusable.
func (q *Queue[T]) Push(v T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}

	if q.err != nil {
		return q.err
	}

	if q.spilling {
		if err := q.encodeLocked(v); err != nil {
			return q.failLocked("write", err)
		}

		q.written++
		q.signalLocked()

		return nil
	}

	if q.size < q.cap {
		q.ring[(q.head+q.size)%q.cap] = v
		q.size++
		q.signalLocked()

		return nil
	}

	if err := q.startSpillLocked(); err != nil {
		return err
	}

	if err := q.encodeLocked(v); err != nil {
		return q.failLocked("write", err)
	}

	q.written++
	q.signalLocked()

	return nil
}

// Pop removes and returns the head item. It blocks while the queue is empty.
// When the context is canceled the context error is returned. When the queue
// is closed and drained, ok is false with a nil error.
func (q *Queue[T]) Pop(ctx context.Context) (v T, ok bool, err error) {
	for {
		q.mu.Lock()

		switch {
		case q.err != nil:
			err = q.err
			q.mu.Unlock()

			return v, false, err
		case q.size > 0:
			v = q.ring[q.head]

			var zero T

			q.ring[q.head] = zero
			q.head = (q.head + 1) % q.cap
			q.size--
			q.signalLocked()
			q.mu.Unlock()

			return v, true, nil
		case q.spilling:
			v, err = q.decodeLocked()
			if err != nil {
				err = q.failLocked("read", err)
				q.mu.Unlock()

				return v, false, err
			}

			q.read++

			if q.read == q.written {
				q.endSpillLocked()
			}

			q.signalLocked()
			q.mu.Unlock()

			return v, true, nil
		case q.closed:
			q.mu.Unlock()

			return v, false, nil
		}

		q.mu.Unlock()

		select {
		case <-q.notify:
		case <-ctx.Done():
			return v, false, ctx.Err()
		}
	}
}

// Len returns the current number of queued items, both in memory and on disk.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	rv := q.size

	if q.spilling {
		rv += q.written - q.read
	}

	return rv
}

// Cap returns the maximum number of items kept in memory.
func (q *Queue[T]) Cap() int {
	return q.cap
}

// MemLen returns the number of items currently held in the in-memory ring.
// It never exceeds Cap.
func (q *Queue[T]) MemLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.size
}

// Spilling reports whether overflow items are currently stored on disk.
func (q *Queue[T]) Spilling() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.spilling
}

// WaitEmpty blocks until the queue is fully drained (Len is zero) or the
// context is canceled. It returns true when the queue became empty.
func (q *Queue[T]) WaitEmpty(ctx context.Context) bool {
	for {
		q.mu.Lock()

		empty := q.size == 0 && (!q.spilling || q.written == q.read)
		closed := q.closed
		q.mu.Unlock()

		if empty {
			return true
		}

		if closed {
			return false
		}

		select {
		case <-q.notify:
		case <-ctx.Done():
			return false
		}
	}
}

// Close marks the queue as closed, unblocking all waiting consumers and
// removing the spill file if one was created. It is idempotent.
func (q *Queue[T]) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	q.closed = true

	var errs []error

	if q.spill != nil {
		if err := q.spill.Close(); err != nil {
			errs = append(errs, &SpillError{Op: "write", Err: err})
		}
	}

	if q.readFile != nil {
		if err := q.readFile.Close(); err != nil {
			errs = append(errs, &SpillError{Op: "read", Err: err})
		}
	}

	if q.spillPath != "" {
		if err := os.Remove(q.spillPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, &SpillError{Op: "remove", Err: err})
		}
	}

	var zero T

	for i := range q.ring {
		q.ring[i] = zero
	}

	q.size = 0
	q.head = 0
	close(q.notify)

	return errors.Join(errs...)
}

func (q *Queue[T]) startSpillLocked() error {
	f, err := q.makeSpill(q.dir, q.name)
	if err != nil {
		return q.failLocked("create", err)
	}

	q.spill = f
	q.spillPath = f.Name()
	q.enc = json.NewEncoder(f)
	q.written = 0

	for i := 0; i < q.size; i++ {
		if err = q.enc.Encode(q.ring[(q.head+i)%q.cap]); err != nil {
			return q.failLocked("write", err)
		}
	}

	q.written = q.size
	q.read = 0

	var zero T

	for i := range q.ring {
		q.ring[i] = zero
	}

	q.head = 0
	q.size = 0
	q.spilling = true

	return nil
}

func (q *Queue[T]) endSpillLocked() {
	if q.spill != nil {
		_ = q.spill.Close()
	}

	if q.readFile != nil {
		_ = q.readFile.Close()
	}

	if q.spillPath != "" {
		_ = os.Remove(q.spillPath)
	}

	q.spill = nil
	q.readFile = nil
	q.enc = nil
	q.dec = nil
	q.spillPath = ""
	q.written = 0
	q.read = 0
	q.spilling = false
}

func (q *Queue[T]) encodeLocked(v T) error {
	return q.enc.Encode(v)
}

func (q *Queue[T]) decodeLocked() (v T, err error) {
	if q.dec == nil {
		var rf *os.File

		if rf, err = os.Open(q.spillPath); err != nil {
			return v, err
		}

		q.readFile = rf
		q.dec = json.NewDecoder(rf)
	}

	if err = q.dec.Decode(&v); err != nil {
		return v, err
	}

	return v, nil
}

func (q *Queue[T]) failLocked(op string, err error) error {
	if errors.Is(err, ErrClosed) {
		q.err = ErrClosed
	} else {
		q.err = &SpillError{Op: op, Err: err}
	}

	q.endSpillLocked()
	q.signalLocked()

	return q.err
}

func (q *Queue[T]) signalLocked() {
	if q.closed {
		return
	}

	select {
	case q.notify <- struct{}{}:
	default:
	}
}
