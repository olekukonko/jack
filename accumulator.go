package jack

import (
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// ErrAccumulatorClosed is returned when operating on a closed accumulator.
var ErrAccumulatorClosed = errors.New("accumulator closed")

// AccumulatorMetrics tracks operational statistics.
type AccumulatorMetrics struct {
	Flushes      atomic.Uint64 // total flush calls
	ItemsAdded   atomic.Uint64 // items successfully added
	ItemsFlushed atomic.Uint64 // items sent to flush handler
	Errors       atomic.Uint64 // flush handler errors
	TimerFlushes atomic.Uint64 // flushes triggered by timer
	SizeFlushes  atomic.Uint64 // flushes triggered by maxSize
}

// AccumulatorOption configures an Accumulator.
type AccumulatorOption func(*accumulatorOpt)

type accumulatorOpt struct {
	maxSize    int
	maxWait    time.Duration
	observable Observable[Event]
}

// AccumulatorWithMaxSize enables automatic flush when buffer reaches n items.
func AccumulatorWithMaxSize(n int) AccumulatorOption {
	return func(o *accumulatorOpt) {
		if n > 0 {
			o.maxSize = n
		}
	}
}

// AccumulatorWithMaxWait enables automatic flush after d since first add.
func AccumulatorWithMaxWait(d time.Duration) AccumulatorOption {
	return func(o *accumulatorOpt) {
		if d > 0 {
			o.maxWait = d
		}
	}
}

// AccumulatorWithObservable attaches an event observer.
func AccumulatorWithObservable(obs Observable[Event]) AccumulatorOption {
	return func(o *accumulatorOpt) { o.observable = obs }
}

// Accumulator batches items and flushes them based on size, time, or explicit
// caller demand — whichever comes first.
//
// Modes:
//   - Size only:  AccumulatorWithMaxSize(n) — flush when buffer full
//   - Time only:  AccumulatorWithMaxWait(d) — flush after d since first add
//   - Both:       size AND time — flush on whichever fires first
//   - Explicit:   neither option — only Flush() triggers flush
//
// maxSize and maxWait are trigger thresholds, not hard limits: a flush drains
// whatever is buffered, so concurrent writers can briefly push a batch past
// maxSize before the flush runs. A flush handler error is returned to the
// caller (Add or Flush) and also counted in Errors, logged, and published as an
// "accumulator_flush_failed" event; the failed batch is not retried.
//
// Thread-safe. Call NewAccumulator to create one.
type Accumulator struct {
	mu      sync.Mutex
	buf     []any
	flush   func([]any) error
	maxSize int
	maxWait time.Duration

	timer    *time.Timer
	firstAdd int64 // unix nano; 0 means no pending batch
	closed   atomic.Bool
	metrics  *AccumulatorMetrics
	opts     accumulatorOpt
	logger   *ll.Logger
}

// NewAccumulator creates a batch accumulator.
func NewAccumulator(flush func([]any) error, opts ...AccumulatorOption) *Accumulator {
	if flush == nil {
		panic("accumulator: flush function required")
	}
	options := accumulatorOpt{}
	for _, opt := range opts {
		opt(&options)
	}
	a := &Accumulator{
		flush:   flush,
		maxSize: options.maxSize,
		maxWait: options.maxWait,
		metrics: &AccumulatorMetrics{},
		opts:    options,
	}
	if logger != nil {
		a.logger = logger.Namespace("accumulator")
	} else {
		a.logger = &ll.Logger{}
	}
	return a
}

// Metrics returns current metrics.
func (a *Accumulator) Metrics() *AccumulatorMetrics {
	return a.metrics
}

// Add appends an item. Triggers auto-flush if maxSize reached or maxWait
// elapsed since first add. Returns ErrAccumulatorClosed if closed, and the
// flush handler's error when this call itself triggered a size flush.
func (a *Accumulator) Add(item any) error {
	if a.closed.Load() {
		return ErrAccumulatorClosed
	}

	a.mu.Lock()
	if a.closed.Load() {
		a.mu.Unlock()
		return ErrAccumulatorClosed
	}

	// Start timer on first add of a new batch
	if len(a.buf) == 0 && a.maxWait > 0 {
		a.firstAdd = time.Now().UnixNano()
		if a.timer == nil {
			a.timer = time.AfterFunc(a.maxWait, a.timerFired)
		} else {
			a.timer.Reset(a.maxWait)
		}
	}

	a.buf = append(a.buf, item)
	shouldFlush := a.maxSize > 0 && len(a.buf) >= a.maxSize
	a.mu.Unlock()

	a.metrics.ItemsAdded.Add(1)

	if shouldFlush {
		flushed, err := a.flushBatch()
		if flushed {
			a.metrics.SizeFlushes.Add(1)
		}
		return err
	}
	return nil
}

// Flush immediately flushes the current batch. Safe concurrently.
// Returns nil if buffer is empty.
func (a *Accumulator) Flush() error {
	_, err := a.flushBatch()
	return err
}

// flushBatch swaps out the pending batch and hands it to the flush handler.
// It reports whether a batch was actually flushed so callers can attribute the
// flush to the right trigger (size, timer, explicit) without double counting
// when a concurrent flush already drained the buffer.
func (a *Accumulator) flushBatch() (bool, error) {
	a.mu.Lock()
	if len(a.buf) == 0 {
		a.mu.Unlock()
		return false, nil
	}
	batch := a.buf
	a.buf = nil
	a.firstAdd = 0
	if a.timer != nil {
		a.timer.Stop()
	}
	a.mu.Unlock()

	a.metrics.Flushes.Add(1)
	a.metrics.ItemsFlushed.Add(uint64(len(batch)))

	err := a.safeFlush(batch)
	if err != nil {
		a.metrics.Errors.Add(1)
		a.logger.Warn("Accumulator flush failed: %v", err)
		if a.opts.observable != nil {
			a.opts.observable.Notify(Event{Type: "accumulator_flush_failed", Time: time.Now(), Err: err})
		}
		return true, err
	}
	if a.opts.observable != nil {
		a.opts.observable.Notify(Event{Type: "accumulator_flush", Time: time.Now()})
	}
	return true, nil
}

// timerFired is called by the time.AfterFunc when maxWait elapses.
func (a *Accumulator) timerFired() {
	a.mu.Lock()
	if a.firstAdd == 0 {
		a.mu.Unlock()
		return
	}
	elapsed := time.Since(time.Unix(0, a.firstAdd))
	if elapsed < a.maxWait {
		// Timer fired early; reset for remaining time
		if a.timer != nil {
			a.timer.Reset(a.maxWait - elapsed)
		}
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()

	if flushed, _ := a.flushBatch(); flushed {
		a.metrics.TimerFlushes.Add(1)
	}
}

// safeFlush invokes flush with panic recovery.
func (a *Accumulator) safeFlush(batch []any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &CaughtPanic{Value: r, Stack: debug.Stack()}
		}
	}()
	return a.flush(batch)
}

// Len returns buffered item count.
func (a *Accumulator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.buf)
}

// Close stops accepting items and flushes remaining batch.
func (a *Accumulator) Close() error {
	if !a.closed.CompareAndSwap(false, true) {
		return ErrAccumulatorClosed
	}
	return a.Flush()
}
