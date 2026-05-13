package jack

import (
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// ErrCoalescerClosed is returned when operating on a closed coalescer.
var ErrCoalescerClosed = errors.New("coalescer closed")

// Coalescer merges discrete items into batches and flushes them together.
// It is useful for write coalescing, metrics aggregation, or event batching.
type Coalescer struct {
	mu      sync.Mutex
	buf     []interface{}
	flush   func([]interface{}) error
	maxSize int
	maxWait time.Duration
	timer   *time.Timer
	closed  atomic.Bool
	metrics *CoalescerMetrics
	opts    coalesceOpt
	logger  *ll.Logger
}

// coalesceOpt holds configuration options for Coalescer.
type coalesceOpt struct {
	observable Observable[Event]
}

// CoalescerOption is a functional option for configuring Coalescer.
type CoalescerOption func(*coalesceOpt)

// CoalescerWithObservable sets an observable for coalescer events.
func CoalescerWithObservable(obs Observable[Event]) CoalescerOption {
	return func(opts *coalesceOpt) { opts.observable = obs }
}

// CoalescerMetrics holds atomic counters for observability.
type CoalescerMetrics struct {
	Batches atomic.Uint64
	Items   atomic.Uint64
	Flushes atomic.Uint64
	Errors  atomic.Uint64
}

// NewCoalescer creates a batching coalescer.
func NewCoalescer(flush func([]interface{}) error, maxSize int, maxWait time.Duration, opts ...CoalescerOption) *Coalescer {
	options := coalesceOpt{}
	for _, opt := range opts {
		opt(&options)
	}
	c := &Coalescer{
		flush:   flush,
		maxSize: maxSize,
		maxWait: maxWait,
		metrics: &CoalescerMetrics{},
		opts:    options,
	}
	if logger != nil {
		c.logger = logger.Namespace("coalescer")
	} else {
		c.logger = &ll.Logger{}
	}
	return c
}

// Metrics returns a snapshot of current metrics.
func (c *Coalescer) Metrics() *CoalescerMetrics {
	return c.metrics
}

// Add appends an item to the batch. If the batch reaches maxSize,
// it is flushed immediately. Otherwise a timer is started for maxWait.
func (c *Coalescer) Add(item interface{}) error {
	if c.closed.Load() {
		return ErrCoalescerClosed
	}

	c.mu.Lock()
	c.buf = append(c.buf, item)
	shouldFlush := len(c.buf) >= c.maxSize
	if !shouldFlush && c.timer == nil {
		c.timer = time.AfterFunc(c.maxWait, func() {
			c.Flush()
		})
	}
	c.mu.Unlock()

	c.metrics.Items.Add(1)

	if shouldFlush {
		return c.Flush()
	}
	return nil
}

// Flush immediately flushes the current batch, if any.
func (c *Coalescer) Flush() error {
	c.mu.Lock()
	if len(c.buf) == 0 {
		c.mu.Unlock()
		return nil
	}
	batch := c.buf
	c.buf = nil
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()

	c.metrics.Batches.Add(1)
	c.metrics.Flushes.Add(1)

	err := c.safeFlush(batch)
	if err != nil {
		c.metrics.Errors.Add(1)
		c.logger.Warn("Coalescer flush failed: %v", err)
		if c.opts.observable != nil {
			c.opts.observable.Notify(Event{Type: "coalescer_flush_failed", Time: time.Now(), Err: err})
		}
	} else if c.opts.observable != nil {
		c.opts.observable.Notify(Event{Type: "coalescer_flush", Time: time.Now()})
	}

	// If new items arrived while we were flushing, ensure they are not stranded.
	c.mu.Lock()
	if len(c.buf) > 0 && c.timer == nil {
		c.timer = time.AfterFunc(c.maxWait, func() {
			c.Flush()
		})
	}
	c.mu.Unlock()

	return err
}

// safeFlush invokes the flush callback with panic recovery.
func (c *Coalescer) safeFlush(batch []interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &CaughtPanic{
				Value: r,
				Stack: debug.Stack(),
			}
		}
	}()
	return c.flush(batch)
}

// Close stops accepting new items and flushes the remaining batch.
func (c *Coalescer) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrCoalescerClosed
	}
	return c.Flush()
}
