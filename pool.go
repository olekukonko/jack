package jack

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// PoolMetrics tracks operational statistics for a Pool.
// All fields use atomic operations and are safe for concurrent reads without locking.
type PoolMetrics struct {
	TasksSubmitted  atomic.Uint64 // total tasks accepted into the queue
	TasksCompleted  atomic.Uint64 // tasks that finished without error
	TasksFailed     atomic.Uint64 // tasks that returned an error or panicked
	TasksRejected   atomic.Uint64 // tasks dropped due to full queue or closed pool
	PanicsRecovered atomic.Uint64 // panics caught inside task execution
	QueueDepth      atomic.Int64  // current number of tasks waiting in the channel
	MaxQueueDepth   atomic.Int64  // high-water mark of QueueDepth
	TotalDurationNs atomic.Int64  // cumulative nanoseconds spent executing tasks
	ActiveWorkers   atomic.Int64  // workers currently executing a task
}

// Pool manages a fixed number of worker goroutines to execute tasks concurrently.
// It supports task submission with or without context, shutdown with timeout, and observability.
type Pool struct {
	tasks      chan job
	quitOnce   sync.Once
	shutdownWg sync.WaitGroup
	observable Observable[Event]
	numWorkers int
	opts       poolingOpt
	metrics    *PoolMetrics

	closed atomic.Bool

	// sendMu protects the channel close operation only.
	// Writers hold RLock; Shutdown acquires Lock before close(tasks).
	sendMu sync.RWMutex

	logger *ll.Logger
}

// poolingOpt holds configuration options for the pool.
type poolingOpt struct {
	queueSize       int
	observable      Observable[Event]
	taskIDGenerator func(interface{}) string
	noID            bool // skip ID generation entirely — zero allocation submit path
}

// Pooling is a functional option type for configuring the pool during creation.
type Pooling func(*poolingOpt)

// PoolingWithObservable sets an observable for event notifications in the pool.
// The observable receives "queued", "run", and "done" events for every task.
func PoolingWithObservable(obs Observable[Event]) Pooling {
	return func(opts *poolingOpt) { opts.observable = obs }
}

// PoolingWithQueueSize sets the task queue buffer size.
// Defaults to 2× the worker count when not provided or negative.
func PoolingWithQueueSize(size int) Pooling {
	return func(opts *poolingOpt) {
		if size >= 0 {
			opts.queueSize = size
		}
	}
}

// PoolingWithIDGenerator sets a custom task ID generator function.
func PoolingWithIDGenerator(fn func(interface{}) string) Pooling {
	return func(opts *poolingOpt) { opts.taskIDGenerator = fn }
}

// PoolingWithNoID disables task ID generation entirely.
// This eliminates all allocations in the Submit hot path when observability
// (logging, event emission) is not needed. The worker will log an empty task ID.
// Use when pool submission is on a latency-critical path and task tracing is not required.
func PoolingWithNoID() Pooling {
	return func(opts *poolingOpt) { opts.noID = true }
}

// NewPool creates a new pool with the specified number of workers and optional configurations.
// Workers start immediately. At least one worker is always created.
func NewPool(numWorkers int, opts ...Pooling) *Pool {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	options := poolingOpt{
		queueSize:       numWorkers * 2,
		taskIDGenerator: defaultIDTask,
	}
	for _, opt := range opts {
		opt(&options)
	}
	p := &Pool{
		numWorkers: numWorkers,
		tasks:      make(chan job, options.queueSize),
		observable: options.observable,
		opts:       options,
		metrics:    &PoolMetrics{},
	}
	if logger != nil {
		p.logger = logger.Namespace("pool")
	} else {
		p.logger = &ll.Logger{}
	}
	p.shutdownWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		w := newWorker(i+1, p.tasks, &p.shutdownWg, p.observable, p.metrics)
		w.start()
	}
	return p
}

// Metrics returns the pool's operational metrics.
func (p *Pool) Metrics() *PoolMetrics {
	return p.metrics
}

// Logger sets a custom logger for the pool, namespacing it as "pool".
func (p *Pool) Logger(extLogger *ll.Logger) *Pool {
	if extLogger != nil {
		p.logger = extLogger.Namespace("pool")
	}
	return p
}

// Do submits a void function as a task, discarding any submission error.
func (p *Pool) Do(fn func()) {
	_ = p.Submit(Func(func() error { fn(); return nil }))
}

// DoCtx submits a context-aware void function as a task, discarding any submission error.
func (p *Pool) DoCtx(ctx context.Context, fn func(ctx context.Context)) {
	_ = p.SubmitCtx(ctx, FuncCtx(func(ctx context.Context) error { fn(ctx); return nil }))
}

// tryEnqueue attempts to send a job to the pool's task channel.
// Returns (sent, poolClosed).
func (p *Pool) tryEnqueue(j job, ctx context.Context, nonBlocking bool) (sent, poolClosed bool) {
	if p.closed.Load() {
		return false, true
	}
	if nonBlocking {
		select {
		case p.tasks <- j:
			return true, false
		default:
			if p.closed.Load() {
				return false, true
			}
			return false, false
		}
	}
	if p.closed.Load() {
		return false, true
	}
	p.sendMu.RLock()
	defer p.sendMu.RUnlock()
	if p.closed.Load() {
		return false, true
	}
	select {
	case p.tasks <- j:
		return true, false
	case <-ctx.Done():
		return false, false
	}
}

// recordEnqueue stores the current queue depth and updates the high-water mark.
func (p *Pool) recordEnqueue(depth int) {
	d := int64(depth)
	p.metrics.QueueDepth.Store(d)
	for {
		cur := p.metrics.MaxQueueDepth.Load()
		if d <= cur {
			break
		}
		if p.metrics.MaxQueueDepth.CompareAndSwap(cur, d) {
			break
		}
	}
}

// Submit enqueues one or more tasks for execution without context.
// Returns ErrPoolClosed if the pool is shut down, ErrQueueFull if the queue is at capacity.
func (p *Pool) Submit(ts ...Task) error {
	for i, t := range ts {
		if t == nil {
			p.logger.Info("Pool.Submit received nil task at index %d", i)
			return fmt.Errorf("nil task at index %d in batch", i)
		}
		j := &tasker{
			task:            t,
			ctx:             context.Background(),
			taskIDGenerator: p.opts.taskIDGenerator,
			defaultIDPrefix: "task",
			noID:            p.opts.noID,
		}
		taskID := j.ID()
		if p.observable != nil {
			p.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
		}
		sent, poolClosed := p.tryEnqueue(j, context.Background(), true)
		if poolClosed {
			p.metrics.TasksRejected.Add(1)
			return ErrPoolClosed
		}
		if !sent {
			p.metrics.TasksRejected.Add(1)
			p.logger.Warn("Pool.Submit: queue full for task %s (index %d)", taskID, i)
			return ErrQueueFull
		}
		p.metrics.TasksSubmitted.Add(1)
		p.recordEnqueue(len(p.tasks))
		p.logger.Debug("Pool.Submit: enqueued task %s", taskID)
	}
	return nil
}

// SubmitCtx enqueues one or more context-aware tasks for execution.
// Blocks until the task is queued, the context is cancelled, or the pool is closed.
func (p *Pool) SubmitCtx(ctx context.Context, ts ...TaskCtx) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for i, t := range ts {
		if t == nil {
			p.logger.Info("Pool.SubmitCtx received nil TaskCtx at index %d", i)
			return fmt.Errorf("nil TaskCtx at index %d in batch", i)
		}
		j := &tasker{
			task:            t,
			ctx:             ctx,
			taskIDGenerator: p.opts.taskIDGenerator,
			defaultIDPrefix: "task",
			noID:            p.opts.noID,
		}
		taskID := j.ID()
		if p.observable != nil {
			p.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
		}
		sent, poolClosed := p.tryEnqueue(j, ctx, false)
		if poolClosed {
			p.metrics.TasksRejected.Add(1)
			return ErrPoolClosed
		}
		if !sent {
			p.metrics.TasksRejected.Add(1)
			p.logger.Info("Pool.SubmitCtx: context done for task %s (index %d): %v", taskID, i, ctx.Err())
			return ctx.Err()
		}
		p.metrics.TasksSubmitted.Add(1)
		p.recordEnqueue(len(p.tasks))
		p.logger.Debug("Pool.SubmitCtx: enqueued task %s", taskID)
	}
	return nil
}

// Shutdown gracefully stops the pool and waits for all workers to finish.
// Returns ErrShutdownTimedOut if workers do not exit within the timeout.
func (p *Pool) Shutdown(timeout time.Duration) error {
	if !p.closed.CompareAndSwap(false, true) {
		return ErrPoolClosed
	}
	p.sendMu.Lock()
	close(p.tasks)
	p.sendMu.Unlock()

	p.logger.Info("Pool shutdown started, workers: %d, goroutines: %d", p.numWorkers, runtime.NumGoroutine())
	p.quitOnce.Do(func() {})

	done := make(chan struct{})
	go func() {
		p.shutdownWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("Pool shutdown completed successfully.")
		return nil
	case <-time.After(timeout):
		p.logger.Warn("Pool shutdown timed out after %v", timeout)
		return ErrShutdownTimedOut
	}
}

// QueueSize returns the current number of pending tasks in the buffer.
func (p *Pool) QueueSize() int { return len(p.tasks) }

// Workers returns the number of worker goroutines in the pool.
func (p *Pool) Workers() int { return p.numWorkers }

// IsClosed returns true if the pool has been shut down.
func (p *Pool) IsClosed() bool { return p.closed.Load() }
