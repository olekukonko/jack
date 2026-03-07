package jack

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/olekukonko/ll"
)

// Pool manages a fixed number of worker goroutines to execute tasks concurrently.
// It supports task submission with or without context, shutdown with timeout, and observability.
// The pool uses a channel for task queuing and a wait group for graceful shutdown.
type Pool struct {
	tasks      chan job
	quitOnce   sync.Once
	shutdownWg sync.WaitGroup
	observable Observable[Event]
	numWorkers int
	opts       poolingOpt
	mu         sync.RWMutex
	sendMu     sync.RWMutex
	closed     bool
	logger     *ll.Logger
}

// poolingOpt holds configuration options for the pool, such as queue size, observable, and ID generator.
// These options are applied during pool creation to customize behavior.
// Defaults include queue size based on workers and a default task ID generator.
type poolingOpt struct {
	queueSize            int
	observable           Observable[Event]
	taskIDGenerator      func(interface{}) string
	defaultWorkerContext context.Context
}

// Pooling is a functional option type for configuring the pool during creation.
// It allows setting observable, queue size, ID generator, etc., in a flexible manner.
// Multiple options can be passed to NewPool for combined configuration.
type Pooling func(*poolingOpt)

// PoolingWithObservable sets an observable for event notifications in the pool options.
// The observable will receive events like queued, run, done for tasks.
// Useful for monitoring and logging pool activities externally.
func PoolingWithObservable(obs Observable[Event]) Pooling {
	return func(opts *poolingOpt) {
		opts.observable = obs
	}
}

// PoolingWithQueueSize sets the task queue size in the pool options.
// If size is negative, it is ignored and defaults to twice the number of workers.
// A larger queue allows more pending tasks but may increase memory usage.
func PoolingWithQueueSize(size int) Pooling {
	return func(opts *poolingOpt) {
		if size >= 0 {
			opts.queueSize = size
		}
	}
}

// PoolingWithIDGenerator sets a custom task ID generator function in the pool options.
// The function takes the task interface and returns a unique string ID.
// Defaults to a built-in generator if not provided.
func PoolingWithIDGenerator(fn func(interface{}) string) Pooling {
	return func(opts *poolingOpt) {
		opts.taskIDGenerator = fn
	}
}

// NewPool creates a new pool with the specified number of workers and optional configurations.
// Ensures at least one worker; initializes task channel, observable, and logger.
// Starts all workers immediately and returns the ready pool instance.
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
	}
	if logger != nil {
		p.logger = logger.Namespace("pool")
	} else {
		p.logger = &ll.Logger{}
	}
	p.shutdownWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		w := newWorker(i+1, p.tasks, &p.shutdownWg, p.observable)
		w.start()
	}
	return p
}

// Logger sets a custom logger for the pool, namespacing it as "pool".
// If the provided logger is nil, it retains the existing logger.
// Returns the pool for method chaining.
func (p *Pool) Logger(extLogger *ll.Logger) *Pool {
	if extLogger != nil {
		p.logger = extLogger.Namespace("pool")
	}
	return p
}

// Do is a shorthand for pool.Submit(Func(...)) but discards any returned error.
// It exists purely for ergonomic, fire-and-forget usage.
func (p *Pool) Do(fn func()) {
	_ = p.Submit(Func(func() error { fn(); return nil }))
}

// DoCtx is a shorthand for pool.SubmitCtx(FuncCtx(...)) but discards any returned error.
func (p *Pool) DoCtx(ctx context.Context, fn func(ctx context.Context)) {
	_ = p.SubmitCtx(ctx, FuncCtx(func(ctx context.Context) error { fn(ctx); return nil }))
}

// tryEnqueue attempts to send a job to the pool's task channel.
// It returns (sent, poolClosed) where sent indicates successful enqueue,
// and poolClosed indicates the pool was closed during the attempt.
func (p *Pool) tryEnqueue(job job, ctx context.Context, nonBlocking bool) (sent, poolClosed bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false, true
	}
	if nonBlocking {
		select {
		case p.tasks <- job:
			p.mu.Unlock()
			return true, false
		default:
			p.mu.Unlock()
			return false, false
		}
	}
	// Release state lock before blocking.
	// Hold sendMu read lock so Shutdown cannot close(p.tasks) while we're sending.
	p.mu.Unlock()
	p.sendMu.RLock()
	defer p.sendMu.RUnlock()
	// Re-check closed: Shutdown sets p.closed before acquiring sendMu write lock.
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return false, true
	}
	p.mu.RUnlock()

	select {
	case p.tasks <- job:
		return true, false
	case <-ctx.Done():
		return false, false
	}
}

// Submit enqueues one or more tasks to the pool for execution without context.
// Checks if pool is closed; returns error for nil tasks or full queue.
// Notifies observable of queued events and logs submission details.
func (p *Pool) Submit(ts ...Task) error {
	for i, t := range ts {
		if t == nil {
			if p.logger != nil {
				p.logger.Info("Pool.Submit received nil task at index %d", i)
			}
			return fmt.Errorf("nil task at index %d in batch", i)
		}
		job := &tasker{
			task:            t,
			ctx:             context.Background(),
			taskIDGenerator: p.opts.taskIDGenerator,
			defaultIDPrefix: "task",
		}
		taskID := job.ID()
		if p.observable != nil {
			p.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
		}
		sent, poolClosed := p.tryEnqueue(job, context.Background(), true)
		if poolClosed {
			return ErrPoolClosed
		}
		if !sent {
			if p.logger != nil {
				p.logger.Warn("Pool.Submit: failed to enqueue task %s (index %d): queue full", taskID, i)
			}
			return ErrQueueFull
		}
		if p.logger != nil {
			p.logger.Debug("Pool.Submit: enqueued task %s", taskID)
		}
	}
	return nil
}

// SubmitCtx enqueues one or more context-aware tasks to the pool.
// First checks parent context; then pool closure; handles nil tasks and context cancellation during submission.
// Notifies observable and logs; returns errors for closure, nil tasks, or queue issues.
func (p *Pool) SubmitCtx(ctx context.Context, ts ...TaskCtx) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for i, t := range ts {
		if t == nil {
			if p.logger != nil {
				p.logger.Info("Pool.SubmitCtx received nil TaskCtx at index %d", i)
			}
			return fmt.Errorf("nil TaskCtx at index %d in batch", i)
		}
		job := &tasker{
			task:            t,
			ctx:             ctx,
			taskIDGenerator: p.opts.taskIDGenerator,
			defaultIDPrefix: "task",
		}
		taskID := job.ID()
		if p.observable != nil {
			p.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
		}
		sent, poolClosed := p.tryEnqueue(job, ctx, false)
		if poolClosed {
			return ErrPoolClosed
		}
		if !sent {
			if p.logger != nil {
				p.logger.Info("Pool.SubmitCtx: context done while submitting task %s (index %d): %v", taskID, i, ctx.Err())
			}
			return ctx.Err()
		}
		if p.logger != nil {
			p.logger.Debug("Pool.SubmitCtx: enqueued task %s", taskID)
		}
	}
	return nil
}

// Shutdown gracefully stops the pool, closing the task channel and waiting for workers with a timeout.
// Idempotent; logs shutdown process and returns timeout error if workers don't finish in time.
// Ensures no new tasks are accepted after initiation.
func (p *Pool) Shutdown(timeout time.Duration) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.closed = true
	p.mu.Unlock()
	// Acquire write lock to wait for all in-flight blocking senders to finish.
	// Senders hold RLock; once they see p.closed=true they return without sending.
	// This ensures no goroutine is blocked on p.tasks when we close it.
	p.sendMu.Lock()
	close(p.tasks)
	p.sendMu.Unlock()
	if p.logger != nil {
		p.logger.Info("Pool shutdown started, workers: %d, goroutines: %d", p.numWorkers, runtime.NumGoroutine())
	}
	p.quitOnce.Do(func() {})
	done := make(chan struct{})
	go func() {
		if p.logger != nil {
			p.logger.Info("Waiting for %d workers to shut down...", p.numWorkers)
		}
		p.shutdownWg.Wait()
		if p.logger != nil {
			p.logger.Info("All %d workers shut down, goroutines: %d", p.numWorkers, runtime.NumGoroutine())
		}
		close(done)
	}()
	select {
	case <-done:
		if p.logger != nil {
			p.logger.Info("Pool shutdown completed successfully.")
		}
		return nil
	case <-time.After(timeout):
		if p.logger != nil {
			p.logger.Warn("Pool shutdown timed out after %v, goroutines: %d", timeout, runtime.NumGoroutine())
		}
		return ErrShutdownTimedOut
	}
}

// QueueSize returns the current number of pending tasks in the queue.
// Useful for monitoring pool load and backpressure.
// Thread-safe due to channel len being atomic.
func (p *Pool) QueueSize() int {
	return len(p.tasks)
}

// Workers returns the number of worker goroutines configured in the pool.
// This is fixed at creation and does not change dynamically.
// Helpful for querying pool capacity.
func (p *Pool) Workers() int {
	return p.numWorkers
}
