package jack

import (
	"context"
	"fmt"
	"github.com/olekukonko/ll" // Assuming this provides a logger type *ll.Logger with a Namespace method
	"runtime"
	"sync"
	"time"
)

// === Pool implementation (Fixes applied to Submit/SubmitCtx) ===
type Pool struct {
	tasks      chan job
	quitOnce   sync.Once
	shutdownWg sync.WaitGroup
	observable Observable[Event]
	numWorkers int
	opts       poolingOpt
	mu         sync.RWMutex
	closed     bool
	logger     *ll.Logger
}

type poolingOpt struct {
	queueSize            int
	observable           Observable[Event]
	taskIDGenerator      func(interface{}) string
	defaultWorkerContext context.Context // This field is in struct but not used in provided code.
}

type Pooling func(*poolingOpt)

func PoolingWithObservable(obs Observable[Event]) Pooling {
	return func(opts *poolingOpt) {
		opts.observable = obs
	}
}

func PoolingWithQueueSize(size int) Pooling {
	return func(opts *poolingOpt) {
		if size >= 0 {
			opts.queueSize = size
		}
	}
}

func PoolingWithIDGenerator(fn func(interface{}) string) Pooling {
	return func(opts *poolingOpt) {
		opts.taskIDGenerator = fn
	}
}

func NewPool(numWorkers int, opts ...Pooling) *Pool {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	options := poolingOpt{
		queueSize:       numWorkers * 2, // Default queue size
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

	// Initialize logger for the pool instance
	if logger != nil { // Assuming 'logger' is the global logger instance
		p.logger = logger.Namespace("pool")
	} else {
		p.logger = &ll.Logger{} // Fallback dummy logger
	}

	p.shutdownWg.Add(numWorkers) // Correctly Add for all workers once.
	for i := 0; i < numWorkers; i++ {
		// Note: worker ID is i, not i+1 as in original, to match common 0-indexed loop
		// If 1-indexed is desired, newWorker(i+1, ...) is fine.
		// The test log shows "worker-1", so newWorker(i+1, ...) was likely intended. Let's keep it.
		w := newWorker(i+1, p.tasks, &p.shutdownWg, p.observable)
		// If worker needs its own logger instance different from the global one,
		// it can be set here, e.g. w.Logger(p.logger.Namespace("worker"))
		// The current newWorker uses the global 'logger'.
		w.start()
	}
	return p
}

// Logger method for Pool (as in original code)
func (p *Pool) Logger(extLogger *ll.Logger) *Pool {
	if extLogger != nil {
		p.logger = extLogger.Namespace("pool")
	}
	return p
}

func (p *Pool) Submit(ts ...Task) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.mu.RUnlock()

	for i, t := range ts {
		if t == nil {
			if p.logger != nil {
				p.logger.Info("Pool.Submit received nil task at index %d", i)
			}
			return fmt.Errorf("nil task at index %d in batch", i) // Fail fast for this batch
		}

		job := &tasker{
			task:            t,
			ctx:             context.Background(), // Tasks submitted via Submit get a background context
			taskIDGenerator: p.opts.taskIDGenerator,
			defaultIDPrefix: "task",
		}

		taskID := job.ID() // Get ID for logging and event
		if p.observable != nil {
			p.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
		}

		// This select is non-blocking for submitting to the tasks channel.
		select {
		case p.tasks <- job:
			// Successfully submitted this job, continue to the next in the batch.
			if p.logger != nil {
				p.logger.Debug("Pool.Submit: enqueued task %s", taskID)
			}
		default:
			// Queue is full.
			if p.logger != nil {
				p.logger.Warn("Pool.Submit: failed to enqueue task %s (index %d): queue full", taskID, i)
			}
			return ErrQueueFull // Stop processing this batch and return error.
		}
	}
	return nil // All tasks in the batch were successfully submitted.
}

func (p *Pool) SubmitCtx(ctx context.Context, ts ...TaskCtx) error {
	// Check context before doing anything else.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.mu.RUnlock()

	for i, t := range ts {
		if t == nil {
			if p.logger != nil {
				p.logger.Info("Pool.SubmitCtx received nil TaskCtx at index %d", i)
			}
			return fmt.Errorf("nil TaskCtx at index %d in batch", i) // Fail fast
		}
		job := &tasker{
			task:            t,
			ctx:             ctx, // Tasks submitted via SubmitCtx get the provided context
			taskIDGenerator: p.opts.taskIDGenerator,
			defaultIDPrefix: "task",
		}

		taskID := job.ID()
		if p.observable != nil {
			p.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
		}

		// This select respects the context for the submission attempt itself.
		select {
		case p.tasks <- job:
			// Successfully submitted this job, continue.
			if p.logger != nil {
				p.logger.Debug("Pool.SubmitCtx: enqueued task %s", taskID)
			}
		case <-ctx.Done():
			// Context was cancelled while trying to submit this job.
			if p.logger != nil {
				p.logger.Info("Pool.SubmitCtx: context done while submitting task %s (index %d): %v", taskID, i, ctx.Err())
			}
			return ctx.Err() // Stop processing batch and return context error.
		}
	}
	return nil // All tasks in the batch were successfully submitted.
}

func (p *Pool) Shutdown(timeout time.Duration) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.closed = true
	p.mu.Unlock()

	if p.logger != nil {
		p.logger.Info("Pool shutdown started, workers: %d, goroutines: %d", p.numWorkers, runtime.NumGoroutine())
	}
	p.quitOnce.Do(func() {
		close(p.tasks) // Close task channel to signal workers to stop.
	})

	done := make(chan struct{})
	go func() {
		if p.logger != nil {
			p.logger.Info("Waiting for %d workers to shut down...", p.numWorkers)
		}
		p.shutdownWg.Wait() // Wait for all worker goroutines to call Done().
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

func (p *Pool) QueueSize() int {
	// Reading len of channel is generally safe.
	// RLock might be for consistency if other fields protected by mu are read simultaneously.
	// p.mu.RLock()
	// defer p.mu.RUnlock()
	return len(p.tasks)
}

func (p *Pool) Workers() int {
	return p.numWorkers
}
