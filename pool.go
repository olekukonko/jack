package jack

import (
	"context"
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/ll" // Assuming this provides a logger type *ll.Logger with a Namespace method
	"runtime"
	"strings"
	"sync"
	"time"
)

// === job interface (as defined in the problem) ===
type job interface {
	Run(ctx context.Context) error
	ID() string
	Context() context.Context
}

// === tasker implementation (as defined in the problem) ===
type tasker struct {
	task            interface{}     // Either Task or TaskCtx
	ctx             context.Context // Background for Task, user-provided for TaskCtx
	taskIDGenerator func(interface{}) string
	defaultIDPrefix string
}

func (tj *tasker) Run(_ context.Context) error { // The argument ctx is from worker, tasker uses its own tj.ctx
	if taskCtx, ok := tj.task.(TaskCtx); ok {
		return taskCtx.Do(tj.ctx)
	}
	if task, ok := tj.task.(Task); ok {
		return task.Do()
	}
	return errors.New("invalid task type")
}

func (tj *tasker) ID() string {
	if tj.taskIDGenerator != nil {
		if id := tj.taskIDGenerator(tj.task); id != "" {
			return id
		}
	}
	if identifiable, ok := tj.task.(Identifiable); ok {
		if id := identifiable.ID(); id != "" { // Ensure identifiable.ID() is not empty
			return id
		}
	}
	if tj.task == nil {
		// Fallback for nil task to prevent panic with ulid.Make() if prefix is also empty
		return strings.Join([]string{tj.defaultIDPrefix, "nil_task", ulid.Make().String()}, ".")
	}
	return strings.Join([]string{tj.defaultIDPrefix, ulid.Make().String()}, ".")
}

func (tj *tasker) Context() context.Context {
	if tj.ctx == nil { // Should ideally not be nil; initialized in Submit/SubmitCtx
		return context.Background()
	}
	return tj.ctx
}

// === worker implementation (Fix applied here) ===
type worker struct {
	id         int
	taskChan   <-chan job
	wg         *sync.WaitGroup
	observable Observable[Event]
	logger     *ll.Logger
}

func newWorker(id int, taskChan <-chan job, wg *sync.WaitGroup, obs Observable[Event]) *worker {
	// Assuming global 'logger' is initialized and has Namespace.
	// If 'logger' can be nil, checks or alternative initialization needed.
	var workerLogger *ll.Logger
	if logger != nil { // Check if global logger is non-nil
		workerLogger = logger.Namespace(fmt.Sprintf("worker-%d", id))
	} else {
		workerLogger = &ll.Logger{} // Fallback to a dummy logger
	}

	return &worker{
		id:         id,
		taskChan:   taskChan,
		wg:         wg,
		observable: obs,
		logger:     workerLogger,
	}
}

func (w *worker) start() {
	// CRITICAL FIX: Removed w.wg.Add(1) from here.
	// The Pool's NewPool method already calls Add for this worker.
	go func() {
		defer func() {
			w.wg.Done()
			if w.logger != nil {
				w.logger.Info("Worker %d wait group done, goroutines: %d", w.id, runtime.NumGoroutine())
			}
		}()

		workerIDStr := fmt.Sprintf("worker-%d", w.id)
		if w.logger != nil {
			w.logger.Info("Worker %d starting, goroutines: %d", w.id, runtime.NumGoroutine())
		}

		for {
			select {
			case job, ok := <-w.taskChan:
				if !ok {
					if w.logger != nil {
						w.logger.Info("Worker %d task channel closed, exiting", w.id)
					}
					return
				}
				if job == nil {
					if w.logger != nil {
						w.logger.Info("Worker %d received nil job, skipping", w.id)
					}
					continue
				}
				taskID := job.ID()
				if taskID == "" { // Should not happen with current tasker.ID()
					if w.logger != nil {
						w.logger.Info("Worker %d received job with empty taskID", w.id)
					}
					taskID = "unknown-task-" + ulid.Make().String()
				}

				originalCtx := job.Context() // This is tj.ctx from the tasker

				if w.observable != nil {
					w.observable.Notify(Event{Type: "run", WorkerID: workerIDStr, TaskID: taskID, Time: time.Now()})
				}

				startTime := time.Now()
				var err error // Task execution error

				executeDone := make(chan struct{})
				go func() {
					defer func() {
						if r := recover(); r != nil {
							// In a real app, capture stack trace with runtime.Stack
							err = &CaughtPanic{Val: r, Stack: []byte("stack trace unavailable in simplified example")}
							if w.logger != nil {
								w.logger.Info("PANIC in task execution (Worker %d, TaskID %s): %v", w.id, taskID, r)
							}
						}
						close(executeDone)
					}()
					// job.Run ignores its argument and uses its own context (job.Context())
					err = job.Run(context.Background())
				}()

				select {
				case <-executeDone:
					// err is set (or nil)
				case <-originalCtx.Done(): // If the job's own context is cancelled
					if err == nil { // Only override if task hasn't already finished with an error
						err = originalCtx.Err()
					}
				}

				if w.observable != nil {
					w.observable.Notify(Event{
						Type:     "done",
						WorkerID: workerIDStr,
						TaskID:   taskID,
						Time:     time.Now(),
						Duration: time.Since(startTime),
						Err:      err,
					})
				}
			}
		}
	}()
}

// Logger method for worker (as in original code)
func (w *worker) Logger(extLogger *ll.Logger) *worker {
	if extLogger != nil {
		w.logger = extLogger.Namespace(fmt.Sprintf("worker-%d", w.id))
	}
	return w
}

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
