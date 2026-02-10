package jack

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/olekukonko/ll"
)

// Runner executes tasks asynchronously using a buffered task queue.
// It supports observability and logging, processing tasks in a single goroutine.
// Thread-safe via mutex, wait group, and channel operations.
type Runner struct {
	tasks      chan job       // Channel for task submission
	quitOnce   sync.Once      // Ensures shutdown is called once
	shutdownWg sync.WaitGroup // Waits for process goroutine
	opts       runnerOptions  // Configuration options
	mu         sync.RWMutex   // Protects closed state
	closed     bool           // Indicates if runner is closed
	logger     *ll.Logger     // Logger with runner namespace
}

// runnerOptions holds configuration for a Runner.
type runnerOptions struct {
	queueSize       int                      // Size of the task queue
	observable      Observable[Event]        // Observable for task events
	taskIDGenerator func(interface{}) string // Optional task ID generator
}

// RunnerOption configures a Runner during creation.
type RunnerOption func(*runnerOptions)

// WithRunnerObservable sets the observable for task events.
// Example:
//
//	runner := NewRunner(WithRunnerObservable(obs)) // Configures observable
func WithRunnerObservable(obs Observable[Event]) RunnerOption {
	return func(opts *runnerOptions) {
		opts.observable = obs
	}
}

// WithRunnerQueueSize sets the task queue size.
// Non-positive values are ignored.
// Example:
//
//	runner := NewRunner(WithRunnerQueueSize(20)) // Sets queue size to 20
func WithRunnerQueueSize(size int) RunnerOption {
	return func(opts *runnerOptions) {
		if size >= 0 {
			opts.queueSize = size
		}
	}
}

// WithRunnerIDGenerator sets the task ID generator function.
// Example:
//
//	runner := NewRunner(WithRunnerIDGenerator(customIDFunc)) // Sets custom ID generator
func WithRunnerIDGenerator(fn func(interface{}) string) RunnerOption {
	return func(opts *runnerOptions) {
		opts.taskIDGenerator = fn
	}
}

// NewRunner creates a new Runner with the specified options.
// It initializes a task queue and starts a processing goroutine.
// Thread-safe via initialization and logger namespace.
// Example:
//
//	runner := NewRunner(WithRunnerQueueSize(10), WithRunnerObservable(obs)) // Creates runner with queue size 10
func NewRunner(opts ...RunnerOption) *Runner {
	options := runnerOptions{
		queueSize:       10,
		taskIDGenerator: defaultIDRunner,
	}
	for _, opt := range opts {
		opt(&options)
	}
	r := &Runner{
		tasks: make(chan job, options.queueSize),
		opts:  options,
	}
	r.shutdownWg.Add(1)
	go r.process()
	if logger != nil {
		r.logger = logger.Namespace("runner")
	} else {
		r.logger = &ll.Logger{}
	}
	return r
}

// submit sends a job to the task queue, respecting the submission context.
// It returns an error if the runner is closed, the queue is full, or the context is done.
// Thread-safe via mutex and channel operations.
func (r *Runner) submit(submitCtx context.Context, job job) error {
	if job == nil {
		r.logger.Info("nil job rejected")
		return errors.New("nil job")
	}
	taskID := job.ID()
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		r.logger.Info("rejected job %s: runner closed", taskID)
		if submitCtx != nil {
			select {
			case <-submitCtx.Done():
				return submitCtx.Err()
			default:
			}
		}
		return ErrRunnerClosed
	}
	r.mu.RUnlock()
	r.logger.Debugf("attempting to submit job %s (queue len %d)", taskID, len(r.tasks))
	if submitCtx != nil {
		select {
		case r.tasks <- job:
			if r.opts.observable != nil {
				r.opts.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
			}
			r.logger.Debugf("submitted job %s with context", taskID)
			return nil
		case <-submitCtx.Done():
			return submitCtx.Err()
		}
	} else {
		select {
		case r.tasks <- job:
			if r.opts.observable != nil {
				r.opts.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
			}
			r.logger.Debugf("submitted job %s", taskID)
			return nil
		default:
			r.logger.Info("rejected job %s: queue full", taskID)
			return ErrQueueFull
		}
	}
}

// Do submits a Task for execution in the runner’s queue.
// It returns an error if the task is nil or the runner is closed.
// Thread-safe via submit and channel operations.
// Example:
//
//	runner.Do(myTask) // Submits a task
func (r *Runner) Do(t Task) error {
	if t == nil {
		r.logger.Info("Runner.Do: received nil task")
		return errors.New("nil task")
	}
	job := &tasker{
		task:            t,
		ctx:             context.Background(),
		taskIDGenerator: r.opts.taskIDGenerator,
		defaultIDPrefix: "runner",
	}
	return r.submit(nil, job)
}

// DoCtx submits a TaskCtx for execution with the given context.
// It returns an error if the context or task is nil, the context is done, or the runner is closed.
// Thread-safe via submit and channel operations.
// Example:
//
//	runner.DoCtx(ctx, myTaskCtx) // Submits a context-aware task
func (r *Runner) DoCtx(ctx context.Context, t TaskCtx) error {
	if ctx == nil {
		return errors.New("DoCtx requires a non-nil context")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	job := &tasker{
		task:            t,
		ctx:             ctx,
		taskIDGenerator: r.opts.taskIDGenerator,
		defaultIDPrefix: "runner",
	}
	return r.submit(ctx, job)
}

// process runs the runner’s task processing loop in a goroutine.
// It executes tasks from the queue and emits observability events.
// Thread-safe via channel operations and wait group.
func (r *Runner) process() {
	defer r.shutdownWg.Done()
	runnerWorkerID := "runner-0"
	for job := range r.tasks {
		if job == nil {
			r.logger.Info("Runner received nil job, skipping")
			continue
		}
		taskID := job.ID()
		r.logger.Info("Runner processing job: type=%T, taskID=%s", job, taskID)
		originalCtx := job.Context()
		if r.opts.observable != nil {
			r.opts.observable.Notify(Event{Type: "run", WorkerID: runnerWorkerID, TaskID: taskID, Time: time.Now()})
		}
		startTime := time.Now()
		errCh := make(chan error, 1)
		executeDone := make(chan struct{})
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					errCh <- &CaughtPanic{Val: rec, Stack: debug.Stack()}
					r.logger.Info("PANIC in runner task execution (TaskID %s): %v", taskID, rec)
				}
				close(executeDone)
			}()
			errCh <- job.Run(originalCtx)
		}()
		var err error
		select {
		case <-executeDone:
			err = <-errCh
		case <-originalCtx.Done():
			<-executeDone
			err = <-errCh
			if err == nil {
				err = originalCtx.Err()
			}
		}
		if r.opts.observable != nil {
			r.opts.observable.Notify(Event{
				Type:     "done",
				WorkerID: runnerWorkerID,
				TaskID:   taskID,
				Time:     time.Now(),
				Duration: time.Since(startTime),
				Err:      err,
			})
		}
	}
}

// Shutdown closes the task queue and waits for the processing goroutine to finish.
// It returns an error if the shutdown times out.
// Thread-safe via mutex, once, and wait group.
// Example:
//
//	runner.Shutdown(5 * time.Second) // Shuts down runner with 5-second timeout
func (r *Runner) Shutdown(timeout time.Duration) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRunnerClosed
	}
	r.closed = true
	r.mu.Unlock()
	r.quitOnce.Do(func() {
		close(r.tasks)
	})
	done := make(chan struct{})
	go func() {
		r.shutdownWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrShutdownTimedOut
	}
}

// QueueSize returns the current number of tasks in the queue.
// Thread-safe via channel length access.
// Example:
//
//	size := runner.QueueSize() // Gets current queue size
func (r *Runner) QueueSize() int {
	return len(r.tasks)
}
