package jack

import (
	"context"
	"errors"
	"github.com/olekukonko/ll"
	"sync"
	"time"
	// Assuming 'logger' is a global or package-level variable like in pool.go
	// For self-contained compilation, we might need a stub if it's not provided.
	// var logger *ll.Logger // Example, assuming it's initialized elsewhere.
)

//// === tasker implementation (adapted from pool.go) ===
//type tasker struct {
//	task            interface{} // Either Task or TaskCtx
//	ctx             context.Context
//	taskIDGenerator func(interface{}) string
//	defaultIDPrefix string
//	// id              string // Removed, ID is generated on-the-fly or cached by Identifiable/generator
//	// idOnce          sync.Once // Removed
//}

//func (tj *tasker) Run(_ context.Context) error { // Runner's process loop calls job.Run(context.Background())
//	if taskCtx, ok := tj.task.(TaskCtx); ok {
//		return taskCtx.Do(tj.ctx) // TaskCtx runs with its original context (tj.ctx)
//	}
//	if task, ok := tj.task.(Task); ok {
//		return task.Do() // Task runs
//	}
//	return errors.New("invalid task type for Run")
//}
//
//func (tj *tasker) ID() string {
//	// 1. If task itself is Identifiable and provides a non-empty ID
//	if identifiable, ok := tj.task.(Identifiable); ok {
//		if id := identifiable.ID(); id != "" {
//			return id
//		}
//	}
//	// 2. Use taskIDGenerator if provided and returns non-empty
//	//    (This handles custom ID generation and specific default for Runner tasks without explicit ID)
//	if tj.taskIDGenerator != nil {
//		if id := tj.taskIDGenerator(tj.task); id != "" {
//			return id
//		}
//	}
//	// 3. Fallback to defaultIDPrefix + ULID
//	if tj.task == nil { // Should not happen with current Do/DoCtx checks
//		return strings.Join([]string{tj.defaultIDPrefix, "nil_task", ulid.Make().String()}, ".")
//	}
//	return strings.Join([]string{tj.defaultIDPrefix, ulid.Make().String()}, ".")
//}
//
//func (tj *tasker) Context() context.Context {
//	if tj.ctx == nil { // Should be set by Do/DoCtx
//		return context.Background()
//	}
//	return tj.ctx
//}

// === Runner implementation ===
type Runner struct {
	tasks      chan job
	quitOnce   sync.Once
	shutdownWg sync.WaitGroup
	opts       runnerOptions
	mu         sync.RWMutex
	closed     bool
	logger     *ll.Logger
}

type runnerOptions struct {
	queueSize       int
	observable      Observable[Event]
	taskIDGenerator func(interface{}) string
}

type RunnerOption func(*runnerOptions)

func WithRunnerObservable(obs Observable[Event]) RunnerOption {
	return func(opts *runnerOptions) {
		opts.observable = obs
	}
}

func WithRunnerQueueSize(size int) RunnerOption {
	return func(opts *runnerOptions) {
		if size >= 0 {
			opts.queueSize = size
		}
	}
}

func WithRunnerIDGenerator(fn func(interface{}) string) RunnerOption {
	return func(opts *runnerOptions) {
		opts.taskIDGenerator = fn
	}
}

func NewRunner(opts ...RunnerOption) *Runner {
	options := runnerOptions{
		queueSize:       10,
		taskIDGenerator: defaultIDRunner, // Use the Runner-specific default
	}
	for _, opt := range opts {
		opt(&options)
	}

	r := &Runner{
		tasks: make(chan job, options.queueSize),
		opts:  options,
	}

	r.shutdownWg.Add(1) // For the process goroutine
	go r.process()

	if logger != nil { // Assuming global 'logger'
		r.logger = logger.Namespace("runner")
	} else {
		r.logger = &ll.Logger{} // Fallback
	}
	return r
}

func (r *Runner) submit(submitCtx context.Context, job job) error {
	if job == nil {
		if r.logger != nil {
			r.logger.Info("nil job rejected")
		}
		return errors.New("nil job")
	}

	// Eagerly get TaskID for logging and events, as job.ID() could have logic.
	taskID := job.ID()

	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		if r.logger != nil {
			r.logger.Info("rejected job %s: runner closed", taskID)
		}
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

	if r.logger != nil {
		r.logger.Debug("attempting to submit job %s (queue len %d)", taskID, len(r.tasks))
	}

	if submitCtx != nil { // Path for DoCtx (blocking with context)
		select {
		case r.tasks <- job:
			// Successfully submitted
			if r.opts.observable != nil { // Emit "queued" event
				r.opts.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
			}
			if r.logger != nil {
				r.logger.Debug("submitted job %s with context", taskID)
			}
			return nil
		case <-submitCtx.Done():
			return submitCtx.Err()
		}
	} else { // Path for Do (non-blocking)
		select {
		case r.tasks <- job:
			// Successfully submitted
			if r.opts.observable != nil { // Emit "queued" event
				r.opts.observable.Notify(Event{Type: "queued", TaskID: taskID, Time: time.Now()})
			}
			if r.logger != nil {
				r.logger.Debug("submitted job %s", taskID)
			}
			return nil
		default:
			// Queue is full
			if r.logger != nil {
				r.logger.Info("rejected job %s: queue full", taskID)
			}
			return ErrQueueFull
		}
	}
}

func (r *Runner) Do(t Task) error {
	if t == nil {
		if r.logger != nil {
			r.logger.Info("Runner.Do: received nil task")
		}
		return errors.New("nil task")
	}
	job := &tasker{
		task:            t,
		ctx:             context.Background(), // Tasks via Do get a background context for their tasker
		taskIDGenerator: r.opts.taskIDGenerator,
		defaultIDPrefix: "runner", // Used by tasker.ID if no other ID source
	}
	return r.submit(nil, job) // Pass nil context for non-blocking submit
}

func (r *Runner) DoCtx(ctx context.Context, t TaskCtx) error {
	if ctx == nil {
		// It's generally better to return an error or default, rather than panic on nil context.Done()
		return errors.New("DoCtx requires a non-nil context")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	job := &tasker{
		task:            t,
		ctx:             ctx, // Tasks via DoCtx get the user-provided context for their tasker
		taskIDGenerator: r.opts.taskIDGenerator,
		defaultIDPrefix: "runner",
	}
	return r.submit(ctx, job) // Pass the context for blocking submit
}

func (r *Runner) process() {
	defer r.shutdownWg.Done()
	runnerWorkerID := "runner-0" // Runner has a single conceptual worker for its process loop
	for job := range r.tasks {
		if job == nil {
			if r.logger != nil {
				r.logger.Info("Runner received nil job, skipping")
			}
			continue
		}

		taskID := job.ID() // Get ID once
		if r.logger != nil {
			r.logger.Info("Runner processing job: type=%T, taskID=%s", job, taskID)
		}

		originalCtx := job.Context() // This is the context associated with the tasker (e.g., from DoCtx)

		if r.opts.observable != nil {
			r.opts.observable.Notify(Event{Type: "run", WorkerID: runnerWorkerID, TaskID: taskID, Time: time.Now()})
		}

		startTime := time.Now()
		var err error

		executeDone := make(chan struct{})
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					// For a real implementation, use runtime.Stack to get stack trace.
					err = &CaughtPanic{Val: rec, Stack: []byte("stack trace unavailable in simplified example")}
					if r.logger != nil {
						r.logger.Info("PANIC in runner task execution (TaskID %s): %v", taskID, rec)
					}
				}
				close(executeDone)
			}()
			// job.Run itself will use job.Context() if it's a TaskCtx, or ignore context if it's Task.
			// The context.Background() passed here is ignored by the current tasker.Run.
			err = job.Run(context.Background())
		}()

		select {
		case <-executeDone:
			// err is set (or nil) by the goroutine
		case <-originalCtx.Done(): // If the task's own context (from DoCtx) is cancelled
			if err == nil { // Prioritize task's own error
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

func (r *Runner) QueueSize() int {
	return len(r.tasks)
}
