// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"context"
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/ll"
	"strings"
	"time"
)

// defaultNumNotifyWorkers specifies the default number of notification workers in the pool.
const defaultNumNotifyWorkers = 100

// retryScheduler specifies the number of retry attempts for submitting tasks when the queue is full.
const retryScheduler = 3

// Errors returned by the worker pool and task execution.
var (
	// ErrPoolClosed indicates the worker pool has been closed.
	ErrPoolClosed = errors.New("pool has been closed")
	// ErrRunnerClosed indicates the runner has been closed.
	ErrRunnerClosed = errors.New("runner has been closed")
	// ErrSchedulerClosed indicates the scheduler has been closed.
	ErrSchedulerClosed = errors.New("scheduler has been closed")
	// ErrTaskTimeout indicates a task exceeded its execution timeout.
	ErrTaskTimeout = errors.New("task execution timed out")
	// ErrTaskPanic indicates a task panicked during execution.
	ErrTaskPanic = errors.New("task panicked during execution")
	// ErrQueueFull indicates the task queue is full and cannot accept new tasks.
	ErrQueueFull = errors.New("task queue is full")
	// ErrShutdownTimedOut indicates the pool shutdown exceeded its timeout.
	ErrShutdownTimedOut = errors.New("shutdown timed out")
)

var (
	ErrSchedulerJobAlreadyRunning = errors.New("scheduler: job is already running")
	ErrSchedulerNotRunning        = errors.New("scheduler: job is not running or already stopped")
	ErrSchedulerPoolNil           = errors.New("scheduler: pool cannot be nil")
	ErrSchedulerNameMissing       = errors.New("scheduler: name cannot be empty")
)

// logger is the default logger for the jack package, initialized with the "jack" namespace.
var logger = ll.New("jack")

// Identifiable is an optional interface for tasks to provide a custom ID for logging, metrics, and tracing.
type Identifiable interface {
	// ID returns a unique identifier for the task.
	ID() string
}

// job defines a task that can be executed by a worker in the pool.
// It includes methods for execution, identification, and context retrieval.
type job interface {
	// Run executes the task with the given context and returns any error.
	Run(ctx context.Context) error
	// ID returns a unique identifier for the task.
	ID() string
	// Context returns the task's associated context.
	Context() context.Context
}

// Task represents a simple task that can be executed without a context.
type Task interface {
	// Do executes the task and returns any error.
	Do() error
}

// TaskCtx represents a context-aware task that can be executed with a context.
type TaskCtx interface {
	// Do executes the task with the given context and returns any error.
	Do(ctx context.Context) error
}

// CaughtPanic captures a panic during task execution with its value and stack trace.
// Thread-safe for use in error handling across goroutines.
type CaughtPanic struct {
	Val   any    // The panic value
	Stack []byte // The stack trace
}

// Error returns a formatted string describing the panic and its stack trace.
func (e *CaughtPanic) Error() string {
	return fmt.Sprintf("panic: %q\n%s", e.Val, string(e.Stack))
}

// Event captures details of task execution for observability.
// Thread-safe for use in notification across goroutines.
type Event struct {
	Type     string        // Type of event: "queued", "run", "done"
	TaskID   string        // Optional task identifier
	WorkerID string        // Identifier of the worker processing the task
	Time     time.Time     // Time of the event
	Duration time.Duration // Duration of task execution (for "done" events)
	Err      error         // Error, if any (for "done" events)
}

// Func converts a function returning an error into a Task.
// Example:
//
//	pool.Submit(jack.Func(func() error { return nil })) // Submits a simple task
type Func func() error

// Do executes the function to satisfy the Task interface.
func (f Func) Do() error { return f() }

// FuncCtx converts a context-aware function into a TaskCtx.
// Example:
//
//	pool.SubmitCtx(ctx, jack.FuncCtx(func(ctx context.Context) error { return nil })) // Submits a context-aware task
type FuncCtx func(ctx context.Context) error

// Do executes the function with the given context to satisfy the TaskCtx interface.
func (f FuncCtx) Do(ctx context.Context) error { return f(ctx) }

// Logger returns the default logger for the jack package.
// Thread-safe as it returns a pre-initialized logger.
// Example:
//
//	log := jack.Logger() // Retrieves the default logger
func Logger() *ll.Logger {
	return logger
}

// Routine configures recurring task execution with an interval and maximum runs.
// Thread-safe for use in scheduling configurations.
type Routine struct {
	Interval time.Duration // Interval between task executions
	MaxRuns  int           // Maximum number of runs (0 for unlimited)
}

// tasker wraps a Task or TaskCtx with a context and ID generation logic.
// It implements the job interface for execution in the worker pool.
// Thread-safe via context and immutable fields.
type tasker struct {
	task            interface{}              // Task or TaskCtx to execute
	ctx             context.Context          // Context for TaskCtx or Background for Task
	taskIDGenerator func(interface{}) string // Optional ID generator
	defaultIDPrefix string                   // Prefix for generated IDs
}

// Context returns the task's associated context, defaulting to context.Background if none is set.
func (tj *tasker) Context() context.Context {
	if tj.ctx == nil {
		return context.Background()
	}
	return tj.ctx
}

// ID generates a unique identifier for the task.
// It uses the taskIDGenerator, Identifiable.ID, or a generated ID with the default prefix.
func (tj *tasker) ID() string {
	if tj.taskIDGenerator != nil {
		if id := tj.taskIDGenerator(tj.task); id != "" {
			return id
		}
	}
	if identifiable, ok := tj.task.(Identifiable); ok {
		if id := identifiable.ID(); id != "" {
			return id
		}
	}
	if tj.task == nil {
		return strings.Join([]string{tj.defaultIDPrefix, "nil_task", ulid.Make().String()}, ".")
	}
	return strings.Join([]string{tj.defaultIDPrefix, ulid.Make().String()}, ".")
}

// Run executes the wrapped Task or TaskCtx using the stored context.
// It returns an error if the task type is invalid.
func (tj *tasker) Run(_ context.Context) error {
	if taskCtx, ok := tj.task.(TaskCtx); ok {
		return taskCtx.Do(tj.ctx)
	}
	if task, ok := tj.task.(Task); ok {
		return task.Do()
	}
	return errors.New("invalid task type")
}
