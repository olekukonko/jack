// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olekukonko/ll"
)

// defaultNumNotifyWorkers specifies the default number of notification workers in the pool.
const defaultNumNotifyWorkers = 100

// retryScheduler specifies the number of retry attempts for submitting tasks when the queue is full.
const (
	retryScheduler        = 3
	retrySchedulerBackoff = time.Millisecond * 100
)

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

// Package jack provides utilities for safe, context-aware function execution with mutex protection.
// It includes methods to execute functions with panic recovery, context cancellation support,
// and mutex locking, eliminating the need for verbose boilerplate when handling timeouts or cancellations.

// CaughtPanic represents a panic that was caught during execution.
type CaughtPanic struct {
	Val   interface{} // The value passed to panic()
	Stack []byte      // The stack trace (may be empty if not collected)
}

// Error implements the error interface.
func (c *CaughtPanic) Error() string {
	if c.Stack != nil {
		return fmt.Sprintf("panic: %v\nstack:\n%s", c.Val, c.Stack)
	}
	return fmt.Sprintf("panic: %v", c.Val)
}

// String provides a formatted string representation of the panic.
func (c *CaughtPanic) String() string {
	return c.Error()
}

// Unwrap provides compatibility with errors.Is/As.
func (c *CaughtPanic) Unwrap() error {
	if err, ok := c.Val.(error); ok {
		return err
	}
	return nil
}

// Do wraps a function with no return value into a Task.
// Useful for fire-and-forget or simple operations that don’t produce errors.
//
// Example:
//
//	pool.Submit(jack.Do(func() {
//	    fmt.Println("Hello from task")
//	}))
func Do(fn func()) Task {
	return Func(func() error {
		fn()
		return nil
	})
}

// DoCtx wraps a context-aware function with no return value into a TaskCtx.
// Useful when you only need context propagation without returning an error.
//
// Example:
//
//	pool.SubmitCtx(ctx, jack.DoCtx(func(ctx context.Context) {
//	    fmt.Println("Running with context:", ctx)
//	}))
func DoCtx(fn func(ctx context.Context)) TaskCtx {
	return FuncCtx(func(ctx context.Context) error {
		fn(ctx)
		return nil
	})
}
