package jack

import (
	"errors" // Import the standard errors package
	"fmt"
	"github.com/olekukonko/ll"
	"time"
)

const (
	defaultNumNotifyWorkers = 100

	//
	retryScheduler = 3
)

// Event captures task execution details.
type Event struct {
	Type     string // "queued", "run", "done" (failed is implied by Err != nil on "done")
	TaskID   string // Optional: An identifier for the task
	WorkerID string // Identifier for the worker processing the task
	Time     time.Time
	Duration time.Duration // Only relevant for "done" event
	Err      error         // Only relevant for "done" event
}

// CaughtPanic wraps a panic with a stack trace.
type CaughtPanic struct {
	Val   any
	Stack []byte
}

func (e *CaughtPanic) Error() string {
	return fmt.Sprintf("panic: %q\n%s", e.Val, string(e.Stack))
}

var (
	ErrPoolClosed       = errors.New("pool has been closed")
	ErrRunnerClosed     = errors.New("runner has been closed")
	ErrSchedulerClosed  = errors.New("scheduler has been closed") // Added for Scheduler
	ErrTaskTimeout      = errors.New("task execution timed out")
	ErrTaskPanic        = errors.New("task panicked during execution")
	ErrQueueFull        = errors.New("task queue is full") // Added for non-blocking submit
	ErrShutdownTimedOut = errors.New("shutdown timed out") // Added for graceful shutdown

	logger = ll.New("jack").Enable()
)
