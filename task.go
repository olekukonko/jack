package jack

import (
	"context"
	"time"
)

type Task interface {
	Do() error
}

type TaskCtx interface {
	Do(ctx context.Context) error
}

// Func converts a func() error to a Task.
// Usage: pool.Submit(jack.Func(myFunction))
type Func func() error

// Do makes Func satisfy the Task interface.
func (f Func) Do() error { return f() }

// FuncCtx converts a func(context.Context) error to a TaskCtx.
// Usage: pool.SubmitCtx(ctx, jack.FuncCtx(myContextAwareFunction))
type FuncCtx func(ctx context.Context) error

// Do makes FuncCtx satisfy the TaskCtx interface.
func (f FuncCtx) Do(ctx context.Context) error { return f(ctx) }

// Routine configures recurring execution
type Routine struct {
	Interval time.Duration
	MaxRuns  int // 0 = unlimited
}

// Identifiable is an optional interface that tasks can implement
// to provide a custom ID for logging, metrics, and tracing.
type Identifiable interface {
	ID() string
}
