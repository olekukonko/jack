package jack

import (
	"context"
	"time"
)

type Vitals struct {
	Start     HookCtx
	End       CallbackCtx
	Timed     CallbackCtx
	TimedWait time.Duration
}

type VitalsOption func(*Vitals)

// VitalsWithStart configures a hook function to run at the beginning of a vital operation.
// The hook receives the context and operation ID, and can return an error to abort execution.
// Use this option to inject pre-operation logic like logging, metrics, or validation.
func VitalsWithStart(hook HookCtx) VitalsOption {
	return func(l *Vitals) { l.Start = hook }
}

// VitalsWithEnd configures a callback function to run after successful operation completion.
// The callback receives the context and operation ID but does not return an error.
// Use this option for post-operation cleanup, notifications, or result processing.
func VitalsWithEnd(callback CallbackCtx) VitalsOption {
	return func(l *Vitals) { l.End = callback }
}

// VitalsWithTimed configures a timed callback with a wait duration for delayed execution.
// The callback receives context and ID after the specified wait period elapses.
// Use this option for deferred operations like timeout handling or scheduled cleanup.
func VitalsWithTimed(callback CallbackCtx, wait time.Duration) VitalsOption {
	return func(l *Vitals) {
		l.Timed = callback
		l.TimedWait = wait
	}
}

// NewVitals creates a new Vitals instance configured with the provided functional options.
// Each option modifies the Vitals configuration before returning the initialized instance.
// If no options are provided, returns a Vitals with all fields set to their zero values.
func NewVitals(opts ...VitalsOption) *Vitals {
	l := &Vitals{}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Execute runs an operation with optional start and end hooks while propagating errors.
// It invokes the Start hook first, then the operation, and finally the End callback on success.
// If Start or the operation returns an error, execution stops and the error is returned immediately.
func (l *Vitals) Execute(ctx context.Context, id string, operation Func) error {
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}
	if err := operation(); err != nil {
		return err
	}
	if l.End != nil {
		l.End(ctx, id)
	}
	return nil
}

// ExecuteCtx runs a context-aware operation with optional start and end hooks.
// It invokes the Start hook first, then the operation with context, and finally the End callback.
// Errors from Start or the operation are propagated immediately, skipping subsequent steps.
func (l *Vitals) ExecuteCtx(ctx context.Context, id string, operation FuncCtx) error {
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}
	if err := operation(ctx); err != nil {
		return err
	}
	if l.End != nil {
		l.End(ctx, id)
	}
	return nil
}

// VitalsWithRun creates a Vitals instance and executes an operation with it in one call.
// It accepts functional options to configure hooks before immediately running the operation.
// This convenience function combines NewVitals and Execute for simplified inline usage.
func VitalsWithRun(ctx context.Context, id string, operation Func, opts ...VitalsOption) error {
	lifetime := NewVitals(opts...)
	return lifetime.Execute(ctx, id, operation)
}

// VitalsWithRunCtx creates a Vitals instance and executes a context-aware operation in one call.
// It accepts functional options to configure hooks before immediately running the operation with context.
// This convenience function combines NewVitals and ExecuteCtx for simplified inline usage.
func VitalsWithRunCtx(ctx context.Context, id string, operation FuncCtx, opts ...VitalsOption) error {
	lifetime := NewVitals(opts...)
	return lifetime.ExecuteCtx(ctx, id, operation)
}
