package jack

import "context"

// Func converts a function returning an error into a Task.
// Example:
//
// pool.Submit(jack.Func(func() error { return nil })) // Submits a simple task
type Func func() error

// Do executes the function to satisfy the Task interface.
func (f Func) Do() error { return f() }

// FuncCtx converts a context-aware function into a TaskCtx.
// Example:
//
// pool.SubmitCtx(ctx, jack.FuncCtx(func(ctx context.Context) error { return nil })) // Submits a context-aware task
type FuncCtx func(ctx context.Context) error

// Do executes the function with the given context to satisfy the TaskCtx interface.
func (f FuncCtx) Do(ctx context.Context) error { return f(ctx) }

// Caller defines a group of calls that execute together
type Caller struct {
	funcs []Func
}

// NewFuncGroup creates a new Caller
func NewCaller(calls ...Func) *Caller {
	cg := &Caller{}
	for _, call := range calls {
		if call != nil {
			cg.funcs = append(cg.funcs, call)
		}
	}
	return cg
}

// Append adds more calls to the group
func (cg *Caller) Append(calls ...Func) *Caller {
	for _, call := range calls {
		if call != nil {
			cg.funcs = append(cg.funcs, call)
		}
	}
	return cg
}

// Run runs all calls in the group
func (cg *Caller) Run() error {
	var err error
	for _, call := range cg.funcs {
		err = call()
		if err != nil {
			return err
		}
	}
	return err
}

// Same as run but iignore errors
func (cg *Caller) Execute() Func {
	return func() error {
		for _, call := range cg.funcs {
			_ = call()
		}
		return nil
	}
}

// Func converts Caller to a Func
func (cg *Caller) Func() Func {
	return func() error {
		return cg.Run()
	}
}
