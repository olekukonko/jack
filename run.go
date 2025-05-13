// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"context"
	"runtime/debug"
	"sync"
)

// Group coordinates concurrent execution of functions with error and panic handling.
// It supports context cancellation and worker limits, collecting errors in a channel.
// Thread-safe via wait group, mutex, and channel operations.
type Group struct {
	wg         sync.WaitGroup  // Tracks running goroutines
	errCh      chan error      // Receives first error from goroutines
	errOnce    sync.Once       // Ensures errCh is closed once
	cancelOnce sync.Once       // Ensures context cancellation is called once
	sem        chan struct{}   // Limits concurrent workers
	ctx        context.Context // Group context for cancellation
	cancel     func()          // Cancels the group context
}

// NewGroup creates a new Group for running functions concurrently.
// It initializes an error channel with a buffer of 1.
// Thread-safe via initialization.
// Example:
//
//	group := NewGroup() // Creates a new group
func NewGroup() *Group {
	return &Group{
		errCh: make(chan error, 1),
	}
}

// WithContext associates a context with the Group, enabling cancellation.
// It cancels any previous context and creates a new cancellable context.
// Thread-safe via context operations.
// Example:
//
//	group.WithContext(ctx) // Sets group context
func (g *Group) WithContext(ctx context.Context) *Group {
	if g.cancel != nil {
		g.cancel()
	}
	g.ctx, g.cancel = context.WithCancel(ctx)
	return g
}

// WithLimit sets the maximum number of concurrent workers in the Group.
// Non-positive values remove the limit.
// Thread-safe via semaphore initialization.
// Example:
//
//	group.WithLimit(5) // Limits to 5 concurrent workers
func (g *Group) WithLimit(n int) *Group {
	if n <= 0 {
		g.sem = nil
		return g
	}
	if cap(g.sem) != n {
		g.sem = make(chan struct{}, n)
	}
	return g
}

// doWork executes a job with panic recovery and error handling.
// It sends the first error to the error channel and triggers context cancellation.
// Thread-safe via channel and once operations.
func (g *Group) doWork(job func() error) {
	if g.sem != nil {
		g.sem <- struct{}{}
		defer func() { <-g.sem }()
	}

	var errToSend error
	defer func() {
		if errToSend != nil {
			select {
			case g.errCh <- errToSend:
			default:
			}
			if g.cancel != nil {
				g.cancelOnce.Do(g.cancel)
			}
		}
	}()

	func() {
		defer func() {
			if v := recover(); v != nil {
				errToSend = &CaughtPanic{
					Val:   v,
					Stack: debug.Stack(),
				}
			}
		}()
		errToSend = job()
	}()
}

// Go runs a function in a new goroutine with error and panic handling.
// The first non-nil error or panic is sent to the Errors channel.
// If a context is set, it is cancelled on the first error.
// Thread-safe via wait group and doWork.
// Example:
//
//	group.Go(func() error { return nil }) // Runs a function
func (g *Group) Go(f func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.doWork(f)
	}()
}

// GoCtx runs a context-aware function in a new goroutine.
// It requires a context set via WithContext and passes it to the function.
// The first error, panic, or context cancellation is sent to the Errors channel,
// and the context is cancelled.
// Thread-safe via wait group and doWork.
// Example:
//
//	group.WithContext(ctx).GoCtx(func(ctx context.Context) error { return nil }) // Runs a context-aware function
func (g *Group) GoCtx(f func(context.Context) error) {
	if g.ctx == nil {
		panic("jack.Group: must use WithContext before GoCtx")
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		job := func() error {
			if err := g.ctx.Err(); err != nil {
				return err
			}
			return f(g.ctx)
		}

		if g.sem != nil {
			g.sem <- struct{}{}
			defer func() { <-g.sem }()
			if err := g.ctx.Err(); err != nil {
				select {
				case g.errCh <- err:
				default:
				}
				if g.cancel != nil {
					g.cancelOnce.Do(g.cancel)
				}
				return
			}
		} else {
			if err := g.ctx.Err(); err != nil {
				select {
				case g.errCh <- err:
				default:
				}
				if g.cancel != nil {
					g.cancelOnce.Do(g.cancel)
				}
				return
			}
		}

		g.doWork(job)
	}()
}

// Errors returns a channel that receives the first error from Go or GoCtx goroutines.
// The channel has a buffer of 1 and is closed by Wait.
// Thread-safe via channel operations.
// Example:
//
//	for err := range group.Errors() { ... } // Reads errors
func (g *Group) Errors() <-chan error {
	return g.errCh
}

// Wait blocks until all Go and GoCtx goroutines complete.
// It closes the Errors channel and cancels the Group’s context, if set.
// Thread-safe via wait group and once.
// Example:
//
//	group.Wait() // Waits for all goroutines
func (g *Group) Wait() {
	g.wg.Wait()
	g.errOnce.Do(func() {
		close(g.errCh)
		if g.cancel != nil {
			g.cancel()
		}
	})
}

// Go runs a function in a standalone goroutine with error and panic handling.
// It returns a buffered channel that receives the function’s error or a CaughtPanic.
// The channel is closed after the error or completion.
// Thread-safe via goroutine and channel operations.
// Example:
//
//	errCh := Go(func() error { return nil }) // Runs a function and returns error channel
func Go(f func() error) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		var errToSend error
		defer func() {
			if errToSend != nil {
				errCh <- errToSend
			}
			close(errCh)
		}()
		func() {
			defer func() {
				if v := recover(); v != nil {
					errToSend = &CaughtPanic{
						Val:   v,
						Stack: debug.Stack(),
					}
				}
			}()
			errToSend = f()
		}()
	}()
	return errCh
}

// Safe executes a function with panic recovery.
// It returns the function’s error or a CaughtPanic if a panic occurs.
// Thread-safe as a standalone function.
// Example:
//
//	err := Safe(func() error { return nil }) // Executes function safely
func Safe(f func() error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &CaughtPanic{
				Val:   v,
				Stack: debug.Stack(),
			}
		}
	}()
	return f()
}

// SafeCtx executes a context-aware function with panic recovery.
// It returns the context’s error if done, the function’s error, or a CaughtPanic if a panic occurs.
// Thread-safe as a standalone function.
// Example:
//
//	err := SafeCtx(ctx, func(ctx context.Context) error { return nil }) // Executes context-aware function safely
func SafeCtx(ctx context.Context, f func(context.Context) error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &CaughtPanic{
				Val:   v,
				Stack: debug.Stack(),
			}
		}
	}()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return f(ctx)
}
