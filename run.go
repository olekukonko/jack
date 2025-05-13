package jack

import (
	"context"
	"runtime/debug"
	"sync"
)

// Group allows running multiple functions with error and panic handling
type Group struct {
	wg         sync.WaitGroup
	errCh      chan error
	errOnce    sync.Once     // Ensures errCh is closed only once
	cancelOnce sync.Once     // Ensures cancel is called only once on error
	sem        chan struct{} // Worker limit semaphore
	ctx        context.Context
	cancel     func()
}

// NewGroup creates a new Group without context or worker limits
func NewGroup() *Group {
	return &Group{
		errCh: make(chan error, 1), // Buffer of 1 for the first error
	}
}

// WithContext associates a context with the Group.
// If a previous context was associated, its cancel function is called.
// The Group's context will be cancelled when the first error occurs in a Go/GoCtx
// goroutine, or when Wait is called.
func (g *Group) WithContext(ctx context.Context) *Group {
	if g.cancel != nil {
		// Cancel previous context if WithContext is called multiple times
		g.cancel()
	}
	g.ctx, g.cancel = context.WithCancel(ctx)
	return g
}

// WithLimit sets the maximum number of concurrent workers for the Group.
// If n <= 0, no limit is applied (or a previously set limit is removed).
func (g *Group) WithLimit(n int) *Group {
	if n <= 0 {
		g.sem = nil // Remove limit
		return g
	}
	if cap(g.sem) != n { // Avoid re-allocating if limit is the same
		g.sem = make(chan struct{}, n)
	}
	return g
}

func (g *Group) doWork(job func() error) {
	if g.sem != nil {
		g.sem <- struct{}{}
		defer func() { <-g.sem }()
	}

	var errToSend error
	defer func() {
		// This deferred function handles sending the error and triggering cancellation.
		if errToSend != nil {
			// Try to send the error. Non-blocking.
			select {
			case g.errCh <- errToSend:
				// Error sent.
			default:
				// errCh is full (another error likely already sent) or closed.
				// This error is effectively dropped from the channel.
			}

			// If we have a cancel function, trigger cancellation for other goroutines.
			// This happens regardless of whether the error was sent to the channel,
			// as any error should trigger cancellation.
			if g.cancel != nil {
				g.cancelOnce.Do(g.cancel)
			}
		}
	}()

	// Execute the job with panic recovery
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

// Go runs the function f in a new goroutine.
// The first non-nil error returned by f or a panic will be sent to the Errors channel.
// If WithContext was used, the Group's context will be cancelled on the first error.
func (g *Group) Go(f func() error) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()
		g.doWork(f)
	}()
}

// GoCtx runs the context-aware function f in a new goroutine.
// It requires WithContext to have been called first.
// The function f receives the Group's context.
// If the Group's context is already cancelled before f starts, f will not run,
// and the context's error will be reported.
// The first non-nil error returned by f, a panic, or a context cancellation
// will be sent to the Errors channel and will cause the Group's context to be cancelled.
func (g *Group) GoCtx(f func(context.Context) error) {
	if g.ctx == nil {
		panic("jack.Group: must use WithContext before GoCtx")
	}

	g.wg.Add(1)

	go func() {
		defer g.wg.Done()

		// Check context before starting work, but after acquiring semaphore (if any)
		// to ensure semaphore is released even if context is done.
		job := func() error {
			// Double check context status right before execution,
			// as it might have been cancelled while waiting for the semaphore.
			if err := g.ctx.Err(); err != nil {
				return err // Context cancelled or deadline exceeded
			}
			return f(g.ctx)
		}

		// If semaphore is used, check context *after* acquiring it.
		// Otherwise, check immediately.
		if g.sem != nil {
			g.sem <- struct{}{}
			defer func() { <-g.sem }()

			// Check context *again* after acquiring semaphore
			if err := g.ctx.Err(); err != nil {
				// Handle error (non-blocking send, cancelOnce) similarly to doWork's defer
				select {
				case g.errCh <- err:
				default:
				}
				if g.cancel != nil {
					g.cancelOnce.Do(g.cancel)
				}
				return // Don't run the job
			}
		} else {
			// No semaphore, check context before running job directly
			if err := g.ctx.Err(); err != nil {
				// Handle error (non-blocking send, cancelOnce)
				select {
				case g.errCh <- err:
				default:
				}
				if g.cancel != nil {
					g.cancelOnce.Do(g.cancel)
				}
				return // Don't run the job
			}
		}

		// At this point, context was not done when checked.
		// Proceed with doWork which contains its own panic recovery and error handling.
		// We pass a closure to doWork that wraps the actual job.
		g.doWork(job)
	}()
}

// Errors returns a channel that will receive the first error from any of the
// goroutines started by Go or GoCtx. The channel has a buffer of 1.
// It will be closed by Wait. Callers should not close the channel.
func (g *Group) Errors() <-chan error {
	return g.errCh
}

// Wait blocks until all functions passed to Go or GoCtx have returned.
// It then closes the Errors channel and cancels the Group's context (if any).
func (g *Group) Wait() {
	g.wg.Wait()
	g.errOnce.Do(func() {
		close(g.errCh)
		// Also ensure context is cancelled on Wait, in case no errors occurred
		// or cancel was not called for some reason.
		if g.cancel != nil {
			g.cancel() // This will be a no-op if cancelOnce already did it.
		}
	})
}

// Go runs a function in a goroutine (standalone version).
// It returns a channel that will receive the error from f (or a CaughtPanic if f panics),
// or nil if f completes successfully. The channel is buffered by 1 and will be closed
// after the error is sent (or after f completes if no error).
// The caller is responsible for draining this channel to prevent goroutine leaks if an error occurs.
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

// Safe executes a function f and recovers from any panics.
// If f returns an error, that error is returned.
// If f panics, a *CaughtPanic error is returned.
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

// SafeCtx executes a context-aware function f and recovers from any panics.
// If the context is already done, its error is returned immediately.
// If f returns an error, that error is returned.
// If f panics, a *CaughtPanic error is returned.
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
