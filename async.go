package jack

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Common errors
var (
	ErrFutureCanceled = fmt.Errorf("future was canceled")
	ErrFutureTimeout  = fmt.Errorf("future timed out")
)

// Future represents a value that will be available asynchronously.
// It is safe for concurrent use by multiple goroutines.
type Future[T any] struct {
	wg       sync.WaitGroup
	result   T
	err      error
	done     chan struct{}
	canceled atomic.Bool
}

// NewFuture creates a new Future that will execute the provided function.
// The function receives a context that can be used to check for cancellation.
// If fn is nil, it returns a Future that immediately completes with an error.
func NewFuture[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	if fn == nil {
		f := &Future[T]{done: make(chan struct{})}
		f.err = fmt.Errorf("nil function provided")
		close(f.done)
		return f
	}
	f := &Future[T]{
		done: make(chan struct{}),
	}

	f.wg.Add(1)
	go f.execute(ctx, fn)

	return f
}

// execute runs the provided function and handles panics and cancellation.
func (f *Future[T]) execute(ctx context.Context, fn func(context.Context) (T, error)) {
	defer f.wg.Done()
	defer close(f.done)
	defer f.handlePanic()

	// Check if already canceled
	if err := ctx.Err(); err != nil {
		f.err = err
		if err == context.Canceled {
			f.canceled.Store(true)
		}
		return
	}

	// Run the function in a separate goroutine to handle cancellation
	resultCh := make(chan struct {
		val T
		err error
	}, 1)

	go f.runFuncWithRecovery(fn, ctx, resultCh)

	// Wait for either result or cancellation
	select {
	case <-ctx.Done():
		f.err = ctx.Err()
		if ctx.Err() == context.Canceled {
			f.canceled.Store(true)
		}
	case res := <-resultCh:
		f.result = res.val
		f.err = res.err
	}
}

// runFuncWithRecovery executes the user function with panic recovery.
func (f *Future[T]) runFuncWithRecovery(fn func(context.Context) (T, error), ctx context.Context, resultCh chan<- struct {
	val T
	err error
}) {
	defer func() {
		if r := recover(); r != nil {
			resultCh <- struct {
				val T
				err error
			}{err: &CaughtPanic{Value: r, Stack: debug.Stack()}}
		}
	}()

	val, err := fn(ctx)
	resultCh <- struct {
		val T
		err error
	}{val: val, err: err}
}

// handlePanic catches panics from the execute method itself
func (f *Future[T]) handlePanic() {
	if r := recover(); r != nil {
		f.err = &CaughtPanic{Value: r, Stack: debug.Stack()}
	}
}

// Cancel attempts to cancel the future execution.
// Returns true if the future was actually canceled.
// Note: This marks the future as canceled, causing Await to return ErrFutureCanceled after completion.
// It does not interrupt the running task; use context cancellation for cooperative interruption.
func (f *Future[T]) Cancel() bool {
	return f.canceled.CompareAndSwap(false, true)
}

// IsCanceled returns true if the future was canceled.
func (f *Future[T]) IsCanceled() bool {
	return f.canceled.Load()
}

// IsDone returns true if the future has completed (success, error, or panic).
func (f *Future[T]) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// Await waits for the future to complete and returns the result or error.
// If the future was canceled, it returns ErrFutureCanceled.
func (f *Future[T]) Await() (T, error) {
	<-f.done
	f.wg.Wait() // Ensure cleanup is complete

	var zero T
	if f.err != nil {
		return zero, f.err
	}
	if f.IsCanceled() {
		return zero, ErrFutureCanceled
	}

	return f.result, nil
}

// AwaitWithTimeout waits for the future to complete with a timeout.
// Returns ErrFutureTimeout if the timeout is reached.
func (f *Future[T]) AwaitWithTimeout(timeout time.Duration) (T, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-f.done:
		return f.Await()
	case <-timer.C:
		var zero T
		return zero, ErrFutureTimeout
	}
}

// AwaitWithContext waits for the future to complete or for the context to be done.
func (f *Future[T]) AwaitWithContext(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.Await()
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Then chains a transformation on the future result.
// The transformation is applied asynchronously when the original future completes.
func (f *Future[T]) Then(ctx context.Context, fn func(T) (any, error)) *Future[any] {
	return NewFuture(ctx, func(ctx context.Context) (any, error) {
		val, err := f.Await()
		if err != nil {
			return nil, err
		}
		return fn(val)
	})
}

// Recover handles errors in the future by providing a fallback value or function.
func (f *Future[T]) Recover(ctx context.Context, fn func(error) (T, error)) *Future[T] {
	return NewFuture(ctx, func(ctx context.Context) (T, error) {
		val, err := f.Await()
		if err != nil {
			return fn(err)
		}
		return val, nil
	})
}

// Async creates a future from a function that returns a value and error.
// This is provided for backward compatibility with your original API.
func Async[T any](fn func() (T, error)) *Future[T] {
	return NewFuture(context.Background(), func(context.Context) (T, error) {
		return fn()
	})
}

// AsyncWithContext creates a future from a context-aware function.
func AsyncWithContext[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	return NewFuture(ctx, fn)
}

// WaitAll waits for all futures to complete and returns their results.
// Returns an error if any future fails.
func WaitAll[T any](futures ...*Future[T]) ([]T, error) {
	results := make([]T, len(futures))

	for i, f := range futures {
		val, err := f.Await()
		if err != nil {
			return nil, fmt.Errorf("future %d failed: %w", i, err)
		}
		results[i] = val
	}

	return results, nil
}

// WaitAny waits for the first future to complete and returns its index and result.
func WaitAny[T any](futures ...*Future[T]) (int, T, error) {
	caseCount := len(futures)
	if caseCount == 0 {
		var zero T
		return -1, zero, fmt.Errorf("no futures provided")
	}

	type result struct {
		index int
		val   T
		err   error
	}

	resultCh := make(chan result, caseCount)

	for i, f := range futures {
		go func(idx int, future *Future[T]) {
			val, err := future.Await()
			resultCh <- result{idx, val, err}
		}(i, f)
	}

	res := <-resultCh
	return res.index, res.val, res.err
}

// Select is like WaitAny but with a context for cancellation.
func Select[T any](ctx context.Context, futures ...*Future[T]) (int, T, error) {
	if len(futures) == 0 {
		var zero T
		return -1, zero, fmt.Errorf("no futures provided")
	}

	type result struct {
		index int
		val   T
		err   error
	}

	resultCh := make(chan result, len(futures))

	for i, f := range futures {
		go func(idx int, future *Future[T]) {
			val, err := future.Await()
			resultCh <- result{idx, val, err}
		}(i, f)
	}

	select {
	case <-ctx.Done():
		var zero T
		return -1, zero, ctx.Err()
	case res := <-resultCh:
		return res.index, res.val, res.err
	}
}
