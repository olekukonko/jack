package jack

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrFutureCanceled = fmt.Errorf("future was canceled")
	ErrFutureTimeout  = fmt.Errorf("future timed out")
)

var resultPool = sync.Pool{
	New: func() interface{} {
		ch := make(chan futureResult, 1)
		return &ch
	},
}

type futureResult struct {
	val interface{}
	err error
}

type Future[T any] struct {
	wg       sync.WaitGroup
	result   T
	err      error
	done     chan struct{}
	canceled atomic.Bool
	closed   atomic.Bool
	nilFunc  atomic.Bool
	mu       sync.Mutex
	fn       func(context.Context) (T, error)
	ctx      context.Context
}

// NewFuture creates a Future that executes fn asynchronously in a new goroutine.
// The context governs cancellation; fn receives it directly. If fn is nil,
// the Future is returned in an already-failed state.
func NewFuture[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	if fn == nil {
		f := &Future[T]{done: make(chan struct{})}
		f.err = fmt.Errorf("nil function provided")
		f.nilFunc.Store(true)
		f.closed.Store(true)
		close(f.done)
		return f
	}

	f := &Future[T]{
		done: make(chan struct{}),
		fn:   fn,
		ctx:  ctx,
	}

	f.wg.Add(1)
	go f.execute()

	return f
}

// execute runs fn inside a goroutine, stores the result, and closes done.
func (f *Future[T]) execute() {
	defer f.wg.Done()

	f.mu.Lock()
	fn := f.fn
	ctx := f.ctx
	f.fn = nil
	f.ctx = nil
	f.mu.Unlock()

	defer func() {
		if f.closed.CompareAndSwap(false, true) {
			close(f.done)
		}
	}()

	var result futureResult
	done := make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				result = futureResult{
					err: &CaughtPanic{Value: r, Stack: debug.Stack()},
				}
			}
			close(done)
		}()
		val, err := fn(ctx)
		result = futureResult{val: val, err: err}
	}()

	select {
	case <-ctx.Done():
		f.err = ctx.Err()
		if ctx.Err() == context.Canceled {
			f.canceled.Store(true)
		}
		<-done // Wait for goroutine to finish
		// If function completed successfully before context canceled, use result
		if result.err == nil && result.val != nil {
			f.result = result.val.(T)
			f.err = nil
		}
	case <-done:
		if result.val != nil {
			f.result = result.val.(T)
		}
		f.err = result.err
	}
}

// Cancel signals the Future to stop. Returns true if this call caused the cancellation.
func (f *Future[T]) Cancel() bool {
	return f.canceled.CompareAndSwap(false, true)
}

// IsCanceled reports whether the Future was cancelled before completing.
func (f *Future[T]) IsCanceled() bool {
	return f.canceled.Load()
}

// IsDone reports whether the Future has finished, either successfully or with an error.
func (f *Future[T]) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// Await blocks until the Future completes and returns its result and any error.
func (f *Future[T]) Await() (T, error) {
	<-f.done
	f.wg.Wait()
	var zero T
	if f.err != nil {
		return zero, f.err
	}
	if f.IsCanceled() {
		return zero, ErrFutureCanceled
	}
	return f.result, nil
}

// AwaitWithTimeout blocks until the Future completes or timeout elapses.
// Returns ErrFutureTimeout if the deadline is exceeded.
func (f *Future[T]) AwaitWithTimeout(timeout time.Duration) (T, error) {
	select {
	case <-f.done:
		return f.Await()
	default:
	}
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

// AwaitWithContext blocks until the Future completes or ctx is cancelled.
func (f *Future[T]) AwaitWithContext(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.Await()
	default:
	}
	select {
	case <-f.done:
		return f.Await()
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Then chains a transformation on a successful result, returning a new Future.
// The chained fn is called only if the original Future completes without error.
func (f *Future[T]) Then(ctx context.Context, fn func(T) (any, error)) *Future[any] {
	select {
	case <-f.done:
		val, err := f.result, f.err
		if err != nil {
			f2 := &Future[any]{done: make(chan struct{})}
			f2.err = err
			close(f2.done)
			return f2
		}
		newVal, newErr := fn(val)
		f2 := &Future[any]{done: make(chan struct{})}
		f2.result = newVal
		f2.err = newErr
		close(f2.done)
		return f2
	default:
		return NewFuture(ctx, func(context.Context) (any, error) {
			val, err := f.Await()
			if err != nil {
				return nil, err
			}
			return fn(val)
		})
	}
}

// Recover chains an error handler that can substitute a value when the Future fails.
// The handler fn is called only if the original Future returns a non-nil error.
func (f *Future[T]) Recover(ctx context.Context, fn func(error) (T, error)) *Future[T] {
	select {
	case <-f.done:
		val, err := f.result, f.err
		if err != nil {
			newVal, newErr := fn(err)
			f2 := &Future[T]{done: make(chan struct{})}
			f2.result = newVal
			f2.err = newErr
			close(f2.done)
			return f2
		}
		f2 := &Future[T]{done: make(chan struct{})}
		f2.result = val
		close(f2.done)
		return f2
	default:
		return NewFuture(ctx, func(context.Context) (T, error) {
			val, err := f.Await()
			if err != nil {
				return fn(err)
			}
			return val, nil
		})
	}
}

// Async runs fn in a new goroutine and returns a Future for its result.
func Async[T any](fn func() (T, error)) *Future[T] {
	return NewFuture(context.Background(), func(context.Context) (T, error) {
		return fn()
	})
}

// AsyncWithContext runs fn in a new goroutine under the given context.
func AsyncWithContext[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	return NewFuture(ctx, fn)
}

// WaitAll blocks until all futures complete and returns their results in order.
// Returns an error immediately if any Future fails.
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

type waitAnyResult[T any] struct {
	index int
	val   T
	err   error
}

// WaitAny blocks until the first Future completes and returns its index and value.
// Remaining futures continue running; only the first result is returned.
func WaitAny[T any](futures ...*Future[T]) (int, T, error) {
	n := len(futures)
	if n == 0 {
		var zero T
		return -1, zero, fmt.Errorf("no futures provided")
	}

	if n == 1 {
		val, err := futures[0].Await()
		return 0, val, err
	}

	if n <= 8 {
		return waitAnyGeneric(futures...)
	}

	return waitAnyReflect(futures...)
}

// waitAnyGeneric waits for the first Future to complete using channel selection.
func waitAnyGeneric[T any](futures ...*Future[T]) (int, T, error) {
	resultCh := make(chan waitAnyResult[T], 1)

	go func() {
		cases := make([]reflect.SelectCase, len(futures))
		for i, f := range futures {
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(f.done),
			}
		}
		chosen, _, _ := reflect.Select(cases)
		val, err := futures[chosen].Await()
		resultCh <- waitAnyResult[T]{index: chosen, val: val, err: err}
	}()

	res := <-resultCh
	return res.index, res.val, res.err
}

// waitAnyReflect waits for the first Future using reflect.Select for large slices.
func waitAnyReflect[T any](futures ...*Future[T]) (int, T, error) {
	cases := make([]reflect.SelectCase, len(futures))
	for i, f := range futures {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(f.done),
		}
	}
	chosen, _, _ := reflect.Select(cases)
	val, err := futures[chosen].Await()
	return chosen, val, err
}

// Select blocks until ctx is cancelled or the first Future completes.
// Returns the index and value of the winning Future.
func Select[T any](ctx context.Context, futures ...*Future[T]) (int, T, error) {
	n := len(futures)
	if n == 0 {
		var zero T
		return -1, zero, fmt.Errorf("no futures provided")
	}

	if n == 1 {
		select {
		case <-ctx.Done():
			var zero T
			return -1, zero, ctx.Err()
		case <-futures[0].done:
			val, err := futures[0].Await()
			return 0, val, err
		}
	}

	return selectReflect(ctx, futures...)
}

// selectReflect selects the first completing Future via reflect.Select.
func selectReflect[T any](ctx context.Context, futures ...*Future[T]) (int, T, error) {
	cases := make([]reflect.SelectCase, 0, len(futures)+1)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for _, f := range futures {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(f.done),
		})
	}
	chosen, _, _ := reflect.Select(cases)
	if chosen == 0 {
		var zero T
		return -1, zero, ctx.Err()
	}
	val, err := futures[chosen-1].Await()
	return chosen - 1, val, err
}
