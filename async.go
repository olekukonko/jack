// async.go - Optimized Future implementation (Final - Hybrid Approach)
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

// Common errors
var (
	ErrFutureCanceled = fmt.Errorf("future was canceled")
	ErrFutureTimeout  = fmt.Errorf("future timed out")
)

// resultPool pools result channels to reduce allocations
var resultPool = sync.Pool{
	New: func() interface{} {
		ch := make(chan futureResult, 1)
		return &ch
	},
}

// futureResult is the internal result type
type futureResult struct {
	val interface{}
	err error
}

// Future represents a value that will be available asynchronously.
// Optimized version uses pooled channels and fast paths for composition.
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

// NewFuture creates a new Future that will execute the provided function.
// Eager spawn - starts immediately to ensure IsDone works without Await.
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

	// Eager spawn required for IsDone() to work without Await()
	f.wg.Add(1)
	go f.execute()

	return f
}

func (f *Future[T]) execute() {
	defer f.wg.Done()

	// Get fn and ctx under lock, then clear for GC
	f.mu.Lock()
	fn := f.fn
	ctx := f.ctx
	f.fn = nil
	f.ctx = nil
	f.mu.Unlock()

	// Ensure done is closed exactly once
	defer func() {
		if f.closed.CompareAndSwap(false, true) {
			close(f.done)
		}
	}()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			f.err = &CaughtPanic{Value: r, Stack: debug.Stack()}
		}
	}()

	if err := ctx.Err(); err != nil {
		f.err = err
		if err == context.Canceled {
			f.canceled.Store(true)
		}
		return
	}

	// Get pooled result channel
	resultChPtr := resultPool.Get().(*chan futureResult)
	resultCh := *resultChPtr

	// Run function with cancellation support
	go f.runFuncWithRecovery(fn, ctx, resultCh)

	select {
	case <-ctx.Done():
		f.err = ctx.Err()
		if ctx.Err() == context.Canceled {
			f.canceled.Store(true)
		}
		// Drain to prevent goroutine leak
		select {
		case <-resultCh:
		default:
		}
	case res := <-resultCh:
		// Safe type assertion with nil check
		if res.val != nil {
			f.result = res.val.(T)
		}
		f.err = res.err
	}

	// Return channel to pool
	select {
	case <-resultCh:
	default:
	}
	resultPool.Put(resultChPtr)
}

func (f *Future[T]) runFuncWithRecovery(fn func(context.Context) (T, error), ctx context.Context, resultCh chan<- futureResult) {
	defer func() {
		if r := recover(); r != nil {
			resultCh <- futureResult{
				err: &CaughtPanic{Value: r, Stack: debug.Stack()},
			}
		}
	}()
	val, err := fn(ctx)
	resultCh <- futureResult{val: val, err: err}
}

func (f *Future[T]) Cancel() bool {
	return f.canceled.CompareAndSwap(false, true)
}

func (f *Future[T]) IsCanceled() bool {
	return f.canceled.Load()
}

func (f *Future[T]) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

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

// Then chains a transformation. Fast path if already done.
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

// Recover handles errors. Fast path if already done.
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

func Async[T any](fn func() (T, error)) *Future[T] {
	return NewFuture(context.Background(), func(context.Context) (T, error) {
		return fn()
	})
}

func AsyncWithContext[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	return NewFuture(ctx, fn)
}

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

// waitAnyResult is used for the generic channel approach
type waitAnyResult[T any] struct {
	index int
	val   T
	err   error
}

// WaitAny - Hybrid: fast path for small N (1 goroutine), reflect for large N
func WaitAny[T any](futures ...*Future[T]) (int, T, error) {
	n := len(futures)
	if n == 0 {
		var zero T
		return -1, zero, fmt.Errorf("no futures provided")
	}

	// Fast path: single future
	if n == 1 {
		val, err := futures[0].Await()
		return 0, val, err
	}

	// Medium path: 2-8 futures use 1 goroutine (low overhead)
	if n <= 8 {
		return waitAnyGeneric(futures...)
	}

	// Slow path: >8 futures use reflect.Select (scales better, no extra goroutine)
	return waitAnyReflect(futures...)
}

// waitAnyGeneric: 1 goroutine + channel, good for small N
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

// waitAnyReflect: direct reflect.Select, good for large N
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

// Select - Hybrid: fast path for small N, reflect for large N
func Select[T any](ctx context.Context, futures ...*Future[T]) (int, T, error) {
	n := len(futures)
	if n == 0 {
		var zero T
		return -1, zero, fmt.Errorf("no futures provided")
	}

	// Fast path: single future with context
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

	// Medium/Slow path: use reflect (context adds complexity to generic version)
	return selectReflect(ctx, futures...)
}

// selectReflect: direct reflect.Select with context
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
