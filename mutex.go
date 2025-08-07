package jack

import (
	"context"
	"runtime/debug"
	"sync"
)

// Mutex wraps sync.Mutex with safe execution methods.
type Mutex struct {
	sync.Mutex
}

// Do executes fn while holding the mutex lock.
// The function is guaranteed to run with exclusive access, ensuring thread safety.
// If fn is nil, Do will panic.
// Example:
//
//	var mu Mutex
//	mu.Do(func() {
//	    // Critical section
//	    fmt.Println("Safe execution")
//	})
func (m *Mutex) Do(fn func()) {
	m.Lock()
	defer m.Unlock()
	fn()
}

// DoCtx executes fn while holding the mutex lock, with context support.
// The lock is acquired, then the context is checked. If the context is done,
// its error is returned and fn is not executed. Otherwise, fn is executed in a goroutine,
// allowing it to be interrupted if the context is canceled or times out (e.g., via context.WithTimeout).
// If fn is nil, DoCtx will return a *CaughtPanic.
// If ctx is nil, DoCtx will panic when ctx.Err() is called.
// This method does not recover from panics in fn; use SafeCtx for panic recovery.
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
//	defer cancel()
//	err := mu.DoCtx(ctx, func() error {
//	    // Critical section, no need to check ctx
//	    time.Sleep(100 * time.Millisecond)
//	    return nil
//	})
//	// err will likely be context.DeadlineExceeded due to timeout
func (m *Mutex) DoCtx(ctx context.Context, fn func() error) error {
	m.Lock()
	defer m.Unlock()
	// Delegate to safeCtx without stack capture, as DoCtx doesn't need panic recovery
	return safeCtx(ctx, fn, false)
}

// Safe executes fn while holding the mutex lock, with panic recovery.
// If fn panics, a *CaughtPanic error is returned. Otherwise, the error returned by fn is returned.
// If fn is nil, Safe will return a *CaughtPanic.
// Example:
//
//	var mu Mutex
//	err := mu.Safe(func() error {
//	    // Critical section
//	    return nil
//	})
//	if err != nil {
//	    fmt.Println("Error:", err)
//	}
func (m *Mutex) Safe(fn func() error) error {
	m.Lock()
	defer m.Unlock()
	return safe(fn, true)
}

// SafeCtx executes fn with panic recovery and context awareness while holding the mutex lock.
// The lock is acquired, then fn is executed via the standalone SafeCtx function.
// This ensures fn can be canceled by the context (e.g., via context.WithTimeout),
// and any panics are recovered. The lock is held until fn completes or the context is canceled.
// If fn is nil, SafeCtx will return a *CaughtPanic.
// If ctx is nil, SafeCtx will panic when ctx.Err() is called.
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
//	defer cancel()
//	err := mu.SafeCtx(ctx, func() error {
//	    // Critical section, no need to check ctx
//	    time.Sleep(100 * time.Millisecond)
//	    return nil
//	})
//	// err will likely be context.DeadlineExceeded due to timeout
func (m *Mutex) SafeCtx(ctx context.Context, fn func() error) error {
	m.Lock()
	defer m.Unlock()
	return SafeCtx(ctx, fn)
}

// safe is the internal implementation with configurable stack collection.
func safe(fn func() error, captureStack bool) (err error) {
	defer func() {
		if v := recover(); v != nil {
			cp := &CaughtPanic{Val: v}
			if captureStack {
				cp.Stack = debug.Stack()
			}
			err = cp
		}
	}()
	return fn()
}

// Safe executes fn with panic recovery.
// If fn panics, a *CaughtPanic error is returned. Otherwise, the error returned by fn is returned.
// If fn is nil, Safe will return a *CaughtPanic.
// Example:
//
//	err := Safe(func() error {
//	    // May panic or return an error
//	    return nil
//	})
//	if cp, ok := err.(*CaughtPanic); ok {
//	    fmt.Printf("Caught panic: %v\nStack: %s\n", cp.Val, cp.Stack)
//	}
func Safe(fn func() error) error {
	return safe(fn, true)
}

// SafeCtx executes fn with context support and panic recovery.
// The function fn will be interrupted if the context is canceled (e.g., via context.WithTimeout).
// If fn is nil, SafeCtx will return a *CaughtPanic.
// If ctx is nil, SafeCtx will panic when ctx.Err() is called.
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
//	defer cancel()
//	err := SafeCtx(ctx, func() error {
//	    // No need to check ctx
//	    time.Sleep(100 * time.Millisecond)
//	    return nil
//	})
//	// err will likely be context.DeadlineExceeded due to timeout
func SafeCtx(ctx context.Context, fn func() error) error {
	return safeCtx(ctx, fn, true)
}

// safeCtx is the internal context-aware implementation with configurable stack collection.
func safeCtx(ctx context.Context, fn func() error, captureStack bool) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	resultChan := make(chan error, 1)
	go func() {
		resultChan <- safe(fn, captureStack)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-resultChan:
		return err
	}
}

// SafeNoStack executes fn with panic recovery but no stack trace.
// If fn panics, a *CaughtPanic error (without stack) is returned. Otherwise, fn's error.
// If fn is nil, SafeNoStack will return a *CaughtPanic.
// Example:
//
//	err := SafeNoStack(func() error {
//	    panic("error")
//	    return nil
//	})
//	if cp, ok := err.(*CaughtPanic); ok {
//	    fmt.Printf("Caught panic: %v, Stack: %v\n", cp.Val, cp.Stack == nil)
//	}
func SafeNoStack(fn func() error) error {
	return safe(fn, false)
}

// SafeCtxNoStack executes fn with context support, panic recovery, but no stack trace.
// The function fn will be interrupted if the context is canceled (e.g., via context.WithTimeout).
// If fn is nil, SafeCtxNoStack will return a *CaughtPanic (without stack).
// If ctx is nil, SafeCtxNoStack will panic when ctx.Err() is called.
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
//	defer cancel()
//	err := SafeCtxNoStack(ctx, func() error {
//	    // No need to check ctx
//	    time.Sleep(100 * time.Millisecond)
//	    return nil
//	})
//	// err will likely be context.DeadlineExceeded due to timeout
func SafeCtxNoStack(ctx context.Context, fn func() error) error {
	return safeCtx(ctx, fn, false)
}
