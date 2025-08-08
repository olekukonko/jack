package jack

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSafely tests the Safely type and its methods (Do, DoCtx, Safe, SafeCtx).
func TestSafely(t *testing.T) {
	t.Run("Do", func(t *testing.T) {
		var mu Safely
		var counter int
		var wg sync.WaitGroup
		wg.Add(2)

		// Test concurrent execution with Safely protection
		go func() {
			defer wg.Done()
			mu.Do(func() {
				tmp := counter
				time.Sleep(10 * time.Millisecond)
				counter = tmp + 1
			})
		}()
		go func() {
			defer wg.Done()
			mu.Do(func() {
				tmp := counter
				time.Sleep(10 * time.Millisecond)
				counter = tmp + 1
			})
		}()
		wg.Wait()
		if counter != 2 {
			t.Errorf("Do: expected counter = 2, got %d", counter)
		}

		// Test nil fn (should panic, but we can't recover in Do)
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Do: expected panic with nil fn, got none")
			}
		}()
		mu.Do(nil)
	})

	t.Run("DoCtx", func(t *testing.T) {
		var mu Safely

		// Test normal execution
		var called bool
		err := mu.DoCtx(context.Background(), func() error {
			called = true
			return nil
		})
		if err != nil || !called {
			t.Errorf("DoCtx: expected nil error and called=true, got err=%v, called=%v", err, called)
		}

		// Test context timeout
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err = mu.DoCtx(ctx, func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("DoCtx: expected context.DeadlineExceeded, got %v", err)
		}

		// Test canceled context
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		err = mu.DoCtx(ctx, func() error {
			t.Fatal("DoCtx: fn should not be called with canceled context")
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("DoCtx: expected context.Canceled, got %v", err)
		}

		// Test nil fn
		err = mu.DoCtx(context.Background(), nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("DoCtx: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}

		// Test nil ctx
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("DoCtx: expected panic with nil ctx, got none")
			}
		}()
		_ = mu.DoCtx(nil, func() error { return nil })
	})

	t.Run("Safe", func(t *testing.T) {
		var mu Safely

		// Test normal execution
		err := mu.Safe(func() error { return nil })
		if err != nil {
			t.Errorf("Safe: expected nil error, got %v", err)
		}

		// Test panic recovery
		err = mu.Safe(func() error { panic("test panic") })
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val != "test panic" || len(cp.Stack) == 0 {
			t.Errorf("Safe: expected *CaughtPanic with Val='test panic' and non-empty stack, got %v", err)
		}

		// Test nil fn
		err = mu.Safe(nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("Safe: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}
	})

	t.Run("SafeCtx", func(t *testing.T) {
		var mu Safely

		// Test normal execution
		err := mu.SafeCtx(context.Background(), func() error { return nil })
		if err != nil {
			t.Errorf("SafeCtx: expected nil error, got %v", err)
		}

		// Test context timeout
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err = mu.SafeCtx(ctx, func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("SafeCtx: expected context.DeadlineExceeded, got %v", err)
		}

		// Test panic recovery
		err = mu.SafeCtx(context.Background(), func() error { panic("test panic") })
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val != "test panic" || len(cp.Stack) == 0 {
			t.Errorf("SafeCtx: expected *CaughtPanic with Val='test panic' and non-empty stack, got %v", err)
		}

		// Test nil fn
		err = mu.SafeCtx(context.Background(), nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("SafeCtx: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}

		// Test nil ctx
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("SafeCtx: expected panic with nil ctx, got none")
			}
		}()
		_ = mu.SafeCtx(nil, func() error { return nil })
	})
}

// TestStandalone tests the standalone functions (Safe, SafeCtx, SafeNoStack, SafeCtxNoStack).
func TestStandalone(t *testing.T) {
	t.Run("Safe", func(t *testing.T) {
		// Test normal execution
		err := Safe(func() error { return nil })
		if err != nil {
			t.Errorf("Safe: expected nil error, got %v", err)
		}

		// Test panic recovery
		err = Safe(func() error { panic("test panic") })
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val != "test panic" || len(cp.Stack) == 0 {
			t.Errorf("Safe: expected *CaughtPanic with Val='test panic' and non-empty stack, got %v", err)
		}

		// Test nil fn
		err = Safe(nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("Safe: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}
	})

	t.Run("SafeCtx", func(t *testing.T) {
		// Test normal execution
		err := SafeCtx(context.Background(), func() error { return nil })
		if err != nil {
			t.Errorf("SafeCtx: expected nil error, got %v", err)
		}

		// Test context timeout
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err = SafeCtx(ctx, func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("SafeCtx: expected context.DeadlineExceeded, got %v", err)
		}

		// Test panic recovery
		err = SafeCtx(context.Background(), func() error { panic("test panic") })
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val != "test panic" || len(cp.Stack) == 0 {
			t.Errorf("SafeCtx: expected *CaughtPanic with Val='test panic' and non-empty stack, got %v", err)
		}

		// Test nil fn
		err = SafeCtx(context.Background(), nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("SafeCtx: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}

		// Test nil ctx
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("SafeCtx: expected panic with nil ctx, got none")
			}
		}()
		_ = SafeCtx(nil, func() error { return nil })
	})

	t.Run("SafeNoStack", func(t *testing.T) {
		// Test normal execution
		err := SafeNoStack(func() error { return nil })
		if err != nil {
			t.Errorf("SafeNoStack: expected nil error, got %v", err)
		}

		// Test panic recovery
		err = SafeNoStack(func() error { panic("test panic") })
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val != "test panic" || len(cp.Stack) != 0 {
			t.Errorf("SafeNoStack: expected *CaughtPanic with Val='test panic' and empty stack, got %v", err)
		}

		// Test nil fn
		err = SafeNoStack(nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("SafeNoStack: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}
	})

	t.Run("SafeCtxNoStack", func(t *testing.T) {
		// Test normal execution
		err := SafeCtxNoStack(context.Background(), func() error { return nil })
		if err != nil {
			t.Errorf("SafeCtxNoStack: expected nil error, got %v", err)
		}

		// Test context timeout
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err = SafeCtxNoStack(ctx, func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("SafeCtxNoStack: expected context.DeadlineExceeded, got %v", err)
		}

		// Test panic recovery
		err = SafeCtxNoStack(context.Background(), func() error { panic("test panic") })
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val != "test panic" || len(cp.Stack) != 0 {
			t.Errorf("SafeCtxNoStack: expected *CaughtPanic with Val='test panic' and empty stack, got %v", err)
		}

		// Test nil fn
		err = SafeCtxNoStack(context.Background(), nil)
		if cp, ok := err.(*CaughtPanic); !ok || cp.Val == nil {
			t.Errorf("SafeCtxNoStack: expected *CaughtPanic with non-nil Val for nil fn, got %v", err)
		}

		// Test nil ctx
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("SafeCtxNoStack: expected panic with nil ctx, got none")
			}
		}()
		_ = SafeCtxNoStack(nil, func() error { return nil })
	})
}

// TestCaughtPanic tests the CaughtPanic type's methods.
func TestCaughtPanic(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		cp := &CaughtPanic{Val: "test panic", Stack: []byte("stack trace")}
		errStr := cp.Error()
		if !strings.Contains(errStr, "panic: test panic") || !strings.Contains(errStr, "stack trace") {
			t.Errorf("CaughtPanic.Error: expected 'panic: test panic' and stack, got %s", errStr)
		}

		cp = &CaughtPanic{Val: "test panic"}
		errStr = cp.Error()
		if errStr != "panic: test panic" {
			t.Errorf("CaughtPanic.Error: expected 'panic: test panic', got %s", errStr)
		}
	})

	t.Run("String", func(t *testing.T) {
		cp := &CaughtPanic{Val: "test panic", Stack: []byte("stack trace")}
		str := cp.String()
		if !strings.Contains(str, "panic: test panic") || !strings.Contains(str, "stack trace") {
			t.Errorf("CaughtPanic.String: expected 'panic: test panic' and stack, got %s", str)
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		// Test with error as Val
		innerErr := fmt.Errorf("inner error")
		cp := &CaughtPanic{Val: innerErr}
		if unwrapped := cp.Unwrap(); unwrapped != innerErr {
			t.Errorf("CaughtPanic.Unwrap: expected %v, got %v", innerErr, unwrapped)
		}

		// Test with non-error Val
		cp = &CaughtPanic{Val: "not an error"}
		if unwrapped := cp.Unwrap(); unwrapped != nil {
			t.Errorf("CaughtPanic.Unwrap: expected nil, got %v", unwrapped)
		}
	})
}

// TestConcurrent tests concurrent usage of Safely methods.
func TestConcurrent(t *testing.T) {
	var mu Safely
	var counter int
	var wg sync.WaitGroup
	n := 100

	// Test Safe concurrently
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			err := mu.Safe(func() error {
				tmp := counter
				time.Sleep(time.Millisecond)
				counter = tmp + 1
				return nil
			})
			if err != nil {
				t.Errorf("Safe: unexpected error %v", err)
			}
		}()
	}
	wg.Wait()
	if counter != n {
		t.Errorf("Safe concurrent: expected counter = %d, got %d", n, counter)
	}

	// Test SafeCtx concurrently
	counter = 0
	wg.Add(n)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			err := mu.SafeCtx(ctx, func() error {
				tmp := counter
				time.Sleep(time.Millisecond)
				counter = tmp + 1
				return nil
			})
			if err != nil {
				t.Errorf("SafeCtx: unexpected error %v", err)
			}
		}()
	}
	wg.Wait()
	if counter != n {
		t.Errorf("SafeCtx concurrent: expected counter = %d, got %d", n, counter)
	}
}
