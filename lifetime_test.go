package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifetime_Execute_Success(t *testing.T) {
	var startCalled, endCalled, operationCalled atomic.Bool

	lifetime := NewLifetime(
		LifetimeWithStart(func(ctx context.Context, id string) error {
			startCalled.Store(true)
			return nil
		}),
		LifetimeWithEnd(func(ctx context.Context, id string) {
			endCalled.Store(true)
		}),
	)

	err := lifetime.Execute(context.Background(), "test-id", Func(func() error {
		operationCalled.Store(true)
		return nil
	}))

	if err != nil {
		t.Errorf("Execute() error = %v, want nil", err)
	}
	if !startCalled.Load() {
		t.Error("Start hook was not called")
	}
	if !operationCalled.Load() {
		t.Error("Operation was not called")
	}
	if !endCalled.Load() {
		t.Error("End hook was not called")
	}
}

func TestLifetime_Execute_StartError(t *testing.T) {
	expectedErr := context.Canceled

	lifetime := NewLifetime(
		LifetimeWithStart(func(ctx context.Context, id string) error {
			return expectedErr
		}),
		LifetimeWithEnd(func(ctx context.Context, id string) {
			t.Error("End should not be called when Start fails")
		}),
	)

	var operationCalled atomic.Bool
	err := lifetime.Execute(context.Background(), "test-id", Func(func() error {
		operationCalled.Store(true)
		return nil
	}))

	if err != expectedErr {
		t.Errorf("Execute() error = %v, want %v", err, expectedErr)
	}
	if operationCalled.Load() {
		t.Error("Operation should not be called when Start returns error")
	}
}

func TestLifetime_Execute_OperationError(t *testing.T) {
	var startCalled, endCalled atomic.Bool

	lifetime := NewLifetime(
		LifetimeWithStart(func(ctx context.Context, id string) error {
			startCalled.Store(true)
			return nil
		}),
		LifetimeWithEnd(func(ctx context.Context, id string) {
			endCalled.Store(true)
		}),
	)

	expectedErr := context.Canceled
	err := lifetime.Execute(context.Background(), "test-id", Func(func() error {
		return expectedErr
	}))

	if err != expectedErr {
		t.Errorf("Execute() error = %v, want %v", err, expectedErr)
	}
	if !startCalled.Load() {
		t.Error("Start hook should be called")
	}
	if endCalled.Load() {
		t.Error("End hook should not be called when operation fails")
	}
}

func TestLifetimeManager_ExecuteWithLifetime_SuccessWithTimed(t *testing.T) {
	lm := NewLifetimeManager()
	defer lm.StopAll()

	var timedCalled atomic.Bool
	lifetime := NewLifetime(
		LifetimeWithTimed(func(ctx context.Context, id string) {
			timedCalled.Store(true)
		}, 100*time.Millisecond),
	)

	var operationCalled atomic.Bool

	err := lm.ExecuteWithLifetime(context.Background(), "test-id", lifetime, Func(func() error {
		operationCalled.Store(true)
		return nil
	}))

	if err != nil {
		t.Errorf("ExecuteWithLifetime() error = %v, want nil", err)
	}
	if !operationCalled.Load() {
		t.Error("Operation was not called")
	}

	// Check that timer was scheduled
	if !lm.HasPending("test-id") {
		t.Error("Timer should be pending after ExecuteWithLifetime")
	}

	// Wait for timer to fire
	time.Sleep(150 * time.Millisecond)

	if !timedCalled.Load() {
		t.Error("Timed callback should have been called")
	}
	if lm.HasPending("test-id") {
		t.Error("Timer should be removed after firing")
	}
}

func TestLifetime_WithEndNoError(t *testing.T) {
	// Test that End hook doesn't return error (matching zevent)
	var endCalled atomic.Bool

	lifetime := NewLifetime(
		LifetimeWithEnd(func(ctx context.Context, id string) {
			endCalled.Store(true)
			// No return value - matches zevent
		}),
	)

	// This should compile and work
	err := lifetime.Execute(context.Background(), "test-id", Func(func() error {
		return nil
	}))

	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}
	if !endCalled.Load() {
		t.Error("End hook not called")
	}
}

func TestLifetime_Chaining(t *testing.T) {
	// Test that With methods can be chained (like original zevent)
	var startCalled, endCalled atomic.Bool

	lifetime := NewLifetime(
		LifetimeWithStart(func(ctx context.Context, id string) error {
			startCalled.Store(true)
			return nil
		}),
		LifetimeWithEnd(func(ctx context.Context, id string) {
			endCalled.Store(true)
		}),
	)

	err := lifetime.Execute(context.Background(), "test-id", Func(func() error {
		return nil
	}))

	if err != nil {
		t.Errorf("Chained lifetime failed: %v", err)
	}
	if !startCalled.Load() {
		t.Error("Start not called")
	}
	if !endCalled.Load() {
		t.Error("End not called")
	}
}

func TestLifetime_NoHooks(t *testing.T) {
	// Test lifetime with no hooks (should just run operation)
	lifetime := NewLifetime()

	var operationCalled atomic.Bool
	err := lifetime.Execute(context.Background(), "test-id", Func(func() error {
		operationCalled.Store(true)
		return nil
	}))

	if err != nil {
		t.Errorf("Execute with no hooks failed: %v", err)
	}
	if !operationCalled.Load() {
		t.Error("Operation not called")
	}
}

func TestLifetime_OnlyTimed(t *testing.T) {
	// Test lifetime with only Timed hook
	var timedCalled atomic.Bool
	lifetime := NewLifetime(
		LifetimeWithTimed(func(ctx context.Context, id string) {
			timedCalled.Store(true)
		}, 50*time.Millisecond),
	)

	lm := NewLifetimeManager()
	defer lm.StopAll()

	err := lm.ExecuteWithLifetime(context.Background(), "test-id", lifetime, Func(func() error {
		return nil
	}))

	if err != nil {
		t.Errorf("Execute with only timed failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if !timedCalled.Load() {
		t.Error("Timed hook not called")
	}
}

func TestLifetimeManager_ContextPropagation(t *testing.T) {
	lm := NewLifetimeManager()
	defer lm.StopAll()

	// Test that context is passed to callbacks
	var startCtx, endCtx context.Context
	var startID, endID string

	lifetime := NewLifetime(
		LifetimeWithStart(func(ctx context.Context, id string) error {
			startCtx = ctx
			startID = id
			return nil
		}),
		LifetimeWithEnd(func(ctx context.Context, id string) {
			endCtx = ctx
			endID = id
		}),
	)

	expectedID := "test-context"
	ctx := context.WithValue(context.Background(), "test-key", "test-value")

	err := lm.ExecuteWithLifetime(ctx, expectedID, lifetime, Func(func() error {
		return nil
	}))

	if err != nil {
		t.Fatalf("ExecuteWithLifetime failed: %v", err)
	}

	// Check Start hook context
	if startCtx == nil {
		t.Error("Start hook did not receive context")
	} else if val := startCtx.Value("test-key"); val != "test-value" {
		t.Error("Start hook context does not have expected value")
	}
	if startID != expectedID {
		t.Errorf("Start hook id = %v, want %v", startID, expectedID)
	}

	// Check End hook context
	if endCtx == nil {
		t.Error("End hook did not receive context")
	} else if val := endCtx.Value("test-key"); val != "test-value" {
		t.Error("End hook context does not have expected value")
	}
	if endID != expectedID {
		t.Errorf("End hook id = %v, want %v", endID, expectedID)
	}
}

func TestLifetimeManager_ExecuteWithLifetime_NoLifetime(t *testing.T) {
	lm := NewLifetimeManager()
	defer lm.StopAll()

	var operationCalled atomic.Bool
	err := lm.ExecuteWithLifetime(context.Background(), "test-id", nil, Func(func() error {
		operationCalled.Store(true)
		return nil
	}))

	if err != nil {
		t.Errorf("ExecuteWithLifetime with nil lifetime failed: %v", err)
	}
	if !operationCalled.Load() {
		t.Error("Operation not called")
	}
}

func TestLifetimeManager_ExecuteWithLifetime_OperationError(t *testing.T) {
	lm := NewLifetimeManager()
	defer lm.StopAll()

	var timedCalled atomic.Bool
	lifetime := NewLifetime(
		LifetimeWithTimed(func(ctx context.Context, id string) {
			timedCalled.Store(true)
		}, 100*time.Millisecond),
	)

	err := lm.ExecuteWithLifetime(context.Background(), "test-id", lifetime, Func(func() error {
		return context.Canceled
	}))

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
	if lm.HasPending("test-id") {
		t.Error("Timer should not be scheduled when operation fails")
	}

	time.Sleep(150 * time.Millisecond)
	if timedCalled.Load() {
		t.Error("Timed hook should not be called when operation fails")
	}
}

func TestLifetimeManager_ConcurrentScheduleTimed(t *testing.T) {
	lm := NewLifetimeManager()
	defer lm.StopAll()

	var wg sync.WaitGroup
	var callCount atomic.Int32

	// Schedule many timers concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lm.ScheduleTimed(context.Background(),
				string(rune('A'+(id%26))),
				func(ctx context.Context, id string) {
					callCount.Add(1)
				},
				time.Duration(10+id%10)*time.Millisecond,
			)
		}(i)
	}

	wg.Wait()

	// Wait for all timers
	time.Sleep(200 * time.Millisecond)

	// Verify all cleaned up
	if lm.PendingCount() != 0 {
		t.Errorf("Expected 0 pending timers, got %d", lm.PendingCount())
	}
}

func TestLifetimeManager_MultipleResets(t *testing.T) {
	lm := NewLifetimeManager()
	defer lm.StopAll()

	var called atomic.Bool
	id := "test-reset"

	lm.ScheduleTimed(context.Background(), id, func(ctx context.Context, id string) {
		called.Store(true)
	}, 200*time.Millisecond)

	// Reset multiple times
	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond)
		if !lm.ResetTimed(id) {
			t.Errorf("Reset failed on iteration %d", i)
		}
	}

	// Timer should still not have fired
	time.Sleep(100 * time.Millisecond)
	if called.Load() {
		t.Error("Timer fired despite multiple resets")
	}

	// Let it fire
	time.Sleep(200 * time.Millisecond)
	if !called.Load() {
		t.Error("Timer should have fired after final reset")
	}
}

func TestLifetimeManager_StopBeforeTimer(t *testing.T) {
	lm := NewLifetimeManager()

	var called atomic.Bool
	lm.ScheduleTimed(context.Background(), "test-stop", func(ctx context.Context, id string) {
		called.Store(true)
	}, 100*time.Millisecond)

	// Stop immediately
	lm.StopAll()

	// Wait past when timer would have fired
	time.Sleep(150 * time.Millisecond)

	if called.Load() {
		t.Error("Timer should not fire after StopAll")
	}
	if lm.PendingCount() != 0 {
		t.Errorf("PendingCount should be 0 after StopAll, got %d", lm.PendingCount())
	}
}
