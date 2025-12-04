package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestShutdownDefaults verifies the default configuration values.
func TestShutdownDefaults(t *testing.T) {
	sm := NewShutdown()

	if sm.timeout != 30*time.Second {
		t.Fatalf("expected default timeout 30s, got %v", sm.timeout)
	}
	if sm.concurrent {
		t.Fatalf("expected default Concurrent=false")
	}
	if len(sm.signals) == 0 {
		t.Fatalf("expected default Signals must not be empty")
	}
	if sm.forceQuitTimeout != 0 {
		t.Fatalf("expected default ForceQuitTimeout=0, got %v", sm.forceQuitTimeout)
	}
}

// TestShutdownTimeout verifies that the global timeout cancels the context.
func TestShutdownTimeout(t *testing.T) {
	// Set a very short timeout
	sm := NewShutdown(ShutdownWithTimeout(50 * time.Millisecond))

	// Trigger shutdown to start the clock
	go sm.TriggerShutdown()

	select {
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout expected but did not occur")
	case <-sm.Done():
		// Success: shutdown finished (likely due to timeout cancelling the context)
	}
}

// TestShutdownForceQuit verifies that the force quit monitor cancels the context
// if the tasks take too long, even if the main timeout is infinite (0).
func TestShutdownForceQuit(t *testing.T) {
	sm := NewShutdown(
		ShutdownWithTimeout(0), // Infinite wait
		ShutdownWithForceQuit(20*time.Millisecond),
	)

	// Register a task that blocks longer than the force quit timeout
	// and respects context cancellation.
	sm.RegisterWithContext("blocker", func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err() // Should happen after ~20ms
		}
	})

	start := time.Now()
	sm.TriggerShutdown()
	duration := time.Since(start)

	if duration >= 1*time.Second {
		t.Fatal("Force quit failed; waited for full task duration")
	}

	stats := sm.GetStats()
	if stats.FailedEvents == 0 {
		t.Fatal("Expected task to fail due to context cancellation")
	}
}

// TestShutdownLIFO verifies that tasks run in Last-In-First-Out order (Sequential).
func TestShutdownLIFO(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(100 * time.Millisecond))

	var order []string
	var mu sync.Mutex

	// 1. Registered First
	_ = sm.Register(func() {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
	})

	// 2. Registered Second
	_ = sm.Register(func() {
		mu.Lock()
		order = append(order, "second")
		mu.Unlock()
	})

	sm.executeShutdown()

	// Should execute LIFO: Second, then First
	if len(order) != 2 || order[0] != "second" || order[1] != "first" {
		t.Fatalf("wrong order (expected LIFO): %v", order)
	}
}

// TestShutdownConcurrent verifies that tasks run in parallel.
func TestShutdownConcurrent(t *testing.T) {
	sm := NewShutdown(
		ShutdownWithTimeout(500*time.Millisecond),
		ShutdownConcurrent(),
	)

	var wg sync.WaitGroup
	wg.Add(3)

	// Register 3 tasks that sleep. If sequential, this would take 300ms.
	// If concurrent, it should take ~100ms.
	sleeper := func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
	}

	sm.Register(sleeper)
	sm.Register(sleeper)
	sm.Register(sleeper)

	start := time.Now()
	sm.executeShutdown()
	duration := time.Since(start)

	if duration > 250*time.Millisecond {
		t.Fatalf("Concurrent execution took too long: %v (expected ~100ms)", duration)
	}
}

// TestShutdownPanicRecovery verifies that a panic in a callback is caught and recorded as an error.
func TestShutdownPanicRecovery(t *testing.T) {
	sm := NewShutdown()

	_ = sm.Register(func() {
		panic("oops")
	})

	stats := sm.executeShutdown()

	if stats.FailedEvents != 1 {
		t.Fatalf("expected 1 failure, got %d", stats.FailedEvents)
	}
	if len(stats.Errors) != 1 {
		t.Fatal("expected error recorded")
	}
	// Verify it's a shutdown error wrapping the panic
	if stats.Errors[0].Error() == "" {
		t.Fatal("empty error message")
	}
}

// TestShutdownTypes verifies that Register accepts all supported types/interfaces.
func TestShutdownTypes(t *testing.T) {
	sm := NewShutdown()

	// 1. func()
	sm.Register(func() {})
	// 2. func() error (jack.Func)
	sm.Register(func() error { return nil })
	// 3. func(context.Context) error (jack.FuncCtx)
	sm.RegisterWithContext("ctx", func(ctx context.Context) error { return nil })
	// 4. io.Closer
	sm.Register(&fakeCloser{})
	// 5. Explicit jack.Func
	sm.Register(Func(func() error { return nil }))

	sm.executeShutdown()

	stats := sm.GetStats()
	if stats.TotalEvents != 5 {
		t.Fatalf("expected 5 events, got %d", stats.TotalEvents)
	}
	if stats.CompletedEvents != 5 {
		t.Fatalf("expected 5 completed events, got %d", stats.CompletedEvents)
	}
}

// TestShutdownSignal verifies that sending an OS signal triggers the shutdown.
func TestShutdownSignal(t *testing.T) {
	sm := NewShutdown()

	var ran atomic.Bool
	_ = sm.Register(func() {
		ran.Store(true)
	})

	// Send signal into the unexported channel directly to avoid relying on OS delivery timing
	go func() {
		sm.signalChan <- syscall.SIGTERM
	}()

	stats := sm.Wait()
	if stats == nil {
		t.Fatal("Wait returned nil stats")
	}
	if !ran.Load() {
		t.Fatal("callback never executed")
	}
}

// TestShutdownWaitChan verifies the async wait channel.
func TestShutdownWaitChan(t *testing.T) {
	sm := NewShutdown()
	_ = sm.Register(func() {})

	go func() { sm.signalChan <- syscall.SIGTERM }()

	stats := <-sm.WaitChan()
	if stats == nil {
		t.Fatal("WaitChan returned nil")
	}
}

// TestShutdownTrigger verifies programmatic triggering.
func TestShutdownTrigger(t *testing.T) {
	sm := NewShutdown()
	_ = sm.Register(func() {})

	stats := sm.TriggerShutdown()
	if stats == nil {
		t.Fatal("nil stats")
	}
	if !sm.IsShuttingDown() {
		t.Fatal("not shutting down")
	}
}

// TestShutdownDoubleExecution verifies calling it twice doesn't panic or re-run tasks.
func TestShutdownDoubleExecution(t *testing.T) {
	sm := NewShutdown()

	count := 0
	sm.Register(func() { count++ })

	// First run
	sm.executeShutdown()

	// Second run (simulate race or manual call)
	sm.executeShutdown()

	if count != 1 {
		t.Fatalf("Callback executed %d times, expected 1", count)
	}
}

// TestShutdownError verifies error unwrapping.
func TestShutdownError(t *testing.T) {
	inner := errors.New("boom")
	se := &ShutdownError{Name: "db", Err: inner, Timestamp: time.Now()}

	if se.Error() != "db: boom" {
		t.Fatalf("bad Error(): %q", se.Error())
	}
	if !errors.Is(se, inner) {
		t.Fatal("errors.Is failed")
	}
	if !errors.Is(inner, se.Unwrap()) {
		t.Fatal("Unwrap failed")
	}
}

// fakeCloser helper for io.Closer test
type fakeCloser struct{}

func (f *fakeCloser) Close() error { return nil }
