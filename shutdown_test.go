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
	sm := NewShutdown(ShutdownWithTimeout(50 * time.Millisecond))

	go sm.TriggerShutdown()

	select {
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout expected but did not occur")
	case <-sm.Done():
	}
}

// TestShutdownForceQuit verifies that the force quit monitor cancels the context
// if the tasks take too long, even if the main timeout is infinite (0).
func TestShutdownForceQuit(t *testing.T) {
	sm := NewShutdown(
		ShutdownWithTimeout(0),
		ShutdownWithForceQuit(20*time.Millisecond),
	)

	sm.RegisterWithContext("blocker", func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
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

	_ = sm.Register(func() {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
	})

	_ = sm.Register(func() {
		mu.Lock()
		order = append(order, "second")
		mu.Unlock()
	})

	sm.executeShutdown()

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
	if stats.Errors[0].Error() == "" {
		t.Fatal("empty error message")
	}
}

// TestShutdownTypes verifies that Register accepts all supported types/interfaces.
func TestShutdownTypes(t *testing.T) {
	sm := NewShutdown()

	sm.Register(func() {})
	sm.Register(func() error { return nil })
	sm.RegisterWithContext("ctx", func(ctx context.Context) error { return nil })
	sm.Register(&fakeCloser{})
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

	sm.executeShutdown()
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

// TestRegisterWithPriorityBasic verifies basic priority ordering works correctly.
// Priority 0 executes first, then 1, then 2 (ascending).
// Within each priority: LIFO (last registered in that group runs first).
func TestRegisterWithPriorityBasic(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(500 * time.Millisecond))

	var order []string
	var mu sync.Mutex

	_ = sm.RegisterWithPriority("p2", 2,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "p2a")
			mu.Unlock()
			return nil
		},
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "p2b")
			mu.Unlock()
			return nil
		},
	)

	_ = sm.RegisterWithPriority("p0", 0,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "p0a")
			mu.Unlock()
			return nil
		},
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "p0b")
			mu.Unlock()
			return nil
		},
	)

	_ = sm.RegisterWithPriority("p1", 1,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "p1")
			mu.Unlock()
			return nil
		},
	)

	sm.executeShutdown()

	expected := []string{"p0b", "p0a", "p1", "p2b", "p2a"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %s, got %s\nFull order: %v", i, v, order[i], order)
		}
	}
}

// TestRegisterWithPriorityLIFO verifies LIFO ordering within same priority.
func TestRegisterWithPriorityLIFO(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(100 * time.Millisecond))

	var order []string
	var mu sync.Mutex

	sm.RegisterWithPriority("group", 0,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "first-registered")
			mu.Unlock()
			return nil
		},
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "second-registered")
			mu.Unlock()
			return nil
		},
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "third-registered")
			mu.Unlock()
			return nil
		},
	)

	sm.executeShutdown()

	expected := []string{"third-registered", "second-registered", "first-registered"}
	if len(order) != 3 {
		t.Fatalf("expected 3 items, got %d: %v", len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("LIFO failed at position %d: expected %s, got %s", i, v, order[i])
		}
	}
}

// TestRegisterWithPriorityMultipleGroups verifies multiple groups at same priority merge correctly.
func TestRegisterWithPriorityMultipleGroups(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(100 * time.Millisecond))

	var order []string
	var mu sync.Mutex

	sm.RegisterWithPriority("group-a", 0,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "a1")
			mu.Unlock()
			return nil
		},
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "a2")
			mu.Unlock()
			return nil
		},
	)

	sm.RegisterWithPriority("group-b", 0,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "b1")
			mu.Unlock()
			return nil
		},
	)

	sm.executeShutdown()

	expected := []string{"b1", "a2", "a1"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %s, got %s\nFull order: %v", i, v, order[i], order)
		}
	}
}

// TestRegisterWithPriorityMixedWithNonPriority verifies priority and non-priority tasks coexist.
func TestRegisterWithPriorityMixedWithNonPriority(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(500 * time.Millisecond))

	var order []string
	var mu sync.Mutex

	_ = sm.Register(func() {
		mu.Lock()
		order = append(order, "non-priority-1")
		mu.Unlock()
	})
	_ = sm.Register(func() {
		mu.Lock()
		order = append(order, "non-priority-2")
		mu.Unlock()
	})

	_ = sm.RegisterWithPriority("critical", 0,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "critical")
			mu.Unlock()
			return nil
		},
	)

	_ = sm.RegisterWithPriority("normal", 1,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "normal")
			mu.Unlock()
			return nil
		},
	)

	sm.executeShutdown()

	expected := []string{"critical", "normal", "non-priority-2", "non-priority-1"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %s, got %s\nFull order: %v", i, v, order[i], order)
		}
	}
}

// TestRegisterWithPriorityNilFunction verifies nil functions are rejected.
func TestRegisterWithPriorityNilFunction(t *testing.T) {
	sm := NewShutdown()

	validFn := func(ctx context.Context) error { return nil }
	nilFn := FuncCtx(nil)

	err := sm.RegisterWithPriority("test", 0, validFn, nilFn, validFn)
	if err == nil {
		t.Fatal("expected error for nil function")
	}
	if err.Error() != "callback cannot be nil" {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sm.RegisterWithPriority("empty", 0)
	if err == nil {
		t.Fatal("expected error for empty functions")
	}
	if err.Error() != "at least one function required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRegisterWithPriorityAfterShutdown verifies registration after shutdown is rejected.
func TestRegisterWithPriorityAfterShutdown(t *testing.T) {
	sm := NewShutdown()
	sm.TriggerShutdown()

	err := sm.RegisterWithPriority("late", 0, func(ctx context.Context) error { return nil })
	if err == nil {
		t.Fatal("expected error when registering after shutdown")
	}
	if err.Error() != "cannot register after shutdown started" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRegisterWithPriorityPanicRecovery verifies panics in priority functions are caught.
func TestRegisterWithPriorityPanicRecovery(t *testing.T) {
	sm := NewShutdown()

	_ = sm.RegisterWithPriority("panicky", 0,
		func(ctx context.Context) error {
			panic("priority panic!")
		},
	)

	stats := sm.executeShutdown()

	if stats.FailedEvents != 1 {
		t.Fatalf("expected 1 failure, got %d", stats.FailedEvents)
	}
	if len(stats.Errors) != 1 {
		t.Fatal("expected 1 error recorded")
	}

	var se *ShutdownError
	if !errors.As(stats.Errors[0], &se) {
		t.Fatal("error should be ShutdownError")
	}
	if se.Name != "panicky" {
		t.Fatalf("expected error name 'panicky', got %q", se.Name)
	}
}

// TestRegisterWithPriorityConcurrent verifies that within each priority wave,
// tasks run concurrently — but waves themselves are sequential.
//
// Wave semantics with ShutdownConcurrent():
//   - Priority 0 wave: all p0 tasks start simultaneously, wait for all to finish
//   - Priority 1 wave: all p1 tasks start simultaneously, wait for all to finish
//   - Regular tasks: run last
//
// Total time = slowest p0 task + slowest p1 task (not all tasks in parallel).
// This is by design — it ensures Listeners drain before TrafficManager closes,
// even when concurrent execution is configured.
func TestRegisterWithPriorityConcurrent(t *testing.T) {
	sm := NewShutdown(
		ShutdownWithTimeout(2*time.Second),
		ShutdownConcurrent(),
	)

	var mu sync.Mutex
	completedAt := make(map[string]time.Time)

	makeTask := func(name string, delay time.Duration) FuncCtx {
		return func(ctx context.Context) error {
			time.Sleep(delay)
			mu.Lock()
			completedAt[name] = time.Now()
			mu.Unlock()
			return nil
		}
	}

	// Priority 0 wave: two tasks running concurrently — wave takes max(50ms, 150ms) = 150ms
	sm.RegisterWithPriority("p0", 0,
		makeTask("p0-fast", 50*time.Millisecond),
		makeTask("p0-slow", 150*time.Millisecond),
	)

	// Priority 1 wave: starts only after p0 wave completes — takes 100ms
	sm.RegisterWithPriority("p1", 1,
		makeTask("p1-task", 100*time.Millisecond),
	)

	start := time.Now()
	sm.executeShutdown()
	totalDuration := time.Since(start)

	// All tasks should have completed.
	if len(completedAt) != 3 {
		t.Fatalf("expected 3 completed tasks, got %d: %v", len(completedAt), completedAt)
	}

	// p0-fast and p0-slow ran concurrently — p0-slow finished ~150ms after start.
	// p1-task started only after p0-slow finished — completed ~250ms after start.
	// Total should be ~250ms (150ms p0 wave + 100ms p1 wave).
	if totalDuration > 500*time.Millisecond {
		t.Fatalf("total duration too long: %v (expected ~250ms)", totalDuration)
	}

	// Key correctness assertion: p1 must have started AFTER p0-slow completed.
	// This is the wave guarantee — p1 task completion must be >= p0-slow completion + 100ms.
	mu.Lock()
	p0SlowDone := completedAt["p0-slow"]
	p1Done := completedAt["p1-task"]
	mu.Unlock()

	if p1Done.Before(p0SlowDone) || p1Done.Equal(p0SlowDone) {
		t.Fatalf("p1-task completed (%v) before or at same time as p0-slow (%v) — wave ordering not enforced",
			p1Done.Format(time.RFC3339Nano), p0SlowDone.Format(time.RFC3339Nano))
	}

	// p1-task should complete approximately 100ms after p0-slow.
	gap := p1Done.Sub(p0SlowDone)
	if gap < 80*time.Millisecond {
		t.Fatalf("p1-task completed too quickly after p0-slow (%v) — p1 may have started before p0 finished", gap)
	}
}

// TestRegisterWithPriorityWavesAreSequential is the definitive ordering test.
// It verifies that no task in wave N+1 begins before all tasks in wave N complete,
// even with ShutdownConcurrent() enabled.
func TestRegisterWithPriorityWavesAreSequential(t *testing.T) {
	sm := NewShutdown(
		ShutdownWithTimeout(2*time.Second),
		ShutdownConcurrent(),
	)

	var mu sync.Mutex
	var p0Running bool
	p1StartedWhileP0Running := false

	// p0 task: runs for 100ms
	sm.RegisterWithPriority("p0", 0, func(ctx context.Context) error {
		mu.Lock()
		p0Running = true
		mu.Unlock()

		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		p0Running = false
		mu.Unlock()
		return nil
	})

	// p1 task: checks whether p0 is still running when it starts
	sm.RegisterWithPriority("p1", 1, func(ctx context.Context) error {
		mu.Lock()
		if p0Running {
			p1StartedWhileP0Running = true
		}
		mu.Unlock()

		time.Sleep(50 * time.Millisecond)
		return nil
	})

	sm.executeShutdown()

	if p1StartedWhileP0Running {
		t.Fatal("WAVE ORDERING VIOLATED: p1 task started while p0 task was still running — waves must be sequential")
	}
}

// TestRegisterWithPriorityNegativePriority verifies negative priorities work.
func TestRegisterWithPriorityNegativePriority(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(100 * time.Millisecond))

	var order []string
	var mu sync.Mutex

	_ = sm.RegisterWithPriority("neg", -1,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "negative")
			mu.Unlock()
			return nil
		},
	)

	_ = sm.RegisterWithPriority("zero", 0,
		func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "zero")
			mu.Unlock()
			return nil
		},
	)

	sm.executeShutdown()

	expected := []string{"negative", "zero"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %s, got %s", i, v, order[i])
		}
	}
}

// TestRegisterWithPrioritySingleFunction tests registering a single function per call.
func TestRegisterWithPrioritySingleFunction(t *testing.T) {
	sm := NewShutdown(ShutdownWithTimeout(100 * time.Millisecond))

	var ran bool
	_ = sm.RegisterWithPriority("single", 42,
		func(ctx context.Context) error {
			ran = true
			return nil
		},
	)

	stats := sm.executeShutdown()

	if !ran {
		t.Fatal("single function did not execute")
	}
	if stats.CompletedEvents != 1 {
		t.Fatalf("expected 1 completed event, got %d", stats.CompletedEvents)
	}
}

// fakeCloser helper for io.Closer test
type fakeCloser struct{}

func (f *fakeCloser) Close() error { return nil }
