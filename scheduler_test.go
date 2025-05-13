// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestTask is a test implementation of the Task interface for scheduler testing.
// It tracks execution count and supports optional failure.
// Thread-safe via mutex.
type TestTask struct {
	mu         sync.Mutex // Protects runCount
	runCount   int        // Number of times Do is called
	shouldFail bool       // Whether to return an error
	id         string     // Task identifier
}

// Do executes the test task, incrementing the run count and returning an error if shouldFail is true.
// Thread-safe via mutex.
func (t *TestTask) Do() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.runCount++
	if t.shouldFail {
		return errors.New("task failed")
	}
	return nil
}

// ID returns the test task’s identifier.
func (t *TestTask) ID() string {
	return t.id
}

// TestTaskCtx is a test implementation of the TaskCtx interface for scheduler testing.
// It tracks execution count and supports optional failure with context awareness.
// Thread-safe via mutex.
type TestTaskCtx struct {
	mu         sync.Mutex // Protects runCount
	runCount   int        // Number of times Do is called
	shouldFail bool       // Whether to return an error
	id         string     // Task identifier
}

// Do executes the test task with the given context, incrementing the run count and returning an error if shouldFail is true.
// Thread-safe via mutex.
func (t *TestTaskCtx) Do(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.runCount++
	if t.shouldFail {
		return errors.New("task failed")
	}
	return nil
}

// ID returns the test task’s identifier.
func (t *TestTaskCtx) ID() string {
	return t.id
}

// TestNewScheduler verifies the NewScheduler function’s behavior.
// It tests successful creation, empty name errors, and nil pool errors.
func TestNewScheduler(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pool := NewPool(1)
		defer pool.Shutdown(5 * time.Second)
		scheduler, err := NewScheduler("test", pool, Routine{Interval: time.Second})
		if err != nil {
			t.Fatalf("NewScheduler() error = %v, want nil", err)
		}
		if scheduler == nil {
			t.Fatal("NewScheduler() returned nil")
		}
	})

	t.Run("empty name", func(t *testing.T) {
		pool := NewPool(1)
		defer pool.Shutdown(5 * time.Second)
		_, err := NewScheduler("", pool, Routine{Interval: time.Second})
		if !errors.Is(err, ErrSchedulerNameMissing) {
			t.Fatalf("NewScheduler() error = %v, want %v", err, ErrSchedulerNameMissing)
		}
	})

	t.Run("nil pool", func(t *testing.T) {
		_, err := NewScheduler("test", nil, Routine{Interval: time.Second})
		if !errors.Is(err, ErrSchedulerPoolNil) {
			t.Fatalf("NewScheduler() error = %v, want %v", err, ErrSchedulerPoolNil)
		}
	})
}

// TestScheduler_Do verifies the Do method of Scheduler.
// It tests task execution with a limited number of runs.
func TestScheduler_Do(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 100 * time.Millisecond, MaxRuns: 3}
	scheduler, err := NewScheduler("test", pool, schedule)
	if err != nil {
		t.Fatalf("NewScheduler() error = %v, want nil", err)
	}
	task := &TestTask{id: "test-task"}

	t.Log("Starting scheduler.Do")
	err = scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	t.Log("Waiting for scheduler to complete")
	for i := 0; i < 50; i++ {
		if !scheduler.Running() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if scheduler.Running() {
		t.Fatal("scheduler should have stopped after MaxRuns")
	}

	t.Logf("Task run count: %d", task.runCount)
	if task.runCount != 3 {
		t.Errorf("task run count = %d, want %d", task.runCount, 3)
	}
}

// TestScheduler_DoCtx verifies the DoCtx method of Scheduler.
// It tests context-aware task execution with a limited number of runs.
func TestScheduler_DoCtx(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 100 * time.Millisecond, MaxRuns: 3}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTaskCtx{id: "test-task-ctx"}

	t.Log("Starting scheduler.DoCtx")
	ctx := context.Background()
	err := scheduler.DoCtx(ctx, task)
	if err != nil {
		t.Fatalf("DoCtx() error = %v, want nil", err)
	}

	t.Log("Waiting for scheduler to complete")
	for i := 0; i < 50; i++ {
		if !scheduler.Running() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if scheduler.Running() {
		t.Fatal("scheduler should have stopped after MaxRuns")
	}

	t.Logf("Task run count: %d", task.runCount)
	if task.runCount != 3 {
		t.Errorf("task run count = %d, want %d", task.runCount, 3)
	}
}

// TestScheduler_Stop verifies the Stop method of Scheduler.
// It ensures tasks stop executing after Stop is called.
func TestScheduler_Stop(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 100 * time.Millisecond}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTask{id: "test-task-stop"}

	t.Log("Starting scheduler.Do")
	err := scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	time.Sleep(250 * time.Millisecond)

	initialRuns := task.runCount
	if initialRuns == 0 {
		t.Fatal("task should have run at least once")
	}

	t.Log("Stopping scheduler")
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Stop() error = %v, want nil", err)
	}

	time.Sleep(100 * time.Millisecond)

	finalRuns := task.runCount
	if finalRuns != initialRuns {
		t.Errorf("task continued running after stop, final count = %d, want %d", finalRuns, initialRuns)
	}
}

// TestScheduler_ImmediateFirstRun verifies that Scheduler executes the first task immediately.
// It checks for at least one run shortly after starting.
func TestScheduler_ImmediateFirstRun(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 200 * time.Millisecond}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTask{id: "test-task-immediate"}

	t.Log("Starting scheduler.Do")
	err := scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	time.Sleep(50 * time.Millisecond)

	if task.runCount < 1 {
		t.Error("task should have run immediately")
	}

	t.Log("Stopping scheduler")
	scheduler.Stop()
}

// TestScheduler_MaxRuns verifies that Scheduler respects the MaxRuns limit.
// It ensures the task runs exactly the specified number of times.
func TestScheduler_MaxRuns(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 100 * time.Millisecond, MaxRuns: 2}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTask{id: "test-task-maxruns"}

	t.Log("Starting scheduler.Do")
	err := scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	t.Log("Waiting for scheduler to complete")
	for i := 0; i < 50; i++ {
		if !scheduler.Running() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if scheduler.Running() {
		t.Fatal("scheduler should have stopped after MaxRuns")
	}

	t.Logf("Task run count: %d", task.runCount)
	if task.runCount != 2 {
		t.Errorf("task run count = %d, want %d", task.runCount, 2)
	}
}

// TestScheduler_NonIntervalMaxRuns verifies Scheduler behavior with no interval and MaxRuns set.
// It ensures all runs occur quickly without delays.
func TestScheduler_NonIntervalMaxRuns(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{MaxRuns: 3}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTask{id: "test-task-noninterval"}

	t.Log("Starting scheduler.Do")
	err := scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	time.Sleep(100 * time.Millisecond)

	t.Logf("Task run count: %d", task.runCount)
	if task.runCount != 3 {
		t.Errorf("task run count = %d, want %d", task.runCount, 3)
	}
}

// TestScheduler_AlreadyRunning verifies that Scheduler rejects new tasks while running.
// It checks for the correct error when attempting to start a second task.
func TestScheduler_AlreadyRunning(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: time.Second}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTask{id: "test-task-already-running"}

	t.Log("Starting scheduler.Do")
	err := scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	err = scheduler.Do(task)
	if err != ErrSchedulerJobAlreadyRunning {
		t.Fatalf("Do() error = %v, want %v", err, ErrSchedulerJobAlreadyRunning)
	}

	t.Log("Stopping scheduler")
	scheduler.Stop()
}

// TestScheduler_StopNotRunning verifies that Stop fails when the Scheduler is not running.
// It checks for the correct error in the idle state.
func TestScheduler_StopNotRunning(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: time.Second}
	scheduler, _ := NewScheduler("test", pool, schedule)

	err := scheduler.Stop()
	if err != ErrSchedulerNotRunning {
		t.Fatalf("Stop() error = %v, want %v", err, ErrSchedulerNotRunning)
	}
}

// TestScheduler_ContextCancellation verifies that Scheduler respects context cancellation.
// It ensures tasks stop after the context is cancelled.
func TestScheduler_ContextCancellation(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 100 * time.Millisecond}
	scheduler, _ := NewScheduler("test", pool, schedule)
	task := &TestTaskCtx{id: "test-task-ctx-cancel"}

	ctx, cancel := context.WithCancel(context.Background())
	t.Log("Starting scheduler.DoCtx")
	err := scheduler.DoCtx(ctx, task)
	if err != nil {
		t.Fatalf("DoCtx() error = %v, want nil", err)
	}

	time.Sleep(150 * time.Millisecond)

	initialRuns := task.runCount
	if initialRuns == 0 {
		t.Fatal("task should have run at least once")
	}

	t.Log("Cancelling context")
	cancel()
	t.Log("Stopping scheduler")
	scheduler.Stop()

	time.Sleep(50 * time.Millisecond)

	finalRuns := task.runCount
	if finalRuns > initialRuns+1 {
		t.Errorf("task continued running after context cancel, final count = %d, want <= %d", finalRuns, initialRuns+1)
	}
}

// TestScheduler_ObservableEvents verifies that Scheduler emits expected observable events.
// It checks for started, tick, and run limit events during execution.
func TestScheduler_ObservableEvents(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10), PoolingWithObservable(NewObservable[Event]()))
	defer pool.Shutdown(5 * time.Second)
	schedule := Routine{Interval: 100 * time.Millisecond, MaxRuns: 2}
	mockObs := newMockSchedulerObserver()
	mockObs.Add(mockObs)
	scheduler, _ := NewScheduler("test", pool, schedule, SchedulingWithObservable(mockObs))
	task := &TestTask{id: "test-task-events"}

	t.Log("Starting scheduler.Do")
	err := scheduler.Do(task)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	t.Log("Collecting events")
	var events []Schedule
eventLoop:
	for i := 0; i < 50; i++ {
		select {
		case event := <-mockObs.events:
			events = append(events, event)
			if event.Type == "run_limit_reached" {
				break eventLoop
			}
		case <-time.After(100 * time.Millisecond):
			if !scheduler.Running() {
				break eventLoop
			}
		}
	}

	if len(events) == 0 {
		t.Fatal("no events were received")
	}

	foundStarted := false
	foundTick := false
	foundLimit := false
	for _, event := range events {
		switch event.Type {
		case "started":
			foundStarted = true
		case "tick_triggered":
			foundTick = true
		case "run_limit_reached":
			foundLimit = true
		}
	}

	if !foundStarted {
		t.Error("missing 'started' event")
	}
	if !foundTick {
		t.Error("missing 'tick_triggered' event")
	}
	if !foundLimit {
		t.Error("missing 'run_limit_reached' event")
	}

	mockObs.Shutdown()
}

// mockSchedulerObserver is a test implementation of the Observable interface for Schedule events.
// It collects events in a channel for verification.
// Thread-safe via mutex and channel operations.
type mockSchedulerObserver struct {
	events chan Schedule        // Channel for received events
	mu     sync.Mutex           // Protects obs list
	obs    []Observer[Schedule] // Registered observers
}

// newMockSchedulerObserver creates a new mock observer for scheduler events.
// Thread-safe via initialization.
func newMockSchedulerObserver() *mockSchedulerObserver {
	return &mockSchedulerObserver{
		events: make(chan Schedule, 100),
		obs:    make([]Observer[Schedule], 0),
	}
}

// Add registers one or more observers to receive Schedule events.
// Thread-safe via mutex.
func (m *mockSchedulerObserver) Add(observers ...Observer[Schedule]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, observer := range observers {
		m.obs = append(m.obs, observer)
	}
}

// Remove unregisters one or more observers from receiving Schedule events.
// It removes the first occurrence of each observer.
// Thread-safe via mutex.
func (m *mockSchedulerObserver) Remove(observers ...Observer[Schedule]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, observer := range observers {
		for i, obs := range m.obs {
			if obs == observer {
				m.obs = append(m.obs[:i], m.obs[i+1:]...)
				return
			}
		}
	}
}

// Notify sends one or more Schedule events to all registered observers.
// Thread-safe via mutex and observer iteration.
func (m *mockSchedulerObserver) Notify(events ...Schedule) {
	m.mu.Lock()
	observers := make([]Observer[Schedule], len(m.obs))
	copy(observers, m.obs)
	m.mu.Unlock()
	for _, event := range events {
		for _, obs := range observers {
			obs.OnNotify(event)
		}
	}
}

// OnNotify receives a Schedule event and forwards it to the events channel.
// Thread-safe via non-blocking channel send.
func (m *mockSchedulerObserver) OnNotify(event Schedule) {
	select {
	case m.events <- event:
	default:
	}
}

// Shutdown closes the events channel to stop event collection.
// Thread-safe via channel close.
func (m *mockSchedulerObserver) Shutdown() {
	close(m.events)
}
