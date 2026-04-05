package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTask with atomic counter for thread-safe run counting.
type TestTask struct {
	mu       sync.Mutex
	runCount int64
	id       string
}

// Do implements Task by incrementing the run count.
func (t *TestTask) Do() error {
	atomic.AddInt64(&t.runCount, 1)
	return nil
}

// ID returns the task's identifier.
func (t *TestTask) ID() string {
	return t.id
}

// RunCount returns the number of times the task has run.
func (t *TestTask) RunCount() int64 {
	return atomic.LoadInt64(&t.runCount)
}

// TestTaskCtx with atomic counter.
type TestTaskCtx struct {
	mu       sync.Mutex
	runCount int64
	id       string
}

func (t *TestTaskCtx) Do(ctx context.Context) error {
	atomic.AddInt64(&t.runCount, 1)
	return nil
}

// ID returns the task's identifier.
func (t *TestTaskCtx) ID() string {
	return t.id
}

// RunCount returns the number of times the task has run.
func (t *TestTaskCtx) RunCount() int64 {
	return atomic.LoadInt64(&t.runCount)
}

// waitForCondition polls until the condition is true or times out.
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestNewScheduler verifies scheduler creation scenarios.
func TestNewScheduler(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pool := NewPool(1)
		defer pool.Shutdown(1 * time.Second)
		scheduler, err := NewScheduler("test", pool, Routine{Interval: time.Second})
		if err != nil || scheduler == nil {
			t.Fatal("NewScheduler() failed")
		}
	})

	t.Run("empty name", func(t *testing.T) {
		pool := NewPool(1)
		defer pool.Shutdown(1 * time.Second)
		_, err := NewScheduler("", pool, Routine{})
		if !errors.Is(err, ErrSchedulerNameMissing) {
			t.Fatalf("Expected %v, got %v", ErrSchedulerNameMissing, err)
		}
	})

	t.Run("nil pool", func(t *testing.T) {
		_, err := NewScheduler("test", nil, Routine{})
		if !errors.Is(err, ErrSchedulerPoolNil) {
			t.Fatalf("Expected %v, got %v", ErrSchedulerPoolNil, err)
		}
	})
}

// TestScheduler_MaxRuns_SingleTask checks max runs limit for a single task.
func TestScheduler_MaxRuns_SingleTask(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)
	schedule := Routine{Interval: 50 * time.Millisecond, MaxRuns: 3}
	scheduler, _ := NewScheduler("test-maxruns-single", pool, schedule)
	task := &TestTask{id: "task1"}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return task.RunCount() >= 3
	}, "Timed out waiting for task to run 3 times")

	// The scheduler is still "running" from its own perspective until explicitly stopped.
	if !scheduler.Running() {
		t.Error("Scheduler should still be running until explicitly stopped")
	}

	if err := scheduler.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	if scheduler.Running() {
		t.Error("Scheduler should not be running after Stop()")
	}
	if task.RunCount() != 3 {
		t.Errorf("Expected task to run 3 times, got %d", task.RunCount())
	}
}

func TestScheduler_MaxRuns_MultiTask(t *testing.T) {
	pool := NewPool(2, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)
	schedule := Routine{Interval: 50 * time.Millisecond, MaxRuns: 2}
	scheduler, _ := NewScheduler("test-maxruns-multi", pool, schedule)
	task1 := &TestTask{id: "task1"}
	task2 := &TestTask{id: "task2"}

	if err := scheduler.Do(task1, task2); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return task1.RunCount() >= 2 && task2.RunCount() >= 2
	}, "Timed out waiting for both tasks to run 2 times")

	if err := scheduler.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	if task1.RunCount() != 2 {
		t.Errorf("Task1 run count = %d, want 2", task1.RunCount())
	}
	if task2.RunCount() != 2 {
		t.Errorf("Task2 run count = %d, want 2", task2.RunCount())
	}
}

func TestScheduler_Stop(t *testing.T) {
	pool := NewPool(2, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)
	schedule := Routine{Interval: 50 * time.Millisecond}
	scheduler, _ := NewScheduler("test-stop", pool, schedule)
	task1 := &TestTask{id: "task1"}
	task2 := &TestTask{id: "task2"}

	if err := scheduler.Do(task1, task2); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return task1.RunCount() > 1 && task2.RunCount() > 1
	}, "Timed out waiting for tasks to run at least twice")

	initialRuns1 := task1.RunCount()
	initialRuns2 := task2.RunCount()

	if err := scheduler.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
	if scheduler.Running() {
		t.Error("Scheduler should not be running after Stop()")
	}

	time.Sleep(150 * time.Millisecond)

	if task1.RunCount() != initialRuns1 {
		t.Errorf("Task1 kept running after stop: got %d, want %d", task1.RunCount(), initialRuns1)
	}
	if task2.RunCount() != initialRuns2 {
		t.Errorf("Task2 kept running after stop: got %d, want %d", task2.RunCount(), initialRuns2)
	}
}

func TestScheduler_AlreadyRunning(t *testing.T) {
	pool := NewPool(1)
	defer pool.Shutdown(1 * time.Second)
	scheduler, _ := NewScheduler("test", pool, Routine{Interval: time.Second})

	err := scheduler.Do(&TestTask{id: "task1"})
	if err != nil {
		t.Fatalf("First Do() failed: %v", err)
	}

	err = scheduler.Do(&TestTask{id: "task2"})
	if !errors.Is(err, ErrSchedulerJobAlreadyRunning) {
		t.Fatalf("Expected %v, got %v", ErrSchedulerJobAlreadyRunning, err)
	}
	scheduler.Stop()
}

func TestScheduler_StopNotRunning(t *testing.T) {
	pool := NewPool(1)
	defer pool.Shutdown(1 * time.Second)
	scheduler, _ := NewScheduler("test", pool, Routine{})

	err := scheduler.Stop()
	if !errors.Is(err, ErrSchedulerNotRunning) {
		t.Fatalf("Expected %v, got %v", ErrSchedulerNotRunning, err)
	}
}

func TestScheduler_DoCtx_ContextCancellation(t *testing.T) {
	pool := NewPool(2, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)
	schedule := Routine{Interval: 50 * time.Millisecond}
	scheduler, _ := NewScheduler("test-ctx-cancel", pool, schedule)
	task1 := &TestTaskCtx{id: "task1"}
	task2 := &TestTaskCtx{id: "task2"}

	ctx, cancel := context.WithCancel(context.Background())
	err := scheduler.DoCtx(ctx, task1, task2)
	if err != nil {
		t.Fatalf("DoCtx() failed: %v", err)
	}

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return task1.RunCount() > 1 && task2.RunCount() > 1
	}, "Timed out waiting for tasks to run at least twice")

	cancel()

	time.Sleep(150 * time.Millisecond)
	runCount1AfterCancel := task1.RunCount()
	runCount2AfterCancel := task2.RunCount()

	time.Sleep(150 * time.Millisecond)

	if task1.RunCount() != runCount1AfterCancel {
		t.Errorf("Task1 kept running after its context was cancelled")
	}
	if task2.RunCount() != runCount2AfterCancel {
		t.Errorf("Task2 kept running after its context was cancelled")
	}

	scheduler.Stop()
}

// CRON-SPECIFIC TESTS

func TestScheduler_Cron_Basic(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "*/1 * * * * *"} // Every second using 6-field format
	scheduler, _ := NewScheduler("test-cron-every", pool, schedule)

	task := &TestTask{id: "cron-task"}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	// Wait for at least 2 executions (2+ seconds)
	time.Sleep(2100 * time.Millisecond)

	if task.RunCount() < 2 {
		t.Errorf("Expected at least 2 cron executions, got %d", task.RunCount())
	}

	if err := scheduler.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	finalCount := task.RunCount()
	time.Sleep(200 * time.Millisecond)
	if task.RunCount() != finalCount {
		t.Errorf("Task continued running after Stop()")
	}
}

func TestScheduler_Cron_MultipleTasks(t *testing.T) {
	pool := NewPool(2, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "*/1 * * * * *"} // Every second
	scheduler, _ := NewScheduler("test-cron-multi", pool, schedule)

	task1 := &TestTask{id: "cron-task-1"}
	task2 := &TestTask{id: "cron-task-2"}

	if err := scheduler.Do(task1, task2); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	// Wait for at least 2 executions
	time.Sleep(2100 * time.Millisecond)

	if task1.RunCount() < 2 {
		t.Errorf("Task1 expected at least 2 executions, got %d", task1.RunCount())
	}
	if task2.RunCount() < 2 {
		t.Errorf("Task2 expected at least 2 executions, got %d", task2.RunCount())
	}

	scheduler.Stop()
}

func TestScheduler_Cron_DoCtx(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "*/1 * * * * *"} // Every second
	scheduler, _ := NewScheduler("test-cron-ctx", pool, schedule)

	task := &TestTaskCtx{id: "cron-ctx-task"}
	ctx := context.WithValue(context.Background(), "test-key", "test-value")

	if err := scheduler.DoCtx(ctx, task); err != nil {
		t.Fatalf("DoCtx() failed: %v", err)
	}

	// Wait for at least 1 execution
	time.Sleep(1100 * time.Millisecond)

	if task.RunCount() < 1 {
		t.Errorf("Expected at least 1 execution, got %d", task.RunCount())
	}

	scheduler.Stop()
}

func TestScheduler_Cron_InvalidExpression(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "invalid cron expression"}
	scheduler, _ := NewScheduler("test-cron-invalid", pool, schedule)

	task := &TestTask{id: "cron-task"}

	err := scheduler.Do(task)
	if err == nil {
		t.Fatal("Expected error for invalid cron expression, got nil")
	}
}

func TestScheduler_Cron_Entries(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "0 * * * * *"} // Every minute using 6-field
	scheduler, _ := NewScheduler("test-cron-entries", pool, schedule)

	task := &TestTask{id: "cron-task"}

	entries := scheduler.Entries()
	if entries != nil {
		t.Error("Expected nil entries before start")
	}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	entries = scheduler.Entries()
	if len(entries) == 0 {
		t.Error("Expected non-empty entries after start")
	}

	nextRun, ok := scheduler.NextRun()
	if !ok {
		t.Error("Expected valid NextRun")
	}
	if nextRun.IsZero() {
		t.Error("NextRun should not be zero")
	}

	scheduler.Stop()
}

func TestScheduler_Cron_NextRun(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "*/5 * * * * *"} // Every 5 seconds
	scheduler, _ := NewScheduler("test-cron-nextrun", pool, schedule)

	task := &TestTask{id: "cron-task"}

	_, ok := scheduler.NextRun()
	if ok {
		t.Error("NextRun should return false before start")
	}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	nextRun, ok := scheduler.NextRun()
	if !ok {
		t.Fatal("NextRun should return true after start")
	}

	// Next run should be within next 5 seconds
	expectedMax := time.Now().Add(5 * time.Second)
	if nextRun.After(expectedMax) {
		t.Errorf("NextRun too far in future: got %v, expected before %v", nextRun, expectedMax)
	}

	scheduler.Stop()
}

func TestScheduler_Cron_Stop(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{Cron: "*/1 * * * * *"} // Every second
	scheduler, _ := NewScheduler("test-cron-stop", pool, schedule)

	task := &TestTask{id: "cron-task"}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)
	countBefore := task.RunCount()

	if err := scheduler.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)
	countAfter := task.RunCount()

	if countAfter != countBefore {
		t.Errorf("Task continued running after Stop(): before=%d, after=%d", countBefore, countAfter)
	}
}

func TestScheduler_Cron_TerminateWithPool(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))

	schedule := Routine{Cron: "*/1 * * * * *"} // Every second
	scheduler, _ := NewScheduler("test-cron-terminate", pool, schedule)

	task := &TestTask{id: "cron-task"}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	if err := scheduler.Terminate(true); err != nil {
		t.Fatalf("Terminate() failed: %v", err)
	}

	if scheduler.Running() {
		t.Error("Scheduler should not be running after Terminate()")
	}
}

func TestScheduler_Cron_Precedence(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	schedule := Routine{
		Cron:     "*/1 * * * * *", // Every second
		Interval: 500 * time.Millisecond,
	}
	scheduler, _ := NewScheduler("test-cron-precedence", pool, schedule)

	task := &TestTask{id: "cron-task"}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	// Wait for at least 2 executions (2+ seconds)
	time.Sleep(2100 * time.Millisecond)

	if task.RunCount() < 2 {
		t.Errorf("Cron should take precedence: expected >= 2 runs, got %d", task.RunCount())
	}

	scheduler.Stop()
}

func TestScheduler_Cron_WithObservable(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)

	var eventCount int64
	obs := NewObservable[Schedule](1)
	defer obs.Shutdown()

	obs.Add(ObserverFunc[Schedule](func(s Schedule) {
		if s.Type == "task_submitted" {
			atomic.AddInt64(&eventCount, 1)
		}
	}))

	schedule := Routine{Cron: "*/1 * * * * *"} // Every second
	scheduler, _ := NewScheduler("test-cron-obs", pool, schedule, SchedulingWithObservable(obs))

	task := &TestTask{id: "cron-obs-task"}

	if err := scheduler.Do(task); err != nil {
		t.Fatalf("Do() failed: %v", err)
	}

	// Wait for at least 2 executions
	time.Sleep(2100 * time.Millisecond)

	if atomic.LoadInt64(&eventCount) < 2 {
		t.Errorf("Expected at least 2 task_submitted events, got %d", eventCount)
	}

	scheduler.Stop()
}

func TestScheduler_Cron_CommonExpressions(t *testing.T) {
	tests := []struct {
		name    string
		cron    string
		wantErr bool
	}{
		{"every second", "*/1 * * * * *", false}, // 6-field with seconds
		{"every minute", "0 * * * * *", false},   // 6-field: at minute boundary
		{"hourly", "0 0 * * * *", false},         // 6-field: at hour boundary
		{"daily", "0 0 0 * * *", false},          // 6-field: at midnight
		{"weekly", "0 0 0 * * 0", false},         // 6-field: Sunday midnight
		{"monthly", "0 0 0 1 * *", false},        // 6-field: 1st of month
		{"@every", "@every 100ms", false},        // @every descriptor
		{"@hourly", "@hourly", false},            // @hourly descriptor
		{"@daily", "@daily", false},              // @daily descriptor
		{"@weekly", "@weekly", false},            // @weekly descriptor
		{"@monthly", "@monthly", false},          // @monthly descriptor
		{"invalid", "invalid", true},             // Invalid expression
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewPool(1, PoolingWithQueueSize(10))
			defer pool.Shutdown(1 * time.Second)

			schedule := Routine{Cron: tt.cron}
			scheduler, err := NewScheduler("test-cron-"+tt.name, pool, schedule)
			if err != nil {
				t.Fatalf("NewScheduler failed: %v", err)
			}

			task := &TestTask{id: "cron-task"}
			err = scheduler.Do(task)

			if tt.wantErr && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if err == nil {
				scheduler.Stop()
			}
		})
	}
}
