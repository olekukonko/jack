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

// Do implements TaskCtx by incrementing the run count.
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

// TestScheduler_MaxRuns_MultiTask verifies max runs for multiple tasks.
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

// TestScheduler_Stop ensures tasks stop after scheduler termination.
func TestScheduler_Stop(t *testing.T) {
	pool := NewPool(2, PoolingWithQueueSize(10))
	defer pool.Shutdown(1 * time.Second)
	// Run "forever" until stopped
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

	// Wait a bit more to ensure tasks did not continue running
	time.Sleep(150 * time.Millisecond)

	if task1.RunCount() != initialRuns1 {
		t.Errorf("Task1 kept running after stop: got %d, want %d", task1.RunCount(), initialRuns1)
	}
	if task2.RunCount() != initialRuns2 {
		t.Errorf("Task2 kept running after stop: got %d, want %d", task2.RunCount(), initialRuns2)
	}
}

// TestScheduler_AlreadyRunning checks error on duplicate start.
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

// TestScheduler_StopNotRunning verifies error when stopping inactive scheduler.
func TestScheduler_StopNotRunning(t *testing.T) {
	pool := NewPool(1)
	defer pool.Shutdown(1 * time.Second)
	scheduler, _ := NewScheduler("test", pool, Routine{})

	err := scheduler.Stop()
	if !errors.Is(err, ErrSchedulerNotRunning) {
		t.Fatalf("Expected %v, got %v", ErrSchedulerNotRunning, err)
	}
}

// TestScheduler_DoCtx_ContextCancellation tests task halt on context cancel.
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

	// Cancel the context provided to the tasks
	cancel()

	// Wait a bit to see if tasks are still running. They should not be submitted anymore.
	time.Sleep(150 * time.Millisecond)
	runCount1AfterCancel := task1.RunCount()
	runCount2AfterCancel := task2.RunCount()

	// Wait another interval. The run count should not increase.
	time.Sleep(150 * time.Millisecond)

	if task1.RunCount() != runCount1AfterCancel {
		t.Errorf("Task1 kept running after its context was cancelled")
	}
	if task2.RunCount() != runCount2AfterCancel {
		t.Errorf("Task2 kept running after its context was cancelled")
	}

	// The scheduler itself is still running and needs to be cleaned up
	scheduler.Stop()
}
