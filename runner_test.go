// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"context"
	"errors"
	"github.com/oklog/ulid/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunner_NewRunner verifies that NewRunner creates a non-nil Runner instance.
func TestRunner_NewRunner(t *testing.T) {
	r := NewRunner()
	if r == nil {
		t.Fatal("NewRunner returned nil")
	}
	defer r.Shutdown(1 * time.Second)
}

// TestRunner_Do_SimpleTask verifies that Runner executes a simple task correctly.
// It checks task execution, event emission, and error handling.
func TestRunner_Do_SimpleTask(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	runner := NewRunner(WithRunnerObservable(obsable), WithRunnerQueueSize(1))
	defer runner.Shutdown(1 * time.Second)

	task := &testSimpleTask{id: "runnerTask1", duration: 20 * time.Millisecond}
	err := runner.Do(task)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	doneEvent, ok := collector.waitForEvent(t, "runnerTask1", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Did not receive 'done' event for runnerTask1")
	}

	if !task.wasRun.Load() {
		t.Error("Task was not run")
	}
	if doneEvent.Err != nil {
		t.Errorf("Expected no error for successful task, got %v", doneEvent.Err)
	}

	queuedEvents := collector.findEvents("runnerTask1", "queued")
	if len(queuedEvents) != 1 {
		t.Errorf("Expected 1 'queued' event, got %d", len(queuedEvents))
	}
	runEvents := collector.findEvents("runnerTask1", "run")
	if len(runEvents) != 1 {
		t.Errorf("Expected 1 'run' event, got %d", len(runEvents))
	}
	if runEvents[0].WorkerID != "runner-0" {
		t.Errorf("Expected workerID runner-0, got %s", runEvents[0].WorkerID)
	}
}

// TestRunner_Do_SequentialExecution verifies that Runner executes tasks sequentially.
// It checks the order of task execution using a shared run order list.
func TestRunner_Do_SequentialExecution(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](2) // Increased workers for faster event processing
	defer obsable.Shutdown()
	obsable.Add(collector)
	runner := NewRunner(WithRunnerObservable(obsable), WithRunnerQueueSize(5))
	defer runner.Shutdown(1 * time.Second)

	var runOrder []string
	var muRunOrder sync.Mutex

	task1 := Func(func() error {
		muRunOrder.Lock()
		runOrder = append(runOrder, "task1")
		muRunOrder.Unlock()
		time.Sleep(30 * time.Millisecond)
		return nil
	})
	task2 := Func(func() error {
		muRunOrder.Lock()
		runOrder = append(runOrder, "task2")
		muRunOrder.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	runner.Do(task1)
	runner.Do(task2)

	_, ok1 := collector.waitForEvent(t, "", "done", 1*time.Second)
	_, ok2 := collector.waitForEvent(t, "", "done", 1*time.Second)
	if !ok1 || !ok2 {
		t.Fatal("Not all tasks completed")
	}

	muRunOrder.Lock()
	defer muRunOrder.Unlock()
	if len(runOrder) != 2 || runOrder[0] != "task1" || runOrder[1] != "task2" {
		t.Errorf("Expected run order ['task1', 'task2'], got %v", runOrder)
	}
}

// TestRunner_Do_TaskWithErrorAndPanic verifies that Runner handles task errors and panics.
// It checks for correct error reporting and CaughtPanic instances.
func TestRunner_Do_TaskWithErrorAndPanic(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	runner := NewRunner(WithRunnerObservable(obsable))
	defer runner.Shutdown(1 * time.Second)

	errTask := &testSimpleTask{id: "runnerErr", fail: true}
	runner.Do(errTask)
	doneErrEvent, ok := collector.waitForEvent(t, "runnerErr", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("No done event for errTask")
	}
	if doneErrEvent.Err == nil || doneErrEvent.Err.Error() != "task runnerErr failed as requested" {
		t.Errorf("Unexpected error for errTask: %v", doneErrEvent.Err)
	}

	panicTask := &testSimpleTask{id: "runnerPanic", panicMsg: "runner boom"}
	runner.Do(panicTask)
	donePanicEvent, ok := collector.waitForEvent(t, "runnerPanic", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("No done event for panicTask")
	}
	if _, ok := donePanicEvent.Err.(*CaughtPanic); !ok {
		t.Errorf("Expected CaughtPanic for panicTask, got %T", donePanicEvent.Err)
	}
}

// TestRunner_DoCtx_Cancellation verifies that Runner respects context cancellation during task submission.
// It ensures cancelled tasks are not executed.
func TestRunner_DoCtx_Cancellation(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	runner := NewRunner(WithRunnerObservable(obsable), WithRunnerQueueSize(0))
	defer runner.Shutdown(1 * time.Second)

	busyTaskDone := make(chan struct{})
	runner.DoCtx(context.Background(), FuncCtx(func(ctx context.Context) error {
		defer close(busyTaskDone)
		time.Sleep(100 * time.Millisecond)
		return nil
	}))

	taskToCancel := &testCtxTask{id: "runnerCtxCancelSubmit", duration: 1 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())

	var submitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		submitErr = runner.DoCtx(ctx, taskToCancel)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()

	if !errors.Is(submitErr, context.Canceled) {
		t.Errorf("Expected context.Canceled on submit, got %v", submitErr)
	}

	<-busyTaskDone
	time.Sleep(20 * time.Millisecond)

	if len(collector.findEvents("runnerCtxCancelSubmit", "run")) > 0 {
		t.Error("taskToCancel should not have a 'run' event")
	}
}

// TestRunner_DoCtx_TaskRespectsContext verifies that Runner tasks respect their context’s deadline.
// It checks for DeadlineExceeded errors on timeout.
func TestRunner_DoCtx_TaskRespectsContext(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	runner := NewRunner(WithRunnerObservable(obsable))
	defer runner.Shutdown(1 * time.Second)

	task := &testCtxTask{id: "runnerCtxRespect", duration: 1 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	runner.DoCtx(ctx, task)

	doneEvent, ok := collector.waitForEvent(t, "runnerCtxRespect", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("No done event for runnerCtxRespect")
	}
	if !errors.Is(doneEvent.Err, context.DeadlineExceeded) {
		t.Errorf("Expected DeadlineExceeded, got %v", doneEvent.Err)
	}
}

// TestRunner_Do_ErrQueueFull verifies that Runner returns ErrQueueFull when the task queue is full.
// It ensures queue capacity limits are enforced.
func TestRunner_Do_ErrQueueFull(t *testing.T) {
	runner := NewRunner(WithRunnerQueueSize(1)) // Use queue size 1
	defer runner.Shutdown(1 * time.Second)

	// Verify queue capacity
	if cap(runner.tasks) != 1 {
		t.Fatalf("Expected queue capacity 1, got %d", cap(runner.tasks))
	}

	taskStarted := make(chan struct{})
	task1Done := make(chan struct{})
	runner.Do(Func(func() error {
		close(taskStarted)
		time.Sleep(500 * time.Millisecond) // Ensure overlap
		close(task1Done)
		return nil
	}))

	// Wait for first task to start
	select {
	case <-taskStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("First task did not start")
	}

	runner.Do(Func(func() error { return nil })) // Fill queue

	err := runner.Do(&testSimpleTask{id: "runnerQFull"})
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("Expected ErrQueueFull, got %v", err)
	}
	<-task1Done
}

// TestRunner_Shutdown verifies that Runner shuts down correctly and rejects new tasks.
// It checks that all queued tasks run and post-shutdown submissions fail.
func TestRunner_Shutdown(t *testing.T) {
	runner := NewRunner(WithRunnerQueueSize(5))
	var tasksRun int32
	for i := 0; i < 3; i++ {
		runner.Do(Func(func() error {
			atomic.AddInt32(&tasksRun, 1)
			time.Sleep(20 * time.Millisecond)
			return nil
		}))
	}

	time.Sleep(10 * time.Millisecond)
	err := runner.Shutdown(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	if atomic.LoadInt32(&tasksRun) != 3 {
		t.Errorf("Expected 3 tasks to run, got %d", tasksRun)
	}

	err = runner.Do(Func(func() error { t.Error("task submitted after shutdown"); return nil }))
	if !errors.Is(err, ErrRunnerClosed) {
		t.Errorf("Expected ErrRunnerClosed, got %v", err)
	}
}

// TestRunner_Shutdown_Timeout verifies that Runner returns ErrShutdownTimedOut for long-running tasks.
// It ensures timeout behavior during shutdown.
func TestRunner_Shutdown_Timeout(t *testing.T) {
	runner := NewRunner()
	longTaskDone := make(chan struct{})
	runner.Do(Func(func() error {
		defer close(longTaskDone)
		time.Sleep(100 * time.Millisecond)
		return nil
	}))
	time.Sleep(10 * time.Millisecond)

	err := runner.Shutdown(30 * time.Millisecond)
	if !errors.Is(err, ErrShutdownTimedOut) {
		t.Errorf("Expected ErrShutdownTimedOut, got %v", err)
	}
	<-longTaskDone
}

// TestRunner_TaskIDGeneration verifies that Runner generates correct task IDs.
// It tests default, custom, and Identifiable-based ID generation.
func TestRunner_TaskIDGeneration(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)

	runner1 := NewRunner(WithRunnerObservable(obsable))
	defer runner1.Shutdown(1 * time.Second)
	taskWithID := &testSimpleTask{id: "runnerWithID"}
	runner1.Do(taskWithID)
	_, ok := collector.waitForEvent(t, "runnerWithID", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Event for taskWithID not found")
	}
	collector.events = []Event{}

	taskNoID := Func(func() error { return nil })
	runner1.Do(taskNoID)
	time.Sleep(50 * time.Millisecond) // Ensure task completes
	events := collector.findEvents("", "done")
	if len(events) == 0 {
		t.Logf("Available events: %+v", collector.getEvents())
		t.Fatal("Event for taskNoID not found")
	}
	taskID := events[0].TaskID
	if !strings.HasPrefix(taskID, "runner.") {
		t.Errorf("Expected task ID to start with 'runner.', got %s", taskID)
	}
	if _, err := ulid.Parse(strings.TrimPrefix(taskID, "runner.")); err != nil {
		t.Errorf("Expected valid ULID after 'runner.', got %s", taskID)
	}
	collector.events = []Event{}

	customGen := func(taskInput interface{}) string { return "customRunnerID" }
	runner2 := NewRunner(WithRunnerObservable(obsable), WithRunnerIDGenerator(customGen))
	defer runner2.Shutdown(1 * time.Second)
	runner2.Do(&testSimpleTask{id: "ignored"})
	_, ok = collector.waitForEvent(t, "customRunnerID", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Event with customRunnerID not found")
	}
}
