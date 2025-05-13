package jack

import (
	"context"
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testSimpleTask struct {
	id        string
	data      string
	duration  time.Duration
	fail      bool
	panicMsg  string
	idFunc    func() string
	wasRun    atomic.Bool
	runSignal chan struct{}
}

func (t *testSimpleTask) Do() error {
	t.wasRun.Store(true)
	if t.runSignal != nil {
		select {
		case _, isOpen := <-t.runSignal:
			if isOpen {
			}
		default:
			close(t.runSignal)
		}
	}
	if t.duration > 0 {
		time.Sleep(t.duration)
	}
	if t.panicMsg != "" {
		panic(t.panicMsg)
	}
	if t.fail {
		return fmt.Errorf("task %s failed as requested", t.id)
	}
	return nil
}

func (t *testSimpleTask) ID() string {
	if t.idFunc != nil {
		return t.idFunc()
	}
	return t.id
}

type testCtxTask struct {
	id          string
	data        string
	duration    time.Duration
	fail        bool
	panicMsg    string
	idFunc      func() string
	wasRun      atomic.Bool
	runSignal   chan struct{}
	ctxReceived context.Context
	muCtx       sync.Mutex
}

func (t *testCtxTask) Do(ctx context.Context) error {
	t.muCtx.Lock()
	t.ctxReceived = ctx
	t.muCtx.Unlock()

	t.wasRun.Store(true)
	if t.runSignal != nil {
		select {
		case _, isOpen := <-t.runSignal:
			if isOpen {
			}
		default:
			close(t.runSignal)
		}
	}

	select {
	case <-time.After(t.duration):
		if t.panicMsg != "" {
			panic(t.panicMsg)
		}
		if t.fail {
			return fmt.Errorf("ctxTask %s failed as requested", t.id)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *testCtxTask) ID() string {
	if t.idFunc != nil {
		return t.idFunc()
	}
	return t.id
}

func (t *testCtxTask) getReceivedContext() context.Context {
	t.muCtx.Lock()
	defer t.muCtx.Unlock()
	return t.ctxReceived
}

type eventCollector struct {
	mu     sync.Mutex
	events []Event
}

func newEventCollector() *eventCollector {
	return &eventCollector{events: make([]Event, 0)}
}

func (ec *eventCollector) OnNotify(event Event) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.events = append(ec.events, event)
}

func (ec *eventCollector) getEvents() []Event {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	evtsCopy := make([]Event, len(ec.events))
	copy(evtsCopy, ec.events)
	return evtsCopy
}

func (ec *eventCollector) findEvents(taskID string, eventType string) []Event {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	var found []Event
	for _, e := range ec.events {
		matchID := (taskID == "" || e.TaskID == taskID)
		matchType := (eventType == "" || e.Type == eventType)
		if matchID && matchType {
			found = append(found, e)
		}
	}
	return found
}

func (ec *eventCollector) waitForEvent(t *testing.T, taskID string, eventType string, timeout time.Duration) (Event, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := ec.findEvents(taskID, eventType)
		if len(events) > 0 {
			return events[0], true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return Event{}, false
}

func TestPool_NewPool(t *testing.T) {
	p := NewPool(2, PoolingWithQueueSize(5))
	if p == nil {
		t.Fatal("NewPool returned nil")
	}
	defer p.Shutdown(1 * time.Second)

	if p.Workers() != 2 {
		t.Errorf("expected 2 workers, got %d", p.Workers())
	}
}

func TestPool_Submit_SimpleTask(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)

	pool := NewPool(1, PoolingWithQueueSize(1), PoolingWithObservable(obsable))
	defer pool.Shutdown(2 * time.Second)

	task := &testSimpleTask{id: "task1", duration: 50 * time.Millisecond, runSignal: make(chan struct{})}
	err := pool.Submit(task)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-task.runSignal:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for task to start running")
	}

	_, ok := collector.waitForEvent(t, "task1", "done", 1*time.Second)
	if !ok {
		t.Fatal("Did not receive 'done' event for task1")
	}

	if !task.wasRun.Load() {
		t.Error("Task was not run")
	}

	queuedEvents := collector.findEvents("task1", "queued")
	if len(queuedEvents) != 1 {
		t.Errorf("Expected 1 'queued' event for task1, got %d", len(queuedEvents))
	}
	runEvents := collector.findEvents("task1", "run")
	if len(runEvents) != 1 {
		t.Errorf("Expected 1 'run' event for task1, got %d", len(runEvents))
	}
	doneEvents := collector.findEvents("task1", "done")
	if len(doneEvents) != 1 {
		t.Errorf("Expected 1 'done' event for task1, got %d", len(doneEvents))
	}
	if doneEvents[0].Err != nil {
		t.Errorf("Expected no error for successful task, got %v", doneEvents[0].Err)
	}
}

func TestPool_Submit_TaskWithError(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	pool := NewPool(1, PoolingWithObservable(obsable))
	defer pool.Shutdown(1 * time.Second)

	task := &testSimpleTask{id: "failTask", fail: true}
	pool.Submit(task)

	doneEvent, ok := collector.waitForEvent(t, "failTask", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Did not receive 'done' event for failTask")
	}
	if doneEvent.Err == nil {
		t.Error("Expected error for failed task, got nil")
	} else if doneEvent.Err.Error() != "task failTask failed as requested" {
		t.Errorf("Unexpected error message: %v", doneEvent.Err)
	}
}

func TestPool_Submit_TaskWithPanic(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	pool := NewPool(1, PoolingWithObservable(obsable))
	defer pool.Shutdown(1 * time.Second)

	task := &testSimpleTask{id: "panicTask", panicMsg: "oh no!"}
	pool.Submit(task)

	doneEvent, ok := collector.waitForEvent(t, "panicTask", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Did not receive 'done' event for panicTask")
	}
	if doneEvent.Err == nil {
		t.Error("Expected error for panicked task, got nil")
	}
	caughtPanic, ok := doneEvent.Err.(*CaughtPanic)
	if !ok {
		t.Fatalf("Expected error to be CaughtPanic, got %T", doneEvent.Err)
	}
	if caughtPanic.Val != "oh no!" {
		t.Errorf("Unexpected panic value: %v", caughtPanic.Val)
	}
}

func TestPool_SubmitCtx_Simple(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	pool := NewPool(1, PoolingWithObservable(obsable))
	defer pool.Shutdown(1 * time.Second)

	task := &testCtxTask{id: "ctxTask1", duration: 50 * time.Millisecond}
	ctx := context.Background()
	err := pool.SubmitCtx(ctx, task)
	if err != nil {
		t.Fatalf("SubmitCtx failed: %v", err)
	}

	doneEvent, ok := collector.waitForEvent(t, "ctxTask1", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Did not receive 'done' event for ctxTask1")
	}
	if !task.wasRun.Load() {
		t.Error("Task was not run")
	}
	if doneEvent.Err != nil {
		t.Errorf("Expected no error, got %v", doneEvent.Err)
	}
	if task.getReceivedContext() != ctx {
		t.Error("Task did not receive the correct context")
	}
}

func TestPool_SubmitCtx_CancellationBeforeExecution(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	pool := NewPool(1, PoolingWithQueueSize(0), PoolingWithObservable(obsable))
	defer pool.Shutdown(1 * time.Second)

	busyTaskDone := make(chan struct{})
	_ = pool.SubmitCtx(context.Background(), FuncCtx(func(ctx context.Context) error {
		defer close(busyTaskDone)
		time.Sleep(150 * time.Millisecond)
		return nil
	}))

	taskToCancel := &testCtxTask{id: "cancelTask", duration: 1 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())

	var submitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		submitErr = pool.SubmitCtx(ctx, taskToCancel)
	}()

	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()

	if submitErr == nil {
		t.Fatal("SubmitCtx should have returned an error due to context cancellation")
	}
	if !errors.Is(submitErr, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", submitErr)
	}

	select {
	case <-busyTaskDone:
	case <-time.After(200 * time.Millisecond):
		t.Log("Warning: Busy task did not finish in expected time during cancellation test")
	}

	time.Sleep(20 * time.Millisecond)
	if len(collector.findEvents("cancelTask", "run")) > 0 {
		t.Error("cancelTask should not have a 'run' event")
	}
	if len(collector.findEvents("cancelTask", "done")) > 0 {
		t.Error("cancelTask should not have a 'done' event")
	}
	if taskToCancel.wasRun.Load() {
		t.Error("taskToCancel should not have run")
	}
}

func TestPool_SubmitCtx_TaskRespectsCancellation(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)
	pool := NewPool(1, PoolingWithObservable(obsable))
	defer pool.Shutdown(1 * time.Second)

	task := &testCtxTask{id: "ctxCancel", duration: 1 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := pool.SubmitCtx(ctx, task)
	if err != nil {
		t.Fatalf("SubmitCtx failed: %v", err)
	}

	doneEvent, ok := collector.waitForEvent(t, "ctxCancel", "done", 200*time.Millisecond)
	if !ok {
		t.Fatal("Did not receive 'done' event for ctxCancel task")
	}
	if !task.wasRun.Load() {
		t.Error("Task was not run (it should have started before timeout)")
	}
	if doneEvent.Err == nil {
		t.Error("Expected an error due to context timeout, got nil")
	}
	if !errors.Is(doneEvent.Err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", doneEvent.Err)
	}
}

func TestPool_Submit_ErrQueueFull(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(0))
	defer pool.Shutdown(1 * time.Second)

	task1 := &testSimpleTask{id: "task1_qf", duration: 200 * time.Millisecond, runSignal: make(chan struct{})}
	var err error
	for i := 0; i < 10; i++ {
		err = pool.Submit(task1)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Submit task1 failed after retries: %v", err)
	}
	select {
	case <-task1.runSignal:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task1 did not start")
	}

	task2 := &testSimpleTask{id: "task2_qf", duration: 10 * time.Millisecond}
	err = pool.Submit(task2)
	if err == nil {
		t.Fatal("Submit task2 should have failed with ErrQueueFull, got nil")
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("Expected ErrQueueFull, got %v", err)
	}
	if task2.wasRun.Load() {
		t.Error("task2 should not have run")
	}
}

func TestPool_Shutdown(t *testing.T) {
	collector := newEventCollector()
	obs := NewObservable[Event]()
	defer obs.Shutdown()
	obs.Add(collector)

	pool := NewPool(1, PoolingWithObservable(obs), PoolingWithQueueSize(5)) // Reduced to 1 worker
	var tasksRun int32
	for i := 0; i < 5; i++ {
		id := i
		err := pool.Submit(Func(func() error {
			t.Logf("Task %d started", id)
			time.Sleep(100 * time.Millisecond)
			t.Logf("Task %d completed", id)
			atomic.AddInt32(&tasksRun, 1)
			return nil
		}))
		if err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	err := pool.Shutdown(10 * time.Second) // Increased to 10 seconds
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	if atomic.LoadInt32(&tasksRun) != 5 {
		t.Errorf("Expected 5 tasks to run, got %d", tasksRun)
	}

	err = pool.Submit(Func(func() error {
		t.Error("Task submitted after shutdown")
		return nil
	}))
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Expected ErrPoolClosed, got %v", err)
	}
}

func TestPool_Shutdown_Timeout(t *testing.T) {
	pool := NewPool(1, PoolingWithQueueSize(1))

	longTaskDone := make(chan struct{})
	longTask := Func(func() error {
		defer close(longTaskDone)
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	pool.Submit(longTask)

	time.Sleep(10 * time.Millisecond)

	err := pool.Shutdown(50 * time.Millisecond)
	if err == nil {
		t.Fatal("Shutdown should have timed out, got nil error")
	}
	if !errors.Is(err, ErrShutdownTimedOut) {
		t.Errorf("Expected ErrShutdownTimedOut, got %v", err)
	}

	select {
	case <-longTaskDone:
	case <-time.After(300 * time.Millisecond):
		t.Error("Long task did not complete even after shutdown timeout")
	}
}

func TestPool_TaskIDGeneration(t *testing.T) {
	collector := newEventCollector()
	obsable := NewObservable[Event](1)
	defer obsable.Shutdown()
	obsable.Add(collector)

	pool := NewPool(1, PoolingWithObservable(obsable))
	defer pool.Shutdown(1 * time.Second)
	taskWithID := &testSimpleTask{id: "poolWithID"}
	pool.Submit(taskWithID)
	_, ok := collector.waitForEvent(t, "poolWithID", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Event for taskWithID not found")
	}
	collector.events = []Event{}

	taskNoID := Func(func() error { return nil })
	pool.Submit(taskNoID)
	time.Sleep(50 * time.Millisecond)
	events := collector.findEvents("", "done")
	if len(events) == 0 {
		t.Logf("Available events: %+v", collector.getEvents())
		t.Fatal("Event for taskNoID not found")
	}
	taskID := events[0].TaskID
	if !strings.HasPrefix(taskID, "task.") {
		t.Errorf("Expected task ID to start with 'task.', got %s", taskID)
	}
	if _, err := ulid.Parse(strings.TrimPrefix(taskID, "task.")); err != nil {
		t.Errorf("Expected valid ULID after 'task.', got %s", taskID)
	}
	collector.events = []Event{}

	customGen := func(taskInput interface{}) string { return "customPoolID" }
	pool2 := NewPool(1, PoolingWithObservable(obsable), PoolingWithIDGenerator(customGen))
	defer pool2.Shutdown(1 * time.Second)
	pool2.Submit(&testSimpleTask{id: "ignored"})
	_, ok = collector.waitForEvent(t, "customPoolID", "done", 500*time.Millisecond)
	if !ok {
		t.Fatal("Event with customPoolID not found")
	}
}

func TestPool_ZeroWorkers(t *testing.T) {
	pool := NewPool(0)
	defer pool.Shutdown(1 * time.Second)
	if pool.Workers() != 1 {
		t.Errorf("Expected 1 worker when 0 is passed to NewPool, got %d", pool.Workers())
	}
	task := &testSimpleTask{id: "zeroworker", duration: 10 * time.Millisecond}
	err := pool.Submit(task)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	if !task.wasRun.Load() {
		t.Error("Task in 0-worker (defaulted to 1) pool did not run")
	}
}

func TestPool_ConcurrentSubmissions(t *testing.T) {
	numTasks := 100
	numWorkers := 10
	collector := newEventCollector()
	obsable := NewObservable[Event](numWorkers)
	defer obsable.Shutdown()
	obsable.Add(collector)

	pool := NewPool(numWorkers, PoolingWithQueueSize(numTasks), PoolingWithObservable(obsable))
	defer pool.Shutdown(5 * time.Second)

	var wg sync.WaitGroup
	var submitErrors int32
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			taskID := fmt.Sprintf("concurrent-%d", idx)
			var err error
			if idx%2 == 0 {
				err = pool.Submit(&testSimpleTask{id: taskID, duration: time.Duration(idx%5+1) * time.Millisecond})
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				err = pool.SubmitCtx(ctx, &testCtxTask{id: taskID, duration: time.Duration(idx%5+1) * time.Millisecond})
			}
			if err != nil && !errors.Is(err, ErrQueueFull) && !errors.Is(err, context.DeadlineExceeded) {
				t.Logf("Error submitting task %s: %v", taskID, err)
				atomic.AddInt32(&submitErrors, 1)
			}
		}(i)
	}
	wg.Wait()

	if atomic.LoadInt32(&submitErrors) > 0 {
		t.Errorf("Got %d unexpected errors during concurrent submission", atomic.LoadInt32(&submitErrors))
	}

	deadline := time.Now().Add(3 * time.Second)
	var doneCount int
	for time.Now().Before(deadline) {
		doneEvents := collector.findEvents("", "done")
		doneCount = len(doneEvents)
		if doneCount >= numTasks-int(atomic.LoadInt32(&submitErrors)) && doneCount <= numTasks {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	finalDoneEvents := collector.findEvents("", "done")
	finalRunEvents := collector.findEvents("", "run")
	finalQueuedEvents := collector.findEvents("", "queued")

	if len(finalDoneEvents) > numTasks || len(finalDoneEvents) == 0 && numTasks > 0 && atomic.LoadInt32(&submitErrors) == 0 {
		t.Errorf("Unexpected number of 'done' events: got %d for %d tasks submitted (submit errors: %d)", len(finalDoneEvents), numTasks, atomic.LoadInt32(&submitErrors))
	}
	t.Logf("Concurrent test: Queued: %d, Run: %d, Done: %d (submitted %d tasks, %d submit errors)", len(finalQueuedEvents), len(finalRunEvents), len(finalDoneEvents), numTasks, atomic.LoadInt32(&submitErrors))
}
