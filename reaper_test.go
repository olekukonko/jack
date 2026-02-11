package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olekukonko/ll"
)

func TestNewReaper(t *testing.T) {
	ttl := 1 * time.Second
	r := NewReaper(ttl)
	if r == nil {
		t.Fatal("NewReaper() returned nil")
	}
	defer r.Stop()
}

func TestReaperStartStop(t *testing.T) {
	r := NewReaper(1 * time.Second)
	r.Start()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop should work without blocking
	r.Stop()

	// Double stop should be safe
	r.Stop()
}

func TestReaperTouch(t *testing.T) {
	r := NewReaper(100 * time.Millisecond)
	r.Start()
	defer r.Stop()

	lid := "test-id"

	// Touch should add the task
	r.Touch(lid)

	if count := r.Count(); count != 1 {
		t.Errorf("Count after Touch = %d, want 1", count)
	}

	// Touch again should update existing
	time.Sleep(50 * time.Millisecond)
	r.Touch(lid)

	// Should still have 1 task
	if count := r.Count(); count != 1 {
		t.Errorf("Count after second Touch = %d, want 1", count)
	}
}

func TestReaperExpiration(t *testing.T) {
	var executed atomic.Int32
	r := NewReaper(50*time.Millisecond, ReaperWithHandler(func(ctx context.Context, id string) {
		executed.Add(1)
	}))
	r.Start()
	defer r.Stop()

	lid := "test-id"
	r.Touch(lid)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	if count := executed.Load(); count != 1 {
		t.Errorf("Handler executed %d times, want 1", count)
	}

	// Task should be removed after expiration
	if r.Count() != 0 {
		t.Errorf("Count after expiration = %d, want 0", r.Count())
	}
}

func TestReaperRemove(t *testing.T) {
	var executed atomic.Int32
	r := NewReaper(100 * time.Millisecond)
	r.Register(func(ctx context.Context, id string) {
		executed.Add(1)
	})
	r.Start()
	defer r.Stop()

	lid := "test-id"
	r.Touch(lid)

	// Remove before expiration
	time.Sleep(50 * time.Millisecond)
	removed := r.Remove(lid)

	if !removed {
		t.Error("Remove() should return true when task exists")
	}

	if r.Count() != 0 {
		t.Errorf("Count after Remove = %d, want 0", r.Count())
	}

	// Wait past original expiration time
	time.Sleep(100 * time.Millisecond)

	if count := executed.Load(); count != 0 {
		t.Errorf("Handler executed %d times after removal, want 0", count)
	}
}

func TestReaperTouchAt(t *testing.T) {
	var executed atomic.Int32
	r := NewReaper(1*time.Second, ReaperWithHandler(func(ctx context.Context, id string) {
		executed.Add(1)
	}))
	r.Start()
	defer r.Stop()

	lid := "test-id"

	// Schedule for specific time (very soon)
	deadline := time.Now().Add(50 * time.Millisecond)
	r.TouchAt(lid, deadline)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	if count := executed.Load(); count != 1 {
		t.Errorf("Handler executed %d times, want 1", count)
	}
}

func TestReaperMultipleTasks(t *testing.T) {
	var executed atomic.Int32
	r := NewReaper(200 * time.Millisecond)
	r.Register(func(ctx context.Context, id string) {
		executed.Add(1)
	})
	r.Start()
	defer r.Stop()

	// Add multiple tasks with different IDs
	for i := 0; i < 5; i++ {
		lid := string(rune('A' + i))
		r.Touch(lid)
	}

	if count := r.Count(); count != 5 {
		t.Errorf("Initial Count = %d, want 5", count)
	}

	// Wait for all to expire
	time.Sleep(300 * time.Millisecond)

	if count := executed.Load(); count != 5 {
		t.Errorf("Handler executed %d times, want 5", count)
	}

	if r.Count() != 0 {
		t.Errorf("Count after all expirations = %d, want 0", r.Count())
	}
}

func TestReaperClear(t *testing.T) {
	var executed atomic.Int32
	r := NewReaper(100 * time.Millisecond)
	r.Register(func(ctx context.Context, id string) {
		executed.Add(1)
	})
	r.Start()
	defer r.Stop()

	// Add multiple tasks
	for i := 0; i < 3; i++ {
		lid := string(rune('A' + i))
		r.Touch(lid)
	}

	if count := r.Count(); count != 3 {
		t.Fatalf("Initial Count = %d, want 3", count)
	}

	// Remove all
	removed := r.Clear()
	if removed != 3 {
		t.Errorf("Clear() returned %d, want 3", removed)
	}

	if r.Count() != 0 {
		t.Errorf("Count after Clear = %d, want 0", r.Count())
	}

	// Wait past expiration time
	time.Sleep(150 * time.Millisecond)

	if count := executed.Load(); count != 0 {
		t.Errorf("Handler executed %d times after Clear, want 0", count)
	}
}

func TestReaperDeadline(t *testing.T) {
	r := NewReaper(1 * time.Second)
	r.Start()
	defer r.Stop()

	// No tasks initially
	deadline, ok := r.Deadline()
	if ok {
		t.Error("Deadline should return false when no tasks")
	}
	if !deadline.IsZero() {
		t.Error("Deadline should be zero when no tasks")
	}

	// Add a task
	lid1 := "test-1"
	r.Touch(lid1)

	deadline1, ok := r.Deadline()
	if !ok {
		t.Error("Deadline should return true when tasks exist")
	}
	if deadline1.IsZero() {
		t.Error("Deadline should not be zero")
	}

	// Add another task with shorter TTL
	lid2 := "test-2"
	r.TouchAt(lid2, time.Now().Add(50*time.Millisecond))

	deadline2, ok := r.Deadline()
	if !ok {
		t.Error("Deadline should return true")
	}

	// deadline2 should be earlier than deadline1
	if !deadline2.Before(deadline1) {
		t.Error("Deadline should return earliest deadline")
	}
}

func TestReaperConcurrentAccess(t *testing.T) {
	r := NewReaper(100 * time.Millisecond)
	r.Start()
	defer r.Stop()

	var wg sync.WaitGroup
	numGoroutines := 10
	iterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				lid := string(rune('a'+id)) + string(rune('0'+(j%10)))

				switch j % 4 {
				case 0:
					r.Touch(lid)
				case 1:
					r.Remove(lid)
				case 2:
					r.TouchAt(lid, time.Now().Add(time.Duration(j)*time.Millisecond))
				case 3:
					r.Count()
				}
			}
		}(i)
	}

	wg.Wait()

	// Test should not panic or deadlock
}

func TestReaperHandlerNil(t *testing.T) {
	// Should not panic when handler is nil
	r := NewReaper(50 * time.Millisecond)
	r.Start()
	defer r.Stop()

	lid := "test-id"
	r.Touch(lid)

	// Wait for expiration - should not panic
	time.Sleep(100 * time.Millisecond)
}

func TestReaperStopBeforeStart(t *testing.T) {
	r := NewReaper(1 * time.Second)

	// Stop before start should work
	r.Stop()

	// Operations after stop should be safe
	lid := "test-id"
	r.Touch(lid)  // Should be no-op
	r.Remove(lid) // Should be no-op
	if cleared := r.Clear(); cleared != 0 {
		t.Errorf("Clear after stop = %d, want 0", cleared)
	}
}

func TestReaperMultipleTouches(t *testing.T) {
	var executed atomic.Int32
	r := NewReaper(100 * time.Millisecond)
	r.Register(func(ctx context.Context, id string) {
		executed.Add(1)
	})
	r.Start()
	defer r.Stop()

	lid := "test-id"

	// Touch multiple times rapidly
	for i := 0; i < 5; i++ {
		r.Touch(lid)
		time.Sleep(20 * time.Millisecond)
	}

	// Should only have one task
	if count := r.Count(); count != 1 {
		t.Errorf("Count after multiple touches = %d, want 1", count)
	}

	// Wait past the last touch time
	time.Sleep(150 * time.Millisecond)

	// Should only execute once
	if count := executed.Load(); count != 1 {
		t.Errorf("Handler executed %d times, want 1", count)
	}
}

func TestReaperHeapOrdering(t *testing.T) {
	r := NewReaper(1 * time.Hour) // Long TTL, we'll use TouchAt
	r.Start()
	defer r.Stop()

	// Add tasks in reverse order
	now := time.Now()

	// Task 3 expires last
	r.TouchAt("task3", now.Add(300*time.Millisecond))

	// Task 1 expires first
	r.TouchAt("task1", now.Add(100*time.Millisecond))

	// Task 2 expires second
	r.TouchAt("task2", now.Add(200*time.Millisecond))

	// Check next deadline
	deadline, ok := r.Deadline()
	if !ok {
		t.Fatal("Deadline should return true")
	}

	// Should be task1's deadline (100ms from now)
	expected := now.Add(100 * time.Millisecond)
	if deadline.Sub(expected).Abs() > 10*time.Millisecond {
		t.Errorf("Deadline = %v, want approx %v", deadline, expected)
	}
}

func TestReaperHandlerContext(t *testing.T) {
	handlerCtxCh := make(chan context.Context, 1)
	handlerLidCh := make(chan string, 1)

	r := NewReaper(50 * time.Millisecond)
	r.Register(func(ctx context.Context, id string) {
		handlerCtxCh <- ctx
		handlerLidCh <- id
	})
	r.Start()
	defer r.Stop()

	expectedLid := "test-context"
	r.Touch(expectedLid)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	select {
	case handlerCtx := <-handlerCtxCh:
		if handlerCtx == nil {
			t.Error("Handler should receive a context")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for handler to execute")
	}

	select {
	case handlerLid := <-handlerLidCh:
		if handlerLid != expectedLid {
			t.Errorf("Handler received lid = %v, want %v", handlerLid, expectedLid)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for handler to execute")
	}
}

func TestReaperWithOptions(t *testing.T) {
	var executed atomic.Int32

	// Test with handler option
	r := NewReaper(50*time.Millisecond, ReaperWithHandler(func(ctx context.Context, id string) {
		executed.Add(1)
	}))
	r.Start()
	defer r.Stop()

	r.Touch("test-option")
	time.Sleep(100 * time.Millisecond)

	if executed.Load() != 1 {
		t.Errorf("Handler with option executed %d times, want 1", executed.Load())
	}
}

func TestReaperLogger(t *testing.T) {
	// Test that logger can be set via option
	logger := ll.New("test-reaper").Disable()
	r := NewReaper(1*time.Second, ReaperWithLogger(logger))

	if r == nil {
		t.Fatal("NewReaper with logger returned nil")
	}

	r.Stop()
}
