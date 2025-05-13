// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockObserver is a test implementation of the Observer interface for testing Observable behavior.
// It tracks notifications, supports custom logic, and can simulate panics.
// Thread-safe via mutex.
type mockObserver[T any] struct {
	id         string        // Observer identifier
	mu         sync.Mutex    // Protects fields
	notifCount int           // Number of notifications received
	lastValue  T             // Last received value
	values     []T           // All received values
	onNotifyFn func(value T) // Optional custom notification logic
	panicOn    int           // Panics on this notification number if set
}

// newMockObserver creates a new mock observer with the specified ID.
// Thread-safe via initialization.
func newMockObserver[T any](id string) *mockObserver[T] {
	return &mockObserver[T]{id: id, values: make([]T, 0)}
}

// OnNotify processes a notification, updating counts and values, and optionally triggering a panic or custom logic.
// Thread-safe via mutex.
func (m *mockObserver[T]) OnNotify(value T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.notifCount++
	m.lastValue = value
	m.values = append(m.values, value)

	if m.panicOn > 0 && m.notifCount == m.panicOn {
		panic(fmt.Sprintf("mockObserver %s panicking on notification %d", m.id, m.notifCount))
	}

	if m.onNotifyFn != nil {
		m.onNotifyFn(value)
	}
}

// getNotifCount returns the number of notifications received.
// Thread-safe via mutex.
func (m *mockObserver[T]) getNotifCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.notifCount
}

// getLastValue returns the last received value and a boolean indicating if a value was received.
// Thread-safe via mutex.
func (m *mockObserver[T]) getLastValue() (T, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notifCount == 0 {
		var zeroVal T
		return zeroVal, false
	}
	return m.lastValue, true
}

// getAllValues returns a copy of all received values.
// Thread-safe via mutex.
func (m *mockObserver[T]) getAllValues() []T {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy
	valsCopy := make([]T, len(m.values))
	copy(valsCopy, m.values)
	return valsCopy
}

// TestEventObservable_Basic verifies that Observable correctly notifies multiple observers.
// It checks that all observers receive the expected number of notifications.
func TestEventObservable_Basic(t *testing.T) {
	obsable := NewObservable[string](1) // 1 worker for predictable testing if needed, though Notify is async
	defer obsable.Shutdown()

	obs1 := newMockObserver[string]("obs1")
	obs2 := newMockObserver[string]("obs2")

	obsable.Add(obs1)
	obsable.Add(obs2)

	obsable.Notify("hello")
	obsable.Notify("world")

	// Allow time for async notifications to process via the internal pool
	time.Sleep(50 * time.Millisecond)

	if obs1.getNotifCount() != 2 {
		t.Errorf("obs1 expected 2 notifications, got %d", obs1.getNotifCount())
	}
	val1, _ := obs1.getLastValue()
	if val1 != "world" { // Order isn't strictly guaranteed due to goroutines, but likely with few events
		t.Logf("obs1 last value: %s (note: order not strictly guaranteed)", val1)
	}

	if obs2.getNotifCount() != 2 {
		t.Errorf("obs2 expected 2 notifications, got %d", obs2.getNotifCount())
	}
	val2, _ := obs2.getLastValue()
	if val2 != "world" {
		t.Logf("obs2 last value: %s (note: order not strictly guaranteed)", val2)
	}
}

// TestEventObservable_Remove verifies that removing an observer stops its notifications.
// It checks that removed observers no longer receive events while others continue.
func TestEventObservable_Remove(t *testing.T) {
	obsable := NewObservable[int](1)
	defer obsable.Shutdown()

	obs1 := newMockObserver[int]("obs1")
	obs2 := newMockObserver[int]("obs2")

	obsable.Add(obs1)
	obsable.Add(obs2)

	obsable.Notify(1)
	time.Sleep(10 * time.Millisecond) // Let it process

	obsable.Remove(obs1)
	obsable.Notify(2)
	time.Sleep(10 * time.Millisecond) // Let it process

	if obs1.getNotifCount() != 1 {
		t.Errorf("obs1 expected 1 notification, got %d", obs1.getNotifCount())
	}
	if obs2.getNotifCount() != 2 {
		t.Errorf("obs2 expected 2 notifications, got %d", obs2.getNotifCount())
	}
	val2, _ := obs2.getLastValue()
	if val2 != 2 {
		t.Errorf("obs2 last value expected 2, got %v", val2)
	}
}

// TestEventObservable_RemoveNonExistent verifies that removing a non-existent observer is safe.
// It ensures no panics or errors occur and existing observers continue receiving notifications.
func TestEventObservable_RemoveNonExistent(t *testing.T) {
	obsable := NewObservable[int](1)
	defer obsable.Shutdown()
	obs1 := newMockObserver[int]("obs1")
	obsNonExistent := newMockObserver[int]("nonExistent")

	obsable.Add(obs1)
	obsable.Remove(obsNonExistent) // Should not panic or error

	obsable.Notify(100)
	time.Sleep(10 * time.Millisecond)
	if obs1.getNotifCount() != 1 {
		t.Errorf("obs1 should have received notification despite removing non-existent")
	}
}

// TestEventObservable_NotifyEmpty verifies that notifying with no observers is safe.
// It ensures the Observable does not panic when no observers are registered.
func TestEventObservable_NotifyEmpty(t *testing.T) {
	obsable := NewObservable[string](1)
	defer obsable.Shutdown()
	obsable.Notify("test") // Should not panic
}

// TestEventObservable_ObserverPanic verifies that an observer panic does not crash the Observable.
// It ensures other observers continue receiving notifications.
func TestEventObservable_ObserverPanic(t *testing.T) {
	// Note: Go's testing package can sometimes catch panics from goroutines
	// and report them. This test primarily ensures the observable itself doesn't crash.
	// We'd look for "PANIC in observer OnNotify" in logs if log.Printf was active.
	obsable := NewObservable[string](1)
	defer obsable.Shutdown()

	obs1 := newMockObserver[string]("obs1")
	obs2Panics := newMockObserver[string]("obs2Panics")
	obs2Panics.panicOn = 1 // Panics on first notification

	obsable.Add(obs1)
	obsable.Add(obs2Panics)

	obsable.Notify("event1")
	time.Sleep(50 * time.Millisecond) // Allow notifications to process

	if obs1.getNotifCount() != 1 {
		t.Errorf("obs1 should still receive notification even if another observer panics, got count %d", obs1.getNotifCount())
	}
	val1, _ := obs1.getLastValue()
	if val1 != "event1" {
		t.Errorf("obs1 last value expected 'event1', got %v", val1)
	}

	// obs2Panics should have attempted 1 notification
	if obs2Panics.getNotifCount() != 1 {
		t.Errorf("obs2Panics should have attempted 1 notification, got %d", obs2Panics.getNotifCount())
	}
}

// TestEventObservable_ConcurrentAddRemoveNotify verifies concurrent safety of Observable operations.
// It exercises adding, removing, and notifying observers concurrently to ensure no deadlocks or panics.
func TestEventObservable_ConcurrentAddRemoveNotify(t *testing.T) {
	obsable := NewObservable[int](2) // Use a few workers
	defer obsable.Shutdown()

	var wg sync.WaitGroup
	numOps := 100

	// Notifier goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			obsable.Notify(i)
			time.Sleep(1 * time.Millisecond) // Small delay
		}
	}()

	// Adder/Remover goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			obs := newMockObserver[int](fmt.Sprintf("dynamicObs%d", id))
			for j := 0; j < numOps/5; j++ {
				obsable.Add(obs)
				time.Sleep(time.Duration(j%3+1) * time.Millisecond)
				if j%2 == 0 {
					obsable.Remove(obs)
				}
			}
			// Ensure it's removed at the end if not already
			obsable.Remove(obs)
		}(i)
	}

	wg.Wait()
	// Basic check: no panics. Deeper state validation is complex here.
	// This test is more about exercising concurrent access paths.
	t.Log("ConcurrentAddRemoveNotify finished without deadlocks or panics.")
}

// TestEventObservable_Shutdown verifies that Observable shuts down correctly.
// It ensures notifications before shutdown are delivered and post-shutdown notifications are dropped safely.
func TestEventObservable_Shutdown(t *testing.T) {
	obsable := NewObservable[string](2)
	obs1 := newMockObserver[string]("obs1")
	obsable.Add(obs1)

	obsable.Notify("before_shutdown")
	time.Sleep(10 * time.Millisecond) // Let it process

	obsable.Shutdown()
	// Attempt to notify after shutdown (should be logged as dropped, observer won't get it)
	// This test can't easily verify the log output without redirecting logs.
	// It mainly ensures Shutdown doesn't deadlock and subsequent notifies don't panic.
	obsable.Notify("after_shutdown")
	time.Sleep(10 * time.Millisecond)

	if obs1.getNotifCount() != 1 {
		t.Errorf("obs1 expected 1 notification (before shutdown), got %d", obs1.getNotifCount())
	}

	// Try shutting down again (should be idempotent)
	obsable.Shutdown()
}

// TestEventObservable_AddObserverDuringNotification verifies that adding an observer during a notification is safe.
// It ensures snapshotting prevents deadlocks and new observers receive only subsequent events.
func TestEventObservable_AddObserverDuringNotification(t *testing.T) {
	// This tests if adding an observer from within another observer's OnNotify
	// doesn't cause a deadlock, thanks to the snapshotting of observers.
	obsable := NewObservable[string](1) // Single worker for easier reasoning
	defer obsable.Shutdown()

	var obs2AddedByObs1 sync.WaitGroup
	obs2AddedByObs1.Add(1)

	obs2 := newMockObserver[string]("obs2")
	obs1 := newMockObserver[string]("obs1")
	obs1.onNotifyFn = func(value string) {
		if value == "event1" {
			t.Log("obs1 received event1, adding obs2")
			obsable.Add(obs2) // Add obs2 from within obs1's notification
			obs2AddedByObs1.Done()
		}
	}

	obsable.Add(obs1)
	obsable.Notify("event1")

	// Wait for obs1 to add obs2
	if waitTimeout(&obs2AddedByObs1, 100*time.Millisecond) {
		t.Fatal("Timeout waiting for obs1 to add obs2")
	}
	t.Log("obs2 added by obs1")

	// obs2 should not receive "event1" because it was added *after* the observer list for "event1" was snapshotted.
	// It should receive "event2".
	obsable.Notify("event2")
	time.Sleep(50 * time.Millisecond) // Allow notifications to process

	if obs1.getNotifCount() != 2 {
		t.Errorf("obs1 expected 2 notifications, got %d", obs1.getNotifCount())
	}
	if obs2.getNotifCount() != 1 {
		t.Errorf("obs2 expected 1 notification ('event2'), got %d", obs2.getNotifCount())
	}
	val2, _ := obs2.getLastValue()
	if val2 != "event2" {
		t.Errorf("obs2 last value expected 'event2', got %s", val2)
	}
}

// waitTimeout waits for the waitgroup for the specified duration.
// It returns true if waiting timed out.
// Thread-safe via channel operations.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
