package jack

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestDebounce_ExecutesAfterDuration(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(50 * time.Millisecond))
	debouncer.Do(func() {
		atomic.AddInt32(&counter, 1)
	})

	time.Sleep(100 * time.Millisecond) // Wait longer than delay to ensure execution

	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected function to be called once, got %d", atomic.LoadInt32(&counter))
	}
}

func TestDebounce_ResetsOnSubsequentCalls(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(100 * time.Millisecond))

	// First call
	debouncer.Do(func() {
		atomic.AddInt32(&counter, 1)
	})
	time.Sleep(50 * time.Millisecond) // Halfway through delay

	// Second call, should reset the timer
	debouncer.Do(func() {
		atomic.AddInt32(&counter, 1)
	})

	time.Sleep(150 * time.Millisecond) // Wait for the reset delay + buffer

	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected function to be called only once, got %d", atomic.LoadInt32(&counter))
	}
}

func TestDebounce_WithMaxCalls(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(200*time.Millisecond), WithDebounceMaxCalls(3))
	fn := func() {
		atomic.AddInt32(&counter, 1)
	}

	debouncer.Do(fn) // Call 1
	debouncer.Do(fn) // Call 2
	if atomic.LoadInt32(&counter) != 0 {
		t.Error("Premature execution before maxCalls")
	}

	debouncer.Do(fn) // Call 3 - should trigger immediate execution

	time.Sleep(50 * time.Millisecond) // Small wait to allow goroutine to run

	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected function to be called once after 3 calls, got %d", atomic.LoadInt32(&counter))
	}

	time.Sleep(250 * time.Millisecond) // Ensure no extra executions
	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected no additional calls, got %d", atomic.LoadInt32(&counter))
	}
}

func TestDebounce_WithMaxWait(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(500*time.Millisecond), WithDebounceMaxWait(100*time.Millisecond))
	fn := func() {
		atomic.AddInt32(&counter, 1)
	}

	debouncer.Do(fn)                  // Start, t=0
	time.Sleep(60 * time.Millisecond) // t=60
	debouncer.Do(fn)
	time.Sleep(60 * time.Millisecond) // t=120 >100, on this Do, timeLimitReached=true, flush immediate

	time.Sleep(50 * time.Millisecond) // Allow goroutine

	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected function to be called due to maxWait, got %d", atomic.LoadInt32(&counter))
	}

	time.Sleep(550 * time.Millisecond) // Ensure no extra
	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected no additional calls, got %d", atomic.LoadInt32(&counter))
	}
}

func TestDebounce_WithMaxWaitTimerOnly(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(500*time.Millisecond), WithDebounceMaxWait(100*time.Millisecond))
	fn := func() {
		atomic.AddInt32(&counter, 1)
	}

	debouncer.Do(fn)                  // Start, t=0, maxWaitTimer at 100ms
	time.Sleep(50 * time.Millisecond) // t=50 <100
	debouncer.Do(fn)                  // Reset delay timer, but maxWaitTimer continues

	time.Sleep(100 * time.Millisecond) // Wait for maxWaitTimer to fire at t=100

	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected function to be called by maxWait timer, got %d", atomic.LoadInt32(&counter))
	}

	time.Sleep(550 * time.Millisecond) // No extra
	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected no additional calls, got %d", atomic.LoadInt32(&counter))
	}
}

func TestDebounce_Flush(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(200 * time.Millisecond))
	debouncer.Do(func() {
		atomic.AddInt32(&counter, 1)
	})

	time.Sleep(50 * time.Millisecond) // Ensure hasn't fired
	if atomic.LoadInt32(&counter) != 0 {
		t.Error("Premature execution")
	}

	debouncer.Flush()
	time.Sleep(50 * time.Millisecond) // Allow goroutine

	if atomic.LoadInt32(&counter) != 1 {
		t.Errorf("Expected counter to be 1 after flush, got %d", atomic.LoadInt32(&counter))
	}

	time.Sleep(200 * time.Millisecond) // No extra
	if atomic.LoadInt32(&counter) != 1 {
		t.Error("Extra execution after flush")
	}
}

func TestDebounce_Cancel(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(50 * time.Millisecond))
	debouncer.Do(func() {
		atomic.AddInt32(&counter, 1)
	})

	debouncer.Cancel()
	time.Sleep(100 * time.Millisecond) // Wait longer than delay

	if atomic.LoadInt32(&counter) != 0 {
		t.Errorf("Expected counter to be 0 after cancel, got %d", atomic.LoadInt32(&counter))
	}
}

func TestDebounce_LastFunctionWins(t *testing.T) {
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})

	debouncer := NewDebouncer(WithDebounceDelay(50 * time.Millisecond))

	debouncer.Do(func() {
		close(firstDone)
	})
	time.Sleep(10 * time.Millisecond)
	debouncer.Do(func() {
		close(secondDone)
	})

	select {
	case <-secondDone:
		// Success: second function ran
	case <-time.After(100 * time.Millisecond):
		t.Error("Second function was not called")
	}

	select {
	case <-firstDone:
		t.Error("First function was called, but should have been overridden")
	case <-time.After(10 * time.Millisecond):
		// Success: first did not run
	}
}

func TestDebounce_NoExecutionIfNoCalls(t *testing.T) {
	_ = NewDebouncer(WithDebounceDelay(50 * time.Millisecond))
	time.Sleep(100 * time.Millisecond) // Wait, no Do
	// No assertion needed; test passes if no panic or unexpected behavior
}

func TestDebounce_CancelAfterFlush(t *testing.T) {
	var counter int32

	debouncer := NewDebouncer(WithDebounceDelay(200 * time.Millisecond))
	debouncer.Do(func() {
		atomic.AddInt32(&counter, 1)
	})

	debouncer.Flush()
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&counter) != 1 {
		t.Error("No execution after flush")
	}

	debouncer.Cancel() // Should be no-op
	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt32(&counter) != 1 {
		t.Error("Extra execution after cancel post-flush")
	}
}
