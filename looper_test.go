package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLooperBasic(t *testing.T) {
	var counter atomic.Int32

	looper := NewLooper(
		func() error {
			counter.Add(1)
			return nil
		},
		WithLooperInterval(10*time.Millisecond),
		WithLooperName("test-basic"),
	)

	looper.Start()
	time.Sleep(50 * time.Millisecond)
	looper.Stop()

	if count := counter.Load(); count < 4 {
		t.Errorf("expected at least 4 executions, got %d", count)
	}
}

func TestLooperImmediate(t *testing.T) {
	var counter atomic.Int32

	looper := NewLooper(
		func() error {
			counter.Add(1)
			return nil
		},
		WithLooperInterval(50*time.Millisecond),
		WithLooperImmediate(true),
	)

	looper.Start()

	// Should have executed immediately
	if counter.Load() != 1 {
		t.Errorf("expected immediate execution, got %d", counter.Load())
	}

	time.Sleep(60 * time.Millisecond)
	looper.Stop()

	if counter.Load() < 2 {
		t.Errorf("expected at least 2 executions, got %d", counter.Load())
	}
}

func TestLooperBackoff(t *testing.T) {
	var counter atomic.Int32
	failUntil := int32(3)

	looper := NewLooper(
		func() error {
			count := counter.Add(1)
			if count <= failUntil {
				return errors.New("simulated failure")
			}
			return nil
		},
		WithLooperInterval(10*time.Millisecond),
		WithLooperBackoff(true),
		WithLooperMaxInterval(100*time.Millisecond),
	)

	looper.Start()
	time.Sleep(200 * time.Millisecond)
	looper.Stop()

	// Should have backed off after failures
	if counter.Load() < 5 {
		t.Errorf("expected at least 5 executions, got %d", counter.Load())
	}
}

func TestLooperJitter(t *testing.T) {
	var executionTimes []time.Time
	var mu sync.Mutex

	looper := NewLooper(
		func() error {
			mu.Lock()
			executionTimes = append(executionTimes, time.Now())
			mu.Unlock()
			return nil
		},
		WithLooperInterval(20*time.Millisecond),
		WithLooperJitter(0.5), // 50% jitter
	)

	looper.Start()
	time.Sleep(150 * time.Millisecond)
	looper.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(executionTimes) < 3 {
		t.Errorf("expected at least 3 executions, got %d", len(executionTimes))
	}

	// Check that intervals vary (not all exactly 20ms)
	var intervals []time.Duration
	for i := 1; i < len(executionTimes); i++ {
		intervals = append(intervals, executionTimes[i].Sub(executionTimes[i-1]))
	}

	// If all intervals are identical, jitter isn't working
	allSame := true
	for i := 1; i < len(intervals); i++ {
		if intervals[i] != intervals[0] {
			allSame = false
			break
		}
	}

	if allSame && len(intervals) > 1 {
		t.Error("all intervals identical, jitter not applied")
	}
}

func TestLooperDynamicInterval(t *testing.T) {
	var counter atomic.Int32

	looper := NewLooper(
		func() error {
			counter.Add(1)
			return nil
		},
		WithLooperInterval(50*time.Millisecond),
	)

	looper.Start()
	time.Sleep(60 * time.Millisecond)

	// Should have executed once
	if counter.Load() != 1 {
		t.Fatalf("expected 1 execution, got %d", counter.Load())
	}

	// Speed up
	looper.SetInterval(10 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	looper.Stop()

	// Should have executed more times after speedup
	if counter.Load() < 3 {
		t.Errorf("expected at least 3 total executions, got %d", counter.Load())
	}
}

func TestLooperContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	looper := NewLooper(
		func() error {
			return nil
		},
		WithLooperInterval(10*time.Millisecond),
		WithLooperContext(ctx),
	)

	looper.Start()

	// Cancel context
	cancel()

	// Should stop quickly
	done := make(chan struct{})
	go func() {
		looper.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("looper didn't stop after context cancel")
	}
}

func TestLooperPanicRecovery(t *testing.T) {
	var counter atomic.Int32

	looper := NewLooper(
		func() error {
			count := counter.Add(1)
			if count == 1 {
				panic("simulated panic")
			}
			return nil
		},
		WithLooperInterval(10*time.Millisecond),
	)

	looper.Start()
	time.Sleep(50 * time.Millisecond)
	looper.Stop()

	// Should have recovered and continued
	if counter.Load() < 3 {
		t.Errorf("expected at least 3 executions after panic, got %d", counter.Load())
	}
}

func TestLooperStopIdempotent(t *testing.T) {
	var counter atomic.Int32

	looper := NewLooper(
		func() error {
			counter.Add(1)
			return nil
		},
		WithLooperInterval(10*time.Millisecond),
	)

	looper.Start()
	time.Sleep(20 * time.Millisecond)

	// Stop multiple times
	looper.Stop()
	looper.Stop()
	looper.Stop()

	// Should not panic and counter should stop increasing
	count1 := counter.Load()
	time.Sleep(30 * time.Millisecond)
	count2 := counter.Load()

	if count2 != count1 {
		t.Error("counter increased after stop")
	}
}

func TestLooperMetrics(t *testing.T) {
	looper := NewLooper(
		func() error {
			return errors.New("fail")
		},
		WithLooperInterval(10*time.Millisecond),
		WithLooperBackoff(true),
	)

	looper.Start()
	time.Sleep(50 * time.Millisecond)
	looper.Stop()

	metrics := looper.Metrics()

	if metrics.Executions.Load() == 0 {
		t.Error("expected executions > 0")
	}
	if metrics.Failures.Load() == 0 {
		t.Error("expected failures > 0")
	}
	if metrics.BackoffEvents.Load() == 0 {
		t.Error("expected backoff events > 0")
	}
	if metrics.IntervalChanges.Load() == 0 {
		t.Error("expected interval changes > 0")
	}
}

func TestLooperNilTask(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic with nil task")
		}
	}()

	NewLooper(nil)
}
