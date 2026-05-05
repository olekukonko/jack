package jack

import (
	"sync"
	"testing"
	"time"
)

func TestThrottleAllow(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	for i := 0; i < 100; i++ {
		if !at.Allow(PriorityHigh) {
			t.Fatal("expected allow before any signals")
		}
	}
}

func TestThrottleRejection(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	for i := 0; i < 20; i++ {
		at.Rejected(PriorityHigh)
	}

	rejected := 0
	for i := 0; i < 1000; i++ {
		if !at.Allow(PriorityHigh) {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatal("expected some local rejections after upstream rejections")
	}
}

func TestThrottleRecovery(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	for i := 0; i < 20; i++ {
		at.Rejected(PriorityHigh)
	}

	rejected := 0
	for i := 0; i < 100; i++ {
		if !at.Allow(PriorityHigh) {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatal("expected throttling")
	}

	for i := 0; i < 100; i++ {
		at.Accepted(PriorityHigh)
	}

	for i := 0; i < 50; i++ {
		if !at.Allow(PriorityHigh) {
			t.Fatalf("expected allow after recovery at iteration %d", i)
		}
	}
}

func TestThrottlePriorityScaling(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	for i := 0; i < 20; i++ {
		at.Rejected(PriorityCritical)
		at.Rejected(PriorityHigh)
		at.Rejected(PriorityMedium)
		at.Rejected(PriorityLow)
	}

	critRej := 0
	highRej := 0
	medRej := 0
	lowRej := 0

	for i := 0; i < 500; i++ {
		if !at.Allow(PriorityCritical) {
			critRej++
		}
		if !at.Allow(PriorityHigh) {
			highRej++
		}
		if !at.Allow(PriorityMedium) {
			medRej++
		}
		if !at.Allow(PriorityLow) {
			lowRej++
		}
	}

	if critRej >= highRej {
		t.Fatalf("Critical should reject less than High: %d vs %d", critRej, highRej)
	}
	if highRej >= medRej {
		t.Fatalf("High should reject less than Medium: %d vs %d", highRej, medRej)
	}
	if medRej >= lowRej {
		t.Fatalf("Medium should reject less than Low: %d vs %d", medRej, lowRej)
	}
}

func TestThrottleClose(t *testing.T) {
	at := NewThrottle(4)
	at.Close()

	if at.Allow(PriorityHigh) {
		t.Fatal("expected deny after close")
	}
}

func TestThrottleMetrics(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	for i := 0; i < 10; i++ {
		at.Allow(PriorityHigh)
	}
	for i := 0; i < 5; i++ {
		at.Rejected(PriorityHigh)
	}

	m := at.Metrics()
	if m.RequestsTotal.Load() != 10 {
		t.Fatalf("expected 10 total, got %d", m.RequestsTotal.Load())
	}
	if m.Accepted.Load() != 10 {
		t.Fatalf("expected 10 accepted, got %d", m.Accepted.Load())
	}
	if m.RejectedRemote.Load() != 5 {
		t.Fatalf("expected 5 remote rejected, got %d", m.RejectedRemote.Load())
	}
}

func TestThrottleConcurrentStress(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			prio := Priority(id % 4)
			for j := 0; j < 500; j++ {
				if at.Allow(prio) {
					if j%3 == 0 {
						at.Rejected(prio)
					} else {
						at.Accepted(prio)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	m := at.Metrics()
	if m.RequestsTotal.Load() == 0 {
		t.Fatal("expected non-zero total")
	}
}

func TestThrottleOptions(t *testing.T) {
	at := NewThrottle(4,
		ThrottleWithRatio(3.0),
		ThrottleWithWindow(30*time.Second),
	)
	defer at.Close()

	if at.ratio != 3.0 {
		t.Fatalf("expected ratio 3.0, got %f", at.ratio)
	}
	if at.window != 30*time.Second {
		t.Fatalf("expected window 30s, got %v", at.window)
	}
}

func TestThrottleInvalidPriority(t *testing.T) {
	at := NewThrottle(4)
	defer at.Close()

	if !at.Allow(Priority(-1)) {
		t.Fatal("expected allow for invalid priority before signals")
	}
	if !at.Allow(Priority(100)) {
		t.Fatal("expected allow for out-of-range priority before signals")
	}
}
