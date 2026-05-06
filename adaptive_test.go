package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveLimiterBasicCall(t *testing.T) {
	a := NewAdaptiveLimiter(AdaptiveWithInitialLimit(5))
	defer a.Close()

	err := a.Call(context.Background(), PriorityHigh, func(context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if a.Metrics().Acquired.Load() != 1 {
		t.Fatal("expected 1 acquired")
	}
}

func TestAdaptiveLimiterIncreasesUnderLowRTT(t *testing.T) {
	a := NewAdaptiveLimiter(
		AdaptiveWithInitialLimit(5),
		AdaptiveWithTargetP50(200*time.Millisecond),
	)
	defer a.Close()

	initial := a.Limit()
	for i := 0; i < 20; i++ {
		a.Call(context.Background(), PriorityHigh, func(context.Context) error {
			time.Sleep(time.Microsecond) // << target, triggers increase
			return nil
		}) //nolint:errcheck
	}

	if a.Limit() <= initial {
		t.Logf("limit did not increase: initial=%d current=%d (may be at max already)", initial, a.Limit())
	}
	if a.Metrics().LimitIncr.Load() == 0 {
		t.Fatal("expected at least one limit increase")
	}
}

func TestAdaptiveLimiterDecreasesUnderHighRTT(t *testing.T) {
	a := NewAdaptiveLimiter(
		AdaptiveWithInitialLimit(20),
		AdaptiveWithTargetP50(time.Microsecond), // very tight target
		AdaptiveWithMinLimit(1),
	)
	defer a.Close()

	for i := 0; i < 30; i++ {
		a.Call(context.Background(), PriorityHigh, func(context.Context) error {
			time.Sleep(5 * time.Millisecond) // >> 1µs target, triggers decrease
			return nil
		}) //nolint:errcheck
	}

	if a.Metrics().LimitDecr.Load() == 0 {
		t.Fatal("expected at least one limit decrease")
	}
}

func TestAdaptiveLimiterMetrics(t *testing.T) {
	a := NewAdaptiveLimiter(AdaptiveWithInitialLimit(3))
	defer a.Close()

	for i := 0; i < 5; i++ {
		a.Call(context.Background(), PriorityHigh, func(context.Context) error { return nil }) //nolint:errcheck
	}

	m := a.Metrics()
	if m.Acquired.Load() != 5 {
		t.Fatalf("expected 5 acquired, got %d", m.Acquired.Load())
	}
	if m.CurrentLimit.Load() <= 0 {
		t.Fatal("expected positive current limit")
	}
	if m.AvgRTTNs.Load() <= 0 {
		t.Fatal("expected positive EWMA RTT")
	}
}

func TestAdaptiveLimiterLimitBounds(t *testing.T) {
	a := NewAdaptiveLimiter(
		AdaptiveWithInitialLimit(2),
		AdaptiveWithMinLimit(2),
		AdaptiveWithMaxLimit(4),
		AdaptiveWithTargetP50(time.Microsecond),
	)
	defer a.Close()

	for i := 0; i < 100; i++ {
		a.Call(context.Background(), PriorityHigh, func(context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}) //nolint:errcheck
	}

	if a.Limit() < 2 {
		t.Fatalf("limit dropped below minLimit: %d", a.Limit())
	}
}

func TestAdaptiveLimiterInFlightTracking(t *testing.T) {
	a := NewAdaptiveLimiter(AdaptiveWithInitialLimit(10))
	defer a.Close()

	start := make(chan struct{})
	var wg sync.WaitGroup
	var maxInFlight atomic.Int64

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.Call(context.Background(), PriorityHigh, func(context.Context) error {
				<-start
				cur := a.InFlight()
				for {
					old := maxInFlight.Load()
					if int64(cur) <= old {
						break
					}
					if maxInFlight.CompareAndSwap(old, int64(cur)) {
						break
					}
				}
				return nil
			}) //nolint:errcheck
		}()
	}

	time.Sleep(20 * time.Millisecond)
	close(start)
	wg.Wait()

	if maxInFlight.Load() == 0 {
		t.Fatal("expected non-zero in-flight count during execution")
	}
}

func TestAdaptiveLimiterConcurrentStress(t *testing.T) {
	a := NewAdaptiveLimiter(
		AdaptiveWithInitialLimit(8),
		AdaptiveWithMinLimit(1),
		AdaptiveWithMaxLimit(32),
	)
	defer a.Close()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				a.Call(ctx, PriorityHigh, func(context.Context) error { //nolint:errcheck
					time.Sleep(time.Microsecond)
					return nil
				})
				cancel()
			}
		}()
	}
	wg.Wait()

	if a.Metrics().Acquired.Load() == 0 {
		t.Fatal("expected some acquisitions")
	}
}
