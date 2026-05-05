package jack

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Semaphore benchmarks

// BenchmarkSemaphoreTryAcquire measures the lock-free fast path.
// This should be near-zero allocation; any alloc indicates a regression.
func BenchmarkSemaphoreTryAcquire(b *testing.B) {
	s := NewSemaphore(b.N + 1)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.TryAcquire(PriorityHigh)
	}
}

// BenchmarkSemaphoreAcquireRelease measures the fast-path round-trip under no contention.
func BenchmarkSemaphoreAcquireRelease(b *testing.B) {
	s := NewSemaphore(1)
	defer s.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Acquire(ctx, PriorityHigh) //nolint:errcheck
		s.Release()
	}
}

// BenchmarkSemaphoreContended measures throughput when goroutines compete for a single slot.
// Allocation count here reflects waiter heap allocations on the slow path.
func BenchmarkSemaphoreContended(b *testing.B) {
	const workers = 32
	s := NewSemaphore(1)
	defer s.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(workers)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Acquire(ctx, PriorityHigh) //nolint:errcheck
			s.Release()
		}
	})
}

// BenchmarkSemaphorePriorityContended benchmarks mixed-priority contention.
// Verifies that priority selection overhead stays constant as queue depth grows.
func BenchmarkSemaphorePriorityContended(b *testing.B) {
	s := NewSemaphore(4)
	defer s.Close()
	ctx := context.Background()
	priorities := []Priority{PriorityCritical, PriorityHigh, PriorityMedium, PriorityLow}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p := priorities[i%len(priorities)]
			i++
			s.Acquire(ctx, p) //nolint:errcheck
			s.Release()
		}
	})
}

// RateLimiter benchmarks

// BenchmarkRateLimiterAllow measures the lock-free token consumption fast path.
func BenchmarkRateLimiterAllow(b *testing.B) {
	rl := NewRateLimiter(float64(b.N)*10, b.N+1)
	defer rl.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.Allow(PriorityHigh)
	}
}

// BenchmarkRateLimiterAcquireRelease measures round-trip with a full burst bucket.
func BenchmarkRateLimiterAcquireRelease(b *testing.B) {
	rl := NewRateLimiter(0, 1) // no refill; Release restores the token
	defer rl.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.Acquire(ctx, PriorityHigh) //nolint:errcheck
		rl.Release()
	}
}

// BenchmarkRateLimiterContended measures multi-goroutine throughput under token scarcity.
func BenchmarkRateLimiterContended(b *testing.B) {
	const workers = 32
	rl := NewRateLimiter(0, 1)
	defer rl.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(workers)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.Acquire(ctx, PriorityHigh) //nolint:errcheck
			rl.Release()
		}
	})
}

// Throttle benchmarks

// BenchmarkThrottleAllow measures the hot path when no throttling is active (prob==0).
// Should be a handful of atomics with zero allocations.
func BenchmarkThrottleAllow(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Allow(PriorityHigh)
	}
}

// BenchmarkThrottleAllowUnderLoad measures Allow when throttling is active (prob > 0).
func BenchmarkThrottleAllowUnderLoad(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
	// Drive probability non-zero by recording rejections.
	for i := 0; i < 50; i++ {
		t.Rejected(PriorityHigh)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Allow(PriorityHigh)
	}
}

// BenchmarkThrottleAllowParallel measures concurrent Allow calls to detect false sharing.
func BenchmarkThrottleAllowParallel(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t.Allow(PriorityHigh)
		}
	})
}

// BenchmarkThrottleAdjust measures the cost of the probability recalculation triggered
// by each Accepted/Rejected call. Should not allocate.
func BenchmarkThrottleAdjust(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
	// Seed enough samples so adjust() doesn't return early on total < 10.
	for i := 0; i < 15; i++ {
		t.Accepted(PriorityHigh)
		t.Rejected(PriorityHigh)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			t.Accepted(PriorityHigh)
		} else {
			t.Rejected(PriorityHigh)
		}
	}
}

// Cross-component benchmark

// BenchmarkSemaphoreHighConcurrency stress-tests the semaphore with many goroutines
// to surface lock contention and measure allocations per operation at scale.
func BenchmarkSemaphoreHighConcurrency(b *testing.B) {
	const slots = 16
	s := NewSemaphore(slots)
	defer s.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	start := make(chan struct{})

	work := func() {
		defer wg.Done()
		<-start
		for i := 0; i < b.N/64; i++ {
			s.Acquire(ctx, PriorityHigh) //nolint:errcheck
			time.Sleep(time.Microsecond)
			s.Release()
		}
	}

	const goroutines = 64
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go work()
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
}
