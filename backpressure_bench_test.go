package jack

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Semaphore benchmarks

func BenchmarkSemaphoreTryAcquire(b *testing.B) {
	s := NewSemaphore(b.N + 1)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.TryAcquire(PriorityHigh)
	}
}

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

// BenchmarkSemaphoreContended uses TryAcquire to isolate atomic/mutex contention
// without ever blocking goroutines — avoids deadlock when RunParallel terminates.
func BenchmarkSemaphoreContended(b *testing.B) {
	cap := b.N + 1
	if cap < 256 {
		cap = 256
	}
	s := NewSemaphore(cap)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if s.TryAcquire(PriorityHigh) {
				s.Release()
			}
		}
	})
}

func BenchmarkSemaphorePriorityContended(b *testing.B) {
	cap := b.N + 1
	if cap < 256 {
		cap = 256
	}
	s := NewSemaphore(cap)
	defer s.Close()
	priorities := []Priority{PriorityCritical, PriorityHigh, PriorityMedium, PriorityLow}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p := priorities[i%len(priorities)]
			i++
			if s.TryAcquire(p) {
				s.Release()
			}
		}
	})
}

// BenchmarkSemaphoreBlockingContended measures the slow (blocking) path.
// Each iteration acquires then immediately releases, so the benchmark goroutines
// naturally progress without needing an external releaser goroutine.
// Capacity matches parallelism so every goroutine can acquire without waiting —
// this isolates the slow-path channel overhead from queue-wait latency.
func BenchmarkSemaphoreBlockingContended(b *testing.B) {
	// Give each goroutine its own slot so Acquire always succeeds immediately
	// via the slow path (channel) rather than deadlocking on a shared slot.
	workers := b.N + 1
	if workers < 256 {
		workers = 256
	}
	s := NewSemaphore(workers)
	defer s.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Acquire(ctx, PriorityHigh); err != nil {
				return
			}
			s.Release()
		}
	})
}

// RateLimiter benchmarks

func BenchmarkRateLimiterAllow(b *testing.B) {
	rl := NewRateLimiter(float64(b.N)*10, b.N+1)
	defer rl.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.Allow(PriorityHigh)
	}
}

func BenchmarkRateLimiterAcquireRelease(b *testing.B) {
	rl := NewRateLimiter(0, 1)
	defer rl.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.Acquire(ctx, PriorityHigh) //nolint:errcheck
		rl.Release()
	}
}

// BenchmarkRateLimiterContended measures concurrent Allow (fast-path only, no blocking).
func BenchmarkRateLimiterContended(b *testing.B) {
	total := b.N + 1
	if total < 1<<20 {
		total = 1 << 20
	}
	rl := NewRateLimiter(float64(total)*100, total)
	defer rl.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.Allow(PriorityHigh)
		}
	})
}

// Throttle benchmarks

func BenchmarkThrottleAllow(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Allow(PriorityHigh)
	}
}

func BenchmarkThrottleAllowUnderLoad(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
	for i := 0; i < 50; i++ {
		t.Rejected(PriorityHigh)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Allow(PriorityHigh)
	}
}

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

func BenchmarkThrottleAdjust(b *testing.B) {
	t := NewThrottle(priorityCount)
	defer t.Close()
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

// Queue benchmarks

// BenchmarkQueueEnqueue measures the Enqueue lock+append path.
// Workers consume items instantly so Close() never blocks.
func BenchmarkQueueEnqueue(b *testing.B) {
	q := NewQueue(func(_ context.Context, _ any) error { return nil },
		QueueWithCapacity(b.N+2), QueueWithWorkers(1))
	defer q.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(PriorityHigh, i) //nolint:errcheck
	}
}

func BenchmarkQueueThroughput(b *testing.B) {
	q := NewQueue(func(_ context.Context, _ any) error { return nil },
		QueueWithWorkers(8), QueueWithCapacity(65536))
	defer q.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Enqueue(PriorityHigh, i) //nolint:errcheck
			i++
		}
	})
}

// Pool benchmarks

func BenchmarkPoolSubmit(b *testing.B) {
	p := NewPool(1, PoolingWithQueueSize(b.N+1))
	defer p.Shutdown(5 * time.Second) //nolint:errcheck
	task := Func(func() error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Submit(task) //nolint:errcheck
	}
}

func BenchmarkPoolThroughput(b *testing.B) {
	p := NewPool(8, PoolingWithQueueSize(65536))
	defer p.Shutdown(5 * time.Second) //nolint:errcheck
	task := Func(func() error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Submit(task) //nolint:errcheck
		}
	})
}

// BenchmarkPoolSubmitNoID measures Submit with ID generation disabled.
// Compare against BenchmarkPoolSubmit to see the allocation cost of task IDs.
func BenchmarkPoolSubmitNoID(b *testing.B) {
	p := NewPool(1, PoolingWithQueueSize(b.N+1), PoolingWithNoID())
	defer p.Shutdown(5 * time.Second) //nolint:errcheck
	task := Func(func() error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Submit(task) //nolint:errcheck
	}
}

// BenchmarkPoolThroughputNoID measures full round-trip throughput without ID generation.
func BenchmarkPoolThroughputNoID(b *testing.B) {
	p := NewPool(8, PoolingWithQueueSize(65536), PoolingWithNoID())
	defer p.Shutdown(5 * time.Second) //nolint:errcheck
	task := Func(func() error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Submit(task) //nolint:errcheck
		}
	})
}

// Cross-component

func BenchmarkSemaphoreHighConcurrency(b *testing.B) {
	const slots = 16
	s := NewSemaphore(slots)
	defer s.Close()

	var wg sync.WaitGroup
	start := make(chan struct{})
	const goroutines = 64

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < b.N/goroutines+1; j++ {
				if s.TryAcquire(PriorityHigh) {
					s.Release()
				}
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
}
