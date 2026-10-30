package jack

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestPLocalCounter(t *testing.T) {
	var c PLocalCounter
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Add(1)
			}
		}()
	}
	wg.Wait()
	if v := c.Value(); v != 100000 {
		t.Fatalf("expected 100000, got %d", v)
	}
}

func TestPLocal(t *testing.T) {
	var p PLocal[int]
	p.Set(42)
	if v := p.Get(); v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	p.With(func(v *int) {
		*v = 100
	})
	if v := p.Get(); v != 100 {
		t.Fatalf("expected 100, got %d", v)
	}
}

func TestPLocal_Concurrent(t *testing.T) {
	var p PLocal[int]
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.With(func(v *int) {
				*v++
			})
		}()
	}
	wg.Wait()
	if p.Fold(0, func(a, b int) int { return a + b }) < 1 {
		t.Fatal("expected positive value")
	}
}

// TestPLocalCounterConcurrent checks that concurrent Adds across shards still
// produce the exact total (distribution is irrelevant for a sum).
func TestPLocalCounterConcurrent(t *testing.T) {
	var c PLocalCounter
	const goroutines, per = 8, 1000

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				c.Add(1)
			}
		}()
	}
	wg.Wait()

	if got, want := c.Value(), int64(goroutines*per); got != want {
		t.Fatalf("expected %d, got %d", want, got)
	}
}

// BenchmarkPLocalCounterAddParallel measures the sharded counter against a plain
// atomic under concurrency; the shards must not contend on a shared cache line.
func BenchmarkPLocalCounterAddParallel(b *testing.B) {
	var c PLocalCounter
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkAtomicInt64AddParallel(b *testing.B) {
	var c atomic.Int64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}
