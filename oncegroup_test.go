package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnceGroup_Do(t *testing.T) {
	var g OnceGroup[string, int]
	val, err, shared := g.Do(context.Background(), "key", func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
	if shared {
		t.Fatal("first call should not be shared")
	}
}

func TestOnceGroup_Do_Coalesce(t *testing.T) {
	var g OnceGroup[string, int]
	var calls atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = g.Do(context.Background(), "key", func() (int, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return 1, nil
			})
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
	m := g.Metrics()
	if m.Coalesced != 9 {
		t.Fatalf("expected 9 coalesced, got %d", m.Coalesced)
	}
	if m.Completed != 1 {
		t.Fatalf("expected 1 completed, got %d", m.Completed)
	}
}

func TestOnceGroup_Do_Panic(t *testing.T) {
	var g OnceGroup[string, int]
	var panics atomic.Int32
	var barrier sync.WaitGroup
	barrier.Add(3)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
				}
				wg.Done()
			}()
			barrier.Done()
			barrier.Wait()
			g.Do(context.Background(), "key", func() (int, error) {
				time.Sleep(50 * time.Millisecond)
				panic("boom")
			})
		}()
	}

	wg.Wait()

	if panics.Load() != 3 {
		t.Fatalf("expected 3 panics, got %d", panics.Load())
	}
	if g.Metrics().Panics != 1 {
		t.Fatalf("expected 1 panic metric, got %d", g.Metrics().Panics)
	}
}

func TestOnceGroup_Do_Context(t *testing.T) {
	var g OnceGroup[string, int]

	ctx, cancel := context.WithCancel(context.Background())

	var started sync.WaitGroup
	started.Add(1)

	go func() {
		g.Do(context.Background(), "key", func() (int, error) {
			started.Done()
			time.Sleep(200 * time.Millisecond)
			return 42, nil
		})
	}()

	started.Wait()
	cancel()

	val, err, _ := g.Do(ctx, "key", func() (int, error) {
		return 0, nil
	})
	if err != context.Canceled {
		t.Fatalf("expected canceled, got %v", err)
	}
	if val != 0 {
		t.Fatalf("expected zero value on cancel")
	}
}

func TestOnceGroup_Do_Metrics(t *testing.T) {
	var g OnceGroup[string, int]
	g.Do(context.Background(), "a", func() (int, error) { return 1, nil })
	g.Do(context.Background(), "a", func() (int, error) { return 2, nil })

	m := g.Metrics()
	if m.InFlight != 0 {
		t.Fatalf("expected 0 in-flight, got %d", m.InFlight)
	}
	if m.Completed != 2 {
		t.Fatalf("expected 2 completed, got %d", m.Completed)
	}
	if m.Coalesced != 0 {
		t.Fatalf("expected 0 coalesced, got %d", m.Coalesced)
	}
}
