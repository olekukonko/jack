package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBulkheadBasicIsolation(t *testing.T) {
	bh := NewBulkhead(
		BulkheadWithPartition("a", 2),
		BulkheadWithPartition("b", 1),
	)
	defer bh.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Fill partition b completely.
	hold := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		bh.Call(ctx, "b", PriorityHigh, func(ctx context.Context) error { //nolint:errcheck
			<-hold
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	// Partition a should still be fully available.
	done := make(chan error, 1)
	go func() {
		done <- bh.Call(ctx, "a", PriorityHigh, func(context.Context) error { return nil })
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("partition a should not be affected by partition b: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("partition a call timed out")
	}

	close(hold)
	wg.Wait()
}

func TestBulkheadFull(t *testing.T) {
	bh := NewBulkhead(BulkheadWithPartition("x", 1))
	defer bh.Close()

	hold := make(chan struct{})
	go func() {
		bh.Call(context.Background(), "x", PriorityHigh, func(ctx context.Context) error { //nolint:errcheck
			<-hold
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := bh.Call(ctx, "x", PriorityHigh, func(context.Context) error { return nil })
	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull, got %v", err)
	}
	close(hold)
}

func TestBulkheadAutoCreate(t *testing.T) {
	bh := NewBulkhead(BulkheadWithDefaultCapacity(5))
	defer bh.Close()

	err := bh.Call(context.Background(), "auto", PriorityHigh, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("expected auto-created partition to work, got %v", err)
	}
}

func TestBulkheadNoAutoCreate(t *testing.T) {
	bh := NewBulkhead(BulkheadWithDefaultCapacity(0))
	defer bh.Close()

	err := bh.Call(context.Background(), "missing", PriorityHigh, func(context.Context) error { return nil })
	if !errors.Is(err, ErrBulkheadNotFound) {
		t.Fatalf("expected ErrBulkheadNotFound, got %v", err)
	}
}

func TestBulkheadTryCall(t *testing.T) {
	bh := NewBulkhead(BulkheadWithPartition("q", 1))
	defer bh.Close()

	hold := make(chan struct{})
	go func() {
		bh.Call(context.Background(), "q", PriorityHigh, func(ctx context.Context) error { //nolint:errcheck
			<-hold
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	err := bh.TryCall(context.Background(), "q", PriorityHigh, func(context.Context) error { return nil })
	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull from TryCall, got %v", err)
	}
	close(hold)
}

func TestBulkheadMetrics(t *testing.T) {
	bh := NewBulkhead(BulkheadWithPartition("m", 3))
	defer bh.Close()

	for i := 0; i < 5; i++ {
		bh.Call(context.Background(), "m", PriorityHigh, func(context.Context) error { return nil }) //nolint:errcheck
	}

	m := bh.Metrics("m")
	if m == nil {
		t.Fatal("expected non-nil metrics for partition m")
	}
	if m.AcquiredFast.Load() == 0 {
		t.Fatal("expected non-zero AcquiredFast")
	}
}

func TestBulkheadAvailable(t *testing.T) {
	bh := NewBulkhead(BulkheadWithPartition("av", 4))
	defer bh.Close()

	if bh.Available("av") != 4 {
		t.Fatalf("expected 4 available, got %d", bh.Available("av"))
	}
	if bh.Available("nope") != 0 {
		t.Fatal("expected 0 for unknown partition")
	}
}

func TestBulkheadAddPartition(t *testing.T) {
	bh := NewBulkhead()
	defer bh.Close()

	bh.AddPartition("dynamic", 3)
	if bh.Available("dynamic") != 3 {
		t.Fatalf("expected 3 available, got %d", bh.Available("dynamic"))
	}

	// Adding again should not overwrite.
	bh.AddPartition("dynamic", 99)
	if bh.Available("dynamic") != 3 {
		t.Fatal("AddPartition should not overwrite existing partition")
	}
}

func TestBulkheadConcurrentStress(t *testing.T) {
	bh := NewBulkhead(
		BulkheadWithPartition("svc-a", 10),
		BulkheadWithPartition("svc-b", 5),
	)
	defer bh.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	var completed atomic.Uint64

	partitions := []string{"svc-a", "svc-b"}
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p := partitions[id%len(partitions)]
			for j := 0; j < 50; j++ {
				timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				if err := bh.Call(timeoutCtx, p, PriorityHigh, func(context.Context) error {
					time.Sleep(time.Microsecond)
					return nil
				}); err == nil {
					completed.Add(1)
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()

	if completed.Load() == 0 {
		t.Fatal("expected some completions")
	}
}
