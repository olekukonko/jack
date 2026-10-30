package jack

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestLeaseAcquireRelease(t *testing.T) {
	sem := NewSemaphore(2)
	defer sem.Close()
	lm := NewLeaser(sem)
	defer lm.Close()

	lease, err := lm.Acquire(context.Background(), "op-1", PriorityHigh, time.Second)
	if err != nil {
		t.Fatalf("unexpected acquire error: %v", err)
	}
	if lease.ID() != "op-1" {
		t.Fatalf("expected ID op-1, got %s", lease.ID())
	}
	if sem.Available() != 1 {
		t.Fatalf("expected 1 available slot after acquire, got %d", sem.Available())
	}

	if err := lease.Release(); err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}
	if sem.Available() != 2 {
		t.Fatalf("expected 2 available slots after release, got %d", sem.Available())
	}
}

func TestLeaseDoubleRelease(t *testing.T) {
	sem := NewSemaphore(1)
	defer sem.Close()
	lm := NewLeaser(sem)
	defer lm.Close()

	lease, _ := lm.Acquire(context.Background(), "x", PriorityHigh, time.Second)
	lease.Release() //nolint:errcheck
	err := lease.Release()
	if !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("expected ErrLeaseReleased on second call, got %v", err)
	}
}

func TestLeaseAutoExpiry(t *testing.T) {
	sem := NewSemaphore(1)
	defer sem.Close()
	lm := NewLeaser(sem, LeaserWithTTL(30*time.Millisecond))
	defer lm.Close()

	_, err := lm.Acquire(context.Background(), "expiring", PriorityHigh, 0)
	if err != nil {
		t.Fatalf("unexpected acquire error: %v", err)
	}
	if sem.Available() != 0 {
		t.Fatal("expected slot consumed after acquire")
	}

	// Wait for TTL to expire and reaper to reclaim.
	time.Sleep(150 * time.Millisecond)

	if sem.Available() != 1 {
		t.Fatalf("expected slot reclaimed after TTL, available=%d", sem.Available())
	}
	if lm.Metrics().Expired.Load() != 1 {
		t.Fatalf("expected 1 expired lease, got %d", lm.Metrics().Expired.Load())
	}
}

func TestLeaseMetrics(t *testing.T) {
	sem := NewSemaphore(3)
	defer sem.Close()
	lm := NewLeaser(sem)
	defer lm.Close()

	l1, _ := lm.Acquire(context.Background(), "a", PriorityHigh, time.Second)
	l2, _ := lm.Acquire(context.Background(), "b", PriorityHigh, time.Second)
	l1.Release() //nolint:errcheck
	l2.Release() //nolint:errcheck

	m := lm.Metrics()
	if m.Acquired.Load() != 2 {
		t.Fatalf("expected 2 acquired, got %d", m.Acquired.Load())
	}
	if m.Released.Load() != 2 {
		t.Fatalf("expected 2 released, got %d", m.Released.Load())
	}
	if m.Active.Load() != 0 {
		t.Fatalf("expected 0 active, got %d", m.Active.Load())
	}
}

func TestLeaseContextCancellation(t *testing.T) {
	sem := NewSemaphore(1)
	defer sem.Close()
	lm := NewLeaser(sem)
	defer lm.Close()

	// Fill the semaphore.
	sem.TryAcquire(PriorityHigh)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := lm.Acquire(ctx, "blocked", PriorityHigh, time.Second)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestLeaseDeadline(t *testing.T) {
	sem := NewSemaphore(1)
	defer sem.Close()
	lm := NewLeaser(sem)
	defer lm.Close()

	ttl := 500 * time.Millisecond
	lease, _ := lm.Acquire(context.Background(), "dl", PriorityHigh, ttl)
	defer lease.Release() //nolint:errcheck

	remaining := time.Until(lease.Deadline())
	if remaining <= 0 || remaining > ttl {
		t.Fatalf("unexpected deadline: remaining=%v ttl=%v", remaining, ttl)
	}
}

// TestLeaseReleaseDropsBookkeeping guards against the unbounded growth of the
// internal lease map: a normally released lease must be forgotten.
func TestLeaseReleaseDropsBookkeeping(t *testing.T) {
	sem := NewSemaphore(1)
	defer sem.Close()
	lm := NewLeaser(sem, LeaserWithTTL(time.Hour))
	defer lm.Close()

	const cycles = 500
	for i := 0; i < cycles; i++ {
		lease, err := lm.Acquire(context.Background(), fmt.Sprintf("lease-%d", i), PriorityHigh, time.Hour)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if err := lease.Release(); err != nil {
			t.Fatalf("release: %v", err)
		}
	}

	lm.mu.Lock()
	retained := len(lm.leases)
	lm.mu.Unlock()

	if retained != 0 {
		t.Fatalf("lease bookkeeping retained %d entries after %d acquire/release cycles", retained, cycles)
	}
	if got := sem.Available(); got != 1 {
		t.Fatalf("expected the slot to be returned, available=%d", got)
	}
	if got := lm.Metrics().Active.Load(); got != 0 {
		t.Fatalf("expected 0 active leases, got %d", got)
	}
}

// TestLeaseDuplicateIDsAreIndependent covers leases that share a caller-supplied
// ID: each must own its slot and its own bookkeeping entry.
func TestLeaseDuplicateIDsAreIndependent(t *testing.T) {
	sem := NewSemaphore(2)
	defer sem.Close()
	lm := NewLeaser(sem)
	defer lm.Close()

	l1, err := lm.Acquire(context.Background(), "same-id", PriorityHigh, time.Hour)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	l2, err := lm.Acquire(context.Background(), "same-id", PriorityHigh, time.Hour)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if l1 == l2 {
		t.Fatal("expected two distinct leases")
	}
	if got := sem.Available(); got != 0 {
		t.Fatalf("expected both slots consumed, available=%d", got)
	}

	if err := l1.Release(); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if got := sem.Available(); got != 1 {
		t.Fatalf("expected 1 free slot after first release, available=%d", got)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("release second: %v", err)
	}
	if got := sem.Available(); got != 2 {
		t.Fatalf("expected both slots returned, available=%d", got)
	}

	lm.mu.Lock()
	retained := len(lm.leases)
	lm.mu.Unlock()
	if retained != 0 {
		t.Fatalf("expected no retained lease entries, got %d", retained)
	}
}
