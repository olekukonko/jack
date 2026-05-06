package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrLeaseExpired  = errors.New("lease expired")
	ErrLeaseReleased = errors.New("lease already released")
	ErrLeaseInvalid  = errors.New("lease semaphore closed")
)

// LeaseMetrics tracks lease operational statistics.
type LeaseMetrics struct {
	Acquired atomic.Uint64 // leases successfully granted
	Released atomic.Uint64 // leases explicitly released
	Expired  atomic.Uint64 // leases reclaimed by the reaper
	Active   atomic.Int64  // leases currently held
}

// Lease represents a time-bounded slot acquired from a Semaphore.
// If the holder fails to call Release before the deadline, the Reaper
// automatically reclaims the slot so it is never permanently lost.
//
// Obtain a Lease via Leaser.Acquire, not by constructing directly.
type Lease struct {
	id       string
	sem      *Semaphore
	manager  *Leaser
	released atomic.Bool
	deadline time.Time
}

// ID returns the lease's unique identifier.
func (l *Lease) ID() string { return l.id }

// Deadline returns when this lease will be automatically reclaimed.
func (l *Lease) Deadline() time.Time { return l.deadline }

// Release explicitly returns the semaphore slot and cancels the reaper timer.
// Calling Release more than once is safe — subsequent calls are no-ops.
func (l *Lease) Release() error {
	if !l.released.CompareAndSwap(false, true) {
		return ErrLeaseReleased
	}
	l.manager.reaper.Remove(l.id)
	l.sem.Release()
	l.manager.metrics.Released.Add(1)
	l.manager.metrics.Active.Add(-1)
	return nil
}

// LeaserOption configures a Leaser.
type LeaserOption func(*Leaser)

// LeaserWithTTL sets the default lease lifetime (default 30s).
func LeaserWithTTL(d time.Duration) LeaserOption {
	return func(lm *Leaser) {
		if d > 0 {
			lm.defaultTTL = d
		}
	}
}

// Leaser wraps a Semaphore and a Reaper to provide leases: acquired
// semaphore slots with automatic reclamation on expiry.
//
// This solves the problem where a crashed or slow holder never calls Release,
// permanently consuming a concurrency slot. The Reaper fires after TTL and
// calls Release on the holder's behalf.
type Leaser struct {
	sem        *Semaphore
	reaper     *Reaper
	defaultTTL time.Duration
	mu         sync.Mutex
	leases     map[string]*Lease
	metrics    *LeaseMetrics
}

// NewLeaser creates a Leaser backed by the given Semaphore.
// The caller owns the Semaphore and Reaper lifecycles; call Close on both
// when done.
func NewLeaser(sem *Semaphore, opts ...LeaserOption) *Leaser {
	lm := &Leaser{
		sem:        sem,
		defaultTTL: 30 * time.Second,
		leases:     make(map[string]*Lease),
		metrics:    &LeaseMetrics{},
	}
	for _, opt := range opts {
		opt(lm)
	}
	lm.reaper = NewReaper(lm.defaultTTL, ReaperWithHandler(lm.reclaim))
	return lm
}

// Acquire waits for a semaphore slot and returns a Lease valid for ttl.
// If ttl is zero the manager's default TTL is used.
// The Lease must be Released when the work is done; if not, the Reaper
// reclaims it automatically after ttl.
func (lm *Leaser) Acquire(ctx context.Context, id string, p Priority, ttl time.Duration) (*Lease, error) {
	if ttl <= 0 {
		ttl = lm.defaultTTL
	}
	if err := lm.sem.Acquire(ctx, p); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(ttl)
	lease := &Lease{
		id:       id,
		sem:      lm.sem,
		manager:  lm,
		deadline: deadline,
	}

	lm.mu.Lock()
	lm.leases[id] = lease
	lm.mu.Unlock()

	lm.reaper.TouchAt(id, deadline)
	lm.metrics.Acquired.Add(1)
	lm.metrics.Active.Add(1)
	return lease, nil
}

// Metrics returns the manager's operational metrics.
func (lm *Leaser) Metrics() *LeaseMetrics { return lm.metrics }

// reclaim is called by the Reaper when a lease's TTL expires.
// It releases the semaphore slot on the holder's behalf.
func (lm *Leaser) reclaim(_ context.Context, id string) {
	lm.mu.Lock()
	lease, ok := lm.leases[id]
	if ok {
		delete(lm.leases, id)
	}
	lm.mu.Unlock()

	if !ok {
		return
	}
	if lease.released.CompareAndSwap(false, true) {
		lm.sem.Release()
		lm.metrics.Expired.Add(1)
		lm.metrics.Active.Add(-1)
	}
}

// Close stops the internal Reaper. The backing Semaphore is not closed.
func (lm *Leaser) Close() {
	lm.reaper.Stop()
}
