package jack

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrBulkheadFull     = errors.New("bulkhead full")
	ErrBulkheadClosed   = errors.New("bulkhead closed")
	ErrBulkheadNotFound = errors.New("bulkhead partition not found")
)

// BulkheadMetrics tracks statistics for a single named partition.
type BulkheadMetrics struct {
	Acquired SemaphoreMetrics // reuses semaphore metrics per partition
}

// Bulkhead isolates failure domains by giving each named partition its own
// bounded concurrency budget backed by an independent Semaphore.
//
// When partition A is saturated, partition B is completely unaffected.
// This is the standard pattern for protecting a shared resource (e.g. a DB
// connection pool) from being monopolised by one upstream caller.
//
// Usage:
//
//	bh := jack.NewBulkhead(jack.BulkheadWithPartition("payments", 20),
//	                       jack.BulkheadWithPartition("reports",  5))
//
//	err := bh.Call(ctx, "payments", jack.PriorityHigh, func(ctx context.Context) error {
//	    return db.Query(ctx, ...)
//	})
type Bulkhead struct {
	mu         sync.RWMutex
	partitions map[string]*Semaphore
	defaultCap int
	opts       []SemaphoreOption
	closed     bool
}

// BulkheadOption configures a Bulkhead.
type BulkheadOption func(*Bulkhead)

// BulkheadWithPartition pre-registers a named partition with the given capacity.
func BulkheadWithPartition(name string, capacity int) BulkheadOption {
	return func(b *Bulkhead) {
		b.partitions[name] = NewSemaphore(capacity, b.opts...)
	}
}

// BulkheadWithDefaultCapacity sets the capacity used when auto-creating partitions
// on first use (default 10). Set to 0 to disable auto-creation.
func BulkheadWithDefaultCapacity(capacity int) BulkheadOption {
	return func(b *Bulkhead) { b.defaultCap = capacity }
}

// BulkheadWithSemaphoreOptions passes additional SemaphoreOptions to every
// partition semaphore (e.g. CoDel settings).
func BulkheadWithSemaphoreOptions(opts ...SemaphoreOption) BulkheadOption {
	return func(b *Bulkhead) { b.opts = append(b.opts, opts...) }
}

// NewBulkhead creates a Bulkhead with the given options.
func NewBulkhead(opts ...BulkheadOption) *Bulkhead {
	b := &Bulkhead{
		partitions: make(map[string]*Semaphore),
		defaultCap: 10,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Call executes fn within the named partition's concurrency limit.
// If the partition doesn't exist and defaultCap > 0, it is auto-created.
// Returns ErrBulkheadFull if all slots are taken and the context expires,
// ErrBulkheadNotFound if auto-creation is disabled and the partition is unknown.
func (b *Bulkhead) Call(ctx context.Context, partition string, p Priority, fn func(context.Context) error) error {
	sem, err := b.semaphore(partition)
	if err != nil {
		return err
	}
	if err := sem.Acquire(ctx, p); err != nil {
		if errors.Is(err, ErrSemaphoreClosed) {
			return ErrBulkheadClosed
		}
		return ErrBulkheadFull
	}
	defer sem.Release()
	return fn(ctx)
}

// TryCall executes fn immediately if a slot is available, returning ErrBulkheadFull
// otherwise. Never blocks.
func (b *Bulkhead) TryCall(ctx context.Context, partition string, p Priority, fn func(context.Context) error) error {
	sem, err := b.semaphore(partition)
	if err != nil {
		return err
	}
	if !sem.TryAcquire(p) {
		return ErrBulkheadFull
	}
	defer sem.Release()
	return fn(ctx)
}

// Metrics returns the semaphore metrics for the named partition, or nil if not found.
func (b *Bulkhead) Metrics(partition string) *SemaphoreMetrics {
	b.mu.RLock()
	s, ok := b.partitions[partition]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	return s.Metrics()
}

// Available returns the number of free concurrency slots for the named partition.
func (b *Bulkhead) Available(partition string) int {
	b.mu.RLock()
	s, ok := b.partitions[partition]
	b.mu.RUnlock()
	if !ok {
		return 0
	}
	return s.Available()
}

// Partitions returns a snapshot of all registered partition names.
func (b *Bulkhead) Partitions() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.partitions))
	for k := range b.partitions {
		names = append(names, k)
	}
	return names
}

// AddPartition registers a new named partition at runtime.
// If a partition with that name already exists it is left unchanged.
func (b *Bulkhead) AddPartition(name string, capacity int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.partitions[name]; !ok {
		b.partitions[name] = NewSemaphore(capacity, b.opts...)
	}
}

// Close shuts down all partitions. Subsequent calls return ErrBulkheadClosed.
func (b *Bulkhead) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for _, s := range b.partitions {
		s.Close()
	}
}

// semaphore resolves the named partition's Semaphore, auto-creating it if allowed.
func (b *Bulkhead) semaphore(partition string) (*Semaphore, error) {
	b.mu.RLock()
	s, ok := b.partitions[partition]
	b.mu.RUnlock()

	if ok {
		return s, nil
	}
	if b.defaultCap <= 0 {
		return nil, ErrBulkheadNotFound
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBulkheadClosed
	}
	// Double-check after acquiring write lock.
	if s, ok = b.partitions[partition]; ok {
		return s, nil
	}
	s = NewSemaphore(b.defaultCap, b.opts...)
	b.partitions[partition] = s
	return s, nil
}
