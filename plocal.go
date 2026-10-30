package jack

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
)

const plocalShards = 128

// plocalIndexFast returns a shard index for counters.
// Zero-allocation and contention-free: math/rand/v2's top-level source is
// per-P, so no shared cache line is touched (an earlier implementation used a
// single global atomic counter, which is precisely the contention this type
// exists to avoid and measured ~2x slower than one plain atomic.Int64).
func plocalIndexFast() uint {
	return uint(rand.Uint32()) % plocalShards
}

// getGoroutineID extracts the goroutine ID from the runtime stack trace.
// Used for stable shard affinity in PLocal[T].
func getGoroutineID() uint64 {
	var b [64]byte
	n := runtime.Stack(b[:], false)
	var id uint64
	// Skip "goroutine " prefix (10 bytes)
	for i := 10; i < n && b[i] >= '0' && b[i] <= '9'; i++ {
		id = id*10 + uint64(b[i]-'0')
	}
	return id
}

// plocalIndex returns a stable shard index for the current goroutine.
func plocalIndex() uint {
	return uint(getGoroutineID() % uint64(plocalShards))
}

// PLocalCounter is a high-throughput counter sharded across multiple
// independent slots to eliminate cache-line contention.
type PLocalCounter struct {
	shards [plocalShards]atomic.Int64
}

// Add adds n to the counter.
func (c *PLocalCounter) Add(n int64) {
	c.shards[plocalIndexFast()].Add(n)
}

// Value returns the current sum across all shards.
func (c *PLocalCounter) Value() int64 {
	var sum int64
	for i := range c.shards {
		sum += c.shards[i].Load()
	}
	return sum
}

// PLocal provides goroutine-sharded storage to reduce lock contention.
// Each goroutine accesses an independent shard with stable affinity.
//
// Shard affinity is derived by parsing the current goroutine ID from
// runtime.Stack, which costs a few microseconds per call. Use it for
// low-frequency, large-payload state (buffers, batches, caches), not as a
// drop-in for hot counters — PLocalCounter is the cheap option for those.
type PLocal[T any] struct {
	shards [plocalShards]plocalShard[T]
}

type plocalShard[T any] struct {
	mu  sync.Mutex
	val T
}

// With executes fn against the local shard for the current goroutine.
func (p *PLocal[T]) With(fn func(*T)) {
	i := plocalIndex()
	s := &p.shards[i]
	s.mu.Lock()
	fn(&s.val)
	s.mu.Unlock()
}

// Get returns a copy of the local shard value.
func (p *PLocal[T]) Get() T {
	i := plocalIndex()
	s := &p.shards[i]
	s.mu.Lock()
	v := s.val
	s.mu.Unlock()
	return v
}

// Set stores a copy of v into the local shard.
func (p *PLocal[T]) Set(v T) {
	i := plocalIndex()
	s := &p.shards[i]
	s.mu.Lock()
	s.val = v
	s.mu.Unlock()
}

// Fold aggregates values across all shards using the provided function.
// The accumulator is initialized to zeroValue and fn is called for each shard.
// Useful for summing counters or collecting metrics from all goroutines.
func (p *PLocal[T]) Fold(zeroValue T, fn func(acc, val T) T) T {
	acc := zeroValue
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		acc = fn(acc, s.val)
		s.mu.Unlock()
	}
	return acc
}
