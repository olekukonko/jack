package jack

import (
	"sync"
	"sync/atomic"
)

const plocalShards = 128

var plocalIndexCounter atomic.Uint64

var plocalSlotPool = sync.Pool{
	New: func() any {
		return &plocalSlot{
			idx: uint(plocalIndexCounter.Add(1) % uint64(plocalShards)),
		}
	},
}

type plocalSlot struct {
	idx uint
}

// plocalIndex returns a stable shard index for the current goroutine.
// It uses sync.Pool to leverage the per-P cache, giving high affinity.
func plocalIndex() uint {
	slot := plocalSlotPool.Get().(*plocalSlot)
	defer plocalSlotPool.Put(slot)
	return slot.idx
}

// PLocalCounter is a high-throughput counter sharded across multiple
// independent slots to eliminate cache-line contention.
type PLocalCounter struct {
	shards [plocalShards]atomic.Int64
}

// Add adds n to the counter.
func (c *PLocalCounter) Add(n int64) {
	c.shards[plocalIndex()].Add(n)
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
