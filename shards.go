package jack

import (
	"math/bits"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// ShardMap provides a generic sharded map with cleanup utilities.
// Key K must be comparable; user provides hash func for sharding.
type ShardMap[K comparable, V any] struct {
	shards []shard[K, V]
	mask   uint32
	hash   func(K) uint32
}

type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewShardMap creates a new sharded map. shardCount is rounded up to next power of 2.
// hash is required to compute shard index from key.
func NewShardMap[K comparable, V any](shardCount int, hash func(K) uint32) *ShardMap[K, V] {
	if shardCount <= 0 {
		shardCount = 64
	}
	// Round up to power of 2 for better distribution.
	shardCount = 1 << bits.Len(uint(shardCount-1))
	sm := &ShardMap[K, V]{
		shards: make([]shard[K, V], shardCount),
		mask:   uint32(shardCount - 1),
		hash:   hash,
	}
	for i := range sm.shards {
		sm.shards[i].data = make(map[K]V)
	}
	return sm
}

func (sm *ShardMap[K, V]) shardIndex(key K) int {
	return int(sm.hash(key) & sm.mask)
}

// Get retrieves a value. Safe for concurrent use.
func (sm *ShardMap[K, V]) Get(key K) (V, bool) {
	idx := sm.shardIndex(key)
	shard := &sm.shards[idx]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	v, ok := shard.data[key]
	return v, ok
}

// Set sets a value. Safe for concurrent use.
func (sm *ShardMap[K, V]) Set(key K, val V) {
	idx := sm.shardIndex(key)
	shard := &sm.shards[idx]
	shard.mu.Lock()
	shard.data[key] = val
	shard.mu.Unlock()
}

// Compute allows atomic read-modify-write operations on a key within a shard lock.
// The function fn receives the current value (or zero value) and existence flag.
// It returns the new value and a boolean indicating if the key should be kept (true) or deleted (false).
func (sm *ShardMap[K, V]) Compute(key K, fn func(current V, exists bool) (newValue V, keep bool)) V {
	idx := sm.shardIndex(key)
	shard := &sm.shards[idx]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	curr, exists := shard.data[key]
	newVal, keep := fn(curr, exists)

	if keep {
		shard.data[key] = newVal
	} else {
		delete(shard.data, key)
	}
	return newVal
}

// Delete removes a key. Safe for concurrent use.
func (sm *ShardMap[K, V]) Delete(key K) {
	idx := sm.shardIndex(key)
	shard := &sm.shards[idx]
	shard.mu.Lock()
	delete(shard.data, key)
	shard.mu.Unlock()
}

// ClearAll resets all shards.
func (sm *ShardMap[K, V]) ClearAll() {
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.Lock()
		for k := range shard.data {
			delete(shard.data, k)
		}
		shard.mu.Unlock()
	}
}

// ClearExpired removes entries matching predicate and returns count removed.
func (sm *ShardMap[K, V]) ClearExpired(shouldRemove func(K, V) bool) int {
	var removed int
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.Lock()
		for k, v := range shard.data {
			if shouldRemove(k, v) {
				delete(shard.data, k)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	return removed
}

// Count returns the total number of items across all shards.
func (sm *ShardMap[K, V]) Count() int {
	count := 0
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.RLock()
		count += len(shard.data)
		shard.mu.RUnlock()
	}
	return count
}

// Iterate loops through all items. Callback should not modify the map.
func (sm *ShardMap[K, V]) Iterate(fn func(K, V) bool) {
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.RLock()
		for k, v := range shard.data {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Fnv32 for strings.
func Fnv32(key string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// XxHash32 for strings.
func XxHash32(key string) uint32 {
	h64 := xxhash.Sum64String(key)
	return uint32(h64 ^ (h64 >> 32))
}
