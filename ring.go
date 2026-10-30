package jack

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// defaultRingReplicas is the number of virtual nodes generated per physical
// node. Higher values give a more even key distribution; 128 keeps the skew
// small while keeping membership rebuilds cheap.
const defaultRingReplicas = 128

// RingOption configures a HashRing.
type RingOption func(*ringConfig)

type ringConfig struct {
	replicas int
	hash     func(string) uint64
}

// RingWithReplicas sets the number of virtual nodes per physical node.
// Non-positive values are ignored (the default of 128 is kept).
// More replicas → smoother distribution, larger snapshots.
func RingWithReplicas(n int) RingOption {
	return func(c *ringConfig) {
		if n > 0 {
			c.replicas = n
		}
	}
}

// RingWithHash replaces the default key hash (xxhash.Sum64String).
// The function must be deterministic: the same input must always yield the
// same value, or key placement will be unstable. Nil is ignored.
func RingWithHash(fn func(string) uint64) RingOption {
	return func(c *ringConfig) {
		if fn != nil {
			c.hash = fn
		}
	}
}

// ringSnapshot is an immutable, sorted view of the ring. Reads (Get/GetN) load
// it with a single atomic pointer load, so lookups never take a lock.
type ringSnapshot struct {
	hashes []uint64
	nodes  []string // nodes[i] owns hashes[i]
}

// HashRing is a consistent hash ring with virtual nodes.
//
// Keys map to physical nodes with minimal disruption: adding or removing a node
// remaps only the keys that node owns (plus a small share of virtual-node
// boundaries), instead of reshuffling the whole keyspace the way a modulo hash
// does. That makes it suitable for sharding caches, queues and sticky routing
// where rebalancing everything at once is unacceptable.
//
// Reads are lock-free: membership changes build a new immutable snapshot and
// publish it atomically, so Get/GetN keep serving the previous snapshot until
// the swap. Writes (Add/Remove) are serialized by a mutex.
//
// It is safe for concurrent use by any number of goroutines.
type HashRing struct {
	mu       sync.Mutex
	nodes    map[string]struct{}
	replicas int
	hash     func(string) uint64
	snapshot atomic.Pointer[ringSnapshot]
}

// NewHashRing creates an empty ring. Nodes are added with Add.
//
//	ring := jack.NewHashRing(jack.RingWithReplicas(160))
//	ring.Add("cache-a", "cache-b", "cache-c")
//	node, ok := ring.Get("user:42")
func NewHashRing(opts ...RingOption) *HashRing {
	cfg := ringConfig{
		replicas: defaultRingReplicas,
		hash:     xxhash.Sum64String,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	r := &HashRing{
		nodes:    make(map[string]struct{}),
		replicas: cfg.replicas,
		hash:     cfg.hash,
	}
	r.snapshot.Store(&ringSnapshot{})
	return r
}

// Add inserts nodes into the ring and returns how many were newly added.
// Empty names and duplicates are ignored. Cost is O(nodes × replicas × log)
// because the ring is rebuilt; add nodes in batches rather than one by one.
func (r *HashRing) Add(nodes ...string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	added := 0
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, exists := r.nodes[node]; exists {
			continue
		}
		r.nodes[node] = struct{}{}
		added++
	}
	if added > 0 {
		r.rebuildLocked()
	}
	return added
}

// Remove deletes nodes from the ring and returns how many were present.
// Unknown names and empty strings are ignored. Removing every node leaves an
// empty ring whose Get reports false.
func (r *HashRing) Remove(nodes ...string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, exists := r.nodes[node]; !exists {
			continue
		}
		delete(r.nodes, node)
		removed++
	}
	if removed > 0 {
		r.rebuildLocked()
	}
	return removed
}

// Get returns the node that owns key. The second result is false when the ring
// has no nodes.
func (r *HashRing) Get(key string) (string, bool) {
	snap := r.snapshot.Load()
	if snap == nil || len(snap.hashes) == 0 {
		return "", false
	}
	idx := r.ownerIndex(snap, r.hash(key))
	return snap.nodes[idx], true
}

// GetN returns up to n distinct nodes in clockwise ring order starting at key.
// It is the building block for replica reads/writes: the first element is the
// same node Get returns, the rest are the preferred fallbacks.
func (r *HashRing) GetN(key string, n int) []string {
	if n <= 0 {
		return nil
	}
	snap := r.snapshot.Load()
	if snap == nil || len(snap.hashes) == 0 {
		return nil
	}

	start := r.ownerIndex(snap, r.hash(key))
	out := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for i := 0; i < len(snap.hashes) && len(out) < n; i++ {
		node := snap.nodes[(start+i)%len(snap.hashes)]
		if _, dup := seen[node]; dup {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	return out
}

// Nodes returns the physical node names in sorted order.
func (r *HashRing) Nodes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, 0, len(r.nodes))
	for node := range r.nodes {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// Len returns the number of physical nodes in the ring.
func (r *HashRing) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.nodes)
}

// Replicas returns the number of virtual nodes generated per physical node.
func (r *HashRing) Replicas() int { return r.replicas }

// VirtualNodes returns the number of distinct points on the ring. It is lower
// than Len()×Replicas only when two virtual nodes collide on the same hash.
func (r *HashRing) VirtualNodes() int {
	snap := r.snapshot.Load()
	if snap == nil {
		return 0
	}
	return len(snap.hashes)
}

// ownerIndex returns the index of the first virtual node at or after h,
// wrapping to the start of the ring.
func (r *HashRing) ownerIndex(snap *ringSnapshot, h uint64) int {
	idx := sort.Search(len(snap.hashes), func(i int) bool { return snap.hashes[i] >= h })
	if idx == len(snap.hashes) {
		idx = 0
	}
	return idx
}

// rebuildLocked regenerates the snapshot from the current node set.
// Caller must hold mu.
func (r *HashRing) rebuildLocked() {
	type vnode struct {
		hash uint64
		node string
	}

	vns := make([]vnode, 0, len(r.nodes)*r.replicas)
	for node := range r.nodes {
		for i := 0; i < r.replicas; i++ {
			vns = append(vns, vnode{hash: r.hash(vnodeKey(node, i)), node: node})
		}
	}

	// Sorting by (hash, node) makes the ring independent of Go's map iteration
	// order: identical node sets always produce identical key placement.
	sort.Slice(vns, func(i, j int) bool {
		if vns[i].hash != vns[j].hash {
			return vns[i].hash < vns[j].hash
		}
		return vns[i].node < vns[j].node
	})

	snap := &ringSnapshot{
		hashes: make([]uint64, 0, len(vns)),
		nodes:  make([]string, 0, len(vns)),
	}
	for _, vn := range vns {
		if n := len(snap.hashes); n > 0 && snap.hashes[n-1] == vn.hash {
			continue // keep the lexicographically smallest owner of a hash point
		}
		snap.hashes = append(snap.hashes, vn.hash)
		snap.nodes = append(snap.nodes, vn.node)
	}
	r.snapshot.Store(snap)
}

// vnodeKey builds the hash input for the i-th virtual node of node.
// The NUL separator makes the encoding unambiguous, so node names must not
// contain NUL bytes (they may contain '#' or any other character).
func vnodeKey(node string, i int) string {
	return strconv.Itoa(i) + "\x00" + node
}
