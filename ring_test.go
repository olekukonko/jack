package jack

import (
	"hash/fnv"
	"sync"
	"testing"
)

func TestHashRingEmpty(t *testing.T) {
	r := NewHashRing()
	if _, ok := r.Get("anything"); ok {
		t.Fatal("expected Get to report false on an empty ring")
	}
	if got := r.GetN("anything", 3); got != nil {
		t.Fatalf("expected nil replicas on an empty ring, got %v", got)
	}
	if r.Len() != 0 || len(r.Nodes()) != 0 || r.VirtualNodes() != 0 {
		t.Fatalf("expected empty ring, Len=%d Nodes=%v VNodes=%d", r.Len(), r.Nodes(), r.VirtualNodes())
	}
	if r.Replicas() != defaultRingReplicas {
		t.Fatalf("expected default replicas %d, got %d", defaultRingReplicas, r.Replicas())
	}
}

func TestHashRingSingleNode(t *testing.T) {
	r := NewHashRing()
	if added := r.Add("only"); added != 1 {
		t.Fatalf("expected 1 node added, got %d", added)
	}
	for i := 0; i < 100; i++ {
		node, ok := r.Get("key-" + itoaForTest(i))
		if !ok || node != "only" {
			t.Fatalf("expected only node, got %q ok=%v", node, ok)
		}
	}
	if got := r.GetN("key", 3); len(got) != 1 || got[0] != "only" {
		t.Fatalf("expected [only], got %v", got)
	}
}

func TestHashRingAddRemoveCounts(t *testing.T) {
	r := NewHashRing()
	if got := r.Add("a", "b", "a", ""); got != 2 {
		t.Fatalf("expected 2 added (dupes/empty ignored), got %d", got)
	}
	if got := r.Add("a"); got != 0 {
		t.Fatalf("expected 0 added for duplicate, got 0 != %d", got)
	}
	if got := r.Remove("missing", ""); got != 0 {
		t.Fatalf("expected 0 removed for unknown/empty, got %d", got)
	}
	if got := r.Remove("a", "a"); got != 1 {
		t.Fatalf("expected 1 removed, got %d", got)
	}
	if nodes := r.Nodes(); len(nodes) != 1 || nodes[0] != "b" {
		t.Fatalf("expected [b], got %v", nodes)
	}
	if r.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", r.Len())
	}
}

func TestHashRingRemoveAll(t *testing.T) {
	r := NewHashRing()
	r.Add("a", "b", "c")
	if got := r.Remove("a", "b", "c"); got != 3 {
		t.Fatalf("expected 3 removed, got %d", got)
	}
	if _, ok := r.Get("key"); ok {
		t.Fatal("expected empty ring after removing all nodes")
	}
	if r.VirtualNodes() != 0 {
		t.Fatalf("expected no virtual nodes, got %d", r.VirtualNodes())
	}
}

// TestHashRingDeterministic asserts that ring contents — not insertion order —
// determine key placement. Production rings are built from maps, so this must
// not depend on Go's randomised map iteration.
func TestHashRingDeterministic(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}

	ringA := NewHashRing()
	ringA.Add(nodes...)

	ringB := NewHashRing()
	for i := len(nodes) - 1; i >= 0; i-- {
		ringB.Add(nodes[i])
	}

	for i := 0; i < 5000; i++ {
		key := "user:" + itoaForTest(i)
		a, okA := ringA.Get(key)
		b, okB := ringB.Get(key)
		if !okA || !okB {
			t.Fatalf("unexpected miss for %q", key)
		}
		if a != b {
			t.Fatalf("placement depends on insertion order: %q -> %q vs %q", key, a, b)
		}
	}
}

func TestHashRingDistribution(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	r := NewHashRing()
	r.Add(nodes...)

	const keys = 20000
	counts := make(map[string]int, len(nodes))
	for i := 0; i < keys; i++ {
		node, ok := r.Get("key-" + itoaForTest(i))
		if !ok {
			t.Fatalf("unexpected miss at key %d", i)
		}
		counts[node]++
	}

	ideal := float64(keys) / float64(len(nodes))
	for _, n := range nodes {
		share := float64(counts[n]) / ideal
		if share < 0.7 || share > 1.3 {
			t.Fatalf("uneven distribution for %s: %d keys (%.2fx ideal); counts=%v", n, counts[n], share, counts)
		}
	}
}

// TestHashRingMinimalMovementOnRemove verifies the core consistency property:
// removing a node must not move keys that were owned by another node.
func TestHashRingMinimalMovementOnRemove(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	r := NewHashRing()
	r.Add(nodes...)

	const keys = 5000
	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := "k" + itoaForTest(i)
		node, _ := r.Get(key)
		before[key] = node
	}

	r.Remove("n3")

	for key, prev := range before {
		now, ok := r.Get(key)
		if !ok {
			t.Fatalf("unexpected miss for %q after removal", key)
		}
		if prev == "n3" {
			continue // keys owned by the removed node must move
		}
		if now != prev {
			t.Fatalf("key %q moved from %q to %q although its owner was not removed", key, prev, now)
		}
	}
}

// TestHashRingMinimalMovementOnAdd verifies that adding a node only claims a
// share of keys proportional to its weight (1/N), not the whole keyspace.
func TestHashRingMinimalMovementOnAdd(t *testing.T) {
	r := NewHashRing()
	r.Add("n1", "n2", "n3", "n4")

	const keys = 10000
	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := "k" + itoaForTest(i)
		node, _ := r.Get(key)
		before[key] = node
	}

	r.Add("n5")

	moved := 0
	for key, prev := range before {
		now, _ := r.Get(key)
		if now != prev {
			moved++
			if now != "n5" {
				t.Fatalf("key %q moved from %q to %q, expected only moves to the new node", key, prev, now)
			}
		}
	}

	share := float64(moved) / float64(keys)
	ideal := 1.0 / 5.0
	if share < ideal*0.5 || share > ideal*1.6 {
		t.Fatalf("expected ~%.0f%% of keys to move on add, got %.1f%% (%d keys)", ideal*100, share*100, moved)
	}
}

func TestHashRingGetN(t *testing.T) {
	r := NewHashRing()
	r.Add("a", "b", "c")

	primary, _ := r.Get("replica-key")
	got := r.GetN("replica-key", 3)
	if len(got) != 3 {
		t.Fatalf("expected 3 distinct replicas, got %v", got)
	}
	if got[0] != primary {
		t.Fatalf("expected first replica %q to match Get, got %q", primary, got[0])
	}
	seen := map[string]bool{}
	for _, n := range got {
		if seen[n] {
			t.Fatalf("duplicate node in replicas: %v", got)
		}
		seen[n] = true
	}
	if more := r.GetN("replica-key", 10); len(more) != 3 {
		t.Fatalf("expected replicas capped at node count, got %v", more)
	}
	if none := r.GetN("replica-key", 0); none != nil {
		t.Fatalf("expected nil for n<=0, got %v", none)
	}
}

func TestHashRingStableLookup(t *testing.T) {
	r := NewHashRing()
	r.Add("a", "b", "c")

	first, ok := r.Get("stable-key")
	if !ok {
		t.Fatal("expected a node")
	}
	for i := 0; i < 100; i++ {
		again, _ := r.Get("stable-key")
		if again != first {
			t.Fatalf("lookup not stable: %q vs %q", first, again)
		}
	}
}

func TestHashRingOptions(t *testing.T) {
	r := NewHashRing(RingWithReplicas(10))
	r.Add("a", "b")
	if r.Replicas() != 10 {
		t.Fatalf("expected 10 replicas, got %d", r.Replicas())
	}
	if got := r.VirtualNodes(); got != 20 {
		t.Fatalf("expected 20 virtual nodes, got %d", got)
	}

	// Non-positive replica counts and nil hash functions must be ignored.
	def := NewHashRing(RingWithReplicas(0), RingWithHash(nil))
	if def.Replicas() != defaultRingReplicas {
		t.Fatalf("expected default replicas, got %d", def.Replicas())
	}

	// A custom hash must still produce deterministic placements.
	custom := NewHashRing(RingWithHash(fnv64String))
	custom.Add("a", "b", "c")
	other := NewHashRing(RingWithHash(fnv64String))
	other.Add("c", "b", "a")
	for i := 0; i < 500; i++ {
		key := "k" + itoaForTest(i)
		x, _ := custom.Get(key)
		y, _ := other.Get(key)
		if x != y {
			t.Fatalf("custom hash placement not deterministic for %q: %q vs %q", key, x, y)
		}
	}
}

func TestHashRingVirtualNodesPerNode(t *testing.T) {
	r := NewHashRing(RingWithReplicas(64))
	r.Add("a", "b", "c")
	if got, want := r.VirtualNodes(), 3*64; got != want {
		t.Fatalf("expected %d virtual nodes, got %d", want, got)
	}
	if got := r.Replicas(); got != 64 {
		t.Fatalf("expected 64 replicas, got %d", got)
	}
}

func TestHashRingConcurrentAccess(t *testing.T) {
	r := NewHashRing()

	var wg sync.WaitGroup
	// Writer: churn membership while readers look keys up.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			node := "n" + itoaForTest(i%8)
			r.Add(node)
			r.Remove(node)
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := "g" + itoaForTest(g) + "-" + itoaForTest(i)
				if node, ok := r.Get(key); ok && node == "" {
					t.Error("Get returned an empty node name")
				}
				_ = r.GetN(key, 3)
				_ = r.Len()
				_ = r.Nodes()
				_ = r.VirtualNodes()
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkHashRingGet(b *testing.B) {
	r := NewHashRing()
	r.Add("node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7", "node-8")

	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "user:" + itoaForTest(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := r.Get(keys[i&1023]); !ok {
			b.Fatal("miss")
		}
	}
}

// itoaForTest is a tiny allocation-light int formatter for test keys.
func itoaForTest(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// fnv64String is a stdlib-based alternative hash used to exercise RingWithHash.
func fnv64String(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
