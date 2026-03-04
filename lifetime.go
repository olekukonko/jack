package jack

import (
	"container/heap"
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/olekukonko/ll"
)

// LifetimeHook defines a lifecycle hook function that can return an error.
type LifetimeHook func(ctx context.Context, id string) error

// LifetimeCallback defines a callback function without error return.
type LifetimeCallback func(ctx context.Context, id string)

// Lifetime defines lifecycle hooks for an operation matching zevent.Runner behavior.
type Lifetime struct {
	// Start runs before the operation begins.
	// If it returns an error, the operation is aborted.
	Start LifetimeHook
	// End runs after the operation completes successfully.
	// No error return - matches zevent.Runner.End exactly.
	End LifetimeCallback
	// Timed runs after a specific duration if not cancelled/reset.
	// Used for inactivity/TTL logic. No error return.
	Timed     LifetimeCallback
	TimedWait time.Duration
}

// LifetimeOption configures a Lifetime.
type LifetimeOption func(*Lifetime)

// LifetimeWithStart sets the Start hook.
func LifetimeWithStart(hook LifetimeHook) LifetimeOption {
	return func(l *Lifetime) { l.Start = hook }
}

// LifetimeWithEnd sets the End hook.
func LifetimeWithEnd(callback LifetimeCallback) LifetimeOption {
	return func(l *Lifetime) { l.End = callback }
}

// LifetimeWithTimed sets the Timed hook and wait duration.
func LifetimeWithTimed(callback LifetimeCallback, wait time.Duration) LifetimeOption {
	return func(l *Lifetime) {
		l.Timed = callback
		l.TimedWait = wait
	}
}

// NewLifetime creates a new Lifetime with the given options.
func NewLifetime(opts ...LifetimeOption) *Lifetime {
	l := &Lifetime{}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Execute runs the operation with the lifetime's hooks.
func (l *Lifetime) Execute(ctx context.Context, id string, operation Func) error {
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}
	if err := operation(); err != nil {
		return err
	}
	if l.End != nil {
		l.End(ctx, id)
	}
	return nil
}

// ExecuteCtx runs a context-aware operation with the lifetime's hooks.
func (l *Lifetime) ExecuteCtx(ctx context.Context, id string, operation FuncCtx) error {
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}
	if err := operation(ctx); err != nil {
		return err
	}
	if l.End != nil {
		l.End(ctx, id)
	}
	return nil
}

// scheduledItem tracks expiration metadata for a single timed callback.
type scheduledItem struct {
	callback  LifetimeCallback
	expiresAt time.Time
	duration  time.Duration
	id        string
	index     int
}

// shard represents a single shard of the lifetime manager with its own priority queue.
type shard struct {
	items  map[string]*scheduledItem
	pq     *priorityQueue
	mu     sync.Mutex
	wakeCh chan struct{}
}

// priorityQueue implements heap.Interface for efficient expiration tracking.
type priorityQueue []*scheduledItem

// Len returns the number of items in the queue.
func (pq priorityQueue) Len() int { return len(pq) }

// Less compares expiration times for heap ordering.
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].expiresAt.Before(pq[j].expiresAt)
}

// Swap exchanges two items and updates their indices.
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push adds an item to the queue.
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*scheduledItem)
	item.index = n
	*pq = append(*pq, item)
}

// Pop removes and returns the next item from the queue.
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// LifetimeManager manages lifetimes using sharded priority queues.
// Scales horizontally with configurable shard count to minimize lock contention.
type LifetimeManager struct {
	shards     []*shard
	shardCount uint32
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	minTick    time.Duration
	timerLimit time.Duration
	logger     *ll.Logger
}

// LifetimeManagerOption configures a LifetimeManager.
type LifetimeManagerOption func(*LifetimeManager)

// LifetimeManagerWithShards sets number of shards (must be power of 2, default 16).
func LifetimeManagerWithShards(count uint32) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if count >= 1 && (count&(count-1)) == 0 {
			lm.shardCount = count
		}
	}
}

// LifetimeManagerWithMinTick sets minimum sleep between prune checks (default 10ms).
func LifetimeManagerWithMinTick(tick time.Duration) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if tick > 0 {
			lm.minTick = tick
		}
	}
}

// LifetimeManagerWithTimerLimit sets callback execution timeout (default 1m).
func LifetimeManagerWithTimerLimit(limit time.Duration) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if limit > 0 {
			lm.timerLimit = limit
		}
	}
}

// LifetimeManagerWithLogger sets a custom logger.
func LifetimeManagerWithLogger(l *ll.Logger) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if l != nil {
			lm.logger = l.Namespace("lifetime")
		}
	}
}

// NewLifetimeManager creates a new sharded LifetimeManager.
func NewLifetimeManager(opts ...LifetimeManagerOption) *LifetimeManager {
	ctx, cancel := context.WithCancel(context.Background())
	lm := &LifetimeManager{
		shardCount: 16,
		ctx:        ctx,
		cancel:     cancel,
		minTick:    10 * time.Millisecond,
		timerLimit: 1 * time.Minute,
		logger:     logger.Namespace("lifetime"),
	}
	for _, opt := range opts {
		opt(lm)
	}
	lm.shards = make([]*shard, lm.shardCount)
	for i := uint32(0); i < lm.shardCount; i++ {
		pq := make(priorityQueue, 0)
		heap.Init(&pq)
		lm.shards[i] = &shard{
			items:  make(map[string]*scheduledItem),
			pq:     &pq,
			wakeCh: make(chan struct{}, 1),
		}
		lm.wg.Add(1)
		go lm.pruneLoop(i)
	}
	return lm
}

// getShard returns shard index for given ID using FNV hash.
func (lm *LifetimeManager) getShard(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32() & (lm.shardCount - 1)
}

// pruneLoop runs in dedicated goroutine per shard, processing expirations.
func (lm *LifetimeManager) pruneLoop(idx uint32) {
	defer lm.wg.Done()
	s := lm.shards[idx]
	for {
		s.mu.Lock()
		var nextExpiry time.Time
		var hasItem bool
		if s.pq.Len() > 0 {
			nextExpiry = (*s.pq)[0].expiresAt
			hasItem = true
		}
		s.mu.Unlock()

		if !hasItem {
			select {
			case <-lm.ctx.Done():
				return
			case <-s.wakeCh:
				continue
			}
		}

		now := time.Now()
		if now.After(nextExpiry) || now.Equal(nextExpiry) {
			lm.processExpired(s)
			continue
		}

		wait := nextExpiry.Sub(now)
		if wait < lm.minTick {
			wait = lm.minTick
		}

		select {
		case <-lm.ctx.Done():
			return
		case <-time.After(wait):
			lm.processExpired(s)
		case <-s.wakeCh:
			continue
		}
	}
}

// processExpired executes all callbacks that have reached expiration.
func (lm *LifetimeManager) processExpired(s *shard) {
	now := time.Now()
	s.mu.Lock()
	expired := make([]*scheduledItem, 0)
	for s.pq.Len() > 0 {
		item := (*s.pq)[0]
		if now.After(item.expiresAt) || now.Equal(item.expiresAt) {
			heap.Pop(s.pq)
			delete(s.items, item.id)
			expired = append(expired, item)
		} else {
			break
		}
	}
	s.mu.Unlock()

	for _, item := range expired {
		lm.logger.Debug("processExpired: executing callback for ID %s", item.id)
		ctx, cancel := context.WithTimeout(context.Background(), lm.timerLimit)
		item.callback(ctx, item.id)
		cancel()
	}
}

// ScheduleTimed schedules or resets a timed callback for given ID.
func (lm *LifetimeManager) ScheduleTimed(ctx context.Context, id string, callback LifetimeCallback, wait time.Duration) {
	if callback == nil {
		lm.logger.Debug("ScheduleTimed: nil callback for ID %s", id)
		return
	}
	if wait <= 0 {
		wait = 30 * time.Minute
	}

	idx := lm.getShard(id)
	s := lm.shards[idx]
	item := &scheduledItem{
		callback:  callback,
		expiresAt: time.Now().Add(wait),
		duration:  wait,
		id:        id,
	}

	s.mu.Lock()
	if lm.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	if existing, ok := s.items[id]; ok {
		heap.Remove(s.pq, existing.index)
		delete(s.items, id)
	}
	s.items[id] = item
	heap.Push(s.pq, item)
	s.mu.Unlock()

	lm.logger.Debug("ScheduleTimed: scheduled callback for ID %s in %v", id, wait)
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// ScheduleLifetimeTimed schedules a timed callback from a Lifetime configuration.
func (lm *LifetimeManager) ScheduleLifetimeTimed(ctx context.Context, id string, lifetime *Lifetime) {
	if lifetime == nil || lifetime.Timed == nil {
		lm.ResetTimed(id)
		return
	}
	lm.ScheduleTimed(ctx, id, lifetime.Timed, lifetime.TimedWait)
}

// ExecuteWithLifetime executes an operation with lifetime hooks and manages timed events.
func (lm *LifetimeManager) ExecuteWithLifetime(ctx context.Context, id string, lifetime *Lifetime, operation Func) error {
	if lifetime == nil {
		return operation()
	}
	err := lifetime.Execute(ctx, id, operation)
	if err == nil && lifetime.Timed != nil {
		lm.ScheduleLifetimeTimed(ctx, id, lifetime)
	}
	return err
}

// ExecuteCtxWithLifetime executes a context-aware operation with lifetime hooks.
func (lm *LifetimeManager) ExecuteCtxWithLifetime(ctx context.Context, id string, lifetime *Lifetime, operation FuncCtx) error {
	if lifetime == nil {
		return operation(ctx)
	}
	err := lifetime.ExecuteCtx(ctx, id, operation)
	if err == nil && lifetime.Timed != nil {
		lm.ScheduleLifetimeTimed(ctx, id, lifetime)
	}
	return err
}

// ResetTimed extends expiration of existing item (Keep-Alive).
func (lm *LifetimeManager) ResetTimed(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, exists := s.items[id]; exists {
		item.expiresAt = time.Now().Add(item.duration)
		heap.Fix(s.pq, item.index)
		lm.logger.Debug("ResetTimed: extended timer for ID %s", id)
		select {
		case s.wakeCh <- struct{}{}:
		default:
		}
		return true
	}
	lm.logger.Debug("ResetTimed: no timer found for ID %s", id)
	return false
}

// CancelTimed removes a scheduled callback.
func (lm *LifetimeManager) CancelTimed(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, ok := s.items[id]; ok {
		heap.Remove(s.pq, item.index)
		delete(s.items, id)
		lm.logger.Debug("CancelTimed: cancelled timer for ID %s", id)
		return true
	}
	lm.logger.Debug("CancelTimed: no timer found for ID %s", id)
	return false
}

// StopAll cancels all pending callbacks and stops the manager.
func (lm *LifetimeManager) StopAll() {
	lm.logger.Info("StopAll: stopping lifetime manager")
	lm.cancel()
	lm.wg.Wait()

	for _, s := range lm.shards {
		s.mu.Lock()
		s.items = make(map[string]*scheduledItem)
		*s.pq = (*s.pq)[:0]
		s.mu.Unlock()
	}
	lm.logger.Info("StopAll: all timers stopped")
}

// HasPending checks if there's a pending timed callback for an ID.
func (lm *LifetimeManager) HasPending(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	_, exists := s.items[id]
	s.mu.Unlock()
	return exists
}

// PendingCount returns total number of pending timed callbacks across all shards.
func (lm *LifetimeManager) PendingCount() int {
	count := 0
	for _, s := range lm.shards {
		s.mu.Lock()
		count += len(s.items)
		s.mu.Unlock()
	}
	return count
}

// GetRemainingDuration returns time until expiration for a pending callback.
func (lm *LifetimeManager) GetRemainingDuration(id string) (time.Duration, bool) {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	item, exists := s.items[id]
	s.mu.Unlock()

	if !exists {
		return 0, false
	}
	remaining := time.Until(item.expiresAt)
	if remaining < 0 {
		return 0, false
	}
	return remaining, true
}

// Stop gracefully stops the LifetimeManager.
func (lm *LifetimeManager) Stop() {
	lm.StopAll()
}

// LifetimeWithRun is a convenience function that creates and executes a lifetime.
func LifetimeWithRun(ctx context.Context, id string, operation Func, opts ...LifetimeOption) error {
	lifetime := NewLifetime(opts...)
	return lifetime.Execute(ctx, id, operation)
}

// LifetimeWithRunCtx is a convenience function for context-aware operations.
func LifetimeWithRunCtx(ctx context.Context, id string, operation FuncCtx, opts ...LifetimeOption) error {
	lifetime := NewLifetime(opts...)
	return lifetime.ExecuteCtx(ctx, id, operation)
}
