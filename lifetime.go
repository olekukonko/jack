package jack

import (
	"container/heap"
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// Hook defines a lifecycle hook function that returns an error.
// It accepts a unique identifier string for the operation being tracked.
// Use this type for pre-execution validation or setup logic that may fail.
type Hook func(id string) error

// HookCtx defines a context-aware lifecycle hook function that returns an error.
// It accepts a context for cancellation and an identifier for the tracked operation.
// Use this type when hooks need to respect timeouts or external cancellation signals.
type HookCtx func(ctx context.Context, id string) error

// Callback defines a callback function without error return for post-execution logic.
// It accepts a unique identifier string for the operation that completed.
// Use this type for notifications, cleanup, or metrics that do not affect flow control.
type Callback func(id string)

// CallbackCtx defines a context-aware callback function without error return.
// It accepts a context for cancellation and an identifier for the completed operation.
// Use this type when callbacks need to coordinate with context deadlines or values.
type CallbackCtx func(ctx context.Context, id string)

// LifetimeMetrics tracks operational metrics for the lifetime manager.
type LifetimeMetrics struct {
	ScheduleCalls   atomic.Uint64
	ResetCalls      atomic.Uint64
	CancelCalls     atomic.Uint64
	ExecuteCalls    atomic.Uint64
	ExpiredTotal    atomic.Uint64
	ExpiredHandled  atomic.Uint64
	ExpiredMissed   atomic.Uint64
	ExpiredErrors   atomic.Uint64
	ActiveTimers    atomic.Int64
	MaxActiveTimers atomic.Int64
	TotalTimersSeen atomic.Uint64
	AvgExpirationMs atomic.Int64
	MinExpirationMs atomic.Int64
	MaxExpirationMs atomic.Int64
	LoopIterations  atomic.Uint64
	SignalsSent     atomic.Uint64
	StopsReceived   atomic.Uint64
}

// recordSchedule increments schedule-related counters for metrics tracking.
// It atomically updates both the total schedule calls and total timers seen.
// This method is called internally when a new timer is successfully scheduled.
func (m *LifetimeMetrics) recordSchedule() {
	m.ScheduleCalls.Add(1)
	m.TotalTimersSeen.Add(1)
}

// recordReset increments the reset calls counter for metrics tracking.
// It atomically updates the metric to reflect a timer reset operation.
// This method is called internally when an existing timer is refreshed.
func (m *LifetimeMetrics) recordReset() {
	m.ResetCalls.Add(1)
}

// recordCancel increments the cancel calls counter for metrics tracking.
// It atomically updates the metric to reflect a timer cancellation operation.
// This method is called internally when a scheduled timer is explicitly cancelled.
func (m *LifetimeMetrics) recordCancel() {
	m.CancelCalls.Add(1)
}

// recordActiveCount updates active timer counters with atomic safety for concurrency.
// It tracks both current active timers and the historical maximum observed count.
// This method is called after any queue modification to maintain accurate metrics.
func (m *LifetimeMetrics) recordActiveCount(count int) {
	m.ActiveTimers.Store(int64(count))
	for {
		current := m.MaxActiveTimers.Load()
		if int64(count) > current {
			if m.MaxActiveTimers.CompareAndSwap(current, int64(count)) {
				break
			}
		} else {
			break
		}
	}
}

// recordExpiration updates expiration statistics including timing and outcome metrics.
// It atomically calculates rolling averages and tracks min/max expiration durations.
// This method records whether the expiration was handled successfully and any errors.
func (m *LifetimeMetrics) recordExpiration(elapsed time.Duration, handled bool, err error) {
	m.ExpiredTotal.Add(1)
	elapsedMs := int64(elapsed.Milliseconds())

	for {
		current := m.MinExpirationMs.Load()
		if current == 0 || elapsedMs < current {
			if m.MinExpirationMs.CompareAndSwap(current, elapsedMs) {
				break
			}
		} else {
			break
		}
	}

	for {
		current := m.MaxExpirationMs.Load()
		if elapsedMs > current {
			if m.MaxExpirationMs.CompareAndSwap(current, elapsedMs) {
				break
			}
		} else {
			break
		}
	}

	for {
		oldAvg := m.AvgExpirationMs.Load()
		total := m.ExpiredTotal.Load()
		newAvg := (oldAvg*int64(total-1) + elapsedMs) / int64(total)
		if m.AvgExpirationMs.CompareAndSwap(oldAvg, newAvg) {
			break
		}
	}

	if handled {
		m.ExpiredHandled.Add(1)
	} else {
		m.ExpiredMissed.Add(1)
	}
	if err != nil {
		m.ExpiredErrors.Add(1)
	}
}

type scheduledItem struct {
	callback  CallbackCtx
	expiresAt time.Time
	duration  time.Duration
	id        string
	index     int
}

type priorityQueue []*scheduledItem

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].expiresAt.Before(pq[j].expiresAt) }
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*scheduledItem)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type shard struct {
	items  map[string]*scheduledItem
	pq     *priorityQueue
	mu     sync.Mutex
	wakeCh chan struct{}
}

type Lifetime struct {
	shards     []*shard
	shardCount uint32
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	minTick    time.Duration
	timerLimit time.Duration
	logger     *ll.Logger
	metrics    *LifetimeMetrics
}

type LifetimeOption func(*Lifetime)

// LifetimeWithShards configures the number of shards for concurrent timer management.
// The count must be a power of two for efficient bitwise modulo hashing distribution.
// Use higher shard counts to reduce lock contention under heavy timer scheduling load.
func LifetimeWithShards(count uint32) LifetimeOption {
	return func(lm *Lifetime) {
		if count >= 1 && (count&(count-1)) == 0 {
			lm.shardCount = count
		}
	}
}

// LifetimeWithMinTick sets the minimum polling interval for the expiration check loop.
// If the provided duration is zero or negative, the option is ignored and default applies.
// Use this to balance responsiveness against CPU usage when processing expired timers.
func LifetimeWithMinTick(tick time.Duration) LifetimeOption {
	return func(lm *Lifetime) {
		if tick > 0 {
			lm.minTick = tick
		}
	}
}

// LifetimeWithTimerLimit configures the maximum execution time allowed for timer callbacks.
// If the provided duration is zero or negative, the option is ignored and default applies.
// This prevents long-running callbacks from blocking the expiration processing loop.
func LifetimeWithTimerLimit(limit time.Duration) LifetimeOption {
	return func(lm *Lifetime) {
		if limit > 0 {
			lm.timerLimit = limit
		}
	}
}

// LifetimeWithLogger assigns a namespaced logger instance for structured Lifetime operation logging.
// If the provided logger is nil, the option has no effect and the default logger remains active.
// Use this to integrate Lifetime logs with your application's logging infrastructure and levels.
func LifetimeWithLogger(l *ll.Logger) LifetimeOption {
	return func(lm *Lifetime) {
		if l != nil {
			lm.logger = l.Namespace("lifetime")
		}
	}
}

// NewLifetime creates and initializes a new Lifetime manager with the provided configuration options.
// It spawns one pruning goroutine per shard to handle timer expiration with minimal lock contention.
// The returned Lifetime is immediately active and ready to schedule timed callbacks via ScheduleTimed.
func NewLifetime(opts ...LifetimeOption) *Lifetime {
	ctx, cancel := context.WithCancel(context.Background())
	lm := &Lifetime{
		shardCount: 16,
		ctx:        ctx,
		cancel:     cancel,
		minTick:    10 * time.Millisecond,
		timerLimit: 1 * time.Minute,
		logger:     logger.Namespace("lifetime"),
		metrics:    &LifetimeMetrics{},
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

// Metrics returns a reference to the Lifetime manager's metrics struct for monitoring operational statistics.
// All metric fields use atomic operations, allowing safe concurrent reads without additional locking.
// Use this to expose timer scheduling statistics to dashboards, alerts, or external monitoring systems.
func (lm *Lifetime) Metrics() *LifetimeMetrics {
	return lm.metrics
}

// getShard computes the shard index for a given ID using FNV-32a hashing and bitwise masking.
// It ensures uniform distribution of timers across shards when shardCount is a power of two.
// This method is called internally to route timer operations to the correct concurrent shard.
func (lm *Lifetime) getShard(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32() & (lm.shardCount - 1)
}

// pruneLoop runs the background goroutine that processes expired timers for a specific shard.
// It efficiently waits for the next expiry or wake signal, then processes all due items in batch.
// The loop terminates cleanly when the context is cancelled, ensuring no goroutine leaks.
func (lm *Lifetime) pruneLoop(idx uint32) {
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

// processExpired extracts and executes all timers that have reached their expiration time.
// It runs each callback with a timeout context and records metrics for duration and outcome.
// This method holds the shard lock only during queue extraction to minimize contention.
func (lm *Lifetime) processExpired(s *shard) {
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
	lm.metrics.recordActiveCount(len(s.items))
	s.mu.Unlock()

	for _, item := range expired {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), lm.timerLimit)
		item.callback(ctx, item.id)
		cancel()
		lm.metrics.recordExpiration(time.Since(start), true, nil)
	}
	lm.metrics.LoopIterations.Add(1)
}

// ScheduleTimed registers a callback to execute after the specified wait duration for a given ID.
// If a timer already exists for the ID, it is replaced with the new callback and expiration time.
// The method uses sharding for concurrency and signals the prune loop to re-evaluate scheduling.
func (lm *Lifetime) ScheduleTimed(ctx context.Context, id string, callback CallbackCtx, wait time.Duration) {
	if callback == nil {
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
	lm.metrics.recordActiveCount(len(s.items))
	s.mu.Unlock()

	lm.metrics.recordSchedule()
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// ScheduleLifetimeTimed schedules a timed callback using configuration from a Vitals instance.
// If the Vitals or its Timed callback is nil, it resets any existing timer for the ID instead.
// This convenience method integrates Lifetime timer management with Vitals lifecycle configuration.
func (lm *Lifetime) ScheduleLifetimeTimed(ctx context.Context, id string, lifetime *Vitals) {
	if lifetime == nil || lifetime.Timed == nil {
		lm.ResetTimed(id)
		return
	}
	lm.ScheduleTimed(ctx, id, lifetime.Timed, lifetime.TimedWait)
}

// ExecuteWithLifetime runs an operation with Vitals hooks and schedules a timed callback on success.
// If the operation succeeds and a timed callback is configured, it is scheduled for later execution.
// This method combines immediate operation execution with deferred lifecycle callback management.
func (lm *Lifetime) ExecuteWithLifetime(ctx context.Context, id string, lifetime *Vitals, operation Func) error {
	if lifetime == nil {
		return operation()
	}
	err := lifetime.Execute(ctx, id, operation)
	if err == nil && lifetime.Timed != nil {
		lm.ScheduleLifetimeTimed(ctx, id, lifetime)
	}
	return err
}

// ExecuteCtxWithLifetime runs a context-aware operation with Vitals hooks and schedules timed callback on success.
// If the operation succeeds and a timed callback is configured, it is scheduled for later execution.
// This method combines immediate context-aware execution with deferred lifecycle callback management.
func (lm *Lifetime) ExecuteCtxWithLifetime(ctx context.Context, id string, lifetime *Vitals, operation FuncCtx) error {
	if lifetime == nil {
		return operation(ctx)
	}
	err := lifetime.ExecuteCtx(ctx, id, operation)
	if err == nil && lifetime.Timed != nil {
		lm.ScheduleLifetimeTimed(ctx, id, lifetime)
	}
	return err
}

// ResetTimed extends the expiration time of an existing timer by its original duration from now.
// It returns true if the timer was found and successfully reset, false if no timer existed for the ID.
// This method is useful for implementing keep-alive or activity-based timeout extension patterns.
func (lm *Lifetime) ResetTimed(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, exists := s.items[id]; exists {
		item.expiresAt = time.Now().Add(item.duration)
		heap.Fix(s.pq, item.index)
		lm.metrics.recordReset()
		select {
		case s.wakeCh <- struct{}{}:
		default:
		}
		return true
	}
	return false
}

// CancelTimed removes a scheduled timer by ID, preventing its callback from executing.
// It returns true if the timer was found and successfully cancelled, false if none existed.
// This method is safe to call multiple times and has no effect if the timer already expired.
func (lm *Lifetime) CancelTimed(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, ok := s.items[id]; ok {
		heap.Remove(s.pq, item.index)
		delete(s.items, id)
		lm.metrics.recordActiveCount(len(s.items))
		lm.metrics.recordCancel()
		return true
	}
	return false
}

// StopAll gracefully shuts down the Lifetime manager, cancelling all pending timers and goroutines.
// It signals the context to stop prune loops, waits for them to exit, then clears all shard state.
// After calling StopAll, the Lifetime instance cannot be reused and all scheduled callbacks are discarded.
func (lm *Lifetime) StopAll() {
	lm.metrics.StopsReceived.Add(1)
	lm.cancel()
	lm.wg.Wait()

	for _, s := range lm.shards {
		s.mu.Lock()
		s.items = make(map[string]*scheduledItem)
		*s.pq = (*s.pq)[:0]
		s.mu.Unlock()
	}
	lm.metrics.recordActiveCount(0)
}

// HasPending checks whether a timer is currently scheduled for the given ID.
// It returns true if the ID exists in any shard's queue, false otherwise.
// This method provides thread-safe read access without modifying internal state.
func (lm *Lifetime) HasPending(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	_, exists := s.items[id]
	s.mu.Unlock()
	return exists
}

// PendingCount returns the total number of timers currently scheduled across all shards.
// It iterates through each shard while holding locks to ensure an accurate snapshot count.
// Use this for monitoring queue depth or implementing backpressure in high-load scenarios.
func (lm *Lifetime) PendingCount() int {
	count := 0
	for _, s := range lm.shards {
		s.mu.Lock()
		count += len(s.items)
		s.mu.Unlock()
	}
	return count
}

// GetRemainingDuration returns the time remaining until a scheduled timer expires for the given ID.
// It returns the duration and true if the timer exists and has not yet expired, otherwise zero and false.
// This method is useful for displaying countdowns or making scheduling decisions based on timer state.
func (lm *Lifetime) GetRemainingDuration(id string) (time.Duration, bool) {
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

// Stop cancels timers for the specified IDs, or all timers if no IDs are provided.
// It groups IDs by shard for efficient batch removal and signals prune loops to re-evaluate.
// This method provides a convenient way to clean up multiple timers without individual CancelTimed calls.
func (lm *Lifetime) Stop(ids ...string) {
	if len(ids) == 0 {
		lm.StopAll()
		return
	}

	lm.metrics.StopsReceived.Add(uint64(len(ids)))

	// Group IDs by shard for efficiency
	idsByShard := make(map[uint32][]string, len(ids))
	for _, id := range ids {
		idx := lm.getShard(id)
		idsByShard[idx] = append(idsByShard[idx], id)
	}

	// Stop timers for each shard
	for idx, shardIDs := range idsByShard {
		s := lm.shards[idx]
		s.mu.Lock()
		for _, id := range shardIDs {
			if item, ok := s.items[id]; ok {
				heap.Remove(s.pq, item.index)
				delete(s.items, id)
				lm.metrics.recordCancel()
			}
		}
		lm.metrics.recordActiveCount(len(s.items))
		s.mu.Unlock()

		// Wake up the prune loop to recalculate next expiry
		select {
		case s.wakeCh <- struct{}{}:
		default:
		}
	}
}
