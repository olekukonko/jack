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
type Hook func(id string) error

// HookCtx defines a context-aware lifecycle hook function that returns an error.
type HookCtx func(ctx context.Context, id string) error

// Callback defines a callback function without error return.
type Callback func(id string)

// CallbackCtx defines a context-aware callback function without error return.
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

func (m *LifetimeMetrics) recordSchedule() {
	m.ScheduleCalls.Add(1)
	m.TotalTimersSeen.Add(1)
}

func (m *LifetimeMetrics) recordReset() {
	m.ResetCalls.Add(1)
}

func (m *LifetimeMetrics) recordCancel() {
	m.CancelCalls.Add(1)
}

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

func LifetimeWithShards(count uint32) LifetimeOption {
	return func(lm *Lifetime) {
		if count >= 1 && (count&(count-1)) == 0 {
			lm.shardCount = count
		}
	}
}

func LifetimeWithMinTick(tick time.Duration) LifetimeOption {
	return func(lm *Lifetime) {
		if tick > 0 {
			lm.minTick = tick
		}
	}
}

func LifetimeWithTimerLimit(limit time.Duration) LifetimeOption {
	return func(lm *Lifetime) {
		if limit > 0 {
			lm.timerLimit = limit
		}
	}
}

func LifetimeWithLogger(l *ll.Logger) LifetimeOption {
	return func(lm *Lifetime) {
		if l != nil {
			lm.logger = l.Namespace("lifetime")
		}
	}
}

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

func (lm *Lifetime) Metrics() *LifetimeMetrics {
	return lm.metrics
}

func (lm *Lifetime) getShard(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32() & (lm.shardCount - 1)
}

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

func (lm *Lifetime) ScheduleLifetimeTimed(ctx context.Context, id string, lifetime *Vitals) {
	if lifetime == nil || lifetime.Timed == nil {
		lm.ResetTimed(id)
		return
	}
	lm.ScheduleTimed(ctx, id, lifetime.Timed, lifetime.TimedWait)
}

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

func (lm *Lifetime) HasPending(id string) bool {
	idx := lm.getShard(id)
	s := lm.shards[idx]
	s.mu.Lock()
	_, exists := s.items[id]
	s.mu.Unlock()
	return exists
}

func (lm *Lifetime) PendingCount() int {
	count := 0
	for _, s := range lm.shards {
		s.mu.Lock()
		count += len(s.items)
		s.mu.Unlock()
	}
	return count
}

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

// Stop stops timers for the given IDs. If no IDs are provided, stops all timers.
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
