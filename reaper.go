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

type ReaperMetrics struct {
	TouchCalls      atomic.Uint64
	TouchAtCalls    atomic.Uint64
	RemoveCalls     atomic.Uint64
	ClearCalls      atomic.Uint64
	ExpiredTotal    atomic.Uint64
	ExpiredHandled  atomic.Uint64
	ExpiredMissed   atomic.Uint64
	ExpiredErrors   atomic.Uint64
	ActiveTasks     atomic.Int64
	MaxActiveTasks  atomic.Int64
	TotalTasksSeen  atomic.Uint64
	AvgExpirationMs atomic.Int64
	MinExpirationMs atomic.Int64
	MaxExpirationMs atomic.Int64
	LoopIterations  atomic.Uint64
	SignalsSent     atomic.Uint64
	StopsReceived   atomic.Uint64
}

func (m *ReaperMetrics) recordTouch() {
	m.TouchCalls.Add(1)
	m.TotalTasksSeen.Add(1)
}

func (m *ReaperMetrics) recordTouchAt() {
	m.TouchAtCalls.Add(1)
	m.TotalTasksSeen.Add(1)
}

func (m *ReaperMetrics) recordActiveCount(count int) {
	m.ActiveTasks.Store(int64(count))
	for {
		current := m.MaxActiveTasks.Load()
		if int64(count) > current {
			if m.MaxActiveTasks.CompareAndSwap(current, int64(count)) {
				break
			}
		} else {
			break
		}
	}
}

func (m *ReaperMetrics) recordExpiration(elapsed time.Duration, handled bool, err error) {
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

type ReaperHandler func(context.Context, string)

type ReaperTask struct {
	ID       string
	Deadline time.Time
	index    int
}

type reaperShard struct {
	items  map[string]*ReaperTask
	pq     *reaperHeap
	mu     sync.Mutex
	wakeCh chan struct{}
}

type reaperHeap []*ReaperTask

func (h reaperHeap) Len() int { return len(h) }
func (h reaperHeap) Less(i, j int) bool {
	if h[i].Deadline.IsZero() {
		return true
	}
	if h[j].Deadline.IsZero() {
		return false
	}
	return h[i].Deadline.Before(h[j].Deadline)
}
func (h reaperHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *reaperHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*ReaperTask)
	item.index = n
	*h = append(*h, item)
}
func (h *reaperHeap) Pop() interface{} {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	item.index = -1
	return item
}

type Reaper struct {
	shards     []*reaperShard
	shardCount uint32
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	minTick    time.Duration
	timerLimit time.Duration
	handler    ReaperHandler
	handlerMu  sync.RWMutex
	logger     *ll.Logger
	metrics    *ReaperMetrics
	stopped    atomic.Bool
}

type ReaperOption func(*Reaper)

func ReaperWithLogger(l *ll.Logger) ReaperOption {
	return func(r *Reaper) {
		if l != nil {
			r.logger = l.Namespace("reaper")
		}
	}
}

func ReaperWithHandler(h ReaperHandler) ReaperOption {
	return func(r *Reaper) {
		r.handler = h
	}
}

func ReaperWithShards(count uint32) ReaperOption {
	return func(r *Reaper) {
		if count >= 1 && (count&(count-1)) == 0 {
			r.shardCount = count
		}
	}
}

func ReaperWithMinTick(tick time.Duration) ReaperOption {
	return func(r *Reaper) {
		if tick > 0 {
			r.minTick = tick
		}
	}
}

func ReaperWithTimerLimit(limit time.Duration) ReaperOption {
	return func(r *Reaper) {
		if limit > 0 {
			r.timerLimit = limit
		}
	}
}

func NewReaper(ttl time.Duration, opts ...ReaperOption) *Reaper {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Reaper{
		shardCount: 16,
		ctx:        ctx,
		cancel:     cancel,
		minTick:    10 * time.Millisecond,
		timerLimit: ttl,
		logger:     logger.Namespace("reaper"),
		metrics:    &ReaperMetrics{},
	}
	for _, opt := range opts {
		opt(r)
	}
	r.shards = make([]*reaperShard, r.shardCount)
	for i := uint32(0); i < r.shardCount; i++ {
		pq := make(reaperHeap, 0)
		heap.Init(&pq)
		r.shards[i] = &reaperShard{
			items:  make(map[string]*ReaperTask),
			pq:     &pq,
			wakeCh: make(chan struct{}, 1),
		}
		r.wg.Add(1)
		go r.loop(i)
	}
	return r
}

func (r *Reaper) Metrics() *ReaperMetrics {
	return r.metrics
}

// Start is a no-op for backward compatibility. Reaper starts automatically now.
func (r *Reaper) Start() {}

// Register sets the handler for expired tasks (backward compatible).
func (r *Reaper) Register(h ReaperHandler) {
	r.handlerMu.Lock()
	defer r.handlerMu.Unlock()
	r.handler = h
}

func (r *Reaper) getShard(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32() & (r.shardCount - 1)
}

func (r *Reaper) loop(idx uint32) {
	defer r.wg.Done()
	s := r.shards[idx]
	for {
		s.mu.Lock()
		var nextDeadline time.Time
		var hasItem bool
		if s.pq.Len() > 0 {
			nextDeadline = (*s.pq)[0].Deadline
			hasItem = true
		}
		s.mu.Unlock()

		if !hasItem {
			select {
			case <-r.ctx.Done():
				return
			case <-s.wakeCh:
				continue
			}
		}

		now := time.Now()
		if now.After(nextDeadline) || now.Equal(nextDeadline) {
			r.processExpired(s)
			continue
		}

		wait := nextDeadline.Sub(now)
		if wait < r.minTick {
			wait = r.minTick
		}

		select {
		case <-r.ctx.Done():
			return
		case <-time.After(wait):
			r.processExpired(s)
		case <-s.wakeCh:
			continue
		}
	}
}

func (r *Reaper) processExpired(s *reaperShard) {
	now := time.Now()
	s.mu.Lock()
	expired := make([]*ReaperTask, 0)
	for s.pq.Len() > 0 {
		item := (*s.pq)[0]
		if now.After(item.Deadline) || now.Equal(item.Deadline) {
			heap.Pop(s.pq)
			delete(s.items, item.ID)
			expired = append(expired, item)
		} else {
			break
		}
	}
	r.metrics.recordActiveCount(len(s.items))
	s.mu.Unlock()

	r.handlerMu.RLock()
	handler := r.handler
	r.handlerMu.RUnlock()

	for _, item := range expired {
		start := time.Now()
		var handleErr error
		if handler != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.timerLimit)
			handler(ctx, item.ID)
			cancel()
		} else {
			handleErr = context.Canceled
		}
		r.metrics.recordExpiration(time.Since(start), handler != nil, handleErr)
	}
	r.metrics.LoopIterations.Add(1)
}

func (r *Reaper) Touch(id string) {
	if r.stopped.Load() {
		return
	}
	idx := r.getShard(id)
	s := r.shards[idx]
	deadline := time.Now().Add(r.timerLimit)

	s.mu.Lock()
	r.metrics.recordTouch()

	if task, exists := s.items[id]; exists {
		task.Deadline = deadline
		heap.Fix(s.pq, task.index)
	} else {
		task := &ReaperTask{ID: id, Deadline: deadline}
		heap.Push(s.pq, task)
		s.items[id] = task
	}
	r.metrics.recordActiveCount(len(s.items))
	s.mu.Unlock()

	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (r *Reaper) TouchAt(id string, deadline time.Time) {
	if r.stopped.Load() {
		return
	}
	idx := r.getShard(id)
	s := r.shards[idx]

	s.mu.Lock()
	r.metrics.recordTouchAt()

	if task, exists := s.items[id]; exists {
		task.Deadline = deadline
		heap.Fix(s.pq, task.index)
	} else {
		task := &ReaperTask{ID: id, Deadline: deadline}
		heap.Push(s.pq, task)
		s.items[id] = task
	}
	r.metrics.recordActiveCount(len(s.items))
	s.mu.Unlock()

	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (r *Reaper) Remove(id string) bool {
	if r.stopped.Load() {
		return false
	}
	idx := r.getShard(id)
	s := r.shards[idx]

	s.mu.Lock()
	defer s.mu.Unlock()

	r.metrics.RemoveCalls.Add(1)
	if task, exists := s.items[id]; exists {
		heap.Remove(s.pq, task.index)
		delete(s.items, id)
		r.metrics.recordActiveCount(len(s.items))
		return true
	}
	return false
}

func (r *Reaper) Clear() int {
	if r.stopped.Load() {
		return 0
	}
	count := 0
	for _, s := range r.shards {
		s.mu.Lock()
		count += len(s.items)
		for id := range s.items {
			task := s.items[id]
			heap.Remove(s.pq, task.index)
			delete(s.items, id)
		}
		s.mu.Unlock()
	}
	r.metrics.ClearCalls.Add(1)
	r.metrics.recordActiveCount(0)
	return count
}

func (r *Reaper) Count() int {
	if r.stopped.Load() {
		return 0
	}
	count := 0
	for _, s := range r.shards {
		s.mu.Lock()
		count += len(s.items)
		s.mu.Unlock()
	}
	return count
}

func (r *Reaper) Deadline() (time.Time, bool) {
	if r.stopped.Load() {
		return time.Time{}, false
	}
	var earliest time.Time
	found := false
	for _, s := range r.shards {
		s.mu.Lock()
		if s.pq.Len() > 0 {
			d := (*s.pq)[0].Deadline
			if !found || d.Before(earliest) {
				earliest = d
				found = true
			}
		}
		s.mu.Unlock()
	}
	return earliest, found
}

func (r *Reaper) Stop() {
	if !r.stopped.CompareAndSwap(false, true) {
		return
	}
	r.metrics.StopsReceived.Add(1)
	r.cancel()
	r.wg.Wait()
	for _, s := range r.shards {
		s.mu.Lock()
		s.items = nil
		s.pq = nil
		s.mu.Unlock()
	}
}
