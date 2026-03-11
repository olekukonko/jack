package jack

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// ReaperMetrics tracks Reaper operational metrics.
type ReaperMetrics struct {
	TouchCalls     atomic.Uint64
	TouchAtCalls   atomic.Uint64
	RemoveCalls    atomic.Uint64
	ClearCalls     atomic.Uint64
	ExpiredTotal   atomic.Uint64
	ExpiredHandled atomic.Uint64
	ExpiredMissed  atomic.Uint64
	ExpiredErrors  atomic.Uint64

	ActiveTasks    atomic.Int64
	MaxActiveTasks atomic.Int64
	TotalTasksSeen atomic.Uint64

	AvgExpirationMs atomic.Int64
	MinExpirationMs atomic.Int64
	MaxExpirationMs atomic.Int64

	LoopIterations atomic.Uint64
	SignalsSent    atomic.Uint64
	StopsReceived  atomic.Uint64
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

	// Min
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

	// Max
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

	// Rolling average
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

// ReaperHandler handles the expiration event for the Reaper.
type ReaperHandler func(context.Context, string)

// ReaperTask represents a scheduled expiration.
type ReaperTask struct {
	ID       string
	Deadline time.Time
	index    int
}

// Reaper manages expiration with full metrics.
type Reaper struct {
	mu         sync.Mutex
	tasks      reaperHeap
	taskMap    map[string]*ReaperTask
	handler    ReaperHandler
	signal     chan struct{}
	stop       chan struct{}
	stopped    atomic.Bool
	defaultTTL time.Duration
	wg         sync.WaitGroup
	logger     *ll.Logger
	metrics    *ReaperMetrics
}

// ReaperOption configures a Reaper.
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

// NewReaper creates a new Reaper with metrics.
func NewReaper(ttl time.Duration, opts ...ReaperOption) *Reaper {
	r := &Reaper{
		taskMap:    make(map[string]*ReaperTask),
		signal:     make(chan struct{}, 1),
		stop:       make(chan struct{}),
		defaultTTL: ttl,
		logger:     logger.Namespace("reaper"),
		metrics:    &ReaperMetrics{},
	}
	heap.Init(&r.tasks)
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Metrics returns the reaper's metrics.
func (r *Reaper) Metrics() *ReaperMetrics {
	return r.metrics
}

func (r *Reaper) Start() {
	if r.stopped.Load() {
		return
	}
	r.wg.Add(1)
	go r.loop()
}

func (r *Reaper) Register(h ReaperHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handler = h
}

func (r *Reaper) Touch(id string) {
	if r.stopped.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics.recordTouch()
	deadline := time.Now().Add(r.defaultTTL)

	if task, exists := r.taskMap[id]; exists {
		task.Deadline = deadline
		heap.Fix(&r.tasks, task.index)
	} else {
		task := &ReaperTask{ID: id, Deadline: deadline}
		heap.Push(&r.tasks, task)
		r.taskMap[id] = task
	}

	r.metrics.recordActiveCount(len(r.taskMap))
	r.signalLoop()
}

func (r *Reaper) TouchAt(id string, deadline time.Time) {
	if r.stopped.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics.recordTouchAt()

	if task, exists := r.taskMap[id]; exists {
		task.Deadline = deadline
		heap.Fix(&r.tasks, task.index)
		r.logger.Debugf("Reaper.TouchAt: updated task %s to deadline %v", id, deadline)
	} else {
		task := &ReaperTask{ID: id, Deadline: deadline}
		heap.Push(&r.tasks, task)
		r.taskMap[id] = task
		r.logger.Debugf("Reaper.TouchAt: added new task %s with deadline %v", id, deadline)
	}

	r.metrics.recordActiveCount(len(r.taskMap))
	r.signalLoop()
}

func (r *Reaper) Remove(id string) bool {
	if r.stopped.Load() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics.RemoveCalls.Add(1)
	removed := r.removeLocked(id)
	if removed {
		r.metrics.recordActiveCount(len(r.taskMap))
		r.logger.Debugf("Reaper.Remove: removed task %s", id)
	}
	return removed
}

func (r *Reaper) Clear() int {
	if r.stopped.Load() {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics.ClearCalls.Add(1)
	count := len(r.taskMap)
	for id := range r.taskMap {
		r.removeLocked(id)
	}
	r.metrics.recordActiveCount(0)
	r.logger.Debugf("Reaper.Clear: removed %d tasks", count)
	return count
}

func (r *Reaper) Count() int {
	if r.stopped.Load() {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.taskMap)
}

func (r *Reaper) Deadline() (time.Time, bool) {
	if r.stopped.Load() {
		return time.Time{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks.Len() > 0 {
		return r.tasks[0].Deadline, true
	}
	return time.Time{}, false
}

func (r *Reaper) Stop() {
	if !r.stopped.CompareAndSwap(false, true) {
		return
	}
	r.metrics.StopsReceived.Add(1)
	r.logger.Info("Reaper.Stop: initiating shutdown")
	close(r.stop)
	r.wg.Wait()
	r.mu.Lock()
	r.taskMap = nil
	r.tasks = nil
	r.mu.Unlock()
	r.logger.Info("Reaper.Stop: shutdown complete")
}

func (r *Reaper) signalLoop() {
	r.metrics.SignalsSent.Add(1)
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

func (r *Reaper) loop() {
	defer r.wg.Done()
	r.logger.Info("Reaper.loop: starting background expiration loop")
	timer := time.NewTimer(0)
	<-timer.C

	for {
		r.metrics.LoopIterations.Add(1)

		var sleepDuration time.Duration
		var hasTasks bool
		r.mu.Lock()
		if r.tasks.Len() > 0 {
			hasTasks = true
			now := time.Now()
			earliest := r.tasks[0]
			if earliest.Deadline.Before(now) || earliest.Deadline.Equal(now) {
				task := heap.Pop(&r.tasks).(*ReaperTask)
				delete(r.taskMap, task.ID)
				r.metrics.recordActiveCount(len(r.taskMap))
				handler := r.handler
				r.mu.Unlock()

				start := time.Now()
				var handleErr error
				if handler != nil {
					handler(context.Background(), task.ID)
				} else {
					handleErr = context.Canceled
				}
				elapsed := time.Since(start)

				r.metrics.recordExpiration(elapsed, handler != nil, handleErr)
				continue
			}
			sleepDuration = earliest.Deadline.Sub(now)
		}
		r.mu.Unlock()

		if hasTasks {
			stopAndDrainTimer(timer)
			timer.Reset(sleepDuration)
		} else {
			stopAndDrainTimer(timer)
			timer.Reset(time.Hour)
		}

		select {
		case <-r.stop:
			stopAndDrainTimer(timer)
			return
		case <-r.signal:
			stopAndDrainTimer(timer)
		case <-timer.C:
		}
	}
}

func (r *Reaper) removeLocked(id string) bool {
	if task, exists := r.taskMap[id]; exists {
		heap.Remove(&r.tasks, task.index)
		delete(r.taskMap, id)
		return true
	}
	return false
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
