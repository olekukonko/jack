package jack

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// ReaperHandler handles the expiration event for the Reaper.
type ReaperHandler func(context.Context, string)

// ReaperTask represents a scheduled expiration in the Reaper.
type ReaperTask struct {
	ID       string
	Deadline time.Time
	index    int // internal for heap
}

// Reaper manages expiration of items efficiently using a Min-Heap.
// O(log N) insert/update, O(1) memory per item.
// Single background goroutine. Thread-safe via mutex and atomic operations.
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
}

// ReaperOption configures a Reaper during creation.
type ReaperOption func(*Reaper)

// ReaperWithLogger sets a custom logger for the Reaper.
func ReaperWithLogger(l *ll.Logger) ReaperOption {
	return func(r *Reaper) {
		if l != nil {
			r.logger = l.Namespace("reaper")
		}
	}
}

// ReaperWithHandler sets the expiration handler during creation.
func ReaperWithHandler(h ReaperHandler) ReaperOption {
	return func(r *Reaper) {
		r.handler = h
	}
}

// NewReaper creates a new Reaper with the specified default TTL.
// The TTL determines how long items live before expiration if Touch() is used.
// Call Start() to begin processing after configuration.
func NewReaper(ttl time.Duration, opts ...ReaperOption) *Reaper {
	r := &Reaper{
		taskMap:    make(map[string]*ReaperTask),
		signal:     make(chan struct{}, 1),
		stop:       make(chan struct{}),
		defaultTTL: ttl,
		logger:     logger.Namespace("reaper"),
	}
	heap.Init(&r.tasks)

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Start begins the background expiration loop.
// Must be called after setting a handler via Register or ReaperWithHandler.
// Idempotent - safe to call multiple times.
func (r *Reaper) Start() {
	if r.stopped.Load() {
		return
	}
	r.wg.Add(1)
	go r.loop()
}

// Register sets the expiration callback function.
// Thread-safe via mutex. Can be called before or after Start().
func (r *Reaper) Register(h ReaperHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handler = h
}

// Touch schedules or resets the timer for an ID using the default TTL.
// If the ID exists, its deadline is extended. If not, it is added.
// No-op if Reaper is stopped. Thread-safe.
func (r *Reaper) Touch(id string) {
	if r.stopped.Load() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	deadline := time.Now().Add(r.defaultTTL)

	if task, exists := r.taskMap[id]; exists {
		task.Deadline = deadline
		heap.Fix(&r.tasks, task.index)
		// r.logger.Debugf("Reaper.Touch: updated task %s to deadline %v", id, deadline)
	} else {
		task := &ReaperTask{ID: id, Deadline: deadline}
		heap.Push(&r.tasks, task)
		r.taskMap[id] = task
		// r.logger.Debugf("Reaper.Touch: added new task %s with deadline %v", id, deadline)
	}

	// Non-blocking signal to wake up loop if this new task is sooner
	r.signalLoop()
}

// TouchAt schedules or resets the timer for an ID to a specific deadline.
// Useful for custom expiration times different from the default TTL.
// No-op if Reaper is stopped. Thread-safe.
func (r *Reaper) TouchAt(id string, deadline time.Time) {
	if r.stopped.Load() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

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

	r.signalLoop()
}

// Remove cancels a task for an ID, preventing its expiration.
// Returns true if the task was found and removed, false otherwise.
// Thread-safe.
func (r *Reaper) Remove(id string) bool {
	if r.stopped.Load() {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	removed := r.removeLocked(id)
	if removed {
		r.logger.Debugf("Reaper.Remove: removed task %s", id)
	}
	return removed
}

// Clear removes all tasks and returns the count of removed tasks.
// Useful for bulk cleanup or resetting state.
// Thread-safe.
func (r *Reaper) Clear() int {
	if r.stopped.Load() {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	count := len(r.taskMap)
	for id := range r.taskMap {
		r.removeLocked(id)
	}
	r.logger.Debugf("Reaper.Clear: removed %d tasks", count)
	return count
}

// Count returns the number of pending expiration tasks.
// Thread-safe.
func (r *Reaper) Count() int {
	if r.stopped.Load() {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.taskMap)
}

// Deadline returns the next expiration time and whether there are pending tasks.
// Useful for monitoring or scheduling other activities around expirations.
// Thread-safe.
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

// Stop shuts down the Reaper gracefully, stopping the background loop.
// Waits for current expirations to complete. Idempotent.
// Thread-safe.
func (r *Reaper) Stop() {
	if !r.stopped.CompareAndSwap(false, true) {
		return
	}

	r.logger.Info("Reaper.Stop: initiating shutdown")
	close(r.stop)
	r.wg.Wait()

	// Clear maps after stopping to free memory
	r.mu.Lock()
	r.taskMap = nil
	r.tasks = nil
	r.mu.Unlock()

	r.logger.Info("Reaper.Stop: shutdown complete")
}

type reaperHeap []*ReaperTask

func (h reaperHeap) Len() int { return len(h) }

func (h reaperHeap) Less(i, j int) bool {
	// Handle zero times (shouldn't happen, but be defensive)
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
	// Avoid memory leak
	old[n-1] = nil
	*h = old[0 : n-1]
	item.index = -1 // Mark as removed
	return item
}

// signalLoop sends a non-blocking signal to wake up the loop
func (r *Reaper) signalLoop() {
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

// loop runs the background expiration processing.
// Uses a timer to sleep until the next expiration, waking on new tasks or stop signal.
func (r *Reaper) loop() {
	defer r.wg.Done()
	r.logger.Info("Reaper.loop: starting background expiration loop")

	timer := time.NewTimer(0) // Initially expired to check heap
	<-timer.C                 // Drain initial tick

	for {
		var sleepDuration time.Duration
		var hasTasks bool

		r.mu.Lock()
		if r.tasks.Len() > 0 {
			hasTasks = true
			now := time.Now()
			earliest := r.tasks[0]

			if earliest.Deadline.Before(now) || earliest.Deadline.Equal(now) {
				// Expired! Pop and execute.
				task := heap.Pop(&r.tasks).(*ReaperTask)
				delete(r.taskMap, task.ID)

				// Capture handler locally to run outside lock
				handler := r.handler
				r.mu.Unlock()

				if handler != nil {
					// r.logger.Debugf("Reaper.loop: executing handler for expired task %s", task.ID)
					handler(context.Background(), task.ID)
				} else {
					// r.logger.Warn("Reaper.loop: task %s expired but no handler registered", task.ID)
				}

				// Loop immediately to check for other expired tasks
				continue
			}

			// Not expired yet
			sleepDuration = earliest.Deadline.Sub(now)
		}
		r.mu.Unlock()

		// Set timer
		if hasTasks {
			timer.Reset(sleepDuration)
			// r.logger.Debugf("Reaper.loop: sleeping for %v until next expiration", sleepDuration)
		} else {
			// No tasks, sleep longer
			timer.Reset(time.Hour)
			// r.logger.Debugf("Reaper.loop: no tasks, sleeping for 1 hour")
		}

		select {
		case <-r.stop:
			timer.Stop()
			// r.logger.Info("Reaper.loop: received stop signal, exiting")
			return
		case <-r.signal:
			// Task added or updated, check heap again
			if !timer.Stop() {
				// Drain timer channel if it fired
				select {
				case <-timer.C:
				default:
				}
			}
			// r.logger.Debugf("Reaper.loop: received signal, checking heap again")
		case <-timer.C:
			// Timer fired, loop again to check heap
			// r.logger.Debugf("Reaper.loop: timer fired, checking for expirations")
		}
	}
}

// removeLocked removes a task without acquiring lock (caller must hold lock)
func (r *Reaper) removeLocked(id string) bool {
	if task, exists := r.taskMap[id]; exists {
		heap.Remove(&r.tasks, task.index)
		delete(r.taskMap, id)
		return true
	}
	return false
}
