# Jack

**Production-grade concurrency toolkit for Go**

Jack provides the missing pieces for building robust, observable concurrent systems in Go. No magic, no reflection hacks — just solid patterns you'd otherwise write yourself, with full metrics on every component.

## Why This Exists

Go's concurrency primitives are excellent, but production systems need more:

- Panic recovery that doesn't crash your entire process
- Backpressure when queues fill up, with priority ordering
- Circuit breaking so a slow upstream doesn't take down your service
- Goroutine lifecycle tracking so nothing leaks silently
- Graceful shutdown that finishes in-flight work
- Health checks that degrade and accelerate automatically
- Observability into what every component is actually doing

Jack fills these gaps without getting in your way. Components that manage resources or handle traffic expose a `Metrics()` method with atomic counters safe for concurrent reads.

---

## What's Inside

### Pool
Fixed-size worker pool with backpressure. Tasks queue when workers are busy. Submissions fail fast when the queue is full. Full metrics: submitted, completed, failed, rejected, panics recovered, active workers, queue depth.

```go
pool := jack.NewPool(5, jack.PoolingWithQueueSize(100))
defer pool.Shutdown(30 * time.Second)

pool.Submit(jack.Func(func() error {
    return nil
}))

m := pool.Metrics()
fmt.Println(m.TasksCompleted.Load(), m.ActiveWorkers.Load())
```

### Semaphore
Priority-aware semaphore with CoDel queue management. The fast path (`TryAcquire`, `TryAcquireN`) is lock-free. Blocking acquire uses per-priority queues that switch from FIFO to LIFO under sustained overload. Exposes `Available()`, `Capacity()`, and accurate live `QueueDepth`.

```go
sem := jack.NewSemaphore(10,
    jack.SemaphoreWithTargetSojourn(5*time.Millisecond),
    jack.SemaphoreWithMaxSojourn(500*time.Millisecond),
)
defer sem.Close()

if sem.TryAcquireN(jack.PriorityHigh, 3) {
    defer func() {
        sem.Release()
        sem.Release()
        sem.Release()
    }()
}

if err := sem.Acquire(ctx, jack.PriorityCritical); err != nil {
    return err
}
defer sem.Release()
```

### RateLimiter
Token bucket with priority queueing. `Allow`/`AllowN` are lock-free. `Acquire` blocks with context. `Reserve` returns a non-blocking `Reservation` the caller can inspect and cancel without blocking any goroutine.

```go
rl := jack.NewRateLimiter(1000, 100)
defer rl.Close()

if rl.Allow(jack.PriorityHigh) {
    // proceed
}

res := rl.Reserve(1, jack.ReserveWithMaxDelay(50*time.Millisecond))
if !res.OK() {
    return ErrTooManyRequests
}
if err := res.Wait(ctx); err != nil {
    res.Cancel()
    return err
}

if err := rl.Acquire(ctx, jack.PriorityHigh); err != nil {
    return err
}
```

### Throttle
Client-side self-tuning throttle. Observes upstream acceptance and rejection rates, probabilistically dropping local requests before sending when the upstream is overloaded.

```go
throttle := jack.NewThrottle(jack.priorityCount,
    jack.ThrottleWithRatio(2.0),
    jack.ThrottleWithWindowResetSamples(500),
)
defer throttle.Close()

if upstreamErr != nil {
    throttle.Rejected(jack.PriorityHigh)
} else {
    throttle.Accepted(jack.PriorityHigh)
}

if !throttle.Allow(jack.PriorityHigh) {
    return ErrThrottled
}
```

### Circuit Breaker
Three-state machine (Closed → Open → HalfOpen). All state transitions are lock-free atomic CAS.

```go
breaker := jack.NewBreaker("payments-api",
    jack.BreakerWithThreshold(5),
    jack.BreakerWithOpenTimeout(10*time.Second),
    jack.BreakerWithSuccessThreshold(2),
    jack.BreakerWithOnStateChange(func(name string, from, to jack.BreakerState) {
        metrics.RecordStateChange(name, from.String(), to.String())
    }),
)

err := breaker.Call(ctx, func(ctx context.Context) error {
    return paymentsClient.Charge(ctx, req)
})
if errors.Is(err, jack.ErrBreakerOpen) {
    return ErrServiceUnavailable
}
```

### Bulkhead
Isolates failure domains by giving each named partition its own bounded concurrency budget backed by an independent Semaphore.

```go
bh := jack.NewBulkhead(
    jack.BulkheadWithPartition("payments", 20),
    jack.BulkheadWithPartition("reports", 5),
    jack.BulkheadWithDefaultCapacity(10), // auto-creates unknown partitions
)
defer bh.Close()

err := bh.Call(ctx, "payments", jack.PriorityHigh, func(ctx context.Context) error {
    return db.Query(ctx, ...)
})
```

### Adaptive Concurrency Limiter
AIMD gradient controller that adjusts its concurrency limit dynamically based on observed RTT.

```go
limiter := jack.NewAdaptiveLimiter(
    jack.AdaptiveWithInitialLimit(20),
    jack.AdaptiveWithTargetP50(50*time.Millisecond),
    jack.AdaptiveWithMinLimit(5),
    jack.AdaptiveWithMaxLimit(100),
)
defer limiter.Close()

err := limiter.Call(ctx, jack.PriorityHigh, func(ctx context.Context) error {
    return upstream.Call(ctx, req)
})
```

### Retry
Exponential backoff with full jitter, configurable predicate, and per-call metrics.

```go
policy := jack.NewRetry(
    jack.RetryWithMaxAttempts(5),
    jack.RetryWithBaseDelay(100*time.Millisecond),
    jack.RetryWithRetryIf(func(err error) bool {
        return !errors.Is(err, ErrPermanent)
    }),
)

err := policy.Do(ctx, func(ctx context.Context) error {
    return upstream.Call(ctx, req)
})
```

### Hedged Requests
Fire a duplicate request after a configurable delay. Whichever responds first is returned; the other is cancelled. Adaptive delay from RTT samples.

```go
hedger := jack.NewHedgerOf[*UserResponse](
    jack.HedgeWithPercentile(95),
    jack.HedgeWithMinSamples(20),
    jack.HedgeWithMaxConcurrent(50),
)

user, err := hedger.Do(ctx, func(ctx context.Context) (*UserResponse, error) {
    return userClient.Get(ctx, id)
})
```

### Lease
A time-bounded semaphore slot with automatic reclamation via the Reaper.

```go
sem := jack.NewSemaphore(20)
lm := jack.NewLeaser(sem, jack.LeaserWithTTL(30*time.Second))
defer lm.Close()

lease, err := lm.Acquire(ctx, requestID, jack.PriorityHigh, 0)
if err != nil {
    return err
}
defer lease.Release()
```

### Queue
Bounded, multi-priority, multi-consumer work queue. Per-priority bins with tail-drop under saturation.

```go
q := jack.NewQueue(func(ctx context.Context, item any) error {
    return process(ctx, item.(*Request))
},
    jack.QueueWithCapacity(1000),
    jack.QueueWithWorkers(8),
    jack.QueueWithTimeout(5*time.Second),
)
defer q.Close()

if err := q.Enqueue(jack.PriorityCritical, req); err == jack.ErrQueueFull {
    return ErrBackpressure
}
```

### Routines
Goroutine tracker and lifecycle manager. Every spawned goroutine is registered, tracked by state, captures panic stacks, and is guaranteed joined by `Stop` or `Wait`.

```go
rt := jack.NewRoutines(
    jack.RoutineWithOnPanic(func(info jack.RoutineInfo) {
        log.Printf("panic in %s: %v\n%s", info.ID, info.Err, info.Stack)
    }),
)
defer rt.Stop()

rt.Spawn("fetch-users", func(ctx context.Context) error {
    return fetchUsers(ctx)
})

rt.Background("heartbeat", 0, func(ctx context.Context) error {
    return sendHeartbeat(ctx)
})

info, ok := rt.Info("fetch-users#1")
fmt.Println(info.State, info.StartedAt)
```

### Future/Promise
Type-safe async computation with composition. Wait for results, chain transformations, recover from errors.

```go
f := jack.Async(func() (string, error) {
    return fetchUser()
})

result, err := f.Then(ctx, func(user string) (any, error) {
    return fetchProfile(user)
}).Await()
```

### Doctor
Health check scheduler that degrades and accelerates. Tracks consecutive failures, applies jitter, notifies observers.

```go
doctor := jack.NewDoctor(jack.DoctorWithMaxConcurrent(10))
doctor.Add(jack.NewPatient(jack.PatientConfig{
    ID:          "database",
    Interval:    10 * time.Second,
    MaxFailures: 3,
    Check:       checkDB,
    OnStateChange: func(e jack.PatientEvent) {
        if e.State == jack.PatientFailed {
            triggerAlert(e.ID)
        }
    },
}))
```

### Debouncer
Rate-limit rapid calls. Execute only after a quiet period or when thresholds are hit.

```go
db := jack.NewDebouncer(
    jack.WithDebounceDelay(500*time.Millisecond),
    jack.WithDebounceMaxCalls(10),
)
db.Do(expensiveOperation)
```

### Looper
Background task with exponential backoff and jitter. Perfect for reconciliation loops.

```go
looper := jack.NewLooper(reconcile,
    jack.WithLooperInterval(5*time.Second),
    jack.WithLooperBackoff(true),
    jack.WithLooperMaxInterval(time.Minute),
)
looper.Start()
```

### Shutdown
Graceful termination with signal handling. Register cleanup in LIFO order.

```go
sd := jack.NewShutdown(
    jack.ShutdownWithTimeout(30*time.Second),
    jack.ShutdownConcurrent(),
)
sd.RegisterCloser("db", db)
sd.RegisterFunc("cache", cache.Flush)
sd.RegisterWithContext("grpc", grpcServer.GracefulStop)
sd.Wait()
```

### Reaper
TTL expiration with min-heap and sharding.

```go
reaper := jack.NewReaper(5*time.Minute,
    jack.ReaperWithHandler(func(ctx context.Context, id string) {
        cleanup(id)
    }),
)
reaper.Touch("session-123")
```

### Lifetime
Scheduled callbacks with keep-alive resets.

```go
lm := jack.NewLifetime()
lm.ScheduleTimed(ctx, "heartbeat", func(ctx context.Context, id string) {
    markDead(id)
}, 30*time.Second)
lm.ResetTimed("heartbeat")
```

### PLocal — Goroutine-Sharded Storage
Eliminates cache-line contention by giving each goroutine its own shard. `Get`/`Set`/`With` access only the current goroutine's slot with zero cross-goroutine contention. Use `Fold` to aggregate across all shards when needed.

```go
// High-throughput counter without atomic contention.
var counter jack.PLocalCounter
counter.Add(1)
total := counter.Value()

// Generic sharded storage.
var storage jack.PLocal[map[string]int]
storage.With(func(m *map[string]int) {
    (*m)["key"] = 42
})

// Aggregate across all shards.
sum := storage.Fold(0, func(acc, val int) int { return acc + val })
```

### Flight — Request Coalescing
Deduplicate concurrent executions by key. The first caller becomes the leader and runs `fn` directly; waiters block until the leader finishes and receive the same result. Panics are recovered and returned as `*CaughtPanic` to all waiters.

```go
flight := jack.NewFlight()

res, err := flight.Do("user:123", func() (interface{}, error) {
    return fetchUser(123)
})
// Concurrent callers with the same key receive the same result without re-executing.
```

### OnceGroup — Generic Singleflight
Coalesce in-flight duplicate requests for the same key with typed results. The first call executes `fn`; subsequent calls wait and share the result. Context cancellation affects only the waiter.

```go
var g jack.OnceGroup[string, *User]

user, err, shared := g.Do(ctx, "user-123", func() (*User, error) {
    return fetchUser("user-123")
})
```

### Gate — Reusable Barrier
Open lets all current and future waiters pass. Close blocks future waiters. Pulse wakes current waiters but remains closed.

```go
var g jack.Gate
g.Close()

go func() {
    g.Wait() // blocks
}()

g.Open() // all waiters proceed
```

### Latch — One-Shot Signal
Starts closed; once `Open()` is called, remains open forever.

```go
var l jack.Latch
go func() {
    l.Wait() // blocks until Open()
}()
l.Open()
```

### Coalescer — Batch Flushing
Merge discrete items into batches and flush them together. Useful for write coalescing, metrics aggregation, or event batching.

```go
c := jack.NewCoalescer(func(items []interface{}) error {
    return batchWrite(items)
}, 100, 5*time.Second)

c.Add(event1)
c.Add(event2) // flushes when batch reaches 100 or timer fires
```

### Observable / Observer
Type-safe pub/sub with worker pool for async notification delivery. Panic recovery in observers prevents worker crashes.

```go
obs := jack.NewObservable[jack.Event](5)
obs.Add(myObserver)
obs.Notify(event1, event2)
obs.Shutdown()
```

### Safely — Context-Aware Mutex
Lock with panic recovery and context cancellation support.

```go
var mu jack.Safely
err := mu.SafeCtx(ctx, func() error {
    return criticalSection()
})
```

### Helpers
Convenience functions for common patterns.

```go
// Wait for fn with context cancellation.
err := jack.Wait(ctx, fn)

// Run fn with timeout.
err := jack.WaitTimeout(5*time.Second, fn)

// Run fn and return its error or context cancellation.
err := jack.Execute(ctx, fn)

// Repeat fn at interval until context cancelled.
err := jack.Repeat(ctx, interval, fn)

// Run fn for i in [0,n) concurrently, fail-fast on first error.
err := jack.Parallel(ctx, n, fn)
```

### Runner, Scheduler, Group
Single-worker queue, cron-style scheduling, and coordinated goroutine groups with error collection.

---

## Priority System

All backpressure components share a four-level priority system. Lower numeric value = higher priority = served first.

| Constant | Value | Intended use |
|---|---|---|
| `PriorityCritical` | 0 | Admin ops, user-facing critical paths |
| `PriorityHigh` | 1 | Standard user requests |
| `PriorityMedium` | 2 | Async work, type-ahead |
| `PriorityLow` | 3 | Backfill, batch, probes |

---

## Observability

Components that manage resources or handle traffic expose a `Metrics()` method with atomic counters safe for concurrent reads without locks.

```go
obs := jack.NewObservable[jack.Event](10)
obs.Add(myObserver)
pool := jack.NewPool(5, jack.PoolingWithObservable(obs))

sem.Metrics().QueueDepth.Load()
rl.Metrics().TokensConsumed.Load()
breaker.Metrics().StateChanges.Load()
rt.Metrics().Active.Load()
limiter.Metrics().CurrentLimit.Load()
```

---

## Error Handling

Panics become `*jack.CaughtPanic` with stack traces. No silent failures.

```go
err := jack.Safe(func() error {
    panic("boom")
})
if cp, ok := err.(*jack.CaughtPanic); ok {
    log.Printf("panic: %v\n%s", cp.Value, cp.Stack)
}
```

---

## When To Use What

| Problem | Use |
|---|---|
| Process many independent tasks concurrently | `Pool` |
| Need result from async operation | `Future` |
| Run periodic health checks with degradation | `Doctor` |
| Rate-limit bursty calls (blocking) | `RateLimiter.Acquire` |
| Rate-limit bursty calls (non-blocking inspect) | `RateLimiter.Reserve` |
| Client-side adaptive load shedding | `Throttle` |
| Stop calling a failing upstream | `Breaker` |
| Isolate concurrency budgets per upstream | `Bulkhead` |
| Auto-tune concurrency limit to RTT | `AdaptiveLimiter` |
| Retry transient errors with backoff | `Retry` |
| Reduce tail latency with duplicate requests | `Hedger` / `HedgerOf[T]` |
| Bound concurrent slots with auto-reclaim | `Leaser` |
| Prioritised async work queue | `Queue` |
| Track and terminate all goroutines | `Routines` |
| Bound concurrent access with priorities | `Semaphore` |
| Eliminate cache-line contention on counters | `PLocal` / `PLocalCounter` |
| Deduplicate concurrent identical requests | `Flight` / `OnceGroup` |
| Signal/wait between goroutines | `Gate` / `Latch` |
| Batch items for efficient flushing | `Coalescer` |
| Publish events to multiple observers | `Observable` |
| Rate-limit rapid calls | `Debouncer` |
| Background loop with backoff | `Looper` |
| Graceful shutdown with cleanup ordering | `Shutdown` |
| Expire items after TTL | `Reaper` |
| Schedule callbacks with keep-alive | `Lifetime` |
| Coordinate multiple goroutines, collect errors | `Group` |
| Sequential async processing | `Runner` |
| Cron-style recurring tasks | `Scheduler` |
| Safe locking with timeouts | `Safely` |

---

## Composing Components

```
Request
  → Queue (absorb burst, priority ordering)
  → Breaker (stop calling dead upstream)
  → Bulkhead (cap per-upstream concurrency)
  → AdaptiveLimiter (auto-tune to RTT)
  → Throttle (client-side load shedding)
  → RateLimiter (token bucket)
  → Hedger (cut tail latency)
  → Retry (handle transient errors)
  → Upstream
```

---

## Testing

```bash
go test -v -race ./...
go test -bench=. -benchmem -cpu=8 -run='^$' ./...
```

Race detector is your friend. Jack is race-free by design.

---

## License

MIT