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

Jack fills these gaps without getting in your way. Every component exposes a `Metrics()` method with atomic counters safe for concurrent reads.

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

// Atomic bulk acquire — either all n slots or none.
if sem.TryAcquireN(jack.PriorityHigh, 3) {
    defer func() {
        sem.Release()
        sem.Release()
        sem.Release()
    }()
}

// Blocking acquire respects priority: Critical served before High, etc.
if err := sem.Acquire(ctx, jack.PriorityCritical); err != nil {
    return err
}
defer sem.Release()
```

### RateLimiter
Token bucket with priority queueing. `Allow`/`AllowN` are lock-free. `Acquire` blocks with context. `Reserve` returns a non-blocking `Reservation` the caller can inspect and cancel without blocking any goroutine — ideal for admission control.

```go
rl := jack.NewRateLimiter(1000, 100) // 1000 req/s, burst 100
defer rl.Close()

// Non-blocking fast path.
if rl.Allow(jack.PriorityHigh) {
    // proceed
}

// Non-blocking reservation — inspect delay, then decide.
res := rl.Reserve(1, jack.ReserveWithMaxDelay(50*time.Millisecond))
if !res.OK() {
    return ErrTooManyRequests
}
if err := res.Wait(ctx); err != nil {
    res.Cancel()
    return err
}
// tokens consumed — proceed

// Blocking acquire with priority.
if err := rl.Acquire(ctx, jack.PriorityHigh); err != nil {
    return err
}
```

### Throttle
Client-side self-tuning throttle. Observes upstream acceptance and rejection rates, probabilistically dropping local requests before sending when the upstream is overloaded. Each priority tier has an independent probability — Critical is shed last, Low is shed first. Per-tier live probability via `Probability(p)` and `Metrics().ThrottleProbs`.

```go
throttle := jack.NewThrottle(jack.priorityCount,
    jack.ThrottleWithRatio(2.0),
    jack.ThrottleWithWindowResetSamples(500),
)
defer throttle.Close()

// Record upstream outcomes.
if upstreamErr != nil {
    throttle.Rejected(jack.PriorityHigh)
} else {
    throttle.Accepted(jack.PriorityHigh)
}

// Check before sending.
if !throttle.Allow(jack.PriorityHigh) {
    return ErrThrottled
}

fmt.Printf("critical rejection prob: %.2f%%\n",
    throttle.Probability(jack.PriorityCritical)*100)
```

### Circuit Breaker
Three-state machine (Closed → Open → HalfOpen). All state transitions are lock-free atomic CAS. `Call` is the only entry point — it wraps your function and manages the state machine around it. `onStateChange` callback for alerting and metrics.

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
Isolates failure domains by giving each named partition its own bounded concurrency budget backed by an independent Semaphore. When one partition saturates, others are completely unaffected. Partitions auto-create on first use.

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
if errors.Is(err, jack.ErrBulkheadFull) {
    return ErrServiceBusy
}

fmt.Println(bh.Available("payments"), bh.Metrics("payments").AcquiredFast.Load())
```

### Adaptive Concurrency Limiter
AIMD gradient controller that adjusts its concurrency limit dynamically based on observed RTT. Below target RTT → additive increase. Above target → multiplicative decrease. Bounds enforced at `minLimit`/`maxLimit`. Mirrors Netflix/concurrency-limits and Envoy's adaptive concurrency filter.

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

m := limiter.Metrics()
fmt.Println(m.CurrentLimit.Load(), m.AvgRTTNs.Load())
```

### Retry
Exponential backoff with full jitter, configurable predicate, and per-call metrics. `Retry` is reusable and safe for concurrent use. Supports permanent-error detection to skip retry on non-retryable failures.

```go
policy := jack.NewRetry(
    jack.RetryWithMaxAttempts(5),
    jack.RetryWithBaseDelay(100*time.Millisecond),
    jack.RetryWithMaxDelay(30*time.Second),
    jack.RetryWithJitter(true),
    jack.RetryWithRetryIf(func(err error) bool {
        return !errors.Is(err, ErrPermanent)
    }),
    jack.RetryWithOnRetry(func(attempt int, err error) {
        log.Printf("retry %d: %v", attempt, err)
    }),
)

err := policy.Do(ctx, func(ctx context.Context) error {
    return upstream.Call(ctx, req)
})
if errors.Is(err, jack.ErrRetryExhausted) {
    return ErrMaxRetriesExceeded
}
```

### Hedged Requests
Fire a duplicate request after a configurable delay. Whichever responds first is returned; the other is cancelled. The hedge delay adapts automatically from a lock-free circular RTT sample buffer — no external dependencies. Safe only for idempotent operations. Typed `HedgerOf[T]` wrapper avoids casts.

```go
// Typed — no casts at the call site.
hedger := jack.NewHedgerOf[*UserResponse](
    jack.HedgeWithPercentile(95),   // fire hedge at p95 latency
    jack.HedgeWithMinSamples(20),   // warm-up period before adaptive delay
    jack.HedgeWithMaxConcurrent(50),
)

user, err := hedger.Do(ctx, func(ctx context.Context) (*UserResponse, error) {
    return userClient.Get(ctx, id)
})

m := hedger.Metrics()
fmt.Println(m.Hedged.Load(), m.HedgeWon.Load(), m.PrimaryWon.Load())
```

### Lease
A time-bounded semaphore slot with automatic reclamation. If the holder crashes or forgets to call `Release`, the internal `Reaper` reclaims the slot automatically after TTL — slots are never permanently lost.

```go
sem := jack.NewSemaphore(20)
lm := jack.NewLeaser(sem, jack.LeaserWithTTL(30*time.Second))
defer lm.Close()

lease, err := lm.Acquire(ctx, requestID, jack.PriorityHigh, 0)
if err != nil {
    return err
}
defer lease.Release()

// If this process crashes, the slot is returned after 30s automatically.
```

### Queue
Bounded, multi-priority, multi-consumer work queue. Per-priority bins with tail-drop under saturation. Item timeout eviction. `EnqueueCtx` blocks until space is available. The natural entry point for a load-balancer pipeline before applying `Semaphore` or `RateLimiter` downstream.

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

depths := q.DepthByPriority()
fmt.Printf("critical=%d high=%d medium=%d low=%d\n",
    depths[0], depths[1], depths[2], depths[3])
```

### Routines
Goroutine tracker and lifecycle manager. Every `Go` call registers the goroutine with an ID, tracks its state (Running/Done/Panicked/Cancelled), captures panic stacks, and guarantees it is joined by `Stop` or `Wait`. The `DefaultRoutines` singleton lets you use it without passing the tracker around.

```go
rt := jack.NewRoutines(
    jack.RoutineWithOnPanic(func(info jack.RoutineInfo) {
        log.Printf("panic in %s: %v\n%s", info.ID, info.Err, info.Stack)
    }),
)
defer rt.Stop() // cancels all goroutines and waits

rt.Spawn("fetch-users", func(ctx context.Context) error {
    return fetchUsers(ctx)
})

rt.Background("heartbeat", 0, func(ctx context.Context) error {
    return sendHeartbeat(ctx) // restarts on error, unlimited times
})

// Inspect state of any goroutine.
info, ok := rt.Info("fetch-users#1")
fmt.Println(info.State, info.StartedAt)

// Or use the package-level singleton.
jack.Spawn("background-job", func(ctx context.Context) error {
    return runJob(ctx)
})
```

### Future/Promise
Type-safe async computation with composition. Wait for results, chain transformations, recover from errors.

```go
f := jack.Async(func() (string, error) {
    return fetchUser()
})

f.Then(ctx, func(user string) (any, error) {
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
Graceful termination with signal handling. Register cleanup in LIFO order. Named tasks appear correctly in stats and logs. Supports concurrent execution of cleanup handlers.

```go
sd := jack.NewShutdown(
    jack.ShutdownWithTimeout(30*time.Second),
    jack.ShutdownConcurrent(),
)
sd.RegisterCloser("db", db)
sd.RegisterFunc("cache", cache.Flush)
sd.RegisterWithContext("grpc", grpcServer.GracefulStop)
sd.Wait() // blocks until SIGTERM/SIGINT
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

### Runner, Scheduler, Group
Single-worker queue, cron-style scheduling, and coordinated goroutine groups with error collection.

### Safely
Context-aware mutex with panic recovery.

```go
var mu jack.Safely
err := mu.SafeCtx(ctx, func() error {
    return nil
})
```

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

Every component exposes a `Metrics()` method with atomic counters safe for concurrent reads without locks.

```go
obs := jack.NewObservable[jack.Event](10)
obs.Add(myObserver)
pool := jack.NewPool(5, jack.PoolingWithObservable(obs))

// Scrape metrics from any component:
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
| Rate-limit bursty calls | `Debouncer` |
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

The components are designed to stack. A typical high-volume service endpoint:

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

Each layer is independent. Use only what your service needs.

---

## Testing

```bash
go test -v -race ./...
go test -bench=. -benchmem -cpu=8 -run='^$' ./...
```

Race detector is your friend. Jack is race-free by design. Every public API has benchmarks with `ReportAllocs()` — zero allocations on all fast paths.

---

## License

MIT