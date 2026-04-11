# Jack

**Production-grade concurrency toolkit for Go**

Jack provides the missing pieces for building robust, observable concurrent systems. No magic, no reflection hacks—just solid patterns you'd otherwise write yourself.

## Why This Exists

Go's concurrency primitives are excellent, but production systems need more:
- Panic recovery that doesn't crash your entire process
- Backpressure when queues fill up
- Visibility into what your goroutines are actually doing
- Graceful shutdown that finishes in-flight work
- Health checks that degrade and accelerate automatically

Jack fills these gaps without getting in your way.

## What's Inside

### Pool
Fixed-size worker pool with backpressure. Tasks queue when workers are busy. Submissions fail fast when the queue is full.

```go
pool := jack.NewPool(5, jack.PoolingWithQueueSize(100))
pool.Submit(jack.Func(func() error {
    // work
    return nil
}))
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
Graceful termination with signal handling. Register cleanup in LIFO order.

```go
sd := jack.NewShutdown(jack.ShutdownWithTimeout(30*time.Second))
sd.Register(db.Close)
sd.Register(cache.Flush)
sd.Wait() // blocks until SIGTERM
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
lm.ResetTimed("heartbeat") // extend on activity
```

### Runner, Scheduler, Group
Single-worker queue, cron-style scheduling, and coordinated goroutine groups with error collection.

### Safely
Context-aware mutex with panic recovery.

```go
var mu jack.Safely
err := mu.SafeCtx(ctx, func() error {
    // critical section that respects context cancellation
    return nil
})
```

## Observability

Every component emits events you can hook into:

```go
obs := jack.NewObservable[jack.Event](10)
obs.Add(myObserver)

pool := jack.NewPool(5, jack.PoolingWithObservable(obs))
```

Doctor, Scheduler, and Looper have their own event types for metrics and alerting.

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

## When To Use What

| Problem | Use |
|---------|-----|
| Process many independent tasks concurrently | `Pool` |
| Need result from async operation | `Future` |
| Run periodic health checks with degradation | `Doctor` |
| Rate-limit bursty calls | `Debouncer` |
| Background loop with backoff | `Looper` |
| Graceful shutdown with cleanup ordering | `Shutdown` |
| Expire items after TTL | `Reaper` |
| Schedule callbacks with keep-alive | `Lifetime` |
| Coordinate multiple goroutines, collect errors | `Group` |
| Sequential async processing | `Runner` |
| Cron-style recurring tasks | `Scheduler` |
| Safe locking with timeouts | `Safely` |

## Testing

```bash
go test -v -race ./...
```

Race detector is your friend. Jack is race-free by design.

## License

MIT