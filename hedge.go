package jack

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// hedgeWindowSize is the number of recent RTT samples used to compute the
// hedge delay percentile. It is a power of two for cheap modulo via masking.
const hedgeWindowSize = 64

var ErrHedgeAllFailed = errors.New("all hedged requests failed")

// HedgeMetrics tracks hedged request operational statistics.
type HedgeMetrics struct {
	Requests   atomic.Uint64 // total primary calls
	Hedged     atomic.Uint64 // times a second request was fired
	PrimaryWon atomic.Uint64 // primary response arrived first
	HedgeWon   atomic.Uint64 // hedge response arrived first
	Errors     atomic.Uint64 // all attempts failed
	AvgDelayNs atomic.Int64  // EWMA of hedge fire delay in nanoseconds
}

// HedgeOption configures a Hedger.
type HedgeOption func(*Hedger)

// HedgeWithDelay sets a fixed delay before the hedge fires (default: use latency percentile).
// When set, the latency window is not consulted.
func HedgeWithDelay(d time.Duration) HedgeOption {
	return func(h *Hedger) { h.fixedDelay = d }
}

// HedgeWithPercentile sets which RTT percentile to use as the hedge delay (default 50).
// Valid range: 1–99.
func HedgeWithPercentile(p int) HedgeOption {
	return func(h *Hedger) {
		if p > 0 && p < 100 {
			h.percentile = p
		}
	}
}

// HedgeWithMaxConcurrent sets the maximum number of in-flight hedged pairs (default 100).
// When the limit is reached, no hedge is fired and only the primary runs.
func HedgeWithMaxConcurrent(n int) HedgeOption {
	return func(h *Hedger) {
		if n > 0 {
			h.maxConcurrent = int64(n)
		}
	}
}

// HedgeWithMinSamples sets how many RTT samples must be collected before the
// latency-based hedge delay activates (default 10). Before that, a zero delay
// (fire immediately) is used.
func HedgeWithMinSamples(n int) HedgeOption {
	return func(h *Hedger) {
		if n > 0 {
			h.minSamples = n
		}
	}
}

// Hedger implements the hedged requests pattern.
//
// A primary request is sent immediately. If it has not responded within the
// hedge delay (derived from recent latency or a fixed duration), an identical
// second request is fired. Whichever responds first is returned; the other is
// cancelled via context cancellation.
//
// This reduces tail latency at the cost of occasionally sending duplicate
// requests. It is safe only for idempotent operations (reads, retried writes
// with idempotency keys).
//
// The hedge delay adapts automatically: it is the Nth percentile of recent
// call RTTs measured in a lock-free circular sample buffer. No third-party
// sketch library is used.
type Hedger struct {
	percentile    int
	fixedDelay    time.Duration
	maxConcurrent int64
	minSamples    int

	// rtts is a circular buffer of recent RTT samples in nanoseconds.
	// head points to the next write slot; writes use atomic add + mask.
	rtts    [hedgeWindowSize]atomic.Int64
	head    atomic.Uint64
	samples atomic.Uint64 // total samples recorded (capped for readability)

	inFlight atomic.Int64
	metrics  *HedgeMetrics
}

// NewHedger creates a Hedger with sensible defaults.
func NewHedger(opts ...HedgeOption) *Hedger {
	h := &Hedger{
		percentile:    50,
		maxConcurrent: 100,
		minSamples:    10,
		metrics:       &HedgeMetrics{},
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Metrics returns the hedger's operational metrics.
func (h *Hedger) Metrics() *HedgeMetrics { return h.metrics }

// Do executes fn with hedging. fn receives a context that is cancelled when
// the other attempt wins. The caller's ctx governs the outer deadline.
//
// fn must be idempotent — it may be called twice concurrently.
func (h *Hedger) Do(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	h.metrics.Requests.Add(1)

	type result struct {
		val   any
		err   error
		hedge bool
	}

	ch := make(chan result, 2)
	delay := h.hedgeDelay()

	// Primary attempt.
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	go func() {
		start := time.Now()
		val, err := fn(primaryCtx)
		h.recordRTT(time.Since(start))
		ch <- result{val: val, err: err, hedge: false}
	}()

	h.inFlight.Add(1)
	defer h.inFlight.Add(-1)

	// Decide whether to hedge.
	canHedge := h.maxConcurrent <= 0 || h.inFlight.Load() <= h.maxConcurrent

	var hedgeCancel context.CancelFunc
	var timer *time.Timer

	if canHedge && delay >= 0 {
		hedgeCtx, hc := context.WithCancel(ctx)
		hedgeCancel = hc

		timer = time.AfterFunc(delay, func() {
			h.metrics.Hedged.Add(1)
			go func() {
				val, err := fn(hedgeCtx)
				ch <- result{val: val, err: err, hedge: true}
			}()
		})
	}

	cleanup := func() {
		if timer != nil {
			timer.Stop()
		}
		if hedgeCancel != nil {
			hedgeCancel()
		}
		primaryCancel()
	}

	// Wait for first successful result, or both to fail.
	var firstErr error
	responded := 0
	total := 1
	if canHedge && delay >= 0 {
		total = 2
	}

	for responded < total {
		select {
		case <-ctx.Done():
			cleanup()
			return nil, ctx.Err()
		case r := <-ch:
			responded++
			if r.err == nil {
				cleanup()
				if r.hedge {
					h.metrics.HedgeWon.Add(1)
				} else {
					h.metrics.PrimaryWon.Add(1)
				}
				return r.val, nil
			}
			if firstErr == nil {
				firstErr = r.err
			}
		}
	}

	cleanup()
	h.metrics.Errors.Add(1)
	return nil, errors.Join(ErrHedgeAllFailed, firstErr)
}

// hedgeDelay returns the delay before firing the hedge attempt.
// Uses the configured fixed delay if set, otherwise the Nth percentile RTT.
func (h *Hedger) hedgeDelay() time.Duration {
	if h.fixedDelay > 0 {
		return h.fixedDelay
	}
	samples := h.samples.Load()
	if int(samples) < h.minSamples {
		return 0 // not enough data — fire hedge immediately
	}
	return h.percentileRTT(h.percentile)
}

// recordRTT adds a nanosecond RTT sample to the circular buffer.
func (h *Hedger) recordRTT(d time.Duration) {
	idx := h.head.Add(1) & (hedgeWindowSize - 1)
	h.rtts[idx].Store(d.Nanoseconds())
	if h.samples.Load() < hedgeWindowSize {
		h.samples.Add(1)
	}
}

// percentileRTT computes the Nth percentile of the current sample window.
// Copies to a local slice and sorts — O(64 log 64), effectively constant time.
func (h *Hedger) percentileRTT(p int) time.Duration {
	n := int(h.samples.Load())
	if n > hedgeWindowSize {
		n = hedgeWindowSize
	}
	if n == 0 {
		return 0
	}
	buf := make([]int64, n)
	for i := 0; i < n; i++ {
		buf[i] = h.rtts[i].Load()
	}
	sort.Slice(buf, func(i, j int) bool { return buf[i] < buf[j] })
	idx := (p * n) / 100
	if idx >= n {
		idx = n - 1
	}
	return time.Duration(buf[idx])
}

// convenience typed wrapper

// HedgerOf wraps Hedger for typed results, avoiding any cast at the call site.
type HedgerOf[T any] struct {
	h *Hedger
}

// NewHedgerOf creates a typed hedger.
func NewHedgerOf[T any](opts ...HedgeOption) *HedgerOf[T] {
	return &HedgerOf[T]{h: NewHedger(opts...)}
}

// Do executes fn with hedging and returns a typed result.
func (h *HedgerOf[T]) Do(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	val, err := h.h.Do(ctx, func(ctx context.Context) (any, error) {
		return fn(ctx)
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return val.(T), nil
}

// Metrics returns the underlying hedger's metrics.
func (h *HedgerOf[T]) Metrics() *HedgeMetrics { return h.h.Metrics() }

// HedgeGroup runs multiple hedge strategies concurrently and returns the first
// successful result. Useful when hedging across different endpoints or regions.
type HedgeGroup struct {
	mu      sync.Mutex
	hedgers []*Hedger
}

// NewHedgeGroup creates a group of independent hedgers.
func NewHedgeGroup(hedgers ...*Hedger) *HedgeGroup {
	return &HedgeGroup{hedgers: hedgers}
}

// Do fires each hedger concurrently and returns the first success.
func (g *HedgeGroup) Do(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	type result struct {
		val any
		err error
	}
	ch := make(chan result, len(g.hedgers))
	gCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, h := range g.hedgers {
		h := h
		go func() {
			v, err := h.Do(gCtx, fn)
			ch <- result{v, err}
		}()
	}

	var lastErr error
	for i := 0; i < len(g.hedgers); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-ch:
			if r.err == nil {
				cancel()
				return r.val, nil
			}
			lastErr = r.err
		}
	}
	return nil, errors.Join(ErrHedgeAllFailed, lastErr)
}
