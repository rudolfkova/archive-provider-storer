package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// TimingFields carries optional context for a timed operation.
type TimingFields struct {
	Bag      string
	Provider  string
	Attempt   int
	Budget    time.Duration
	ExtraErr  string // set when err is nil but we still want an error note
}

// TimingEvent is one latency sample written to JSONL.
type TimingEvent struct {
	Op         string `json:"op"`
	LatencyMs  int64  `json:"latency_ms"`
	BudgetMs   int64  `json:"budget_ms,omitempty"`
	TimedOut   bool   `json:"timed_out"`
	Error      string `json:"error,omitempty"`
	Bag        string `json:"bag,omitempty"`
	Provider   string `json:"provider,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	AtUnixMs   int64  `json:"at_unix_ms"`
}

// OpTimingSummary aggregates samples for one op.
type OpTimingSummary struct {
	Count    int   `json:"count"`
	Timeouts int   `json:"timeouts"`
	Errors   int   `json:"errors"`
	AvgMs    int64 `json:"avg_ms"`
	MinMs    int64 `json:"min_ms"`
	MaxMs    int64 `json:"max_ms"`
	P50Ms    int64 `json:"p50_ms"`
	P95Ms    int64 `json:"p95_ms"`
	P99Ms    int64 `json:"p99_ms"`
}

// TimingSummary is written at the end of a loop.
type TimingSummary struct {
	LoopID         string                     `json:"loop_id"`
	LoopStartedAt  string                     `json:"loop_started_at"`
	LoopFinishedAt string                     `json:"loop_finished_at"`
	Ops            map[string]OpTimingSummary `json:"ops"`
}

// TimingCollector accumulates timing events for one loop (mutex-safe).
type TimingCollector struct {
	mu        sync.Mutex
	events    []TimingEvent
	startedAt time.Time
	loopID    string
}

func newTimingCollector(loopID string) *TimingCollector {
	return &TimingCollector{
		events:    make([]TimingEvent, 0, 256),
		startedAt: time.Now(),
		loopID:    loopID,
	}
}

// SetLoopID updates the id used for Flush filenames (e.g. after master seqno is known).
func (c *TimingCollector) SetLoopID(loopID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.loopID = loopID
	c.mu.Unlock()
}

func (c *TimingCollector) Observe(op string, latency time.Duration, fields TimingFields, err error) {
	if c == nil {
		return
	}
	ev := TimingEvent{
		Op:        op,
		LatencyMs: latency.Milliseconds(),
		BudgetMs:  fields.Budget.Milliseconds(),
		TimedOut:  err != nil && errors.Is(err, context.DeadlineExceeded),
		Bag:       fields.Bag,
		Provider:  fields.Provider,
		Attempt:   fields.Attempt,
		AtUnixMs:  time.Now().UnixMilli(),
	}
	if err != nil {
		ev.Error = err.Error()
	} else if fields.ExtraErr != "" {
		ev.Error = fields.ExtraErr
	}

	c.mu.Lock()
	c.events = append(c.events, ev)
	c.mu.Unlock()

	e := log.Info().
		Str("op", op).
		Int64("latency_ms", ev.LatencyMs).
		Int64("budget_ms", ev.BudgetMs).
		Bool("timed_out", ev.TimedOut)
	if fields.Bag != "" {
		e = e.Str("bag", fields.Bag)
	}
	if fields.Provider != "" {
		e = e.Str("provider", fields.Provider)
	}
	if fields.Attempt > 0 {
		e = e.Int("attempt", fields.Attempt)
	}
	if err != nil {
		e = e.Err(err)
	}
	e.Msg("timed op")
}

func (c *TimingCollector) Snapshot() []TimingEvent {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TimingEvent, len(c.events))
	copy(out, c.events)
	return out
}

func (c *TimingCollector) buildSummary(finishedAt time.Time) TimingSummary {
	events := c.Snapshot()
	byOp := map[string][]int64{}
	timeouts := map[string]int{}
	errs := map[string]int{}
	for _, ev := range events {
		byOp[ev.Op] = append(byOp[ev.Op], ev.LatencyMs)
		if ev.TimedOut {
			timeouts[ev.Op]++
		}
		if ev.Error != "" {
			errs[ev.Op]++
		}
	}
	ops := make(map[string]OpTimingSummary, len(byOp))
	for op, samples := range byOp {
		ops[op] = summarizeSamples(samples, timeouts[op], errs[op])
	}
	return TimingSummary{
		LoopID:         c.loopID,
		LoopStartedAt:  c.startedAt.UTC().Format(time.RFC3339Nano),
		LoopFinishedAt: finishedAt.UTC().Format(time.RFC3339Nano),
		Ops:            ops,
	}
}

func summarizeSamples(samples []int64, timeouts, errCount int) OpTimingSummary {
	if len(samples) == 0 {
		return OpTimingSummary{}
	}
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	return OpTimingSummary{
		Count:    len(sorted),
		Timeouts: timeouts,
		Errors:   errCount,
		AvgMs:    sum / int64(len(sorted)),
		MinMs:    sorted[0],
		MaxMs:    sorted[len(sorted)-1],
		P50Ms:    percentile(sorted, 50),
		P95Ms:    percentile(sorted, 95),
		P99Ms:    percentile(sorted, 99),
	}
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := int(math.Ceil(float64(p)/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// Flush writes JSONL events and summary JSON under dir. Returns summary for logging.
func (c *TimingCollector) Flush(dir string) (TimingSummary, error) {
	if c == nil {
		return TimingSummary{}, nil
	}
	finished := time.Now()
	summary := c.buildSummary(finished)
	events := c.Snapshot()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return summary, fmt.Errorf("mkdir timing dir: %w", err)
	}

	base := c.loopID
	if base == "" {
		base = fmt.Sprintf("%d", finished.Unix())
	}

	jsonlPath := filepath.Join(dir, "loop-"+base+".jsonl")
	f, err := os.Create(jsonlPath)
	if err != nil {
		return summary, fmt.Errorf("create jsonl: %w", err)
	}
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			_ = f.Close()
			return summary, fmt.Errorf("encode event: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return summary, fmt.Errorf("close jsonl: %w", err)
	}

	sumPath := filepath.Join(dir, "summary-"+base+".json")
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return summary, fmt.Errorf("marshal summary: %w", err)
	}
	if err := os.WriteFile(sumPath, data, 0o644); err != nil {
		return summary, fmt.Errorf("write summary: %w", err)
	}

	log.Info().
		Str("loop_id", summary.LoopID).
		Str("jsonl", jsonlPath).
		Str("summary", sumPath).
		Int("events", len(events)).
		Interface("ops", summary.Ops).
		Msg("timing stats flushed")

	return summary, nil
}

// timedOp runs fn under a context with budget and records latency when collector is non-nil.
// When collector is nil, creates the timeout context and runs fn without stats overhead beyond timing.
func timedOp(c *TimingCollector, op string, budget time.Duration, fields TimingFields, fn func(ctx context.Context) error) error {
	fields.Budget = budget
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := fn(ctx)
	latency := time.Since(start)
	if c != nil {
		c.Observe(op, latency, fields, err)
	}
	return err
}

// timedOpCtx is like timedOp but uses an existing parent context (no new timeout),
// recording remaining deadline as budget when present.
func timedOpCtx(c *TimingCollector, op string, parent context.Context, fields TimingFields, fn func(ctx context.Context) error) error {
	start := time.Now()
	if dl, ok := parent.Deadline(); ok {
		fields.Budget = time.Until(dl)
		if fields.Budget < 0 {
			fields.Budget = 0
		}
	}
	err := fn(parent)
	latency := time.Since(start)
	if c != nil {
		c.Observe(op, latency, fields, err)
	}
	return err
}
