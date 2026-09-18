package probe

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

// Fields carries optional context for a timed operation.
type Fields struct {
	Provider string
	Attempt  int
	Budget   time.Duration
	ExtraErr string
}

// Event is one latency sample written to JSONL.
type Event struct {
	Op        string `json:"op"`
	LatencyMs int64  `json:"latency_ms"`
	BudgetMs  int64  `json:"budget_ms,omitempty"`
	TimedOut  bool   `json:"timed_out"`
	Error     string `json:"error,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	AtUnixMs  int64  `json:"at_unix_ms"`
}

// OpSummary aggregates samples for one op.
type OpSummary struct {
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

// Summary is written at the end of a probe run.
type Summary struct {
	LoopID         string               `json:"loop_id"`
	Label          string               `json:"label,omitempty"`
	LoopStartedAt  string               `json:"loop_started_at"`
	LoopFinishedAt string               `json:"loop_finished_at"`
	Ops            map[string]OpSummary `json:"ops"`
}

// Collector accumulates timing events for one run (mutex-safe).
type Collector struct {
	mu        sync.Mutex
	events    []Event
	startedAt time.Time
	loopID    string
	label     string
}

// NewCollector creates a collector for the given run id.
func NewCollector(loopID string) *Collector {
	return &Collector{
		events:    make([]Event, 0, 256),
		startedAt: time.Now(),
		loopID:    loopID,
	}
}

// SetLabel stores an optional run label (e.g. baseline/hedge) in the summary.
func (c *Collector) SetLabel(label string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.label = label
	c.mu.Unlock()
}

// SetLoopID updates the id used for Flush filenames.
func (c *Collector) SetLoopID(loopID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.loopID = loopID
	c.mu.Unlock()
}

// Observe records one timing sample.
func (c *Collector) Observe(op string, latency time.Duration, fields Fields, err error) {
	if c == nil {
		return
	}
	ev := Event{
		Op:        op,
		LatencyMs: latency.Milliseconds(),
		BudgetMs:  fields.Budget.Milliseconds(),
		TimedOut:  err != nil && errors.Is(err, context.DeadlineExceeded),
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

// Snapshot returns a copy of collected events.
func (c *Collector) Snapshot() []Event {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}

func (c *Collector) buildSummary(finishedAt time.Time) Summary {
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
	ops := make(map[string]OpSummary, len(byOp))
	for op, samples := range byOp {
		ops[op] = summarizeSamples(samples, timeouts[op], errs[op])
	}
	return Summary{
		LoopID:         c.loopID,
		Label:          c.label,
		LoopStartedAt:  c.startedAt.UTC().Format(time.RFC3339Nano),
		LoopFinishedAt: finishedAt.UTC().Format(time.RFC3339Nano),
		Ops:            ops,
	}
}

func summarizeSamples(samples []int64, timeouts, errCount int) OpSummary {
	if len(samples) == 0 {
		return OpSummary{}
	}
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	return OpSummary{
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

// Flush writes JSONL events and summary JSON under dir.
func (c *Collector) Flush(dir string) (Summary, error) {
	if c == nil {
		return Summary{}, nil
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

// TimedOp runs fn under a context with budget and records latency.
func TimedOp(c *Collector, op string, budget time.Duration, fields Fields, fn func(ctx context.Context) error) error {
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
