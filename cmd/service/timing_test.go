package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTimingCollectorNilNoOp(t *testing.T) {
	var c *TimingCollector
	c.Observe("x", time.Millisecond, TimingFields{}, nil)
	c.SetLoopID("ignored")
	if _, err := c.Flush(t.TempDir()); err != nil {
		t.Fatalf("nil Flush: %v", err)
	}
	if err := timedOp(nil, "op", time.Second, TimingFields{}, func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("nil timedOp: %v", err)
	}
}

func TestTimedOpSuccessAndDeadline(t *testing.T) {
	c := newTimingCollector("test-loop")

	if err := timedOp(c, "fast", time.Second, TimingFields{Bag: "b1"}, func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("fast: %v", err)
	}

	err := timedOp(c, "slow", 20*time.Millisecond, TimingFields{Provider: "aa"}, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}

	ordinary := errors.New("boom")
	if err := timedOp(c, "fail", time.Second, TimingFields{}, func(ctx context.Context) error {
		return ordinary
	}); err == nil || err.Error() != "boom" {
		t.Fatalf("want boom, got %v", err)
	}

	events := c.Snapshot()
	if len(events) != 3 {
		t.Fatalf("events=%d want 3", len(events))
	}
	if events[0].Op != "fast" || events[0].TimedOut || events[0].Bag != "b1" {
		t.Fatalf("fast event: %+v", events[0])
	}
	if !events[1].TimedOut || events[1].BudgetMs != 20 {
		t.Fatalf("slow event: %+v", events[1])
	}
	if events[2].TimedOut || events[2].Error != "boom" {
		t.Fatalf("fail event: %+v", events[2])
	}
}

func TestTimingCollectorFlushSummary(t *testing.T) {
	dir := t.TempDir()
	c := newTimingCollector("42-7")
	c.Observe("request_storage_info", 100*time.Millisecond, TimingFields{Budget: 10 * time.Second}, nil)
	c.Observe("request_storage_info", 200*time.Millisecond, TimingFields{Budget: 10 * time.Second}, nil)
	c.Observe("request_storage_info", 300*time.Millisecond, TimingFields{Budget: 10 * time.Second}, context.DeadlineExceeded)
	c.Observe("get_storage_rates", 50*time.Millisecond, TimingFields{Budget: 15 * time.Second}, errors.New("nope"))

	summary, err := c.Flush(dir)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if summary.LoopID != "42-7" {
		t.Fatalf("loop id %q", summary.LoopID)
	}
	op := summary.Ops["request_storage_info"]
	if op.Count != 3 || op.Timeouts != 1 || op.Errors != 1 {
		t.Fatalf("request_storage_info summary: %+v", op)
	}
	if op.MinMs != 100 || op.MaxMs != 300 || op.P50Ms != 200 {
		t.Fatalf("percentiles/minmax: %+v", op)
	}
	rates := summary.Ops["get_storage_rates"]
	if rates.Count != 1 || rates.Errors != 1 || rates.Timeouts != 0 {
		t.Fatalf("rates summary: %+v", rates)
	}

	jsonlPath := filepath.Join(dir, "loop-42-7.jsonl")
	raw, err := os.ReadFile(jsonlPath)
	if err != nil {
		t.Fatalf("read jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 4 {
		t.Fatalf("jsonl lines=%d want 4", len(lines))
	}
	var ev TimingEvent
	if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.Op != "request_storage_info" || ev.LatencyMs != 100 {
		t.Fatalf("first event: %+v", ev)
	}

	sumPath := filepath.Join(dir, "summary-42-7.json")
	sumRaw, err := os.ReadFile(sumPath)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var decoded TimingSummary
	if err := json.Unmarshal(sumRaw, &decoded); err != nil {
		t.Fatalf("summary json: %v", err)
	}
	if decoded.Ops["request_storage_info"].Count != 3 {
		t.Fatalf("decoded summary: %+v", decoded)
	}
}

func TestPercentile(t *testing.T) {
	samples := []int64{10, 20, 30, 40, 50}
	if got := percentile(samples, 50); got != 30 {
		t.Fatalf("p50=%d", got)
	}
	if got := percentile(samples, 95); got != 50 {
		t.Fatalf("p95=%d", got)
	}
	if got := percentile([]int64{7}, 99); got != 7 {
		t.Fatalf("single=%d", got)
	}
}
