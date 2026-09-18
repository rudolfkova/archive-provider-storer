package probe

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

func TestCollectorNilNoOp(t *testing.T) {
	var c *Collector
	c.Observe("x", time.Millisecond, Fields{}, nil)
	c.SetLoopID("ignored")
	if _, err := c.Flush(t.TempDir()); err != nil {
		t.Fatalf("nil Flush: %v", err)
	}
	if err := TimedOp(nil, "op", time.Second, Fields{}, func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("nil TimedOp: %v", err)
	}
}

func TestTimedOpSuccessAndDeadline(t *testing.T) {
	c := NewCollector("test-loop")

	if err := TimedOp(c, "fast", time.Second, Fields{Provider: "aa"}, func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("fast: %v", err)
	}

	err := TimedOp(c, "slow", 20*time.Millisecond, Fields{Provider: "bb"}, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}

	ordinary := errors.New("boom")
	if err := TimedOp(c, "fail", time.Second, Fields{}, func(ctx context.Context) error {
		return ordinary
	}); err == nil || err.Error() != "boom" {
		t.Fatalf("want boom, got %v", err)
	}

	events := c.Snapshot()
	if len(events) != 3 {
		t.Fatalf("events=%d want 3", len(events))
	}
	if events[0].Op != "fast" || events[0].TimedOut || events[0].Provider != "aa" {
		t.Fatalf("fast event: %+v", events[0])
	}
	if !events[1].TimedOut || events[1].BudgetMs != 20 {
		t.Fatalf("slow event: %+v", events[1])
	}
	if events[2].TimedOut || events[2].Error != "boom" {
		t.Fatalf("fail event: %+v", events[2])
	}
}

func TestCollectorFlushSummary(t *testing.T) {
	dir := t.TempDir()
	c := NewCollector("probe-42")
	c.Observe("storage_rates_connect", 100*time.Millisecond, Fields{Budget: 10 * time.Second}, nil)
	c.Observe("storage_rates_connect", 200*time.Millisecond, Fields{Budget: 10 * time.Second}, nil)
	c.Observe("storage_rates_query", 300*time.Millisecond, Fields{Budget: 10 * time.Second}, context.DeadlineExceeded)
	c.Observe("storage_rates_query", 50*time.Millisecond, Fields{Budget: 10 * time.Second}, errors.New("nope"))

	summary, err := c.Flush(dir)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if summary.LoopID != "probe-42" {
		t.Fatalf("loop id %q", summary.LoopID)
	}
	conn := summary.Ops["storage_rates_connect"]
	if conn.Count != 2 || conn.Timeouts != 0 || conn.Errors != 0 {
		t.Fatalf("connect summary: %+v", conn)
	}
	query := summary.Ops["storage_rates_query"]
	if query.Count != 2 || query.Timeouts != 1 || query.Errors != 2 {
		t.Fatalf("query summary: %+v", query)
	}

	jsonlPath := filepath.Join(dir, "loop-probe-42.jsonl")
	raw, err := os.ReadFile(jsonlPath)
	if err != nil {
		t.Fatalf("read jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 4 {
		t.Fatalf("jsonl lines=%d want 4", len(lines))
	}
	var ev Event
	if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.Op != "storage_rates_connect" || ev.LatencyMs != 100 {
		t.Fatalf("first event: %+v", ev)
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
