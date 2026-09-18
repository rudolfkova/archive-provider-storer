package probe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
)

type fakeRatesClient struct {
	connectCalls int
	queryCalls   int
	connectErr   error
	queryErr     error
}

func (f *fakeRatesClient) EnsureConnected(ctx context.Context, provider []byte) error {
	f.connectCalls++
	return f.connectErr
}

func (f *fakeRatesClient) QueryStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error) {
	f.queryCalls++
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &transport.StorageRatesResponse{Available: true, SpaceAvailableMB: size >> 20}, nil
}

func (f *fakeRatesClient) Disconnect(provider []byte) {}

type ratesClient interface {
	EnsureConnected(ctx context.Context, provider []byte) error
	QueryStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error)
	Disconnect(provider []byte)
}

func runPhasedStorageRatesWith(c *Collector, client ratesClient, budget time.Duration, provider []byte, bagSize uint64) (*transport.StorageRatesResponse, error) {
	providerHex := "aa"
	fields := Fields{Provider: providerHex}

	if err := TimedOp(c, "storage_rates_connect", budget, fields, func(ctx context.Context) error {
		return client.EnsureConnected(ctx, provider)
	}); err != nil {
		return nil, err
	}

	var resp *transport.StorageRatesResponse
	if err := TimedOp(c, "storage_rates_query", budget, fields, func(ctx context.Context) error {
		var e error
		resp, e = client.QueryStorageRates(ctx, provider, bagSize)
		return e
	}); err != nil {
		client.Disconnect(provider)
		return nil, err
	}
	return resp, nil
}

func TestRunPhasedStorageRatesRecordsBothPhases(t *testing.T) {
	c := NewCollector("phased")
	f := &fakeRatesClient{}

	resp, err := runPhasedStorageRatesWith(c, f, 10*time.Second, []byte{1}, 1<<30)
	if err != nil {
		t.Fatalf("RunPhasedStorageRates: %v", err)
	}
	if !resp.Available {
		t.Fatalf("expected available")
	}
	if f.connectCalls != 1 || f.queryCalls != 1 {
		t.Fatalf("connect=%d query=%d", f.connectCalls, f.queryCalls)
	}

	events := c.Snapshot()
	if len(events) != 2 {
		t.Fatalf("events=%d want 2", len(events))
	}
	if events[0].Op != "storage_rates_connect" || events[0].BudgetMs != 10_000 {
		t.Fatalf("connect event: %+v", events[0])
	}
	if events[1].Op != "storage_rates_query" || events[1].BudgetMs != 10_000 {
		t.Fatalf("query event: %+v", events[1])
	}
}

func TestRunPhasedStorageRatesSkipsQueryOnConnectError(t *testing.T) {
	f := &fakeRatesClient{connectErr: context.DeadlineExceeded}
	_, err := runPhasedStorageRatesWith(nil, f, 10*time.Second, []byte{1}, 1<<30)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	if f.connectCalls != 1 || f.queryCalls != 0 {
		t.Fatalf("connect=%d query=%d", f.connectCalls, f.queryCalls)
	}
}

func TestQueryStorageRatesNotConnected(t *testing.T) {
	c := NewClient(nil, nil)
	_, err := c.QueryStorageRates(context.Background(), []byte{1, 2, 3}, 1024)
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("want ErrNotConnected, got %v", err)
	}
}

func TestRunPhasedStorageRatesSkipsQueryOnConnectTimeout(t *testing.T) {
	slow := &slowConnectRatesClient{delay: 15 * time.Millisecond}
	c := NewCollector("slow-connect")

	_, err := runPhasedStorageRatesWith(c, slow, 10*time.Millisecond, []byte{1}, 1<<30)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	if slow.queryCalls != 0 {
		t.Fatalf("query should not run")
	}
}

type slowConnectRatesClient struct {
	delay      time.Duration
	queryCalls int
}

func (p *slowConnectRatesClient) EnsureConnected(ctx context.Context, provider []byte) error {
	select {
	case <-time.After(p.delay):
		return context.DeadlineExceeded
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *slowConnectRatesClient) QueryStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error) {
	p.queryCalls++
	return &transport.StorageRatesResponse{Available: true}, nil
}

func (p *slowConnectRatesClient) Disconnect(provider []byte) {}
