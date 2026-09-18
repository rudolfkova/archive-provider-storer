package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
)

type phasedFakeProvider struct {
	connectCalls int
	queryCalls   int
	connectErr   error
	queryErr     error
}

func (p *phasedFakeProvider) EnsureConnected(ctx context.Context, provider []byte) error {
	p.connectCalls++
	return p.connectErr
}

func (p *phasedFakeProvider) QueryStorageInfo(ctx context.Context, provider []byte, contractAddr *address.Address, byteToProof uint64) (*transport.StorageResponse, error) {
	p.queryCalls++
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	return &transport.StorageResponse{Status: "active"}, nil
}

func (p *phasedFakeProvider) GetStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error) {
	return nil, errors.New("not implemented")
}

func TestRunPhasedStorageInfoRecordsBothPhases(t *testing.T) {
	c := newTimingCollector("phased")
	p := &phasedFakeProvider{}
	addr := address.MustParseAddr("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")

	resp, err := runPhasedStorageInfo(c, "request_storage_info", p, TimingFields{Provider: "aa"}, []byte{1}, addr, 42)
	if err != nil {
		t.Fatalf("runPhasedStorageInfo: %v", err)
	}
	if resp.Status != "active" {
		t.Fatalf("status=%q", resp.Status)
	}
	if p.connectCalls != 1 || p.queryCalls != 1 {
		t.Fatalf("connect=%d query=%d", p.connectCalls, p.queryCalls)
	}

	events := c.Snapshot()
	if len(events) != 2 {
		t.Fatalf("events=%d want 2", len(events))
	}
	if events[0].Op != "request_storage_info_connect" || events[0].BudgetMs != 10_000 {
		t.Fatalf("connect event: %+v", events[0])
	}
	if events[1].Op != "request_storage_info_query" || events[1].BudgetMs != 10_000 {
		t.Fatalf("query event: %+v", events[1])
	}
}

func TestRunPhasedStorageInfoSkipsQueryOnConnectError(t *testing.T) {
	p := &phasedFakeProvider{connectErr: context.DeadlineExceeded}
	_, err := runPhasedStorageInfo(nil, "notify_storage_info", p, TimingFields{}, []byte{1}, address.MustParseAddr("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"), 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	if p.connectCalls != 1 || p.queryCalls != 0 {
		t.Fatalf("connect=%d query=%d", p.connectCalls, p.queryCalls)
	}
}

func TestRunPhasedStorageInfoSkipsQueryOnConnectTimeout(t *testing.T) {
	slowConnect := &slowConnectProvider{delay: 15 * time.Millisecond}
	c := newTimingCollector("slow-connect")

	_, err := runPhasedStorageInfo(c, "request_storage_info", slowConnect, TimingFields{}, []byte{1}, address.MustParseAddr("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"), 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	if slowConnect.queryCalls != 0 {
		t.Fatalf("query should not run")
	}
}

type slowConnectProvider struct {
	delay      time.Duration
	queryCalls int
}

func (p *slowConnectProvider) EnsureConnected(ctx context.Context, provider []byte) error {
	select {
	case <-time.After(p.delay):
		return context.DeadlineExceeded
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *slowConnectProvider) QueryStorageInfo(ctx context.Context, provider []byte, contractAddr *address.Address, byteToProof uint64) (*transport.StorageResponse, error) {
	p.queryCalls++
	return &transport.StorageResponse{Status: "active"}, nil
}

func (p *slowConnectProvider) GetStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error) {
	return nil, errors.New("not implemented")
}
