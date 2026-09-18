package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	tonaddress "github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/adnl"
	adnladdress "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
)

const storageInfoPhaseBudget = 10 * time.Second

type providerPeer struct {
	rldp *rldp.RLDP
	conn adnl.Peer

	mx sync.Mutex
}

type storerProviderClient struct {
	gate   *adnl.Gateway
	dht    transport.DHT
	active map[string]*providerPeer
	mx     sync.Mutex
}

func newStorerProviderClient(gate *adnl.Gateway, dht transport.DHT) *storerProviderClient {
	return &storerProviderClient{
		gate:   gate,
		dht:    dht,
		active: map[string]*providerPeer{},
	}
}

func (c *storerProviderClient) connect(ctx context.Context, providerKey []byte) (*providerPeer, error) {
	c.mx.Lock()
	p := c.active[string(providerKey)]
	if p == nil {
		p = &providerPeer{}
		c.active[string(providerKey)] = p
	}
	c.mx.Unlock()

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.conn != nil {
		return p, nil
	}

	channelKeyID, err := tl.Hash(keys.PublicKeyED25519{Key: providerKey})
	if err != nil {
		return nil, fmt.Errorf("failed to calc hash of provider key %s: %w", hex.EncodeToString(providerKey), err)
	}

	dhtVal, _, err := c.dht.FindValue(ctx, &dht.Key{
		ID:    channelKeyID,
		Name:  []byte("storage-provider"),
		Index: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find storage-provider in dht of %s: %w", hex.EncodeToString(providerKey), err)
	}

	var nodeAddr transport.ProviderDHTRecord
	if _, err = tl.Parse(&nodeAddr, dhtVal.Data, true); err != nil {
		return nil, fmt.Errorf("failed to parse node dht value of %s: %w", hex.EncodeToString(providerKey), err)
	}

	list, key, err := c.dht.FindAddresses(ctx, nodeAddr.ADNLAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to find address in dht of %s: %w", hex.EncodeToString(providerKey), err)
	}

	if len(list.Addresses) == 0 {
		return nil, fmt.Errorf("no addresses for %s", hex.EncodeToString(providerKey))
	}
	addr, err := firstDialAddress(list)
	if err != nil {
		return nil, fmt.Errorf("no dialable address for %s: %w", hex.EncodeToString(providerKey), err)
	}

	peer, err := c.gate.RegisterClient(addr, key)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer of %s at %s: %w", hex.EncodeToString(providerKey), addr, err)
	}

	peer.SetDisconnectHandler(func(addr string, _ ed25519.PublicKey) {
		c.mx.Lock()
		defer c.mx.Unlock()

		delete(c.active, string(providerKey))
	})

	p.rldp = rldp.NewClientV2(peer)
	p.conn = peer

	return p, nil
}

func firstDialAddress(list *adnladdress.List) (string, error) {
	if list == nil {
		return "", fmt.Errorf("address list is nil")
	}

	for _, addr := range list.Addresses {
		dialAddr, err := adnladdress.DialString(addr)
		if err == nil {
			return dialAddr, nil
		}
	}

	return "", fmt.Errorf("no dialable addresses")
}

func (c *storerProviderClient) EnsureConnected(ctx context.Context, provider []byte) error {
	_, err := c.connect(ctx, provider)
	return err
}

func (c *storerProviderClient) QueryStorageInfo(ctx context.Context, provider []byte, contractAddr *tonaddress.Address, byteToProof uint64) (*transport.StorageResponse, error) {
	p, err := c.connect(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to provider: %w", err)
	}

	now := time.Now()
	var res transport.StorageResponse
	if err = p.rldp.DoQuery(ctx, 1024*64, transport.StorageRequest{
		ContractAddress: contractAddr.Data(),
		ByteToProof:     byteToProof,
	}, &res); err != nil {
		if time.Since(now) > 5*time.Second {
			p.conn.Close()
		}
		return nil, fmt.Errorf("failed to do request: %w", err)
	}
	return &res, nil
}

func (c *storerProviderClient) GetStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error) {
	p, err := c.connect(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to provider: %w", err)
	}

	now := time.Now()
	var res transport.StorageRatesResponse
	if err = p.rldp.DoQuery(ctx, 8196, transport.StorageRatesRequest{Size: size}, &res); err != nil {
		if time.Since(now) > 3*time.Second {
			p.conn.Close()
		}
		return nil, fmt.Errorf("failed to do request: %w", err)
	}
	return &res, nil
}

func runPhasedStorageInfo(
	c *TimingCollector,
	prefix string,
	p providerAPI,
	fields TimingFields,
	provider []byte,
	addr *tonaddress.Address,
	toProof uint64,
) (*transport.StorageResponse, error) {
	if err := timedOp(c, prefix+"_connect", storageInfoPhaseBudget, fields, func(ctx context.Context) error {
		return p.EnsureConnected(ctx, provider)
	}); err != nil {
		return nil, err
	}

	var resp *transport.StorageResponse
	if err := timedOp(c, prefix+"_query", storageInfoPhaseBudget, fields, func(ctx context.Context) error {
		var e error
		resp, e = p.QueryStorageInfo(ctx, provider, addr, toProof)
		return e
	}); err != nil {
		return nil, err
	}
	return resp, nil
}
