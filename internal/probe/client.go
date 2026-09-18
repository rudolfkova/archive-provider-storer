package probe

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	adnladdress "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
)

var ErrNotConnected = errors.New("not connected")

type providerPeer struct {
	rldp *rldp.RLDP
	conn adnl.Peer

	mx sync.Mutex
}

// Client talks to storage providers over DHT + ADNL + RLDP.
type Client struct {
	gate   *adnl.Gateway
	dht    transport.DHT
	active map[string]*providerPeer
	mx     sync.Mutex
}

// NewClient creates a provider client.
func NewClient(gate *adnl.Gateway, dhtClient transport.DHT) *Client {
	return &Client{
		gate:   gate,
		dht:    dhtClient,
		active: map[string]*providerPeer{},
	}
}

func (c *Client) connect(ctx context.Context, providerKey []byte) (*providerPeer, error) {
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

// EnsureConnected establishes DHT + ADNL connection to the provider.
func (c *Client) EnsureConnected(ctx context.Context, provider []byte) error {
	_, err := c.connect(ctx, provider)
	return err
}

// QueryStorageRates runs RLDP storage rates query on an existing connection.
func (c *Client) QueryStorageRates(ctx context.Context, provider []byte, size uint64) (*transport.StorageRatesResponse, error) {
	c.mx.Lock()
	p := c.active[string(provider)]
	c.mx.Unlock()
	if p == nil {
		return nil, fmt.Errorf("%w to provider %s", ErrNotConnected, hex.EncodeToString(provider))
	}

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.rldp == nil || p.conn == nil {
		return nil, fmt.Errorf("%w to provider %s", ErrNotConnected, hex.EncodeToString(provider))
	}

	now := time.Now()
	var res transport.StorageRatesResponse
	if err := p.rldp.DoQuery(ctx, 8196, transport.StorageRatesRequest{Size: size}, &res); err != nil {
		if time.Since(now) > 3*time.Second {
			p.conn.Close()
			c.mx.Lock()
			delete(c.active, string(provider))
			c.mx.Unlock()
		}
		return nil, fmt.Errorf("failed to do request: %w", err)
	}
	return &res, nil
}

// Disconnect closes the active connection to a provider, if any.
func (c *Client) Disconnect(provider []byte) {
	c.mx.Lock()
	p := c.active[string(provider)]
	delete(c.active, string(provider))
	c.mx.Unlock()
	if p == nil {
		return
	}
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.conn != nil {
		p.conn.Close()
	}
}

// RunPhasedStorageRates measures connect and query phases separately.
// Query is skipped when connect fails.
func RunPhasedStorageRates(c *Collector, client *Client, budget time.Duration, provider []byte, bagSize uint64, attempt int) (*transport.StorageRatesResponse, error) {
	providerHex := hex.EncodeToString(provider)
	fields := Fields{Provider: providerHex, Attempt: attempt}

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
