package probe

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
)

const (
	defaultNetworkConfigURL = "https://ton.org/global.config.json"
	defaultProvidersURL     = "https://mytonprovider.org/api/v1/providers/search"
	defaultLimitProviders   = 50
	defaultBagSizeBytes     = 1 << 30
	defaultHTTPBudget       = 10 * time.Second
)

// Config holds probe configuration.
type Config struct {
	NetworkConfigURL string `json:"network_config_url"`
	ProvidersURL     string `json:"providers_url"`
	LimitProviders   int    `json:"limit_providers"`
	BagSizeBytes     uint64 `json:"bag_size_bytes"`
}

// DefaultConfig returns defaults for probe.
func DefaultConfig() Config {
	return Config{
		NetworkConfigURL: defaultNetworkConfigURL,
		ProvidersURL:     defaultProvidersURL,
		LimitProviders:   defaultLimitProviders,
		BagSizeBytes:     defaultBagSizeBytes,
	}
}

// LoadConfig reads probe config from a JSON file.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.NetworkConfigURL == "" {
		cfg.NetworkConfigURL = defaultNetworkConfigURL
	}
	if cfg.ProvidersURL == "" {
		cfg.ProvidersURL = defaultProvidersURL
	}
	if cfg.LimitProviders <= 0 {
		cfg.LimitProviders = defaultLimitProviders
	}
	if cfg.BagSizeBytes == 0 {
		cfg.BagSizeBytes = defaultBagSizeBytes
	}
	return cfg, nil
}

// RunOptions configures a single probe run.
type RunOptions struct {
	Config         Config
	OutputDir      string
	PhaseBudget    time.Duration
	Samples        int
	Label          string
	LimitProviders int // if >0, overrides Config.LimitProviders
	HTTPClient     *http.Client
	ProviderGate   *adnl.Gateway
	DHT            *dht.Client
}

type providersResponse struct {
	Providers []struct {
		PubKey string `json:"pubkey"`
	} `json:"providers"`
}

// Run executes one probe run and flushes timing results.
func Run(ctx context.Context, opts RunOptions) (Summary, error) {
	if opts.PhaseBudget <= 0 {
		opts.PhaseBudget = 10 * time.Second
	}
	if opts.Samples <= 0 {
		opts.Samples = 1
	}
	if opts.LimitProviders > 0 {
		opts.Config.LimitProviders = opts.LimitProviders
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: defaultHTTPBudget}
	}
	if opts.ProviderGate == nil || opts.DHT == nil {
		return Summary{}, fmt.Errorf("provider gate and dht client are required")
	}

	loopID := fmt.Sprintf("probe-%d", time.Now().Unix())
	if opts.Label != "" {
		loopID = fmt.Sprintf("probe-%s-%d", opts.Label, time.Now().Unix())
	}
	collector := NewCollector(loopID)
	collector.SetLabel(opts.Label)
	client := NewClient(opts.ProviderGate, opts.DHT)

	providerKeys, err := fetchProviders(ctx, collector, opts)
	if err != nil {
		return Summary{}, err
	}

	log.Info().Int("providers", len(providerKeys)).Int("samples", opts.Samples).Msg("probing storage rates")

	for sample := 1; sample <= opts.Samples; sample++ {
		for _, key := range providerKeys {
			select {
			case <-ctx.Done():
				return Summary{}, ctx.Err()
			default:
			}
			if _, err := RunPhasedStorageRates(collector, client, opts.PhaseBudget, key, opts.Config.BagSizeBytes, sample); err != nil {
				log.Debug().Err(err).Str("provider", hex.EncodeToString(key)).Int("sample", sample).Msg("probe failed")
			}
			client.Disconnect(key)
		}
	}

	return collector.Flush(opts.OutputDir)
}

func fetchProviders(ctx context.Context, collector *Collector, opts RunOptions) ([][]byte, error) {
	body := []byte(fmt.Sprintf(`{
		"filters": {
			"uptime_gt_percent": 40
		},
		"sort": {
			"column": "rating",
			"order": "desc"
		},
		"exact": [],
		"limit": %d,
		"offset": 0
	}`, opts.Config.LimitProviders))

	httpBudget := defaultHTTPBudget
	if opts.HTTPClient.Timeout > 0 {
		httpBudget = opts.HTTPClient.Timeout
	}

	var list providersResponse
	err := TimedOp(collector, "http_providers", httpBudget, Fields{}, func(reqCtx context.Context) error {
		req, e := http.NewRequestWithContext(reqCtx, http.MethodPost, opts.Config.ProvidersURL, bytes.NewReader(body))
		if e != nil {
			return e
		}
		req.Header.Set("Content-Type", "application/json")
		resp, e := opts.HTTPClient.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if e = json.NewDecoder(resp.Body).Decode(&list); e != nil {
			return fmt.Errorf("failed to decode providers: %w", e)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fetch providers: %w", err)
	}

	keys := make([][]byte, 0, len(list.Providers))
	for _, p := range list.Providers {
		key, e := hex.DecodeString(p.PubKey)
		if e != nil {
			log.Warn().Str("pubkey", p.PubKey).Msg("skip invalid provider pubkey")
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no providers returned from %s", opts.Config.ProvidersURL)
	}
	return keys, nil
}

// InitNetwork loads TON config and starts DHT + provider ADNL gateways.
func InitNetwork(ctx context.Context, networkConfigURL string) (*liteclient.GlobalConfig, *adnl.Gateway, *dht.Client, error) {
	lsCfg, err := liteclient.GetConfigFromUrl(ctx, networkConfigURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("network config: %w", err)
	}

	_, dhtKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dht key: %w", err)
	}
	_, providerKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("provider key: %w", err)
	}

	dhtGate := adnl.NewGateway(dhtKey)
	if err = dhtGate.StartClient(); err != nil {
		return nil, nil, nil, fmt.Errorf("dht gateway: %w", err)
	}

	dhtClient, err := dht.NewClientFromConfig(dhtGate, lsCfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dht client: %w", err)
	}

	providerGate := adnl.NewGateway(providerKey)
	if err = providerGate.StartClient(); err != nil {
		return nil, nil, nil, fmt.Errorf("provider gateway: %w", err)
	}

	return lsCfg, providerGate, dhtClient, nil
}
