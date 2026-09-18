package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/xssnick/archive-provider-storer/internal/probe"
)

var (
	configPath     = flag.String("config", "config.probe.json", "Path to probe configuration JSON")
	outputDir      = flag.String("output-dir", "timings", "Directory for JSONL/summary output")
	phaseBudget    = flag.Duration("phase-budget", 10*time.Second, "Timeout budget per connect/query phase")
	once           = flag.Bool("once", false, "Run one probe and exit")
	interval       = flag.Duration("interval", 15*time.Minute, "Interval between probe runs")
	samples        = flag.Int("samples", 1, "Number of connect+query samples per provider per run")
	label          = flag.String("label", "", "Optional run label (e.g. baseline, hedge) stored in summary/loop_id")
	limitProviders = flag.Int("limit-providers", 0, "Override config limit_providers when > 0")
)

func main() {
	flag.Parse()
	log.Logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	cfg, err := probe.LoadConfig(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, providerGate, dhtClient, err := probe.InitNetwork(initCtx, cfg.NetworkConfigURL)
	cancel()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init network")
	}

	run := func() error {
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()
		_, err := probe.Run(runCtx, probe.RunOptions{
			Config:         cfg,
			OutputDir:      *outputDir,
			PhaseBudget:    *phaseBudget,
			Samples:        *samples,
			Label:          *label,
			LimitProviders: *limitProviders,
			ProviderGate:   providerGate,
			DHT:            dhtClient,
		})
		return err
	}

	if *once {
		if err := run(); err != nil {
			log.Fatal().Err(err).Msg("probe run failed")
		}
		return
	}

	for {
		if err := run(); err != nil {
			log.Error().Err(err).Msg("probe run failed")
		}
		select {
		case <-ctx.Done():
			log.Info().Msg("shutdown requested")
			return
		case <-time.After(*interval):
		}
	}
}
