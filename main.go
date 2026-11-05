// Command spot-bidder is a long-lived controller that keeps enough Rackspace
// Spot VMs bid for and assigned, across several VM Cloudspaces/regions, so
// that pods tolerating the spot taint are never left Pending.
//
// See README.md for the algorithm and deployment steps.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1 "github.com/rackspace-spot/spot-go-sdk/api/v1"

	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/bidder"
	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/config"
	k8sclient "github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/k8s"
	"github.com/edevos/rackspace-spot-multi-cloudspace-k8s-bidder/internal/spot"
)

func main() {
	if os.Getenv("LOG_LEVEL") == "debug" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "err", err.Error())
		os.Exit(2)
	}

	slog.Info("starting rackspace-spot-multi-cloudspace-k8s-bidder",
		"org", cfg.SpotOrg,
		"regions", cfg.Regions,
		"server_classes", cfg.ServerClasses,
		"bid_price", cfg.BidPrice,
		"min_memory", cfg.MinMemoryRaw,
		"max_vms", cfg.MaxVMs,
		"provision_timeout", cfg.ProvisionTimeout.String(),
		"region_cooldown", cfg.RegionCooldown.String(),
		"interval", cfg.Interval.String(),
		"dry_run", cfg.DryRun)

	// Build the Spot SDK client.
	spotClient, err := v1.NewSpotClient(&v1.Config{
		RefreshToken: cfg.SpotRefreshToken,
		BaseURL:      cfg.SpotBaseURL,
		OAuthURL:     cfg.SpotAuthURL,
	})
	if err != nil {
		slog.Error("failed to create spot client", "err", err.Error())
		os.Exit(1)
	}
	if _, err := spotClient.Authenticate(context.Background()); err != nil {
		slog.Error("spot authentication failed", "err", err.Error())
		os.Exit(1)
	}
	spotC := &spot.Client{
		Spot:       spotClient,
		HTTP:       spotClient.HTTPClient,
		BaseURL:    cfg.SpotBaseURL,
		Org:        cfg.SpotOrg,
		NamePrefix: cfg.NamePrefix,
		SSHKeyName: cfg.SSHKeyName,
		VMImage:    cfg.VMImage,
		UserData:   cfg.UserData,
	}

	// Build the in-cluster k8s client (we run as a ServiceAccount in k3s).
	cs, err := k8sclient.NewInCluster()
	if err != nil {
		slog.Error("failed to create kubernetes client", "err", err.Error())
		os.Exit(1)
	}
	kClient := &k8sclient.Client{
		Clientset:        cs,
		SpotTaint:        cfg.SpotTaint,
		SpotNodeSelector: cfg.SpotNodeSelector,
		SpotNodeLabels:   cfg.SpotNodeLabels,
		PendingGrace:     cfg.PendingGrace,
	}

	bd := bidder.New(bidder.Settings{
		Regions:              cfg.Regions,
		Families:             cfg.ServerClasses,
		BidPrice:             cfg.BidPrice,
		NamePrefix:           cfg.NamePrefix,
		MinMemory:            cfg.MinMemory,
		MaxVMs:               cfg.MaxVMs,
		MaxPerRegion:         cfg.MaxPerRegion,
		AllocMemRatio:        cfg.AllocatableMemoryRatio,
		ProvisionTimeout:     cfg.ProvisionTimeout,
		RegionCooldown:       cfg.RegionCooldown,
		BootGrace:            cfg.BootGrace,
		ScaleDown:            cfg.ScaleDown,
		ScaleDownDelay:       cfg.ScaleDownDelay,
		ScaleDownUtilization: cfg.ScaleDownUtilization,
		CountUnmanaged:       cfg.CountUnmanaged,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()

	// Unlike the cleaner, a failed tick is not immediately fatal: the
	// bidder's timers (pending bids, region cooldowns) live in memory and a
	// restart would reset them. A tick that fails mid-way is simply
	// re-planned from fresh state on the next one; only a persistent failure
	// exits so the Pod restart policy (and alerting on restarts) kicks in.
	failures := 0
	run := func() {
		start := time.Now()
		sum, err := bd.Reconcile(ctx, start, spotC, kClient, cfg.DryRun)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("reconcile aborted by shutdown", "err", err.Error())
				return
			}
			failures++
			slog.Error("reconcile failed", "err", err.Error(), "consecutive_failures", failures,
				"duration", time.Since(start).String())
			if failures >= cfg.MaxConsecutiveFailures {
				os.Exit(1)
			}
			return
		}
		failures = 0
		level := slog.LevelInfo
		if sum.NoOffering {
			level = slog.LevelWarn
		}
		slog.Log(ctx, level, "reconcile complete",
			"won", sum.Won,
			"won_memory_gi", sum.WonMemory>>30,
			"in_flight", sum.InFlight,
			"booting", sum.Booting,
			"pending_pods", sum.PendingPods,
			"pending_vms", sum.PendingVMs,
			"unplaceable_pods", sum.Unplaceable,
			"required", sum.Required,
			"cooling_down", sum.CoolingDown,
			"no_eligible_offering", sum.NoOffering,
			"dry_run", cfg.DryRun,
			"duration", time.Since(start).String())
	}

	run()
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down", "reason", ctx.Err().Error())
			return
		case <-tick.C:
			run()
		}
	}
}
