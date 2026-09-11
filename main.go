// Command gha-docker-scaleset runs ephemeral GitHub Actions runners as Docker
// containers: one daemon listens for jobs on one or more runner scale sets and
// starts a fresh container per job, under a single cap on the number of
// runners alive at the same time.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	version = "dev"
	commit  = "unknown"
)

const (
	watchInterval = 15 * time.Second
	imageInterval = time.Minute
	stopTimeout   = 20 * time.Second
)

func main() {
	configPath := flag.String("config", "/etc/gha-docker-scaleset/config.yml", "path to the configuration file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version, commit)
		return
	}

	log := newLogger(os.Getenv("LOG_LEVEL"))
	if err := run(*configPath, log); err != nil {
		log.Error("Exiting", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath string, log *slog.Logger) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	creds, err := CredentialsFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, err := NewDockerRuntime(cfg.Runner)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.Runner.WorkDir, 0o755); err != nil {
		log.Warn("The work directory is not writable from the daemon, so runner work directories will not be cleaned up",
			slog.String("path", cfg.Runner.WorkDir), slog.String("error", err.Error()))
	}

	broker := NewBroker(cfg.MaxRunners)
	existing, err := rt.ListRunners(ctx)
	if err != nil {
		return fmt.Errorf("listing runner containers: %w", err)
	}
	known := make(map[string]bool, len(cfg.ScaleSets))
	for _, s := range cfg.ScaleSets {
		known[s.Name] = true
	}
	byScaleSet := make(map[string][]RunnerContainer)
	for _, c := range existing {
		if c.Running {
			broker.Adopt()
		}
		if known[c.ScaleSet] {
			byScaleSet[c.ScaleSet] = append(byScaleSet[c.ScaleSet], c)
			continue
		}
		go retireForeign(ctx, rt, broker, c, log)
	}

	// Adopted runners are counted before the image manager looks for an idle
	// moment, so it never replaces the externals under a running job.
	images := NewImages(rt, broker, cfg.Runner, log.With(slog.String("component", "images")))
	images.Init(ctx)
	broker.OnRelease(func() { images.TrySwitch(ctx) })

	owner, err := os.Hostname()
	if err != nil || owner == "" {
		owner = "gha-docker-scaleset"
	}

	var wg sync.WaitGroup
	scalers := make([]*Scaler, 0, len(cfg.ScaleSets))
	for _, ssCfg := range cfg.ScaleSets {
		client, err := newScaleSetClient(ssCfg.URL, creds)
		if err != nil {
			return fmt.Errorf("scale set %s: %w", ssCfg.Name, err)
		}
		scaleLog := log.With(slog.String("scaleSet", ssCfg.Name))
		scaler := NewScaler(ssCfg, cfg.Runner.WorkDir, client, rt, images, broker, scaleLog)
		scaler.Adopt(ctx, byScaleSet[ssCfg.Name])
		broker.OnRelease(func() { scaler.Kick(ctx) })
		scalers = append(scalers, scaler)
		wg.Go(func() { runScaleSet(ctx, client, ssCfg, scaler, owner, scaleLog) })
	}
	wg.Go(func() { maintain(ctx, scalers, images) })

	log.Info("Started", slog.String("version", version), slog.Int("maxRunners", cfg.MaxRunners), slog.Int("scaleSets", len(scalers)))
	<-ctx.Done()
	log.Info("Stopping")
	wg.Wait()

	stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	for _, s := range scalers {
		s.Shutdown(stopCtx)
	}
	return nil
}

func maintain(ctx context.Context, scalers []*Scaler, images *Images) {
	watch := time.NewTicker(watchInterval)
	defer watch.Stop()
	image := time.NewTicker(imageInterval)
	defer image.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-watch.C:
			for _, s := range scalers {
				s.Watch(ctx)
			}
		case <-image.C:
			images.Tick(ctx)
		}
	}
}

// retireForeign handles a container of a scale set that is no longer
// configured: a stopped one is removed at once, a running one keeps its slot
// until its job is done.
func retireForeign(ctx context.Context, rt *DockerRuntime, broker *Broker, c RunnerContainer, log *slog.Logger) {
	log = log.With(slog.String("runner", c.Name), slog.String("scaleSet", c.ScaleSet))
	if c.Running {
		log.Info("Runner belongs to a scale set that is no longer configured; waiting for it to stop")
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(watchInterval):
			}
			st, err := rt.InspectRunner(ctx, c.ID)
			if err == nil && !st.Running {
				break
			}
		}
		defer broker.Release()
	}
	if err := rt.RemoveContainer(ctx, c.ID); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("Removing the container failed", slog.String("error", err.Error()))
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
