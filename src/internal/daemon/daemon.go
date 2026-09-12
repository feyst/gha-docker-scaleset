// Package daemon wires the parts together: the Docker runtime, the shared
// pool of runner slots, the image manager and one scaler per scale set.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/feyst/gha-docker-scaleset/src/internal/actions"
	"github.com/feyst/gha-docker-scaleset/src/internal/config"
	"github.com/feyst/gha-docker-scaleset/src/internal/docker"
	"github.com/feyst/gha-docker-scaleset/src/internal/images"
	"github.com/feyst/gha-docker-scaleset/src/internal/pool"
	"github.com/feyst/gha-docker-scaleset/src/internal/runner"
	"github.com/feyst/gha-docker-scaleset/src/internal/scaler"
)

const (
	watchInterval = 15 * time.Second
	imageInterval = time.Minute
	stopTimeout   = 20 * time.Second
)

// Run starts the daemon and returns when ctx is cancelled.
func Run(ctx context.Context, cfg *config.Config, creds actions.Credentials, build actions.Build, log *slog.Logger) error {
	runtime, err := docker.New(docker.Options{
		Command:      cfg.Runner.Command,
		Env:          cfg.Runner.Env,
		Mounts:       cfg.Runner.Mounts,
		ExternalsDir: cfg.Runner.ExternalsDir,
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.Runner.WorkDir, 0o755); err != nil {
		log.Warn("The work directory is not writable from the daemon, so runner work directories will not be cleaned up",
			slog.String("path", cfg.Runner.WorkDir), slog.String("error", err.Error()))
	}

	slots := pool.New(cfg.MaxRunners)
	leftovers, err := runtime.ListRunners(ctx)
	if err != nil {
		return fmt.Errorf("listing runner containers: %w", err)
	}
	byScaleSet := groupLeftovers(ctx, cfg, leftovers, runtime, slots, log)

	// Leftover runners are counted before the image manager looks for a quiet
	// moment, so it never replaces the externals under a running job.
	imageManager := images.New(runtime, slots, images.Options{
		Ref:          cfg.Runner.Image,
		ExternalsDir: cfg.Runner.ExternalsDir,
		Refresh:      cfg.Runner.ImageRefresh,
	}, log.With(slog.String("component", "images")))
	imageManager.Init(ctx)
	slots.OnRelease(func() { imageManager.TrySwitch(ctx) })

	owner, err := os.Hostname()
	if err != nil || owner == "" {
		owner = "gha-docker-scaleset"
	}

	var wg sync.WaitGroup
	scalers := make([]*scaler.Scaler, 0, len(cfg.ScaleSets))
	for _, cfgSet := range cfg.ScaleSets {
		set := actions.ScaleSet{
			Name:        cfgSet.Name,
			URL:         cfgSet.URL,
			Labels:      cfgSet.Labels,
			MaxRunners:  cfgSet.MaxRunners,
			RunnerGroup: cfgSet.RunnerGroup,
		}
		client, err := actions.NewClient(set, creds, build)
		if err != nil {
			return fmt.Errorf("scale set %s: %w", set.Name, err)
		}
		setLog := log.With(slog.String("scaleSet", set.Name))
		s := scaler.New(scaler.Options{Name: set.Name, MaxRunners: set.MaxRunners, WorkRoot: cfg.Runner.WorkDir},
			client, runtime, imageManager, slots, setLog)
		s.Adopt(ctx, byScaleSet[set.Name])
		slots.OnRelease(func() { s.Kick(ctx) })
		scalers = append(scalers, s)
		wg.Go(func() { actions.Listen(ctx, client, set, s, owner, build, setLog) })
	}
	wg.Go(func() { maintain(ctx, scalers, imageManager) })

	log.Info("Started",
		slog.String("version", build.Version),
		slog.Int("maxRunners", cfg.MaxRunners),
		slog.Int("scaleSets", len(scalers)),
	)
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

// groupLeftovers sorts the runner containers of a previous run by scale set
// and counts the running ones, so their slots are taken before anything else
// starts. Containers of a scale set that is no longer configured are retired
// separately.
func groupLeftovers(ctx context.Context, cfg *config.Config, leftovers []runner.Container, runtime *docker.Runtime, slots *pool.Slots, log *slog.Logger) map[string][]runner.Container {
	known := make(map[string]bool, len(cfg.ScaleSets))
	for _, s := range cfg.ScaleSets {
		known[s.Name] = true
	}

	byScaleSet := make(map[string][]runner.Container)
	for _, c := range leftovers {
		if c.Running {
			slots.Adopt()
		}
		if known[c.ScaleSet] {
			byScaleSet[c.ScaleSet] = append(byScaleSet[c.ScaleSet], c)
			continue
		}
		go retireForeign(ctx, runtime, slots, c, log)
	}
	return byScaleSet
}

func maintain(ctx context.Context, scalers []*scaler.Scaler, imageManager *images.Manager) {
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
			imageManager.Tick(ctx)
		}
	}
}

// retireForeign handles a container of a scale set that is no longer
// configured: a stopped one is removed at once, a running one keeps its slot
// until its job is done.
func retireForeign(ctx context.Context, runtime *docker.Runtime, slots *pool.Slots, c runner.Container, log *slog.Logger) {
	log = log.With(slog.String("runner", c.Name), slog.String("scaleSet", c.ScaleSet))
	if c.Running {
		log.Info("Runner belongs to a scale set that is no longer configured; waiting for it to stop")
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(watchInterval):
			}
			if state, err := runtime.InspectRunner(ctx, c.ID); err == nil && !state.Running {
				break
			}
		}
		defer slots.Release()
	}
	if err := runtime.RemoveContainer(ctx, c.ID); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("Removing the container failed", slog.String("error", err.Error()))
	}
}
