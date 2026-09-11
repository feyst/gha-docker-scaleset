package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// drainAfter is how long a newer image may wait for an idle moment before
	// new runners are held back to create one.
	drainAfter = 30 * time.Minute
	stampFile  = ".image-id"
)

// ImageRuntime is what Images needs from the container runtime.
type ImageRuntime interface {
	PullImage(ctx context.Context, ref string) error
	ImageID(ctx context.Context, ref string) (string, error)
	HasImage(ctx context.Context, id string) (bool, error)
	SyncExternals(ctx context.Context, imageID, dir string) error
}

// Images decides which image new runners start from. A newer image is only
// taken into use once no runner is alive: the runner's externals live in one
// host directory shared by every runner, and replacing it under a running job
// breaks that job.
type Images struct {
	rt        ImageRuntime
	broker    *Broker
	cfg       RunnerConfig
	log       *slog.Logger
	now       func() time.Time
	readStamp func(path string) (string, error)

	mu           sync.Mutex
	current      string
	pending      string
	pendingSince time.Time
	lastRefresh  time.Time
	draining     bool
}

// NewImages returns an image manager for the configured runner image.
func NewImages(rt ImageRuntime, broker *Broker, cfg RunnerConfig, log *slog.Logger) *Images {
	return &Images{
		rt:     rt,
		broker: broker,
		cfg:    cfg,
		log:    log,
		now:    time.Now,
		readStamp: func(path string) (string, error) {
			data, err := os.ReadFile(path)
			return strings.TrimSpace(string(data)), err
		},
	}
}

// Init picks up the image the host's externals were last copied from, then
// pulls the configured image.
func (i *Images) Init(ctx context.Context) {
	if i.cfg.ExternalsDir != "" {
		stamp, err := i.readStamp(filepath.Join(i.cfg.ExternalsDir, stampFile))
		if err == nil && stamp != "" {
			if ok, _ := i.rt.HasImage(ctx, stamp); ok {
				i.mu.Lock()
				i.current = stamp
				i.mu.Unlock()
			}
		}
	}
	i.Refresh(ctx)
}

// Current is the image ID new runners start from, or "" if there is none yet.
func (i *Images) Current() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.current
}

// Refresh pulls the image and schedules a switch when its ID changed.
func (i *Images) Refresh(ctx context.Context) {
	i.mu.Lock()
	i.lastRefresh = i.now()
	i.mu.Unlock()

	if err := i.rt.PullImage(ctx, i.cfg.Image); err != nil {
		i.log.Warn("Pulling the runner image failed", slog.String("image", i.cfg.Image), slog.String("error", err.Error()))
	}
	id, err := i.rt.ImageID(ctx, i.cfg.Image)
	if err != nil || id == "" {
		i.log.Error("The runner image is not available", slog.String("image", i.cfg.Image))
		return
	}

	i.mu.Lock()
	switch id {
	case i.current:
		i.pending = ""
	case i.pending:
	default:
		i.pending = id
		i.pendingSince = i.now()
		i.log.Info("New runner image found", slog.String("image", i.cfg.Image), slog.String("id", id))
	}
	i.mu.Unlock()
	i.TrySwitch(ctx)
}

// Tick refreshes when that is due and otherwise retries a pending switch.
func (i *Images) Tick(ctx context.Context) {
	i.mu.Lock()
	due := i.current == "" || i.now().Sub(i.lastRefresh) >= i.cfg.ImageRefresh
	i.mu.Unlock()
	if due {
		i.Refresh(ctx)
		return
	}
	i.TrySwitch(ctx)
}

// TrySwitch moves new runners to the pending image once no runner is alive,
// and holds new runners back when that moment has been waited for too long.
func (i *Images) TrySwitch(ctx context.Context) {
	i.mu.Lock()
	pending := i.pending
	i.mu.Unlock()
	if pending == "" {
		return
	}
	if i.cfg.ExternalsDir == "" {
		i.adopt(pending)
		return
	}

	ran := i.broker.WhenIdle(func() {
		if err := i.rt.SyncExternals(ctx, pending, i.cfg.ExternalsDir); err != nil {
			i.log.Error("Copying the runner externals failed", slog.String("error", err.Error()))
			i.mu.Lock()
			i.pendingSince = i.now()
			i.draining = false
			i.mu.Unlock()
			return
		}
		i.adopt(pending)
	})
	if ran {
		return
	}

	i.mu.Lock()
	late := !i.draining && i.now().Sub(i.pendingSince) >= drainAfter
	if late {
		i.draining = true
	}
	i.mu.Unlock()
	if late {
		i.log.Info("Holding back new runners until the running ones finish, to switch the runner image")
		i.broker.Drain()
	}
}

func (i *Images) adopt(id string) {
	i.mu.Lock()
	if i.pending != id {
		i.mu.Unlock()
		return
	}
	i.current = id
	i.pending = ""
	i.draining = false
	i.mu.Unlock()
	i.log.Info("Runners now start from a new image", slog.String("id", id))
}
