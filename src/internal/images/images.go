// Package images decides which image new runners start from. A newer image is
// only taken into use once no runner is alive, because the runner's externals
// live in one host directory shared by every runner, and replacing that under
// a running job breaks the job.
package images

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	// drainAfter is how long a newer image may wait for a quiet moment before
	// new runners are held back to create one.
	drainAfter = 30 * time.Minute
)

// Runtime is what this package needs from the container runtime.
type Runtime interface {
	PullImage(ctx context.Context, ref string) error
	ImageID(ctx context.Context, ref string) (string, error)
	HasImage(ctx context.Context, id string) (bool, error)
	ExternalsStamp(dir string) (string, error)
	SyncExternals(ctx context.Context, imageID, dir string) error
}

// Slots is what this package needs from the runner pool.
type Slots interface {
	WhenIdle(fn func()) bool
	Drain()
}

// Options configure which image is watched and where its externals go.
type Options struct {
	Ref string
	// ExternalsDir empty means no externals are copied at all.
	ExternalsDir string
	Refresh      time.Duration
}

// Manager keeps the runner image current without disturbing running jobs.
type Manager struct {
	rt    Runtime
	slots Slots
	opts  Options
	log   *slog.Logger
	now   func() time.Time

	mu           sync.Mutex
	current      string
	pending      string
	pendingSince time.Time
	lastRefresh  time.Time
	draining     bool
}

// New returns a manager for the configured runner image.
func New(rt Runtime, slots Slots, opts Options, log *slog.Logger) *Manager {
	return &Manager{rt: rt, slots: slots, opts: opts, log: log, now: time.Now}
}

// Init picks up the image the host's externals were last copied from, then
// pulls the configured image.
func (m *Manager) Init(ctx context.Context) {
	if m.opts.ExternalsDir != "" {
		if stamp, err := m.rt.ExternalsStamp(m.opts.ExternalsDir); err == nil && stamp != "" {
			if ok, _ := m.rt.HasImage(ctx, stamp); ok {
				m.mu.Lock()
				m.current = stamp
				m.mu.Unlock()
			}
		}
	}
	m.Refresh(ctx)
}

// Current is the image ID new runners start from, or "" if there is none yet.
func (m *Manager) Current() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// Refresh pulls the image and schedules a switch when its ID changed.
func (m *Manager) Refresh(ctx context.Context) {
	m.mu.Lock()
	m.lastRefresh = m.now()
	m.mu.Unlock()

	if err := m.rt.PullImage(ctx, m.opts.Ref); err != nil {
		m.log.Warn("Pulling the runner image failed", slog.String("image", m.opts.Ref), slog.String("error", err.Error()))
	}
	id, err := m.rt.ImageID(ctx, m.opts.Ref)
	if err != nil || id == "" {
		m.log.Error("The runner image is not available", slog.String("image", m.opts.Ref))
		return
	}

	m.mu.Lock()
	switch id {
	case m.current:
		m.pending = ""
	case m.pending:
	default:
		m.pending = id
		m.pendingSince = m.now()
		m.log.Info("New runner image found", slog.String("image", m.opts.Ref), slog.String("id", id))
	}
	m.mu.Unlock()
	m.TrySwitch(ctx)
}

// Tick refreshes when that is due and otherwise retries a pending switch.
func (m *Manager) Tick(ctx context.Context) {
	m.mu.Lock()
	due := m.current == "" || m.now().Sub(m.lastRefresh) >= m.opts.Refresh
	m.mu.Unlock()
	if due {
		m.Refresh(ctx)
		return
	}
	m.TrySwitch(ctx)
}

// TrySwitch moves new runners to the pending image once no runner is alive,
// and holds new runners back when that moment has been waited for too long.
func (m *Manager) TrySwitch(ctx context.Context) {
	m.mu.Lock()
	pending := m.pending
	m.mu.Unlock()
	if pending == "" {
		return
	}
	if m.opts.ExternalsDir == "" {
		m.adopt(pending)
		return
	}

	ran := m.slots.WhenIdle(func() {
		if err := m.rt.SyncExternals(ctx, pending, m.opts.ExternalsDir); err != nil {
			m.log.Error("Copying the runner externals failed", slog.String("error", err.Error()))
			// Give the next attempt its own waiting period, so a failure does
			// not hold back runners every minute.
			m.mu.Lock()
			m.pendingSince = m.now()
			m.draining = false
			m.mu.Unlock()
			return
		}
		m.adopt(pending)
	})
	if ran {
		return
	}

	m.mu.Lock()
	late := !m.draining && m.now().Sub(m.pendingSince) >= drainAfter
	if late {
		m.draining = true
	}
	m.mu.Unlock()
	if late {
		m.log.Info("Holding back new runners until the running ones finish, to switch the runner image")
		m.slots.Drain()
	}
}

func (m *Manager) adopt(id string) {
	m.mu.Lock()
	if m.pending != id {
		m.mu.Unlock()
		return
	}
	m.current = id
	m.pending = ""
	m.draining = false
	m.mu.Unlock()
	m.log.Info("Runners now start from a new image", slog.String("id", id))
}
