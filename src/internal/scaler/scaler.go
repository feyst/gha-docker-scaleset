// Package scaler keeps one scale set's runner containers in line with the
// jobs GitHub assigned to it, within that scale set's own cap and the pool's
// shared one.
package scaler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/feyst/gha-docker-scaleset/src/internal/runner"
)

const (
	// idleGrace keeps a fresh runner around for a while before scaling it
	// down: the statistics asking for fewer runners can lag behind the job
	// that is being assigned to it.
	idleGrace = time.Minute
	// exitGrace gives the job messages time to arrive before a stopped
	// container is treated as a runner that died.
	exitGrace = time.Minute
	logTail   = 20
)

// GitHub is what this package needs from the scale set API; *scaleset.Client
// implements it.
type GitHub interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
	GetRunnerByName(ctx context.Context, name string) (*scaleset.RunnerReference, error)
}

// Containers is what this package needs from the container runtime.
type Containers interface {
	StartRunner(ctx context.Context, spec runner.Spec) (string, error)
	RemoveContainer(ctx context.Context, id string) error
	InspectRunner(ctx context.Context, id string) (runner.State, error)
	RunnerLogs(ctx context.Context, id string, lines int) string
}

// Images tells which image new runners start from.
type Images interface {
	Current() string
}

// Slots is what this package needs from the runner pool.
type Slots interface {
	TryAcquire() bool
	Return()
	Release()
}

// Options describe the scale set this scaler serves.
type Options struct {
	Name       string
	MaxRunners int
	// WorkRoot holds one work directory per runner.
	WorkRoot string
}

type managedRunner struct {
	name        string
	containerID string
	runnerID    int64
	busy        bool
	started     time.Time
}

// Scaler starts a runner per assigned job and removes it when the job is done.
type Scaler struct {
	opts       Options
	gh         GitHub
	containers Containers
	images     Images
	slots      Slots
	log        *slog.Logger

	now       func() time.Time
	removeDir func(path string) error

	mu         sync.Mutex
	scaleSetID int
	desired    int
	runners    map[string]*managedRunner
}

var _ listener.Scaler = (*Scaler)(nil)

// New returns a scaler. It starts nothing until SetScaleSetID has been called
// with the ID of the registered scale set.
func New(opts Options, gh GitHub, containers Containers, images Images, slots Slots, log *slog.Logger) *Scaler {
	return &Scaler{
		opts:       opts,
		gh:         gh,
		containers: containers,
		images:     images,
		slots:      slots,
		log:        log,
		now:        time.Now,
		removeDir:  os.RemoveAll,
		runners:    make(map[string]*managedRunner),
	}
}

// SetScaleSetID sets the ID of the scale set new runners join.
func (s *Scaler) SetScaleSetID(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scaleSetID = id
}

// HandleDesiredRunnerCount starts or scales down runners towards count. It
// never returns an error: that would stop the listener, while a failure to
// start a runner is retried on the next message anyway.
func (s *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = count
	s.reconcileLocked(ctx)
	return len(s.runners), nil
}

// HandleJobStarted marks a runner as busy, so it is never scaled down.
func (s *Scaler) HandleJobStarted(_ context.Context, job *scaleset.JobStarted) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runners[job.RunnerName]
	if !ok {
		s.log.Debug("Job started on an unknown runner", slog.String("runner", job.RunnerName))
		return nil
	}
	r.busy = true
	s.log.Info("Job started",
		slog.String("runner", r.name),
		slog.String("repository", job.OwnerName+"/"+job.RepositoryName),
		slog.String("job", job.JobDisplayName),
	)
	return nil
}

// HandleJobCompleted removes the runner's container and work directory.
func (s *Scaler) HandleJobCompleted(ctx context.Context, job *scaleset.JobCompleted) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The statistics that come with this message still count the job; lower
	// the target now, so a wake-up in between does not start a runner for it.
	if s.desired > 0 {
		s.desired--
	}
	r, ok := s.runners[job.RunnerName]
	if !ok {
		s.log.Debug("Job completed on an unknown runner", slog.String("runner", job.RunnerName))
		return nil
	}
	s.log.Info("Job completed",
		slog.String("runner", r.name),
		slog.String("job", job.JobDisplayName),
		slog.String("result", job.Result),
	)
	s.discardLocked(ctx, r)
	return nil
}

// Kick retries starting runners, typically because another scale set freed a
// slot under the shared cap.
func (s *Scaler) Kick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcileLocked(ctx)
}

// Adopt takes over runner containers left by a previous run of the daemon.
// Their slots have already been counted by the caller.
func (s *Scaler) Adopt(ctx context.Context, containers []runner.Container) {
	for _, c := range containers {
		if !c.Running {
			s.cleanUpStopped(ctx, c)
			continue
		}
		r := &managedRunner{name: c.Name, containerID: c.ID, started: s.now()}
		if ref, err := s.gh.GetRunnerByName(ctx, c.Name); err == nil && ref != nil {
			r.runnerID = int64(ref.ID)
		}
		s.mu.Lock()
		s.runners[r.name] = r
		s.mu.Unlock()
		s.log.Info("Took over a running runner", slog.String("runner", r.name))
	}
}

// Watch removes runner containers that stopped without GitHub telling us: a
// runner that died before it ran a job, or a completion message that never
// arrived.
func (s *Scaler) Watch(ctx context.Context) {
	s.mu.Lock()
	snapshot := make([]*managedRunner, 0, len(s.runners))
	for _, r := range s.runners {
		snapshot = append(snapshot, r)
	}
	s.mu.Unlock()

	for _, r := range snapshot {
		state, err := s.containers.InspectRunner(ctx, r.containerID)
		if err != nil {
			s.log.Warn("Inspecting a runner container failed", slog.String("runner", r.name), slog.String("error", err.Error()))
			continue
		}
		if state.Running || (state.Exists && s.now().Sub(state.FinishedAt) < exitGrace) {
			continue
		}
		tail := ""
		if state.Exists {
			tail = s.containers.RunnerLogs(ctx, r.containerID, logTail)
		}

		s.mu.Lock()
		if s.runners[r.name] != r {
			s.mu.Unlock()
			continue
		}
		if r.busy {
			s.log.Warn("Runner container stopped without a job completion message",
				slog.String("runner", r.name), slog.Int("exitCode", state.ExitCode))
		} else {
			s.log.Warn("Runner container stopped before it ran a job",
				slog.String("runner", r.name), slog.Int("exitCode", state.ExitCode), slog.String("logs", tail))
			if r.runnerID != 0 {
				_ = s.gh.RemoveRunner(ctx, r.runnerID)
			}
		}
		s.discardLocked(ctx, r)
		s.mu.Unlock()
	}
}

// Shutdown removes the runners that have no job. Busy runners keep going; the
// next start of the daemon takes them over or cleans them up.
func (s *Scaler) Shutdown(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.sortedRunnersLocked() {
		if r.busy || r.runnerID == 0 {
			continue
		}
		if err := s.gh.RemoveRunner(ctx, r.runnerID); err != nil {
			continue
		}
		s.discardLocked(ctx, r)
	}
}

func (s *Scaler) reconcileLocked(ctx context.Context) {
	if s.scaleSetID == 0 || ctx.Err() != nil {
		return
	}
	target := min(s.opts.MaxRunners, s.desired)

	for len(s.runners) < target {
		if !s.slots.TryAcquire() {
			s.log.Debug("No free runner slot", slog.Int("wanted", target), slog.Int("running", len(s.runners)))
			break
		}
		if err := s.startLocked(ctx); err != nil {
			s.slots.Return()
			s.log.Warn("Starting a runner failed", slog.String("error", err.Error()))
			break
		}
	}

	excess := len(s.runners) - target
	for _, r := range s.sortedRunnersLocked() {
		if excess <= 0 {
			break
		}
		if r.busy || r.runnerID == 0 || s.now().Sub(r.started) < idleGrace {
			continue
		}
		// Removing the registration fails if a job was just assigned to it,
		// which is exactly when the runner must stay.
		if err := s.gh.RemoveRunner(ctx, r.runnerID); err != nil {
			s.log.Debug("Keeping idle runner", slog.String("runner", r.name), slog.String("error", err.Error()))
			continue
		}
		s.discardLocked(ctx, r)
		excess--
	}
}

func (s *Scaler) startLocked(ctx context.Context) error {
	imageID := s.images.Current()
	if imageID == "" {
		return errors.New("no runner image available yet")
	}
	name := s.opts.Name + "-" + randomSuffix()
	workDir := filepath.Join(s.opts.WorkRoot, name)

	jit, err := s.gh.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: name, WorkFolder: workDir}, s.scaleSetID)
	if err != nil {
		return fmt.Errorf("generating a JIT config: %w", err)
	}
	var runnerID int64
	if jit.Runner != nil {
		runnerID = int64(jit.Runner.ID)
	}

	id, err := s.containers.StartRunner(ctx, runner.Spec{
		Name:      name,
		ScaleSet:  s.opts.Name,
		ImageID:   imageID,
		JITConfig: jit.EncodedJITConfig,
		WorkDir:   workDir,
	})
	if err != nil {
		if runnerID != 0 {
			_ = s.gh.RemoveRunner(ctx, runnerID)
		}
		_ = s.removeDir(workDir)
		return fmt.Errorf("starting the runner container: %w", err)
	}

	s.runners[name] = &managedRunner{name: name, containerID: id, runnerID: runnerID, started: s.now()}
	s.log.Info("Runner started", slog.String("runner", name))
	return nil
}

func (s *Scaler) discardLocked(ctx context.Context, r *managedRunner) {
	if err := s.containers.RemoveContainer(ctx, r.containerID); err != nil {
		s.log.Warn("Removing a runner container failed", slog.String("runner", r.name), slog.String("error", err.Error()))
	}
	if err := s.removeDir(filepath.Join(s.opts.WorkRoot, r.name)); err != nil {
		s.log.Warn("Removing a runner work directory failed", slog.String("runner", r.name), slog.String("error", err.Error()))
	}
	delete(s.runners, r.name)
	s.slots.Release()
	s.log.Info("Runner removed", slog.String("runner", r.name))
}

func (s *Scaler) cleanUpStopped(ctx context.Context, c runner.Container) {
	if err := s.containers.RemoveContainer(ctx, c.ID); err != nil {
		s.log.Warn("Removing a stopped runner container failed", slog.String("runner", c.Name), slog.String("error", err.Error()))
	}
	_ = s.removeDir(filepath.Join(s.opts.WorkRoot, c.Name))
	if ref, err := s.gh.GetRunnerByName(ctx, c.Name); err == nil && ref != nil {
		_ = s.gh.RemoveRunner(ctx, int64(ref.ID))
	}
	s.log.Info("Removed a runner left stopped by a previous run", slog.String("runner", c.Name))
}

// sortedRunnersLocked returns the runners oldest first, so scaling down is
// deterministic.
func (s *Scaler) sortedRunnersLocked() []*managedRunner {
	list := make([]*managedRunner, 0, len(s.runners))
	for _, r := range s.runners {
		list = append(list, r)
	}
	slices.SortFunc(list, func(a, b *managedRunner) int { return a.started.Compare(b.started) })
	return list
}

func randomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
