package main

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
)

const (
	// idleGrace keeps a fresh runner around for a while before scaling it
	// down: the statistics that ask for fewer runners can lag behind the job
	// that is being assigned to it.
	idleGrace = time.Minute
	// exitGrace gives the JobStarted and JobCompleted messages time to arrive
	// before a stopped runner container is treated as having died.
	exitGrace = time.Minute
	logTail   = 20
)

// RunnerAPI is what a Scaler needs from GitHub; *scaleset.Client implements it.
type RunnerAPI interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
	GetRunnerByName(ctx context.Context, name string) (*scaleset.RunnerReference, error)
}

// RunnerRuntime is what a Scaler needs from the container runtime.
type RunnerRuntime interface {
	StartRunner(ctx context.Context, spec RunnerSpec) (string, error)
	RemoveContainer(ctx context.Context, id string) error
	InspectRunner(ctx context.Context, id string) (RunnerState, error)
	RunnerLogs(ctx context.Context, id string, lines int) string
}

// ImageSource tells which image new runners start from.
type ImageSource interface {
	Current() string
}

// RunnerSpec is one runner container to start.
type RunnerSpec struct {
	Name      string
	ScaleSet  string
	ImageID   string
	JITConfig string
	WorkDir   string
}

// RunnerState is what the runtime knows about a runner container.
type RunnerState struct {
	Exists     bool
	Running    bool
	ExitCode   int
	FinishedAt time.Time
}

type runner struct {
	name        string
	containerID string
	runnerID    int64
	busy        bool
	started     time.Time
}

// Scaler keeps one scale set's runner containers in line with the jobs GitHub
// assigned to it, within the scale set's own cap and the broker's global one.
type Scaler struct {
	name       string
	maxRunners int
	workRoot   string

	gh     RunnerAPI
	rt     RunnerRuntime
	images ImageSource
	broker *Broker
	log    *slog.Logger

	now       func() time.Time
	removeDir func(path string) error

	mu         sync.Mutex
	scaleSetID int
	desired    int
	runners    map[string]*runner
}

// NewScaler returns a scaler for one scale set. It starts nothing until
// SetScaleSetID has been called.
func NewScaler(cfg ScaleSetConfig, workRoot string, gh RunnerAPI, rt RunnerRuntime, images ImageSource, broker *Broker, log *slog.Logger) *Scaler {
	return &Scaler{
		name:       cfg.Name,
		maxRunners: cfg.MaxRunners,
		workRoot:   workRoot,
		gh:         gh,
		rt:         rt,
		images:     images,
		broker:     broker,
		log:        log,
		now:        time.Now,
		removeDir:  os.RemoveAll,
		runners:    make(map[string]*runner),
	}
}

var _ listener.Scaler = (*Scaler)(nil)

// SetScaleSetID sets the ID of the registered scale set new runners join.
func (s *Scaler) SetScaleSetID(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scaleSetID = id
}

// HandleDesiredRunnerCount starts or scales down runners towards count. It
// never returns an error: that would stop the listener, and a failure to
// start a runner is retried on the next message anyway.
func (s *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = count
	s.reconcileLocked(ctx)
	return len(s.runners), nil
}

// HandleJobStarted marks the runner as busy, so it is never scaled down.
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
// slot under the global cap.
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
func (s *Scaler) Adopt(ctx context.Context, containers []RunnerContainer) {
	for _, c := range containers {
		if !c.Running {
			s.cleanUpStopped(ctx, c)
			continue
		}
		r := &runner{name: c.Name, containerID: c.ID, started: s.now()}
		if ref, err := s.gh.GetRunnerByName(ctx, c.Name); err == nil && ref != nil {
			r.runnerID = int64(ref.ID)
		}
		s.mu.Lock()
		s.runners[r.name] = r
		s.mu.Unlock()
		s.log.Info("Took over a running runner", slog.String("runner", r.name))
	}
}

// Watch removes runner containers that stopped without GitHub telling us:
// a runner that crashed before it ran a job, or a completion message that
// never arrived.
func (s *Scaler) Watch(ctx context.Context) {
	s.mu.Lock()
	snapshot := make([]*runner, 0, len(s.runners))
	for _, r := range s.runners {
		snapshot = append(snapshot, r)
	}
	s.mu.Unlock()

	for _, r := range snapshot {
		st, err := s.rt.InspectRunner(ctx, r.containerID)
		if err != nil {
			s.log.Warn("Inspecting a runner container failed", slog.String("runner", r.name), slog.String("error", err.Error()))
			continue
		}
		if st.Running || (st.Exists && s.now().Sub(st.FinishedAt) < exitGrace) {
			continue
		}
		tail := ""
		if st.Exists {
			tail = s.rt.RunnerLogs(ctx, r.containerID, logTail)
		}

		s.mu.Lock()
		if s.runners[r.name] != r {
			s.mu.Unlock()
			continue
		}
		if r.busy {
			s.log.Warn("Runner container stopped without a job completion message",
				slog.String("runner", r.name), slog.Int("exitCode", st.ExitCode))
		} else {
			s.log.Warn("Runner container stopped before it ran a job",
				slog.String("runner", r.name), slog.Int("exitCode", st.ExitCode), slog.String("logs", tail))
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
	target := min(s.maxRunners, s.desired)

	for len(s.runners) < target {
		if !s.broker.TryAcquire() {
			s.log.Debug("No free runner slot", slog.Int("wanted", target), slog.Int("running", len(s.runners)))
			break
		}
		if err := s.startLocked(ctx); err != nil {
			s.broker.Return()
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
	name := s.name + "-" + randomSuffix()
	workDir := filepath.Join(s.workRoot, name)

	jit, err := s.gh.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: name, WorkFolder: workDir}, s.scaleSetID)
	if err != nil {
		return fmt.Errorf("generating a JIT config: %w", err)
	}
	var runnerID int64
	if jit.Runner != nil {
		runnerID = int64(jit.Runner.ID)
	}

	id, err := s.rt.StartRunner(ctx, RunnerSpec{Name: name, ScaleSet: s.name, ImageID: imageID, JITConfig: jit.EncodedJITConfig, WorkDir: workDir})
	if err != nil {
		if runnerID != 0 {
			_ = s.gh.RemoveRunner(ctx, runnerID)
		}
		_ = s.removeDir(workDir)
		return fmt.Errorf("starting the runner container: %w", err)
	}

	s.runners[name] = &runner{name: name, containerID: id, runnerID: runnerID, started: s.now()}
	s.log.Info("Runner started", slog.String("runner", name))
	return nil
}

func (s *Scaler) discardLocked(ctx context.Context, r *runner) {
	if err := s.rt.RemoveContainer(ctx, r.containerID); err != nil {
		s.log.Warn("Removing a runner container failed", slog.String("runner", r.name), slog.String("error", err.Error()))
	}
	if err := s.removeDir(filepath.Join(s.workRoot, r.name)); err != nil {
		s.log.Warn("Removing a runner work directory failed", slog.String("runner", r.name), slog.String("error", err.Error()))
	}
	delete(s.runners, r.name)
	s.broker.Release()
	s.log.Info("Runner removed", slog.String("runner", r.name))
}

func (s *Scaler) cleanUpStopped(ctx context.Context, c RunnerContainer) {
	if err := s.rt.RemoveContainer(ctx, c.ID); err != nil {
		s.log.Warn("Removing a stopped runner container failed", slog.String("runner", c.Name), slog.String("error", err.Error()))
	}
	_ = s.removeDir(filepath.Join(s.workRoot, c.Name))
	if ref, err := s.gh.GetRunnerByName(ctx, c.Name); err == nil && ref != nil {
		_ = s.gh.RemoveRunner(ctx, int64(ref.ID))
	}
	s.log.Info("Removed a runner left stopped by a previous run", slog.String("runner", c.Name))
}

// sortedRunnersLocked returns the runners oldest first, so scaling down is
// deterministic.
func (s *Scaler) sortedRunnersLocked() []*runner {
	list := make([]*runner, 0, len(s.runners))
	for _, r := range s.runners {
		list = append(list, r)
	}
	slices.SortFunc(list, func(a, b *runner) int { return a.started.Compare(b.started) })
	return list
}

func randomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
