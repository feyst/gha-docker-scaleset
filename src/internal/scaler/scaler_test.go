package scaler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/feyst/gha-docker-scaleset/src/internal/pool"
	"github.com/feyst/gha-docker-scaleset/src/internal/runner"
)

type fakeGitHub struct {
	mu      sync.Mutex
	nextID  int
	removed []int64
	busy    map[int64]bool
	jitErr  error
	byName  map[string]int
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{busy: map[int64]bool{}, byName: map[string]int{}}
}

func (f *fakeGitHub) GenerateJitRunnerConfig(_ context.Context, s *scaleset.RunnerScaleSetJitRunnerSetting, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jitErr != nil {
		return nil, f.jitErr
	}
	f.nextID++
	f.byName[s.Name] = f.nextID
	return &scaleset.RunnerScaleSetJitRunnerConfig{
		Runner:           &scaleset.RunnerReference{ID: f.nextID, Name: s.Name},
		EncodedJITConfig: "jit",
	}, nil
}

func (f *fakeGitHub) RemoveRunner(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.busy[id] {
		return errors.New("runner is busy")
	}
	f.removed = append(f.removed, id)
	return nil
}

func (f *fakeGitHub) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.byName[name]; ok {
		return &scaleset.RunnerReference{ID: id, Name: name}, nil
	}
	return nil, nil
}

type fakeContainer struct {
	spec     runner.Spec
	running  bool
	finished time.Time
}

type fakeContainers struct {
	mu       sync.Mutex
	byID     map[string]*fakeContainer
	startErr error
}

func newFakeContainers() *fakeContainers {
	return &fakeContainers{byID: map[string]*fakeContainer{}}
}

func (f *fakeContainers) StartRunner(_ context.Context, spec runner.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return "", f.startErr
	}
	id := spec.Name + "-id"
	f.byID[id] = &fakeContainer{spec: spec, running: true}
	return id, nil
}

func (f *fakeContainers) RemoveContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.byID, id)
	return nil
}

func (f *fakeContainers) InspectRunner(_ context.Context, id string) (runner.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.byID[id]
	if !ok {
		return runner.State{}, nil
	}
	return runner.State{Exists: true, Running: c.running, FinishedAt: c.finished}, nil
}

func (f *fakeContainers) RunnerLogs(context.Context, string, int) string { return "log tail" }

func (f *fakeContainers) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.byID)
}

func (f *fakeContainers) stop(name string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.byID[name+"-id"]
	c.running = false
	c.finished = at
}

type fixedImage string

func (i fixedImage) Current() string { return string(i) }

type harness struct {
	gh    *fakeGitHub
	rt    *fakeContainers
	slots *pool.Slots
	clock time.Time
	mu    sync.Mutex
	wiped []string
}

func newHarness(capacity int) *harness {
	return &harness{
		gh:    newFakeGitHub(),
		rt:    newFakeContainers(),
		slots: pool.New(capacity),
		clock: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC),
	}
}

func (h *harness) scaler(name string, max int) *Scaler {
	var s *Scaler
	waiter := h.slots.Join(func() { s.Kick(context.Background()) })
	s = New(Options{Name: name, MaxRunners: max, WorkRoot: "/work"},
		h.gh, h.rt, fixedImage("sha256:abc"), waiter,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = h.now
	s.removeDir = func(p string) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.wiped = append(h.wiped, p)
		return nil
	}
	s.SetScaleSetID(7)
	return s
}

func (h *harness) now() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clock
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = h.clock.Add(d)
}

func onlyRunner(t *testing.T, s *Scaler) *managedRunner {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.runners) != 1 {
		t.Fatalf("expected one runner, got %d", len(s.runners))
	}
	for _, r := range s.runners {
		return r
	}
	return nil
}

func anyRunner(s *Scaler) *managedRunner {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.runners {
		return r
	}
	return nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStartsOneRunnerPerAssignedJobUpToItsCap(t *testing.T) {
	h := newHarness(10)
	s := h.scaler("api", 3)
	ctx := context.Background()

	if n, _ := s.HandleDesiredRunnerCount(ctx, 2); n != 2 {
		t.Fatalf("expected 2 runners, got %d", n)
	}
	if n, _ := s.HandleDesiredRunnerCount(ctx, 5); n != 3 {
		t.Fatalf("expected the scale set cap of 3, got %d", n)
	}
	if h.rt.count() != 3 || h.slots.InUse() != 3 {
		t.Fatalf("containers %d, slots %d", h.rt.count(), h.slots.InUse())
	}
}

func TestRunnerGetsItsOwnWorkDirOnTheSamePath(t *testing.T) {
	h := newHarness(10)
	s := h.scaler("api", 3)
	s.HandleDesiredRunnerCount(context.Background(), 1)
	r := onlyRunner(t, s)

	c := h.rt.byID[r.containerID]
	if c.spec.WorkDir != "/work/"+r.name || c.spec.ImageID != "sha256:abc" || c.spec.ScaleSet != "api" {
		t.Fatalf("unexpected spec %+v", c.spec)
	}
}

func TestSharedCapAndAFreedSlotGoesToTheWaitingScaleSet(t *testing.T) {
	h := newHarness(2)
	ctx := context.Background()
	api := h.scaler("api", 4)
	portal := h.scaler("portal", 4)

	api.HandleDesiredRunnerCount(ctx, 2)
	if n, _ := portal.HandleDesiredRunnerCount(ctx, 1); n != 0 {
		t.Fatal("portal started a runner beyond the shared cap")
	}

	api.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: anyRunner(api).name})

	waitFor(t, func() bool {
		portal.mu.Lock()
		defer portal.mu.Unlock()
		return len(portal.runners) == 1
	})
	if h.slots.InUse() != 2 {
		t.Fatalf("expected 2 slots in use, got %d", h.slots.InUse())
	}
}

func TestJobCompletedRemovesContainerAndWorkDirAndLowersTarget(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	s.HandleDesiredRunnerCount(ctx, 1)
	r := onlyRunner(t, s)

	s.HandleJobStarted(ctx, &scaleset.JobStarted{RunnerName: r.name})
	s.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: r.name, Result: "succeeded"})

	if h.rt.count() != 0 || h.slots.InUse() != 0 {
		t.Fatalf("containers %d, slots %d", h.rt.count(), h.slots.InUse())
	}
	if len(h.wiped) != 1 || h.wiped[0] != "/work/"+r.name {
		t.Fatalf("work dir not removed: %v", h.wiped)
	}
	// A wake-up between the completion and the next statistics must not start
	// a replacement for the job that just finished.
	s.Kick(ctx)
	if h.rt.count() != 0 {
		t.Fatal("a runner was started for a completed job")
	}
}

func TestUnknownRunnerMessagesAreIgnored(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	if err := s.HandleJobStarted(ctx, &scaleset.JobStarted{RunnerName: "gone"}); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: "gone"}); err != nil {
		t.Fatal(err)
	}
}

func TestFailedStartReturnsTheSlotAndKeepsTheListenerAlive(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	h.rt.startErr = errors.New("docker is down")

	n, err := s.HandleDesiredRunnerCount(context.Background(), 2)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if h.slots.InUse() != 0 {
		t.Fatal("a slot leaked")
	}
	if len(h.gh.removed) != 1 {
		t.Fatal("the registration of the runner that failed to start was not removed")
	}
}

func TestScaleDownOnlyRemovesIdleRunnersPastTheGraceAndNotBusyOnes(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	s.HandleDesiredRunnerCount(ctx, 2)

	if n, _ := s.HandleDesiredRunnerCount(ctx, 0); n != 2 {
		t.Fatal("fresh runners were scaled down within the grace period")
	}

	h.advance(2 * idleGrace)
	busy := anyRunner(s)
	s.HandleJobStarted(ctx, &scaleset.JobStarted{RunnerName: busy.name})

	if n, _ := s.HandleDesiredRunnerCount(ctx, 0); n != 1 {
		t.Fatalf("expected only the busy runner to remain, got %d", n)
	}
	if onlyRunner(t, s) != busy {
		t.Fatal("the busy runner was removed")
	}
}

func TestScaleDownKeepsARunnerGitHubRefusesToRemove(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	s.HandleDesiredRunnerCount(ctx, 1)
	r := onlyRunner(t, s)
	h.gh.busy[r.runnerID] = true
	h.advance(2 * idleGrace)

	if n, _ := s.HandleDesiredRunnerCount(ctx, 0); n != 1 {
		t.Fatal("a runner GitHub still considers busy was removed")
	}
}

func TestWatchRemovesARunnerThatDiedBeforeItsJob(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	s.HandleDesiredRunnerCount(ctx, 1)
	r := onlyRunner(t, s)

	h.rt.stop(r.name, h.now())
	s.Watch(ctx)
	if h.rt.count() != 1 {
		t.Fatal("a just-stopped runner was removed before its messages could arrive")
	}

	h.advance(2 * exitGrace)
	s.Watch(ctx)
	if h.rt.count() != 0 || h.slots.InUse() != 0 {
		t.Fatalf("containers %d, slots %d", h.rt.count(), h.slots.InUse())
	}
	if len(h.gh.removed) != 1 || h.gh.removed[0] != r.runnerID {
		t.Fatal("the dead runner's registration was not removed")
	}
}

func TestAdoptTakesOverRunningAndCleansUpStoppedContainers(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	h.rt.byID["old-id"] = &fakeContainer{running: false}
	h.rt.byID["live-id"] = &fakeContainer{running: true}
	h.gh.byName["old"] = 41
	h.gh.byName["live"] = 42

	s.Adopt(ctx, []runner.Container{
		{ID: "old-id", Name: "old", ScaleSet: "api", Running: false},
		{ID: "live-id", Name: "live", ScaleSet: "api", Running: true},
	})

	if _, ok := h.rt.byID["old-id"]; ok {
		t.Fatal("stopped container not removed")
	}
	if len(h.gh.removed) != 1 || h.gh.removed[0] != 41 {
		t.Fatalf("stale registration not removed: %v", h.gh.removed)
	}
	if r := onlyRunner(t, s); r.name != "live" || r.runnerID != 42 {
		t.Fatalf("running container not adopted: %+v", r)
	}
}

func TestShutdownRemovesIdleRunnersAndLeavesBusyOnes(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	ctx := context.Background()
	s.HandleDesiredRunnerCount(ctx, 2)
	busy := anyRunner(s)
	s.HandleJobStarted(ctx, &scaleset.JobStarted{RunnerName: busy.name})

	s.Shutdown(ctx)
	if onlyRunner(t, s) != busy {
		t.Fatal("expected only the busy runner to survive shutdown")
	}
}

func TestNothingStartsBeforeTheScaleSetIsRegistered(t *testing.T) {
	h := newHarness(5)
	s := h.scaler("api", 3)
	s.SetScaleSetID(0)
	if n, _ := s.HandleDesiredRunnerCount(context.Background(), 2); n != 0 {
		t.Fatal("started runners without a scale set")
	}
}

func runnerCount(s *Scaler) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runners)
}

func TestAFreedSlotGoesToTheScaleSetThatWaitedLongest(t *testing.T) {
	h := newHarness(1)
	ctx := context.Background()
	api, portal, web := h.scaler("api", 4), h.scaler("portal", 4), h.scaler("web", 4)

	api.HandleDesiredRunnerCount(ctx, 1)
	portal.HandleDesiredRunnerCount(ctx, 1)
	web.HandleDesiredRunnerCount(ctx, 1)

	api.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: anyRunner(api).name})
	waitFor(t, func() bool { return runnerCount(portal) == 1 })
	// Give a wrongly woken scale set the chance to show itself.
	time.Sleep(20 * time.Millisecond)
	web.Kick(ctx)
	if runnerCount(web) != 0 || h.slots.InUse() != 1 {
		t.Fatalf("web has %d runners, %d slots in use", runnerCount(web), h.slots.InUse())
	}

	portal.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: anyRunner(portal).name})
	waitFor(t, func() bool { return runnerCount(web) == 1 })
}

func TestAScaleSetWhoseJobsVanishedLeavesTheQueue(t *testing.T) {
	h := newHarness(1)
	ctx := context.Background()
	api, portal, web := h.scaler("api", 4), h.scaler("portal", 4), h.scaler("web", 4)

	api.HandleDesiredRunnerCount(ctx, 1)
	portal.HandleDesiredRunnerCount(ctx, 1)
	web.HandleDesiredRunnerCount(ctx, 1)
	// portal's job was cancelled before it got a runner.
	portal.HandleDesiredRunnerCount(ctx, 0)

	api.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: anyRunner(api).name})
	waitFor(t, func() bool { return runnerCount(web) == 1 })
	if runnerCount(portal) != 0 {
		t.Fatal("portal started a runner it no longer needed")
	}
}

func TestASingleBusyScaleSetUsesTheWholePool(t *testing.T) {
	h := newHarness(3)
	s := h.scaler("api", 3)
	if n, _ := s.HandleDesiredRunnerCount(context.Background(), 5); n != 3 || h.slots.Waiting() != 0 {
		t.Fatalf("expected the whole pool of 3, got %d, %d waiting", n, h.slots.Waiting())
	}
}

func TestAFailingScaleSetGoesToTheBackOfTheQueue(t *testing.T) {
	h := newHarness(1)
	ctx := context.Background()
	holder, api, portal := h.scaler("holder", 4), h.scaler("api", 4), h.scaler("portal", 4)
	holder.HandleDesiredRunnerCount(ctx, 1)
	api.HandleDesiredRunnerCount(ctx, 1)
	portal.HandleDesiredRunnerCount(ctx, 1)

	h.rt.mu.Lock()
	h.rt.startErr = errors.New("docker hiccup")
	h.rt.mu.Unlock()
	// The freed slot is api's turn, but its start fails.
	holder.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: anyRunner(holder).name})
	waitFor(t, func() bool {
		h.gh.mu.Lock()
		defer h.gh.mu.Unlock()
		return len(h.gh.removed) == 1
	})
	h.rt.mu.Lock()
	h.rt.startErr = nil
	h.rt.mu.Unlock()

	api.Kick(ctx)
	if runnerCount(api) != 0 {
		t.Fatal("a scale set whose start failed kept its turn")
	}
	portal.Kick(ctx)
	if runnerCount(portal) != 1 {
		t.Fatal("the failing scale set blocked the queue")
	}
}
