package images

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/feyst/gha-docker-scaleset/src/internal/pool"
)

type fakeRuntime struct {
	mu      sync.Mutex
	id      string
	stamp   string
	present map[string]bool
	synced  []string
	syncErr error
}

func (f *fakeRuntime) PullImage(context.Context, string) error { return nil }

func (f *fakeRuntime) ImageID(context.Context, string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.id, nil
}

func (f *fakeRuntime) HasImage(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.present[id], nil
}

func (f *fakeRuntime) ExternalsStamp(string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stamp == "" {
		return "", errors.New("no stamp")
	}
	return f.stamp, nil
}

func (f *fakeRuntime) SyncExternals(_ context.Context, id, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncErr != nil {
		return f.syncErr
	}
	f.synced = append(f.synced, id)
	return nil
}

func newManager(rt *fakeRuntime, slots Slots, clock *time.Time) *Manager {
	m := New(rt, slots, Options{Ref: "runner:latest", ExternalsDir: "/actions-runner/externals", Refresh: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.now = func() time.Time { return *clock }
	return m
}

func TestFirstStartCopiesExternalsBeforeAnyRunner(t *testing.T) {
	clock := time.Now()
	rt := &fakeRuntime{id: "sha256:new", present: map[string]bool{}}
	m := newManager(rt, pool.New(2), &clock)

	m.Init(context.Background())
	if m.Current() != "sha256:new" || len(rt.synced) != 1 {
		t.Fatalf("current %q, synced %v", m.Current(), rt.synced)
	}
}

func TestMatchingStampNeedsNoCopy(t *testing.T) {
	clock := time.Now()
	rt := &fakeRuntime{id: "sha256:same", stamp: "sha256:same", present: map[string]bool{"sha256:same": true}}
	m := newManager(rt, pool.New(2), &clock)

	m.Init(context.Background())
	if m.Current() != "sha256:same" || len(rt.synced) != 0 {
		t.Fatalf("current %q, synced %v", m.Current(), rt.synced)
	}
}

func TestNewImageWaitsForIdleThenSwitches(t *testing.T) {
	clock := time.Now()
	slots := pool.New(2)
	rt := &fakeRuntime{id: "sha256:old", stamp: "sha256:old", present: map[string]bool{"sha256:old": true}}
	m := newManager(rt, slots, &clock)
	m.Init(context.Background())

	slots.TryAcquire()
	rt.id = "sha256:new"
	m.Refresh(context.Background())
	if m.Current() != "sha256:old" || len(rt.synced) != 0 {
		t.Fatal("switched images while a runner was alive")
	}

	slots.Return()
	m.TrySwitch(context.Background())
	if m.Current() != "sha256:new" || len(rt.synced) != 1 {
		t.Fatalf("did not switch once idle: current %q", m.Current())
	}
}

func TestLongPendingImageDrainsNewRunners(t *testing.T) {
	clock := time.Now()
	slots := pool.New(3)
	rt := &fakeRuntime{id: "sha256:old", stamp: "sha256:old", present: map[string]bool{"sha256:old": true}}
	m := newManager(rt, slots, &clock)
	m.Init(context.Background())

	slots.TryAcquire()
	rt.id = "sha256:new"
	m.Refresh(context.Background())
	if !slots.TryAcquire() {
		t.Fatal("drained before the grace period")
	}
	slots.Return()

	clock = clock.Add(drainAfter + time.Second)
	m.TrySwitch(context.Background())
	if slots.TryAcquire() {
		t.Fatal("new runners were not held back")
	}

	slots.Release()
	m.TrySwitch(context.Background())
	if m.Current() != "sha256:new" || !slots.TryAcquire() {
		t.Fatal("did not switch and resume")
	}
}

func TestFailedCopyKeepsTheOldImage(t *testing.T) {
	clock := time.Now()
	slots := pool.New(2)
	rt := &fakeRuntime{id: "sha256:old", stamp: "sha256:old", present: map[string]bool{"sha256:old": true}}
	m := newManager(rt, slots, &clock)
	m.Init(context.Background())

	rt.id = "sha256:new"
	rt.syncErr = errors.New("disk full")
	m.Refresh(context.Background())
	if m.Current() != "sha256:old" {
		t.Fatal("switched to an image whose externals were not copied")
	}
	if !slots.TryAcquire() {
		t.Fatal("a failed copy left slots blocked")
	}
}
