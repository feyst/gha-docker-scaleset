package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeImages struct {
	mu      sync.Mutex
	id      string
	present map[string]bool
	synced  []string
	syncErr error
}

func (f *fakeImages) PullImage(context.Context, string) error { return nil }
func (f *fakeImages) ImageID(context.Context, string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.id, nil
}
func (f *fakeImages) HasImage(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.present[id], nil
}
func (f *fakeImages) SyncExternals(_ context.Context, id, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncErr != nil {
		return f.syncErr
	}
	f.synced = append(f.synced, id)
	return nil
}

func newTestImages(rt *fakeImages, b *Broker, stamp string, clock *time.Time) *Images {
	i := NewImages(rt, b, RunnerConfig{Image: "runner:latest", ExternalsDir: "/actions-runner/externals", ImageRefresh: time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	i.now = func() time.Time { return *clock }
	i.readStamp = func(string) (string, error) {
		if stamp == "" {
			return "", errors.New("no stamp")
		}
		return stamp, nil
	}
	return i
}

func TestFirstStartCopiesExternalsBeforeAnyRunner(t *testing.T) {
	clock := time.Now()
	rt := &fakeImages{id: "sha256:new", present: map[string]bool{}}
	i := newTestImages(rt, NewBroker(2), "", &clock)

	i.Init(context.Background())
	if i.Current() != "sha256:new" || len(rt.synced) != 1 {
		t.Fatalf("current %q, synced %v", i.Current(), rt.synced)
	}
}

func TestMatchingStampNeedsNoCopy(t *testing.T) {
	clock := time.Now()
	rt := &fakeImages{id: "sha256:same", present: map[string]bool{"sha256:same": true}}
	i := newTestImages(rt, NewBroker(2), "sha256:same", &clock)

	i.Init(context.Background())
	if i.Current() != "sha256:same" || len(rt.synced) != 0 {
		t.Fatalf("current %q, synced %v", i.Current(), rt.synced)
	}
}

func TestNewImageWaitsForIdleThenSwitches(t *testing.T) {
	clock := time.Now()
	b := NewBroker(2)
	rt := &fakeImages{id: "sha256:old", present: map[string]bool{"sha256:old": true}}
	i := newTestImages(rt, b, "sha256:old", &clock)
	i.Init(context.Background())

	b.TryAcquire()
	rt.id = "sha256:new"
	i.Refresh(context.Background())
	if i.Current() != "sha256:old" || len(rt.synced) != 0 {
		t.Fatal("switched images while a runner was alive")
	}

	b.Return()
	i.TrySwitch(context.Background())
	if i.Current() != "sha256:new" || len(rt.synced) != 1 {
		t.Fatalf("did not switch once idle: current %q", i.Current())
	}
}

func TestLongPendingImageDrainsNewRunners(t *testing.T) {
	clock := time.Now()
	b := NewBroker(3)
	rt := &fakeImages{id: "sha256:old", present: map[string]bool{"sha256:old": true}}
	i := newTestImages(rt, b, "sha256:old", &clock)
	i.Init(context.Background())

	b.TryAcquire()
	rt.id = "sha256:new"
	i.Refresh(context.Background())
	if !b.TryAcquire() {
		t.Fatal("drained before the grace period")
	}
	b.Return()

	clock = clock.Add(drainAfter + time.Second)
	i.TrySwitch(context.Background())
	if b.TryAcquire() {
		t.Fatal("new runners were not held back")
	}

	b.Release()
	i.TrySwitch(context.Background())
	if i.Current() != "sha256:new" || !b.TryAcquire() {
		t.Fatal("did not switch and resume")
	}
}

func TestFailedCopyKeepsTheOldImage(t *testing.T) {
	clock := time.Now()
	b := NewBroker(2)
	rt := &fakeImages{id: "sha256:old", present: map[string]bool{"sha256:old": true}}
	i := newTestImages(rt, b, "sha256:old", &clock)
	i.Init(context.Background())

	rt.id = "sha256:new"
	rt.syncErr = errors.New("disk full")
	i.Refresh(context.Background())
	if i.Current() != "sha256:old" {
		t.Fatal("switched to an image whose externals were not copied")
	}
	if !b.TryAcquire() {
		t.Fatal("a failed copy left slots blocked")
	}
}
