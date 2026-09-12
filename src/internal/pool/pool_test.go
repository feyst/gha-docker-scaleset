package pool

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestCapsAcquisitions(t *testing.T) {
	s := New(2)
	if !s.TryAcquire() || !s.TryAcquire() {
		t.Fatal("expected two slots")
	}
	if s.TryAcquire() {
		t.Fatal("a third slot was handed out beyond capacity")
	}
	s.Release()
	if !s.TryAcquire() {
		t.Fatal("a released slot was not handed out again")
	}
}

func TestReleaseWakesListenersButReturnDoesNot(t *testing.T) {
	s := New(1)
	var woken atomic.Int32
	s.OnRelease(func() { woken.Add(1) })

	s.TryAcquire()
	s.Return()
	time.Sleep(20 * time.Millisecond)
	if woken.Load() != 0 {
		t.Fatal("Return woke a listener")
	}

	s.TryAcquire()
	s.Release()
	waitFor(t, func() bool { return woken.Load() == 1 })
}

func TestWhenIdleOnlyRunsWithoutRunners(t *testing.T) {
	s := New(2)
	s.Adopt()
	if s.WhenIdle(func() { t.Fatal("ran while a runner was alive") }) {
		t.Fatal("WhenIdle reported it ran")
	}
	s.Release()

	ran := s.WhenIdle(func() {
		if s.TryAcquire() {
			t.Error("a slot was handed out during maintenance")
		}
	})
	if !ran {
		t.Fatal("WhenIdle did not run on an idle pool")
	}
	if !s.TryAcquire() {
		t.Fatal("slots stayed blocked after maintenance")
	}
}

func TestDrainHoldsSlotsUntilMaintenanceRan(t *testing.T) {
	s := New(3)
	s.Drain()
	if s.TryAcquire() {
		t.Fatal("a slot was handed out while draining")
	}
	s.WhenIdle(func() {})
	if !s.TryAcquire() {
		t.Fatal("draining did not end after maintenance")
	}
}

// waitFor polls until cond holds.
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
