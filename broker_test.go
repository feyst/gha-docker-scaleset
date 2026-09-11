package main

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerCapsAcquisitions(t *testing.T) {
	b := NewBroker(2)
	if !b.TryAcquire() || !b.TryAcquire() {
		t.Fatal("expected two slots")
	}
	if b.TryAcquire() {
		t.Fatal("a third slot was handed out beyond capacity")
	}
	b.Release()
	if !b.TryAcquire() {
		t.Fatal("a released slot was not handed out again")
	}
}

func TestBrokerReleaseWakesListenersButReturnDoesNot(t *testing.T) {
	b := NewBroker(1)
	var woken atomic.Int32
	b.OnRelease(func() { woken.Add(1) })

	b.TryAcquire()
	b.Return()
	time.Sleep(20 * time.Millisecond)
	if woken.Load() != 0 {
		t.Fatal("Return woke a listener")
	}

	b.TryAcquire()
	b.Release()
	waitFor(t, func() bool { return woken.Load() == 1 })
}

func TestBrokerWhenIdleOnlyRunsWithoutRunners(t *testing.T) {
	b := NewBroker(2)
	b.Adopt()
	if b.WhenIdle(func() { t.Fatal("ran while a runner was alive") }) {
		t.Fatal("WhenIdle reported it ran")
	}
	b.Release()

	ran := b.WhenIdle(func() {
		if b.TryAcquire() {
			t.Error("a slot was handed out during maintenance")
		}
	})
	if !ran {
		t.Fatal("WhenIdle did not run on an idle broker")
	}
	if !b.TryAcquire() {
		t.Fatal("slots stayed blocked after maintenance")
	}
}

func TestBrokerDrainHoldsSlotsUntilMaintenanceRan(t *testing.T) {
	b := NewBroker(3)
	b.Drain()
	if b.TryAcquire() {
		t.Fatal("a slot was handed out while draining")
	}
	b.WhenIdle(func() {})
	if !b.TryAcquire() {
		t.Fatal("draining did not end after maintenance")
	}
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
