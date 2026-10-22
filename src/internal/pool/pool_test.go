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

// wakeCounter is a waiter's wake-up function that counts its calls.
type wakeCounter struct{ n atomic.Int32 }

func (c *wakeCounter) wake()        { c.n.Add(1) }
func (c *wakeCounter) count() int32 { return c.n.Load() }

func TestFreedSlotGoesToTheWaiterThatWaitedLongest(t *testing.T) {
	s := New(1)
	var aWoken, bWoken wakeCounter
	a, b := s.Join(aWoken.wake), s.Join(bWoken.wake)
	s.TryAcquire()

	a.Want(1)
	b.Want(1)
	if a.TryAcquire() || b.TryAcquire() {
		t.Fatal("a slot was handed out beyond capacity")
	}

	s.Release()
	waitFor(t, func() bool { return aWoken.count() == 1 })
	time.Sleep(20 * time.Millisecond)
	if bWoken.count() != 0 {
		t.Fatal("the release woke a waiter that is not first in line")
	}
	if b.TryAcquire() {
		t.Fatal("the later waiter took the slot")
	}
	if !a.TryAcquire() {
		t.Fatal("the waiter first in line did not get the slot")
	}
	if s.Waiting() != 1 {
		t.Fatalf("expected only b to be waiting, got %d places", s.Waiting())
	}

	s.Release()
	waitFor(t, func() bool { return bWoken.count() == 1 })
	if !b.TryAcquire() {
		t.Fatal("the next waiter did not get its turn")
	}
}

func TestOrderHoldsPerRunnerNotPerWaiter(t *testing.T) {
	s := New(1)
	a, b := s.Join(nil), s.Join(nil)
	s.TryAcquire()
	a.Want(1)
	b.Want(1)
	a.Want(2) // a's second job arrived after b's

	order := ""
	for range 3 {
		s.Release()
		switch {
		case a.TryAcquire():
			order += "a"
		case b.TryAcquire():
			order += "b"
		}
	}
	if order != "aba" {
		t.Fatalf("slots went in order %q, want aba", order)
	}
}

func TestASingleWaiterGetsEverySlot(t *testing.T) {
	s := New(3)
	a := s.Join(nil)
	a.Want(5)
	got := 0
	for a.TryAcquire() {
		got++
	}
	if got != 3 || s.InUse() != 3 || s.Waiting() != 2 {
		t.Fatalf("got %d, in use %d, waiting %d", got, s.InUse(), s.Waiting())
	}
}

func TestFreeSlotsAreSharedInQueueOrder(t *testing.T) {
	s := New(2)
	a, b := s.Join(nil), s.Join(nil)
	a.Want(1)
	b.Want(1)
	if !b.TryAcquire() {
		t.Fatal("a second free slot was kept from the second waiter")
	}
	if !a.TryAcquire() {
		t.Fatal("the first waiter lost its slot")
	}
}

func TestAWaiterThatNoLongerWantsASlotStopsBlockingTheQueue(t *testing.T) {
	s := New(1)
	var bWoken wakeCounter
	a, b := s.Join(nil), s.Join(bWoken.wake)
	s.TryAcquire()
	a.Want(1)
	b.Want(1)
	s.Release()
	if b.TryAcquire() {
		t.Fatal("b jumped the queue")
	}

	a.Want(0)
	waitFor(t, func() bool { return bWoken.count() == 1 })
	if !b.TryAcquire() {
		t.Fatal("a waiter that left the queue still blocked it")
	}
}

func TestWantingFewerKeepsTheOldestPlaces(t *testing.T) {
	s := New(1)
	a, b := s.Join(nil), s.Join(nil)
	s.TryAcquire()
	a.Want(1)
	b.Want(1)
	a.Want(2)
	a.Want(1) // gives up the place behind b, keeps the one before it
	s.Release()
	if !a.TryAcquire() {
		t.Fatal("a lost its oldest place")
	}
}

func TestRequeueMovesAFailingWaiterBackWithoutWakingAnyone(t *testing.T) {
	s := New(1)
	var bWoken wakeCounter
	a, b := s.Join(nil), s.Join(bWoken.wake)
	a.Want(1)
	b.Want(1)
	if !a.TryAcquire() {
		t.Fatal("a did not get the free slot")
	}
	// The start failed.
	s.Return()
	a.Requeue(1)
	time.Sleep(20 * time.Millisecond)
	if bWoken.count() != 0 {
		t.Fatal("Requeue woke a waiter")
	}
	if a.TryAcquire() {
		t.Fatal("the failing waiter kept its turn")
	}
	if !b.TryAcquire() {
		t.Fatal("the next waiter did not get its turn")
	}
}

func TestAcquiringOutsideTheQueueWaitsForTheQueue(t *testing.T) {
	s := New(2)
	s.Join(nil).Want(2)
	if s.TryAcquire() {
		t.Fatal("an acquisition outside the queue jumped it")
	}
}

func TestMaintenanceGetsItsIdleMomentWhileWaitersQueue(t *testing.T) {
	s := New(2)
	var woken wakeCounter
	a := s.Join(woken.wake)
	s.TryAcquire()
	s.Drain()
	a.Want(3)
	if a.TryAcquire() {
		t.Fatal("a slot was handed out while draining")
	}

	s.Release()
	ran := s.WhenIdle(func() {
		if a.TryAcquire() {
			t.Error("a slot was handed out during maintenance")
		}
	})
	if !ran {
		t.Fatal("maintenance did not get its idle moment")
	}
	waitFor(t, func() bool { return woken.count() >= 1 })
	if !a.TryAcquire() || !a.TryAcquire() {
		t.Fatal("waiters were not served after maintenance")
	}
}
