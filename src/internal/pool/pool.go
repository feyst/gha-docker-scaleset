// Package pool shares a fixed number of runner slots between everything that
// wants to start runners, and creates the quiet moments maintenance needs.
//
// Slots are handed out first come, first served: every runner a scale set is
// waiting for is a place in one queue, in the order the need arose. A free
// slot goes to the oldest place in that queue, whichever scale set it belongs
// to, so a busy repository cannot keep snatching slots from one that has been
// waiting longer. With only one scale set waiting, it gets every free slot.
package pool

import (
	"slices"
	"sync"
)

// Slots hands out runner slots under one capacity shared by every scale set.
type Slots struct {
	mu        sync.Mutex
	capacity  int
	used      int
	exclusive bool
	draining  bool
	listeners []func()
	// queue holds one entry per runner a waiter still wants, oldest first.
	queue []*Waiter
}

// Waiter is one party in the queue, typically one scale set. Each slot it
// wants is a separate place in the queue, so the order holds per job.
type Waiter struct {
	slots *Slots
	wake  func()
}

// New returns a pool with the given number of slots.
func New(capacity int) *Slots {
	return &Slots{capacity: capacity}
}

// Join returns a new waiter. wake runs, in its own goroutine, when a slot has
// become available for this waiter in particular; it should call Want and
// TryAcquire again.
func (s *Slots) Join(wake func()) *Waiter {
	return &Waiter{slots: s, wake: wake}
}

// OnRelease registers fn to run, in its own goroutine, whenever a slot frees
// up or maintenance ends, regardless of who is waiting. It is meant for
// observers such as the image manager; scale sets use Join.
func (s *Slots) OnRelease(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, fn)
}

// TryAcquire takes a slot for someone outside the queue. It only succeeds if
// a slot is left after everyone in the queue has been served.
func (s *Slots) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.freeLocked() <= len(s.queue) {
		return false
	}
	s.used++
	return true
}

// Adopt counts a runner that already existed when the daemon started, even
// beyond capacity: it is alive either way.
func (s *Slots) Adopt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.used++
}

// Return gives back a slot whose runner never started. It wakes nobody up:
// whatever made the start fail would most likely repeat immediately.
func (s *Slots) Return() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used > 0 {
		s.used--
	}
}

// Release frees the slot of a runner that is gone. It wakes the waiters that
// are now first in line for a free slot, and every OnRelease listener.
func (s *Slots) Release() {
	s.mu.Lock()
	if s.used > 0 {
		s.used--
	}
	wake := s.eligibleLocked(nil)
	listeners := slices.Clone(s.listeners)
	s.mu.Unlock()
	run(wake, listeners)
}

// Drain stops handing out slots until the next WhenIdle has run, so a quiet
// moment arrives even while work keeps coming in.
func (s *Slots) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
}

// WhenIdle runs fn if no slot is in use, with new acquisitions refused while
// it runs. It reports whether fn ran.
func (s *Slots) WhenIdle(fn func()) bool {
	s.mu.Lock()
	if s.used > 0 || s.exclusive {
		s.mu.Unlock()
		return false
	}
	s.exclusive = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.exclusive = false
		s.draining = false
		wake := s.eligibleLocked(nil)
		listeners := slices.Clone(s.listeners)
		s.mu.Unlock()
		run(wake, listeners)
	}()
	fn()
	return true
}

// InUse reports how many slots are taken.
func (s *Slots) InUse() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// Waiting reports how many slots are wanted by waiters in the queue.
func (s *Slots) Waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// Want sets how many slots w is waiting for. New places go to the back of the
// queue; when w wants fewer, its newest places are given up, so its oldest
// keep their turn. Waiters that move up to a free slot because of it are woken.
func (w *Waiter) Want(n int) {
	s := w.slots
	s.mu.Lock()
	have := s.countLocked(w)
	for ; have < n; have++ {
		s.queue = append(s.queue, w)
	}
	if have <= n {
		s.mu.Unlock()
		return
	}
	s.dropLocked(w, have-n)
	wake := s.eligibleLocked(w)
	s.mu.Unlock()
	run(wake, nil)
}

// Requeue moves w to the back of the queue with n places, after its start of
// a runner failed, so a scale set that keeps failing does not hold up the
// others. Like Return it wakes nobody: the others get their turn on the next
// release or on their own next attempt.
func (w *Waiter) Requeue(n int) {
	s := w.slots
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropLocked(w, s.countLocked(w))
	for range max(n, 0) {
		s.queue = append(s.queue, w)
	}
}

// TryAcquire takes a slot for w if one is free and w's oldest place in the
// queue is among the first in line for the free slots. That place is then
// used up. A waiter without a place in the queue is treated like
// Slots.TryAcquire.
func (w *Waiter) TryAcquire() bool {
	s := w.slots
	s.mu.Lock()
	i := slices.Index(s.queue, w)
	if i < 0 {
		s.mu.Unlock()
		return s.TryAcquire()
	}
	defer s.mu.Unlock()
	if i >= s.freeLocked() {
		return false
	}
	s.queue = slices.Delete(s.queue, i, i+1)
	s.used++
	return true
}

// Return gives back a slot whose runner never started; see Slots.Return.
func (w *Waiter) Return() { w.slots.Return() }

// Release frees the slot of a runner that is gone; see Slots.Release.
func (w *Waiter) Release() { w.slots.Release() }

// freeLocked is the number of slots that may be handed out right now.
func (s *Slots) freeLocked() int {
	if s.exclusive || s.draining {
		return 0
	}
	return max(s.capacity-s.used, 0)
}

func (s *Slots) countLocked(w *Waiter) int {
	n := 0
	for _, q := range s.queue {
		if q == w {
			n++
		}
	}
	return n
}

// dropLocked removes the newest n places of w.
func (s *Slots) dropLocked(w *Waiter, n int) {
	for i := len(s.queue) - 1; i >= 0 && n > 0; i-- {
		if s.queue[i] == w {
			s.queue = slices.Delete(s.queue, i, i+1)
			n--
		}
	}
}

// eligibleLocked returns the wake-up functions of the waiters, except skip,
// that hold one of the places first in line for the free slots.
func (s *Slots) eligibleLocked(skip *Waiter) []func() {
	var wake []func()
	var seen []*Waiter
	for _, w := range s.queue[:min(s.freeLocked(), len(s.queue))] {
		if w == skip || w.wake == nil || slices.Contains(seen, w) {
			continue
		}
		seen = append(seen, w)
		wake = append(wake, w.wake)
	}
	return wake
}

func run(groups ...[]func()) {
	for _, fns := range groups {
		for _, fn := range fns {
			go fn()
		}
	}
}
