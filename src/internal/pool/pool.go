// Package pool shares a fixed number of runner slots between everything that
// wants to start runners, and creates the quiet moments maintenance needs.
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
}

// New returns a pool with the given number of slots.
func New(capacity int) *Slots {
	return &Slots{capacity: capacity}
}

// OnRelease registers fn to run, in its own goroutine, whenever a slot frees
// up. It is how a scale set that hit the cap learns it may try again.
func (s *Slots) OnRelease(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, fn)
}

// TryAcquire takes a slot if one is free and no maintenance is pending.
func (s *Slots) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exclusive || s.draining || s.used >= s.capacity {
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

// Release frees the slot of a runner that is gone and wakes up every listener.
func (s *Slots) Release() {
	s.mu.Lock()
	if s.used > 0 {
		s.used--
	}
	s.mu.Unlock()
	s.notify()
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
		s.mu.Unlock()
		s.notify()
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

func (s *Slots) notify() {
	s.mu.Lock()
	listeners := slices.Clone(s.listeners)
	s.mu.Unlock()
	for _, fn := range listeners {
		go fn()
	}
}
