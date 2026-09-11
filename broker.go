package main

import (
	"slices"
	"sync"
)

// Broker hands out runner slots under one cap shared by every scale set, and
// runs maintenance that needs every runner to be gone.
type Broker struct {
	mu        sync.Mutex
	capacity  int
	used      int
	exclusive bool
	draining  bool
	listeners []func()
}

// NewBroker returns a broker with the given number of slots.
func NewBroker(capacity int) *Broker {
	return &Broker{capacity: capacity}
}

// OnRelease registers fn to run, in its own goroutine, whenever a slot frees up.
func (b *Broker) OnRelease(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listeners = append(b.listeners, fn)
}

// TryAcquire takes a slot if one is free and no maintenance is pending.
func (b *Broker) TryAcquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.exclusive || b.draining || b.used >= b.capacity {
		return false
	}
	b.used++
	return true
}

// Adopt counts a runner that already existed when the daemon started, even
// beyond capacity: it is alive either way.
func (b *Broker) Adopt() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used++
}

// Return gives back a slot whose runner never started. It wakes nobody up:
// the failure that caused it would most likely repeat immediately.
func (b *Broker) Return() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used > 0 {
		b.used--
	}
}

// Release frees the slot of a runner that is gone and wakes up every listener.
func (b *Broker) Release() {
	b.mu.Lock()
	if b.used > 0 {
		b.used--
	}
	b.mu.Unlock()
	b.notify()
}

// Drain stops handing out slots until the next WhenIdle has run.
func (b *Broker) Drain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.draining = true
}

// WhenIdle runs fn if no slot is in use, with new acquisitions refused while
// it runs. It reports whether fn ran.
func (b *Broker) WhenIdle(fn func()) bool {
	b.mu.Lock()
	if b.used > 0 || b.exclusive {
		b.mu.Unlock()
		return false
	}
	b.exclusive = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.exclusive = false
		b.draining = false
		b.mu.Unlock()
		b.notify()
	}()
	fn()
	return true
}

// InUse reports how many slots are taken.
func (b *Broker) InUse() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func (b *Broker) notify() {
	b.mu.Lock()
	listeners := slices.Clone(b.listeners)
	b.mu.Unlock()
	for _, fn := range listeners {
		go fn()
	}
}
