// Package runner describes a runner container: what it takes to start one and
// what can be observed about it. The scaler decides about these shapes and the
// container runtime realises them, so neither has to know the other.
package runner

import "time"

// Spec is one runner container to start.
type Spec struct {
	// Name is both the container name and the runner name GitHub reports.
	Name      string
	ScaleSet  string
	ImageID   string
	JITConfig string
	// WorkDir is the host directory the runner works in, mounted at the same
	// path inside the container.
	WorkDir string
}

// State is what the runtime knows about a runner container.
type State struct {
	Exists     bool
	Running    bool
	ExitCode   int
	FinishedAt time.Time
}

// Container is a runner container found on the host, used to take over or
// clean up what a previous run of the daemon left behind.
type Container struct {
	ID       string
	Name     string
	ScaleSet string
	Running  bool
}
