// Package docker runs runner containers on a Docker daemon. It is the only
// package that knows Docker exists; everything above it works with the shapes
// from the runner package.
package docker

import (
	"fmt"

	dockerclient "github.com/docker/docker/client"
)

const (
	// LabelManaged marks every container this daemon created, so a restart can
	// find them back.
	LabelManaged = "io.github.gha-docker-scaleset.managed"
	// LabelScaleSet records which scale set a runner belongs to.
	LabelScaleSet = "io.github.gha-docker-scaleset.scale-set"

	logTail = 20
)

// Options are the parts of a runner container that are the same for every
// runner.
type Options struct {
	// Command starts the runner inside the image.
	Command []string
	Env     map[string]string
	// Mounts are extra "host:container[:options]" binds.
	Mounts []string
	// ExternalsDir is mounted read-only into every runner when set.
	ExternalsDir string
}

// Runtime starts, inspects and removes runner containers.
type Runtime struct {
	cli  *dockerclient.Client
	opts Options
}

// New connects to the Docker daemon from the environment (DOCKER_HOST, or the
// default socket).
func New(opts Options) (*Runtime, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connecting to Docker: %w", err)
	}
	return &Runtime{cli: cli, opts: opts}, nil
}
