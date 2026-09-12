package docker

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/feyst/gha-docker-scaleset/src/internal/runner"
)

// StartRunner creates and starts one runner container.
func (r *Runtime) StartRunner(ctx context.Context, spec runner.Spec) (string, error) {
	binds := slices.Clone(r.opts.Mounts)
	// The work directory sits at the same path inside and outside, because a
	// job container is started on the host's daemon with this path.
	binds = append(binds, spec.WorkDir+":"+spec.WorkDir)
	if r.opts.ExternalsDir != "" {
		binds = append(binds, r.opts.ExternalsDir+":"+r.opts.ExternalsDir+":ro")
	}

	env := []string{"ACTIONS_RUNNER_INPUT_JITCONFIG=" + spec.JITConfig}
	for _, k := range slices.Sorted(maps.Keys(r.opts.Env)) {
		env = append(env, k+"="+r.opts.Env[k])
	}

	init := true
	created, err := r.cli.ContainerCreate(ctx,
		&container.Config{
			Image:      spec.ImageID,
			Entrypoint: r.opts.Command,
			Cmd:        []string{},
			Env:        env,
			Labels: map[string]string{
				LabelManaged:  "true",
				LabelScaleSet: spec.ScaleSet,
			},
		},
		&container.HostConfig{
			Binds: binds,
			Init:  &init,
			// One job, one container: a restart would re-run a runner whose
			// registration is already spent.
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
		},
		nil, nil, spec.Name,
	)
	if err != nil {
		return "", err
	}
	if err := r.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = r.RemoveContainer(context.WithoutCancel(ctx), created.ID)
		return "", err
	}
	return created.ID, nil
}

// RemoveContainer force-removes a container; one that is already gone is not
// an error.
func (r *Runtime) RemoveContainer(ctx context.Context, id string) error {
	err := r.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true, RemoveVolumes: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// InspectRunner reports whether a container still exists and runs.
func (r *Runtime) InspectRunner(ctx context.Context, id string) (runner.State, error) {
	info, err := r.cli.ContainerInspect(ctx, id)
	if cerrdefs.IsNotFound(err) {
		return runner.State{}, nil
	}
	if err != nil {
		return runner.State{}, err
	}
	state := runner.State{Exists: true}
	if info.State != nil {
		state.Running = info.State.Running
		state.ExitCode = info.State.ExitCode
		state.FinishedAt, _ = time.Parse(time.RFC3339Nano, info.State.FinishedAt)
	}
	return state, nil
}

// RunnerLogs returns the last lines of a container's output, for the log line
// that explains why a runner died.
func (r *Runtime) RunnerLogs(ctx context.Context, id string, lines int) string {
	rc, err := r.cli.ContainerLogs(ctx, id, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: fmt.Sprint(lines)})
	if err != nil {
		return ""
	}
	defer rc.Close()
	var buf bytes.Buffer
	_, _ = stdcopy.StdCopy(&buf, &buf, rc)
	return strings.TrimSpace(buf.String())
}

// ListRunners returns every runner container this daemon manages, running or
// not.
func (r *Runtime) ListRunners(ctx context.Context) ([]runner.Container, error) {
	list, err := r.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", LabelManaged+"=true")),
	})
	if err != nil {
		return nil, err
	}
	out := make([]runner.Container, 0, len(list))
	for _, c := range list {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		out = append(out, runner.Container{
			ID:       c.ID,
			Name:     name,
			ScaleSet: c.Labels[LabelScaleSet],
			Running:  c.State == container.StateRunning,
		})
	}
	return out, nil
}
