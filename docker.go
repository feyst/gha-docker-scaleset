package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	labelManaged  = "io.github.gha-docker-scaleset.managed"
	labelScaleSet = "io.github.gha-docker-scaleset.scale-set"
	syncTimeout   = 5 * time.Minute
)

// RunnerContainer is a runner container found on the Docker host.
type RunnerContainer struct {
	ID       string
	Name     string
	ScaleSet string
	Running  bool
}

// DockerRuntime runs runners as containers on the local Docker daemon.
type DockerRuntime struct {
	cli *dockerclient.Client
	cfg RunnerConfig
}

// NewDockerRuntime connects to the Docker daemon from the environment
// (DOCKER_HOST, or the default socket).
func NewDockerRuntime(cfg RunnerConfig) (*DockerRuntime, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connecting to Docker: %w", err)
	}
	return &DockerRuntime{cli: cli, cfg: cfg}, nil
}

// StartRunner creates and starts one runner container.
func (d *DockerRuntime) StartRunner(ctx context.Context, spec RunnerSpec) (string, error) {
	binds := slices.Clone(d.cfg.Mounts)
	binds = append(binds, spec.WorkDir+":"+spec.WorkDir)
	if d.cfg.ExternalsDir != "" {
		binds = append(binds, d.cfg.ExternalsDir+":"+d.cfg.ExternalsDir+":ro")
	}

	env := []string{"ACTIONS_RUNNER_INPUT_JITCONFIG=" + spec.JITConfig}
	for _, k := range slices.Sorted(maps.Keys(d.cfg.Env)) {
		env = append(env, k+"="+d.cfg.Env[k])
	}

	init := true
	created, err := d.cli.ContainerCreate(ctx,
		&container.Config{
			Image:      spec.ImageID,
			Entrypoint: d.cfg.Command,
			Cmd:        []string{},
			Env:        env,
			Labels: map[string]string{
				labelManaged:  "true",
				labelScaleSet: spec.ScaleSet,
			},
		},
		&container.HostConfig{
			Binds:         binds,
			Init:          &init,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
		},
		nil, nil, spec.Name,
	)
	if err != nil {
		return "", err
	}
	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = d.RemoveContainer(context.WithoutCancel(ctx), created.ID)
		return "", err
	}
	return created.ID, nil
}

// RemoveContainer force-removes a container; a container that is already
// gone is not an error.
func (d *DockerRuntime) RemoveContainer(ctx context.Context, id string) error {
	err := d.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true, RemoveVolumes: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// InspectRunner reports whether a container still exists and runs.
func (d *DockerRuntime) InspectRunner(ctx context.Context, id string) (RunnerState, error) {
	info, err := d.cli.ContainerInspect(ctx, id)
	if cerrdefs.IsNotFound(err) {
		return RunnerState{}, nil
	}
	if err != nil {
		return RunnerState{}, err
	}
	st := RunnerState{Exists: true}
	if info.State != nil {
		st.Running = info.State.Running
		st.ExitCode = info.State.ExitCode
		st.FinishedAt, _ = time.Parse(time.RFC3339Nano, info.State.FinishedAt)
	}
	return st, nil
}

// RunnerLogs returns the last lines of a container's output.
func (d *DockerRuntime) RunnerLogs(ctx context.Context, id string, lines int) string {
	rc, err := d.cli.ContainerLogs(ctx, id, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: fmt.Sprint(lines)})
	if err != nil {
		return ""
	}
	defer rc.Close()
	var buf bytes.Buffer
	_, _ = stdcopy.StdCopy(&buf, &buf, rc)
	return strings.TrimSpace(buf.String())
}

// ListRunners returns every runner container this daemon manages.
func (d *DockerRuntime) ListRunners(ctx context.Context) ([]RunnerContainer, error) {
	list, err := d.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", labelManaged+"=true")),
	})
	if err != nil {
		return nil, err
	}
	out := make([]RunnerContainer, 0, len(list))
	for _, c := range list {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		out = append(out, RunnerContainer{
			ID:       c.ID,
			Name:     name,
			ScaleSet: c.Labels[labelScaleSet],
			Running:  c.State == container.StateRunning,
		})
	}
	return out, nil
}

// PullImage pulls ref from its registry.
func (d *DockerRuntime) PullImage(ctx context.Context, ref string) error {
	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(io.Discard, rc)
	return err
}

// ImageID resolves ref to the ID of the local image, or "" if there is none.
func (d *DockerRuntime) ImageID(ctx context.Context, ref string) (string, error) {
	info, err := d.cli.ImageInspect(ctx, ref)
	if cerrdefs.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

// HasImage reports whether an image with this ID is still present locally.
func (d *DockerRuntime) HasImage(ctx context.Context, id string) (bool, error) {
	found, err := d.ImageID(ctx, id)
	return found != "", err
}

// SyncExternals copies the externals directory out of the image to the same
// path on the host, via a short-lived container from that image, and stamps
// the copy with the image ID. The copy lands next to the old directory and
// replaces it in two renames.
func (d *DockerRuntime) SyncExternals(ctx context.Context, imageID, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	parent, base := filepath.Dir(dir), filepath.Base(dir)
	script := fmt.Sprintf(`set -e
rm -rf "/out/%[1]s.new" "/out/%[1]s.old"
mkdir -p "/out/%[1]s.new"
cp -a "%[2]s/." "/out/%[1]s.new/"
printf '%%s' '%[3]s' > "/out/%[1]s.new/%[4]s"
if [ -e "/out/%[1]s" ]; then mv "/out/%[1]s" "/out/%[1]s.old"; fi
mv "/out/%[1]s.new" "/out/%[1]s"
rm -rf "/out/%[1]s.old"
`, base, dir, imageID, stampFile)

	created, err := d.cli.ContainerCreate(ctx,
		&container.Config{Image: imageID, User: "0", Entrypoint: []string{"sh", "-c", script}, Cmd: []string{}},
		&container.HostConfig{Binds: []string{parent + ":/out"}},
		nil, nil, "",
	)
	if err != nil {
		return fmt.Errorf("creating the copy container: %w", err)
	}
	defer func() { _ = d.RemoveContainer(context.WithoutCancel(ctx), created.ID) }()

	waitCh, errCh := d.cli.ContainerWait(ctx, created.ID, container.WaitConditionNextExit)
	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting the copy container: %w", err)
	}
	select {
	case res := <-waitCh:
		if res.StatusCode != 0 {
			return fmt.Errorf("copying externals exited with %d: %s", res.StatusCode, d.RunnerLogs(context.WithoutCancel(ctx), created.ID, logTail))
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("waiting for the copy container: %w", err)
	}
}
