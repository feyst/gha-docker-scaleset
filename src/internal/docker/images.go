package docker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
)

const (
	// stampFile records which image the externals on the host were copied
	// from, so a restart knows whether they still match.
	stampFile   = ".image-id"
	syncTimeout = 5 * time.Minute
)

// PullImage pulls ref from its registry.
func (r *Runtime) PullImage(ctx context.Context, ref string) error {
	rc, err := r.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(io.Discard, rc)
	return err
}

// ImageID resolves ref to the ID of the local image, or "" if there is none.
func (r *Runtime) ImageID(ctx context.Context, ref string) (string, error) {
	info, err := r.cli.ImageInspect(ctx, ref)
	if cerrdefs.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

// HasImage reports whether an image with this ID is still present locally.
func (r *Runtime) HasImage(ctx context.Context, id string) (bool, error) {
	found, err := r.ImageID(ctx, id)
	return found != "", err
}

// ExternalsStamp returns the image ID the externals in dir were copied from.
func (r *Runtime) ExternalsStamp(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, stampFile))
	return strings.TrimSpace(string(data)), err
}

// SyncExternals copies the externals out of an image to the same path on the
// host, via a short-lived container from that image, and stamps the copy with
// the image ID. The copy lands next to the old directory and replaces it in
// two renames, so nothing is ever half-replaced.
func (r *Runtime) SyncExternals(ctx context.Context, imageID, dir string) error {
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

	created, err := r.cli.ContainerCreate(ctx,
		&container.Config{Image: imageID, User: "0", Entrypoint: []string{"sh", "-c", script}, Cmd: []string{}},
		&container.HostConfig{Binds: []string{parent + ":/out"}},
		nil, nil, "",
	)
	if err != nil {
		return fmt.Errorf("creating the copy container: %w", err)
	}
	defer func() { _ = r.RemoveContainer(context.WithoutCancel(ctx), created.ID) }()

	waitCh, errCh := r.cli.ContainerWait(ctx, created.ID, container.WaitConditionNextExit)
	if err := r.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting the copy container: %w", err)
	}
	select {
	case res := <-waitCh:
		if res.StatusCode != 0 {
			return fmt.Errorf("copying externals exited with %d: %s", res.StatusCode, r.RunnerLogs(context.WithoutCancel(ctx), created.ID, logTail))
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("waiting for the copy container: %w", err)
	}
}
