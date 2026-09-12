// Package actions talks to GitHub: the credentials, registering a runner
// scale set, and listening for the jobs GitHub assigns to it.
package actions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

const (
	minBackoff = 5 * time.Second
	maxBackoff = 2 * time.Minute
	// stableRun is how long a listener must have run for its next failure to
	// start over at the minimum back-off.
	stableRun = 5 * time.Minute
)

// Build identifies this daemon to GitHub; it shows up in the scale set's
// client information.
type Build struct {
	Version string
	Commit  string
}

// Credentials authenticate against GitHub: a personal access token, or a
// GitHub App.
type Credentials struct {
	Token string
	App   scaleset.GitHubAppAuth
}

// CredentialsFromEnv reads GITHUB_TOKEN, or GITHUB_APP_CLIENT_ID,
// GITHUB_APP_INSTALLATION_ID and GITHUB_APP_PRIVATE_KEY (or
// GITHUB_APP_PRIVATE_KEY_FILE).
func CredentialsFromEnv() (Credentials, error) {
	clientID := os.Getenv("GITHUB_APP_CLIENT_ID")
	if clientID == "" {
		token := os.Getenv("GITHUB_TOKEN")
		if token == "" {
			return Credentials{}, errors.New("set GITHUB_TOKEN, or GITHUB_APP_CLIENT_ID with GITHUB_APP_INSTALLATION_ID and GITHUB_APP_PRIVATE_KEY")
		}
		return Credentials{Token: token}, nil
	}

	installationID, err := strconv.ParseInt(os.Getenv("GITHUB_APP_INSTALLATION_ID"), 10, 64)
	if err != nil {
		return Credentials{}, errors.New("GITHUB_APP_INSTALLATION_ID must be a number")
	}
	key := os.Getenv("GITHUB_APP_PRIVATE_KEY")
	if file := os.Getenv("GITHUB_APP_PRIVATE_KEY_FILE"); key == "" && file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return Credentials{}, fmt.Errorf("reading GITHUB_APP_PRIVATE_KEY_FILE: %w", err)
		}
		key = string(data)
	}
	app := scaleset.GitHubAppAuth{ClientID: clientID, InstallationID: installationID, PrivateKey: key}
	if err := app.Validate(); err != nil {
		return Credentials{}, fmt.Errorf("invalid GitHub App credentials: %w", err)
	}
	return Credentials{App: app}, nil
}

// ScaleSet is one scale set to register and listen on.
type ScaleSet struct {
	Name        string
	URL         string
	Labels      []string
	MaxRunners  int
	RunnerGroup string
}

// API is the part of the scale set API that registering needs;
// *scaleset.Client implements it.
type API interface {
	GetRunnerGroupByName(ctx context.Context, name string) (*scaleset.RunnerGroup, error)
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, name string) (*scaleset.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
	UpdateRunnerScaleSet(ctx context.Context, id int, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
}

// Scaler is what receives the job messages of one scale set.
type Scaler interface {
	listener.Scaler
	SetScaleSetID(id int)
}

// NewClient returns a client for the repository, organization or enterprise
// this scale set belongs to.
func NewClient(set ScaleSet, creds Credentials, build Build) (*scaleset.Client, error) {
	if creds.App.ClientID != "" {
		return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
			GitHubConfigURL: set.URL,
			GitHubAppAuth:   creds.App,
			SystemInfo:      build.systemInfo(0),
		})
	}
	return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     set.URL,
		PersonalAccessToken: creds.Token,
		SystemInfo:          build.systemInfo(0),
	})
}

// EnsureScaleSet finds the scale set by name or creates it, and brings its
// labels and settings in line with the configuration. It never deletes one:
// workflows target it by label, and a scale set that survives restarts keeps
// queued jobs waiting for it.
func EnsureScaleSet(ctx context.Context, api API, set ScaleSet) (*scaleset.RunnerScaleSet, error) {
	groupID := 1
	if !strings.EqualFold(set.RunnerGroup, scaleset.DefaultRunnerGroup) {
		group, err := api.GetRunnerGroupByName(ctx, set.RunnerGroup)
		if err != nil {
			return nil, fmt.Errorf("looking up runner group %q: %w", set.RunnerGroup, err)
		}
		groupID = group.ID
	}

	want := &scaleset.RunnerScaleSet{
		Name:          set.Name,
		RunnerGroupID: groupID,
		Labels:        labelsFor(set.Labels),
		// One job, one container: updating itself is the image's job, not the
		// runner's.
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	}

	existing, err := api.GetRunnerScaleSet(ctx, groupID, set.Name)
	if err != nil {
		return nil, fmt.Errorf("looking up the scale set: %w", err)
	}
	if existing == nil {
		created, err := api.CreateRunnerScaleSet(ctx, want)
		if err != nil {
			return nil, fmt.Errorf("creating the scale set: %w", err)
		}
		return created, nil
	}
	if sameLabels(existing.Labels, want.Labels) && existing.RunnerSetting.DisableUpdate {
		return existing, nil
	}
	updated, err := api.UpdateRunnerScaleSet(ctx, existing.ID, want)
	if err != nil {
		return nil, fmt.Errorf("updating the scale set: %w", err)
	}
	return updated, nil
}

// Listen registers the scale set and handles its job messages until ctx is
// cancelled, reconnecting with back-off after any failure.
func Listen(ctx context.Context, client *scaleset.Client, set ScaleSet, scaler Scaler, owner string, build Build, log *slog.Logger) {
	backoff := minBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := listenOnce(ctx, client, set, scaler, owner, build, log)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > stableRun {
			backoff = minBackoff
		}
		log.Warn("Listener stopped, reconnecting", slog.String("error", fmt.Sprint(err)), slog.Duration("in", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func listenOnce(ctx context.Context, client *scaleset.Client, set ScaleSet, scaler Scaler, owner string, build Build, log *slog.Logger) error {
	registered, err := EnsureScaleSet(ctx, client, set)
	if err != nil {
		return err
	}
	client.SetSystemInfo(build.systemInfo(registered.ID))
	scaler.SetScaleSetID(registered.ID)

	session, err := client.MessageSessionClient(ctx, registered.ID, owner)
	if err != nil {
		return fmt.Errorf("opening a message session: %w", err)
	}
	defer func() { _ = session.Close(context.WithoutCancel(ctx)) }()

	l, err := listener.New(session, listener.Config{
		ScaleSetID: registered.ID,
		MaxRunners: set.MaxRunners,
		Logger:     log.WithGroup("listener"),
	})
	if err != nil {
		return err
	}
	log.Info("Listening for jobs",
		slog.Int("scaleSetID", registered.ID),
		slog.Any("labels", set.Labels),
		slog.Int("maxRunners", set.MaxRunners),
	)
	return l.Run(ctx, scaler)
}

func (b Build) systemInfo(scaleSetID int) scaleset.SystemInfo {
	return scaleset.SystemInfo{
		System:     "gha-docker-scaleset",
		Subsystem:  "gha-docker-scaleset",
		Version:    b.Version,
		CommitSHA:  b.Commit,
		ScaleSetID: scaleSetID,
	}
}

func labelsFor(names []string) []scaleset.Label {
	labels := make([]scaleset.Label, 0, len(names))
	for _, n := range names {
		labels = append(labels, scaleset.Label{Name: strings.TrimSpace(n)})
	}
	return labels
}

func sameLabels(a, b []scaleset.Label) bool {
	names := func(labels []scaleset.Label) []string {
		out := make([]string, 0, len(labels))
		for _, l := range labels {
			out = append(out, strings.ToLower(l.Name))
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(names(a), names(b))
}
