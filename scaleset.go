package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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

// ScaleSetAPI is what registering a scale set needs; *scaleset.Client implements it.
type ScaleSetAPI interface {
	GetRunnerGroupByName(ctx context.Context, name string) (*scaleset.RunnerGroup, error)
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, name string) (*scaleset.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
	UpdateRunnerScaleSet(ctx context.Context, id int, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
}

func newScaleSetClient(url string, creds Credentials) (*scaleset.Client, error) {
	info := scaleset.SystemInfo{System: "gha-docker-scaleset", Subsystem: "gha-docker-scaleset", Version: version, CommitSHA: commit}
	if creds.App.ClientID != "" {
		return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{GitHubConfigURL: url, GitHubAppAuth: creds.App, SystemInfo: info})
	}
	return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{GitHubConfigURL: url, PersonalAccessToken: creds.Token, SystemInfo: info})
}

// ensureScaleSet finds the scale set by name or creates it, and brings its
// labels and settings in line with the configuration. It never deletes one:
// workflows target it by label, and a scale set that survives restarts keeps
// queued jobs waiting for it.
func ensureScaleSet(ctx context.Context, api ScaleSetAPI, cfg ScaleSetConfig) (*scaleset.RunnerScaleSet, error) {
	groupID := 1
	if !strings.EqualFold(cfg.RunnerGroup, defaultRunnerGroup) {
		group, err := api.GetRunnerGroupByName(ctx, cfg.RunnerGroup)
		if err != nil {
			return nil, fmt.Errorf("looking up runner group %q: %w", cfg.RunnerGroup, err)
		}
		groupID = group.ID
	}

	want := &scaleset.RunnerScaleSet{
		Name:          cfg.Name,
		RunnerGroupID: groupID,
		Labels:        labelsFor(cfg.Labels),
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	}

	existing, err := api.GetRunnerScaleSet(ctx, groupID, cfg.Name)
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

func labelsFor(names []string) []scaleset.Label {
	labels := make([]scaleset.Label, 0, len(names))
	for _, n := range names {
		labels = append(labels, scaleset.Label{Name: strings.TrimSpace(n)})
	}
	return labels
}

func sameLabels(a, b []scaleset.Label) bool {
	names := func(ls []scaleset.Label) []string {
		out := make([]string, 0, len(ls))
		for _, l := range ls {
			out = append(out, strings.ToLower(l.Name))
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(names(a), names(b))
}

// runScaleSet registers the scale set and listens for its jobs until ctx is
// cancelled, reconnecting with back-off after any failure.
func runScaleSet(ctx context.Context, client *scaleset.Client, cfg ScaleSetConfig, scaler *Scaler, owner string, log *slog.Logger) {
	backoff := minBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := listenOnce(ctx, client, cfg, scaler, owner, log)
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

func listenOnce(ctx context.Context, client *scaleset.Client, cfg ScaleSetConfig, scaler *Scaler, owner string, log *slog.Logger) error {
	set, err := ensureScaleSet(ctx, client, cfg)
	if err != nil {
		return err
	}
	client.SetSystemInfo(scaleset.SystemInfo{System: "gha-docker-scaleset", Subsystem: "gha-docker-scaleset", Version: version, CommitSHA: commit, ScaleSetID: set.ID})
	scaler.SetScaleSetID(set.ID)

	session, err := client.MessageSessionClient(ctx, set.ID, owner)
	if err != nil {
		return fmt.Errorf("opening a message session: %w", err)
	}
	defer func() { _ = session.Close(context.WithoutCancel(ctx)) }()

	l, err := listener.New(session, listener.Config{ScaleSetID: set.ID, MaxRunners: cfg.MaxRunners, Logger: log.WithGroup("listener")})
	if err != nil {
		return err
	}
	log.Info("Listening for jobs", slog.Int("scaleSetID", set.ID), slog.Any("labels", cfg.Labels), slog.Int("maxRunners", cfg.MaxRunners))
	return l.Run(ctx, scaler)
}
