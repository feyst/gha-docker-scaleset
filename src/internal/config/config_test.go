package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	cfg, err := Parse([]byte(`
max_runners: 4
scale_sets:
  - name: api
    url: https://github.com/owner/repo
`))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.ScaleSets[0]
	if s.MaxRunners != 4 || s.RunnerGroup != "default" || len(s.Labels) != 1 || s.Labels[0] != "api" {
		t.Fatalf("unexpected defaults: %+v", s)
	}
	if cfg.Runner.ImageRefresh != 24*time.Hour || cfg.Runner.Image == "" || len(cfg.Runner.Command) == 0 {
		t.Fatalf("unexpected runner defaults: %+v", cfg.Runner)
	}
}

func TestParsesDurationsAndLists(t *testing.T) {
	cfg, err := Parse([]byte(`
max_runners: 6
runner:
  image: myoung34/github-runner:latest
  command: [/actions-runner/run.sh]
  env: {RUNNER_ALLOW_RUNASROOT: "1"}
  mounts: [/var/run/docker.sock:/var/run/docker.sock]
  work_dir: /var/lib/gha-work/scaleset
  externals_dir: /actions-runner/externals
  image_refresh: 12h
scale_sets:
  - name: pc2-sporten-api
    url: https://github.com/feyst/sporten-api
    labels: [self-hosted, Linux, X64]
    max_runners: 4
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Runner.ImageRefresh != 12*time.Hour || cfg.ScaleSets[0].Labels[2] != "X64" {
		t.Fatalf("unexpected: %+v", cfg)
	}
}

func TestRejectsMistakes(t *testing.T) {
	cases := map[string]string{
		"unknown field":  "max_runners: 1\nmax_runner: 2\nscale_sets: [{name: a, url: https://github.com/o/r}]",
		"no scale sets":  "max_runners: 1",
		"zero cap":       "max_runners: 0\nscale_sets: [{name: a, url: https://github.com/o/r}]",
		"http url":       "max_runners: 1\nscale_sets: [{name: a, url: http://github.com/o/r}]",
		"duplicate":      "max_runners: 1\nscale_sets: [{name: a, url: https://github.com/o/r}, {name: a, url: https://github.com/o/s}]",
		"bad name":       "max_runners: 1\nscale_sets: [{name: 'a b', url: https://github.com/o/r}]",
		"relative dir":   "max_runners: 1\nrunner: {work_dir: work}\nscale_sets: [{name: a, url: https://github.com/o/r}]",
		"bad mount":      "max_runners: 1\nrunner: {mounts: [docker.sock]}\nscale_sets: [{name: a, url: https://github.com/o/r}]",
		"root externals": "max_runners: 1\nrunner: {externals_dir: /externals}\nscale_sets: [{name: a, url: https://github.com/o/r}]",
		"empty label":    "max_runners: 1\nscale_sets: [{name: a, url: https://github.com/o/r, labels: ['']}]",
	}
	for name, yml := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(yml)); err == nil {
				t.Fatalf("expected an error for %s", strings.TrimSpace(name))
			}
		})
	}
}
