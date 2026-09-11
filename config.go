package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/actions/scaleset"
	"gopkg.in/yaml.v3"
)

const defaultRunnerGroup = scaleset.DefaultRunnerGroup

// Config is the file-based configuration of the daemon.
type Config struct {
	// MaxRunners caps the number of runner containers across all scale sets.
	MaxRunners int              `yaml:"max_runners"`
	Runner     RunnerConfig     `yaml:"runner"`
	ScaleSets  []ScaleSetConfig `yaml:"scale_sets"`
}

// RunnerConfig describes the container every runner runs in.
type RunnerConfig struct {
	Image   string            `yaml:"image"`
	Command []string          `yaml:"command"`
	Env     map[string]string `yaml:"env"`
	// Mounts are extra "host:container[:opts]" binds for every runner.
	Mounts []string `yaml:"mounts"`
	// WorkDir is the host directory under which each runner gets its own work
	// directory, mounted at the same path inside the runner. The same path is
	// required because jobs using `container:` or `services:` are started on
	// the host's Docker daemon, which resolves bind mounts on the host.
	WorkDir string `yaml:"work_dir"`
	// ExternalsDir is the runner's externals directory (the node binaries JS
	// actions run on) inside the image. When set, it is copied to the same
	// path on the host and mounted into every runner, for the same reason as
	// WorkDir. Empty disables this.
	ExternalsDir string `yaml:"externals_dir"`
	// ImageRefresh is how often the runner image is pulled.
	ImageRefresh time.Duration `yaml:"image_refresh"`
}

// ScaleSetConfig is one runner scale set, registered on a repository,
// organization or enterprise.
type ScaleSetConfig struct {
	Name        string   `yaml:"name"`
	URL         string   `yaml:"url"`
	Labels      []string `yaml:"labels"`
	MaxRunners  int      `yaml:"max_runners"`
	RunnerGroup string   `yaml:"runner_group"`
}

var (
	scaleSetNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$`)
	safePathPattern     = regexp.MustCompile(`^/[A-Za-z0-9._/-]+$`)
)

// LoadConfig reads and validates the configuration file at path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	return ParseConfig(data)
}

// ParseConfig parses, defaults and validates a YAML configuration.
func ParseConfig(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	r := &c.Runner
	if r.Image == "" {
		r.Image = "ghcr.io/actions/actions-runner:latest"
	}
	if len(r.Command) == 0 {
		r.Command = []string{"/home/runner/run.sh"}
	}
	if r.WorkDir == "" {
		r.WorkDir = "/var/lib/gha-docker-scaleset/work"
	}
	if r.ImageRefresh == 0 {
		r.ImageRefresh = 24 * time.Hour
	}
	for i := range c.ScaleSets {
		s := &c.ScaleSets[i]
		if s.MaxRunners == 0 {
			s.MaxRunners = c.MaxRunners
		}
		if s.RunnerGroup == "" {
			s.RunnerGroup = defaultRunnerGroup
		}
		if len(s.Labels) == 0 {
			s.Labels = []string{s.Name}
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.MaxRunners < 1 {
		errs = append(errs, errors.New("max_runners must be at least 1"))
	}
	if len(c.ScaleSets) == 0 {
		errs = append(errs, errors.New("at least one scale set is required"))
	}

	r := c.Runner
	if !safePathPattern.MatchString(r.WorkDir) {
		errs = append(errs, fmt.Errorf("runner.work_dir %q must be an absolute path", r.WorkDir))
	}
	if r.ExternalsDir != "" && (!safePathPattern.MatchString(r.ExternalsDir) || filepath.Dir(r.ExternalsDir) == "/") {
		errs = append(errs, fmt.Errorf("runner.externals_dir %q must be an absolute path below a directory other than /", r.ExternalsDir))
	}
	for _, m := range r.Mounts {
		parts := strings.Split(m, ":")
		if len(parts) < 2 || len(parts) > 3 || !filepath.IsAbs(parts[0]) || !filepath.IsAbs(parts[1]) {
			errs = append(errs, fmt.Errorf("runner.mounts entry %q must be host-path:container-path[:options]", m))
		}
	}
	if r.ImageRefresh < time.Minute {
		errs = append(errs, errors.New("runner.image_refresh must be at least 1m"))
	}

	seen := make(map[string]bool)
	for i, s := range c.ScaleSets {
		where := fmt.Sprintf("scale_sets[%d]", i)
		if !scaleSetNamePattern.MatchString(s.Name) {
			errs = append(errs, fmt.Errorf("%s.name %q must be 1-40 letters, digits, dots, dashes or underscores", where, s.Name))
		}
		if seen[s.Name] {
			errs = append(errs, fmt.Errorf("%s.name %q is used twice", where, s.Name))
		}
		seen[s.Name] = true
		if u, err := url.Parse(s.URL); err != nil || u.Scheme != "https" || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s.url %q must be an https URL of a repository, organization or enterprise", where, s.URL))
		}
		if s.MaxRunners < 1 {
			errs = append(errs, fmt.Errorf("%s.max_runners must be at least 1", where))
		}
		for _, l := range s.Labels {
			if strings.TrimSpace(l) == "" {
				errs = append(errs, fmt.Errorf("%s.labels contains an empty label", where))
			}
		}
	}
	return errors.Join(errs...)
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
