// Command gha-docker-scaleset runs ephemeral GitHub Actions runners as Docker
// containers: one daemon listens for jobs on one or more runner scale sets and
// starts a fresh container per job, under a single cap on the number of
// runners alive at the same time.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/feyst/gha-docker-scaleset/src/internal/actions"
	"github.com/feyst/gha-docker-scaleset/src/internal/config"
	"github.com/feyst/gha-docker-scaleset/src/internal/daemon"
)

// Set through -ldflags at build time.
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	configPath := flag.String("config", "/etc/gha-docker-scaleset/config.yml", "path to the configuration file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version, commit)
		return
	}

	log := newLogger(os.Getenv("LOG_LEVEL"))
	if err := run(*configPath, log); err != nil {
		log.Error("Exiting", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath string, log *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	creds, err := actions.CredentialsFromEnv()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return daemon.Run(ctx, cfg, creds, actions.Build{Version: version, Commit: commit}, log)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
