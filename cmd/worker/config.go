package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// Defaults for the flags below.
//
// DefaultAuthConcurrency is evidence-derived, not a guess (Phase 4.6; see
// docs/distributed-roadmap.md's Phase 4.6 section for the full
// measurement). A live worker process, under real load, settled at
// roughly 150-165 MB of resident memory after any burst of concurrent
// GetUserContext/InitUserContext calls (each performs an Argon2 key
// derivation or RSA/DS key generation), and two such calls genuinely
// overlapping pushed a single process to as much as ~217 MB observed,
// with four overlapping reliably exceeding 256 MB and triggering an
// OOM kill. 2 admits real concurrency while keeping one worker's expected
// peak (roughly 2 x 160-220 MB) inside deploy/kubernetes/worker-
// deployment.yaml's memory limit, which was raised to match (see that
// file's own comment).
const (
	DefaultListenAddr        = ":50052"
	DefaultShutdownTimeout   = 25 * time.Second
	DefaultReadinessInterval = 5 * time.Second
	DefaultAuthConcurrency   = 2
)

// Config is everything one worker process needs to start.
//
// Unlike cmd/coordinator's ServerConfig, there is no "run without it"
// option for either dependency: a worker with no MongoDB is a worker with
// nowhere to persist SAFER objects, and a worker with no coordinator would
// fall back to SAFER's process-local LockManager, which coordinates with
// nothing else in the deployment -- exactly the V1 limitation this whole
// repository exists to fix. Both are required, and parseConfig fails
// closed if either is missing.
type Config struct {
	ListenAddr        string
	ShutdownTimeout   time.Duration
	ReadinessInterval time.Duration
	// AuthConcurrency bounds how many of this process's
	// GetUserContext/InitUserContext calls (Argon2 key derivation, or
	// RSA/DS key generation) may run at once -- see authlimit.go and
	// DefaultAuthConcurrency's doc for why this exists and how the
	// default was chosen.
	AuthConcurrency int
	Mongo           mongostore.Config
	Coordinator     grpccoord.Config
}

// parseConfig builds a Config from command-line flags and the environment.
//
// It takes an explicit flag set and argument slice, rather than touching
// flag.CommandLine and os.Args directly, so tests can call it repeatedly
// with different environments without fighting Go's single global flag
// set.
func parseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	addr := fs.String("addr", DefaultListenAddr, "address to listen on for gRPC")
	shutdownTimeout := fs.Duration("shutdown-timeout", DefaultShutdownTimeout,
		"how long to wait for in-flight RPCs to finish during a graceful shutdown before forcing the stop")
	readinessInterval := fs.Duration("readiness-interval", DefaultReadinessInterval,
		"how often to re-check MongoDB and coordinator reachability for the readiness probe")
	authConcurrency := fs.Int("auth-concurrency", DefaultAuthConcurrency,
		"max concurrent GetUserContext/InitUserContext calls (Argon2 key derivation, RSA/DS key "+
			"generation) this process runs at once; bounds memory, see DefaultAuthConcurrency's doc")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if *shutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("worker: -shutdown-timeout must be positive")
	}
	if *readinessInterval <= 0 {
		return Config{}, fmt.Errorf("worker: -readiness-interval must be positive")
	}
	if *authConcurrency <= 0 {
		return Config{}, fmt.Errorf("worker: -auth-concurrency must be positive")
	}

	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		return Config{}, fmt.Errorf("worker: mongo configuration: %w", err)
	}
	if !configured {
		return Config{}, fmt.Errorf("worker: %s is required; a worker with no durable storage cannot run", mongostore.EnvURI)
	}

	coordCfg, configured, err := grpccoord.ConfigFromEnv()
	if err != nil {
		return Config{}, fmt.Errorf("worker: coordinator configuration: %w", err)
	}
	if !configured {
		return Config{}, fmt.Errorf("worker: %s is required; a worker with no remote coordinator "+
			"would fall back to process-local locking, which coordinates with no other replica", grpccoord.EnvAddress)
	}

	return Config{
		ListenAddr:        *addr,
		ShutdownTimeout:   *shutdownTimeout,
		ReadinessInterval: *readinessInterval,
		AuthConcurrency:   *authConcurrency,
		Mongo:             mongoCfg,
		Coordinator:       coordCfg,
	}, nil
}
