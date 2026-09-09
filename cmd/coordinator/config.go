package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
)

// Config is everything one coordinator process needs to start.
type Config struct {
	Address       string
	LeaseDuration time.Duration
	SweepInterval time.Duration

	// AllowUnfenced opts out of the fail-closed default, so the coordinator
	// starts even with no durable fencing store. It exists for local
	// development and for tests that exercise lock semantics alone; it is
	// never appropriate for a production deployment, and startup logs
	// loudly when it is set. Production deployments should never set this.
	AllowUnfenced bool
}

// parseConfig builds a Config from command-line flags.
//
// It takes an explicit flag set and argument slice, rather than touching
// flag.CommandLine and os.Args directly, so tests can call it repeatedly
// without fighting Go's single global flag set. MongoDB configuration is
// read separately, from the environment, by main/run -- that decision
// (fail closed without it, unless AllowUnfenced) is production policy, not
// something to bury inside flag parsing.
func parseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	address := fs.String("addr", "127.0.0.1:0",
		"address to listen on; port 0 picks a free port and prints it")
	lease := fs.Duration("lease", grpccoord.DefaultLeaseDuration,
		"how long a transaction's locks survive without a renewal")
	sweep := fs.Duration("sweep", grpccoord.DefaultSweepInterval,
		"how often to look for expired leases")
	allowUnfenced := fs.Bool("allow-unfenced", false,
		"start even with no durable fencing store configured; DEV/TEST ONLY, "+
			"since an unfenced coordinator gives no stale-writer protection at all")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if *lease <= 0 {
		return Config{}, fmt.Errorf("coordinator: -lease must be positive")
	}
	if *sweep <= 0 {
		return Config{}, fmt.Errorf("coordinator: -sweep must be positive")
	}

	return Config{
		Address:       *address,
		LeaseDuration: *lease,
		SweepInterval: *sweep,
		AllowUnfenced: *allowUnfenced,
	}, nil
}
