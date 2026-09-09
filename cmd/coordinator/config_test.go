package main

import (
	"testing"

	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

func clearCoordinatorEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		mongostore.EnvURI, mongostore.EnvDatabase, mongostore.EnvTimeout, mongostore.EnvTransactionTimeout,
	} {
		t.Setenv(key, "")
	}
}

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AllowUnfenced {
		t.Error("AllowUnfenced defaulted to true; fail-closed must be the default")
	}
	if cfg.Address != "127.0.0.1:0" {
		t.Errorf("Address = %q, want 127.0.0.1:0", cfg.Address)
	}
}

func TestParseConfig_AllowUnfencedFlag(t *testing.T) {
	cfg, err := parseConfig([]string{"-allow-unfenced"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.AllowUnfenced {
		t.Error("AllowUnfenced = false after passing -allow-unfenced")
	}
}

func TestParseConfig_RefusesNonPositiveDurations(t *testing.T) {
	for _, args := range [][]string{
		{"-lease", "0s"},
		{"-lease", "-1s"},
		{"-sweep", "0s"},
		{"-sweep", "-1s"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("parseConfig(%v): expected an error, got nil", args)
		}
	}
}

// TestOpenFenceStore_FailsClosedWithoutMongoByDefault is Phase 4.0's core
// guarantee: a production coordinator (AllowUnfenced=false, the default)
// must refuse to start when it has no durable fencing store, rather than
// warning and serving unfenced X-lock coordination anyway.
func TestOpenFenceStore_FailsClosedWithoutMongoByDefault(t *testing.T) {
	clearCoordinatorEnv(t)

	_, _, err := openFenceStore(Config{AllowUnfenced: false})
	if err == nil {
		t.Fatal("expected an error with no MongoDB configured and AllowUnfenced=false, got nil")
	}
}

// TestOpenFenceStore_AllowUnfencedOptsOut is the documented, explicit
// escape hatch for local development and lock-semantics-only tests: with
// AllowUnfenced=true the coordinator may still start unfenced.
func TestOpenFenceStore_AllowUnfencedOptsOut(t *testing.T) {
	clearCoordinatorEnv(t)

	fences, closeFn, err := openFenceStore(Config{AllowUnfenced: true})
	if err != nil {
		t.Fatalf("unexpected error with AllowUnfenced=true: %v", err)
	}
	if fences != nil {
		t.Error("expected a nil FenceStore with no MongoDB configured")
	}
	if closeFn != nil {
		t.Error("expected a nil close function with no fence store opened")
	}
}

// TestOpenFenceStore_FailsClosedOnUnreachableMongo covers the other half
// of fail-closed: MongoDB configured but not actually reachable must also
// refuse to start, not just "unconfigured".
func TestOpenFenceStore_FailsClosedOnUnreachableMongo(t *testing.T) {
	clearCoordinatorEnv(t)
	// A URI pointing at a port nothing listens on, so mongofence.Open's
	// connection attempt fails without needing a real MongoDB absent from
	// this test environment.
	t.Setenv(mongostore.EnvURI, "mongodb://127.0.0.1:1/?connectTimeoutMS=200&serverSelectionTimeoutMS=200")
	t.Setenv(mongostore.EnvTimeout, "1s")

	_, _, err := openFenceStore(Config{AllowUnfenced: false})
	if err == nil {
		t.Fatal("expected an error with an unreachable MongoDB and AllowUnfenced=false, got nil")
	}
}
