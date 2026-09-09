package main

import (
	"testing"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// clearWorkerEnv unsets every environment variable parseConfig consults, so
// each test starts from "nothing configured" regardless of the host
// environment or test execution order.
func clearWorkerEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		mongostore.EnvURI, mongostore.EnvDatabase, mongostore.EnvTimeout, mongostore.EnvTransactionTimeout,
		grpccoord.EnvAddress, grpccoord.EnvTimeout,
	} {
		t.Setenv(key, "")
	}
}

func TestParseConfig_RefusesMissingMongo(t *testing.T) {
	clearWorkerEnv(t)
	t.Setenv(grpccoord.EnvAddress, "127.0.0.1:50051")

	if _, err := parseConfig(nil); err == nil {
		t.Fatal("expected an error with no MongoDB configured, got nil")
	}
}

func TestParseConfig_RefusesMissingCoordinator(t *testing.T) {
	clearWorkerEnv(t)
	t.Setenv(mongostore.EnvURI, "mongodb://localhost:27017")

	if _, err := parseConfig(nil); err == nil {
		t.Fatal("expected an error with no coordinator configured, got nil")
	}
}

func TestParseConfig_RefusesNeither(t *testing.T) {
	clearWorkerEnv(t)

	if _, err := parseConfig(nil); err == nil {
		t.Fatal("expected an error with neither MongoDB nor a coordinator configured, got nil")
	}
}

func TestParseConfig_SucceedsWithBothConfigured(t *testing.T) {
	clearWorkerEnv(t)
	t.Setenv(mongostore.EnvURI, "mongodb://localhost:27017")
	t.Setenv(grpccoord.EnvAddress, "127.0.0.1:50051")

	cfg, err := parseConfig([]string{"-addr", ":0", "-shutdown-timeout", "1s", "-readiness-interval", "1s"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ListenAddr != ":0" {
		t.Errorf("ListenAddr = %q, want :0", cfg.ListenAddr)
	}
	if cfg.Mongo.URI != "mongodb://localhost:27017" {
		t.Errorf("Mongo.URI = %q", cfg.Mongo.URI)
	}
	if cfg.Coordinator.Address != "127.0.0.1:50051" {
		t.Errorf("Coordinator.Address = %q", cfg.Coordinator.Address)
	}
}

func TestParseConfig_RefusesNonPositiveTimeouts(t *testing.T) {
	clearWorkerEnv(t)
	t.Setenv(mongostore.EnvURI, "mongodb://localhost:27017")
	t.Setenv(grpccoord.EnvAddress, "127.0.0.1:50051")

	for _, args := range [][]string{
		{"-shutdown-timeout", "0s"},
		{"-shutdown-timeout", "-1s"},
		{"-readiness-interval", "0s"},
		{"-readiness-interval", "-1s"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("parseConfig(%v): expected an error, got nil", args)
		}
	}
}

func TestParseConfig_DefaultsApplied(t *testing.T) {
	clearWorkerEnv(t)
	t.Setenv(mongostore.EnvURI, "mongodb://localhost:27017")
	t.Setenv(grpccoord.EnvAddress, "127.0.0.1:50051")

	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ListenAddr != DefaultListenAddr {
		t.Errorf("ListenAddr = %q, want default %q", cfg.ListenAddr, DefaultListenAddr)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want default %v", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}
	if cfg.ReadinessInterval != DefaultReadinessInterval {
		t.Errorf("ReadinessInterval = %v, want default %v", cfg.ReadinessInterval, DefaultReadinessInterval)
	}
}
