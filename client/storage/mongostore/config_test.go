package mongostore

import (
	"testing"
	"time"
)

// Configuration is pure logic and runs everywhere; no MongoDB required.

func TestConfigFromEnvUnconfigured(t *testing.T) {
	t.Setenv(EnvURI, "")
	cfg, configured, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unset URI: got err %v, want nil", err)
	}
	if configured {
		t.Fatal("unset URI reported as configured")
	}
	if cfg != (Config{}) {
		t.Fatalf("unset URI: got %+v, want zero Config", cfg)
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv(EnvURI, "mongodb://example:27017")
	t.Setenv(EnvDatabase, "")
	t.Setenv(EnvTimeout, "")

	cfg, configured, err := ConfigFromEnv()
	if err != nil || !configured {
		t.Fatalf("got configured=%v err=%v, want true/nil", configured, err)
	}
	if cfg.Database != DefaultDatabase {
		t.Fatalf("Database: got %q, want %q", cfg.Database, DefaultDatabase)
	}
	if cfg.Timeout != DefaultTimeout {
		t.Fatalf("Timeout: got %v, want %v", cfg.Timeout, DefaultTimeout)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	t.Setenv(EnvURI, "mongodb://example:27017")
	t.Setenv(EnvDatabase, "safer_test")

	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"2s", 2 * time.Second},
		{"1500ms", 1500 * time.Millisecond},
		{"30", 30 * time.Second}, // bare seconds, convenient in manifests
	} {
		t.Setenv(EnvTimeout, tc.raw)
		cfg, _, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("%s=%q: %v", EnvTimeout, tc.raw, err)
		}
		if cfg.Timeout != tc.want {
			t.Fatalf("%s=%q: got %v, want %v", EnvTimeout, tc.raw, cfg.Timeout, tc.want)
		}
		if cfg.Database != "safer_test" {
			t.Fatalf("Database: got %q, want %q", cfg.Database, "safer_test")
		}
	}
}

func TestConfigFromEnvRejectsBadTimeout(t *testing.T) {
	t.Setenv(EnvURI, "mongodb://example:27017")
	for _, raw := range []string{"soon", "-5s", "0"} {
		t.Setenv(EnvTimeout, raw)
		if _, _, err := ConfigFromEnv(); err == nil {
			t.Fatalf("%s=%q: got nil error, want a rejection", EnvTimeout, raw)
		}
	}
}

func TestConfigValidateRequiresURI(t *testing.T) {
	// No hard-coded fallback URI: a missing URI is an error, never a
	// silent connection to some default host.
	if err := (Config{Database: "safer"}).validate(); err == nil {
		t.Fatal("empty URI accepted, want an error")
	}
}
