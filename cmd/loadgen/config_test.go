package main

import "testing"

func TestParseConfig_RequiresAddr(t *testing.T) {
	if _, err := parseConfig([]string{"-count", "10"}); err == nil {
		t.Fatal("expected an error with no -addr, got nil")
	}
}

func TestParseConfig_RequiresCountOrDuration(t *testing.T) {
	if _, err := parseConfig([]string{"-addr", "localhost:1", "-count", "0"}); err == nil {
		t.Fatal("expected an error with neither -count nor -duration positive, got nil")
	}
}

func TestParseConfig_RejectsUnknownWorkload(t *testing.T) {
	if _, err := parseConfig([]string{"-addr", "localhost:1", "-workload", "nonsense"}); err == nil {
		t.Fatal("expected an error for an unknown workload, got nil")
	}
}

func TestParseConfig_SplitsAndTrimsAddresses(t *testing.T) {
	cfg, err := parseConfig([]string{"-addr", " a:1 , b:2 ,c:3"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a:1", "b:2", "c:3"}
	if len(cfg.Addresses) != len(want) {
		t.Fatalf("Addresses = %v, want %v", cfg.Addresses, want)
	}
	for i := range want {
		if cfg.Addresses[i] != want[i] {
			t.Errorf("Addresses[%d] = %q, want %q", i, cfg.Addresses[i], want[i])
		}
	}
}

func TestParseConfig_ForcesSingleUserForSharedWorkloads(t *testing.T) {
	for _, wl := range []string{string(WorkloadSameFileWrites), string(WorkloadReads)} {
		cfg, err := parseConfig([]string{"-addr", "a:1", "-workload", wl, "-users", "50", "-concurrency", "8"})
		if err != nil {
			t.Fatalf("workload %s: unexpected error: %v", wl, err)
		}
		if cfg.Users != 1 {
			t.Errorf("workload %s: Users = %d, want 1 (a caller-supplied -users should be ignored)", wl, cfg.Users)
		}
	}
}

func TestParseConfig_DefaultsUsersToConcurrency(t *testing.T) {
	cfg, err := parseConfig([]string{"-addr", "a:1", "-workload", string(WorkloadIndependentWrites), "-concurrency", "7"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Users != 7 {
		t.Errorf("Users = %d, want 7 (default to -concurrency)", cfg.Users)
	}
}

func TestParseConfig_RejectsNonPositiveConcurrencyAndContentSize(t *testing.T) {
	for _, args := range [][]string{
		{"-addr", "a:1", "-concurrency", "0"},
		{"-addr", "a:1", "-concurrency", "-1"},
		{"-addr", "a:1", "-content-size", "0"},
		{"-addr", "a:1", "-content-size", "-1"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("parseConfig(%v): expected an error, got nil", args)
		}
	}
}
