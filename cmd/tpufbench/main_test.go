package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func newEnvFallbackTestFlagSet(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.Int("workers", 1, "")
	flags.Bool("verbose", false, "")
	flags.Duration("timeout", time.Second, "")
	flags.String("name", "default", "")

	if err := flags.Parse(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return flags
}

func TestApplyEnvFallbacksExplicitFlagWinsOverEnv(t *testing.T) {
	t.Setenv("TPUF_TEST_WORKERS", "99")

	flags := newEnvFallbackTestFlagSet(t, "--workers=7")
	err := applyEnvFallbacks([]envFallback{{FlagName: "workers", EnvName: "TPUF_TEST_WORKERS"}}, flags)
	if err != nil {
		t.Fatalf("applyEnvFallbacks: %v", err)
	}

	got, err := flags.GetInt("workers")
	if err != nil {
		t.Fatalf("GetInt: %v", err)
	}
	if got != 7 {
		t.Fatalf("workers = %d, want explicit flag value 7", got)
	}
}

func TestApplyEnvFallbacksEnvAppliesWhenFlagAbsent(t *testing.T) {
	t.Setenv("TPUF_TEST_WORKERS", "42")

	flags := newEnvFallbackTestFlagSet(t)
	err := applyEnvFallbacks([]envFallback{{FlagName: "workers", EnvName: "TPUF_TEST_WORKERS"}}, flags)
	if err != nil {
		t.Fatalf("applyEnvFallbacks: %v", err)
	}

	got, err := flags.GetInt("workers")
	if err != nil {
		t.Fatalf("GetInt: %v", err)
	}
	if got != 42 {
		t.Fatalf("workers = %d, want env value 42", got)
	}
}

func TestApplyEnvFallbacksEmptyEnvIgnored(t *testing.T) {
	t.Setenv("TPUF_TEST_NAME", "")

	flags := newEnvFallbackTestFlagSet(t)
	err := applyEnvFallbacks([]envFallback{{FlagName: "name", EnvName: "TPUF_TEST_NAME"}}, flags)
	if err != nil {
		t.Fatalf("applyEnvFallbacks: %v", err)
	}

	got, err := flags.GetString("name")
	if err != nil {
		t.Fatalf("GetString: %v", err)
	}
	if got != "default" {
		t.Fatalf("name = %q, want default", got)
	}
}

func TestApplyEnvFallbacksInvalidEnvValuesReturnErrors(t *testing.T) {
	tests := []struct {
		name     string
		flagName string
		envName  string
		envValue string
	}{
		{
			name:     "invalid int",
			flagName: "workers",
			envName:  "TPUF_TEST_WORKERS",
			envValue: "not-an-int",
		},
		{
			name:     "invalid bool",
			flagName: "verbose",
			envName:  "TPUF_TEST_VERBOSE",
			envValue: "not-a-bool",
		},
		{
			name:     "invalid duration",
			flagName: "timeout",
			envName:  "TPUF_TEST_TIMEOUT",
			envValue: "not-a-duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envName, tt.envValue)

			flags := newEnvFallbackTestFlagSet(t)
			err := applyEnvFallbacks([]envFallback{{FlagName: tt.flagName, EnvName: tt.envName}}, flags)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.envName) {
				t.Fatalf("error %q does not mention env var %s", err, tt.envName)
			}
		})
	}
}
