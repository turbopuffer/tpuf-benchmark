package main

import (
	"fmt"
	"os"
	"time"
)

// Root-level flags shared across all subcommands.
var (
	apiKey           string
	endpoint         string
	hostHeader       string
	allowTlsInsecure bool
)

// RunConfig holds all configuration for the "run" subcommand.
type RunConfig struct {
	// Template settings
	UpsertTemplate   string
	DocumentTemplate string
	QueryTemplate    string

	// Namespace settings
	NamespacePrefix              string
	NamespaceCount               int
	NamespaceCombinedSize        int64
	NamespaceSizeDistribution    string
	LogNormalMu                  float64
	LogNormalSigma               float64
	NamespaceSetupConcurrency    int
	NamespaceSetupConcurrencyMax int
	NamespaceSetupBatchSize      int

	// Benchmark settings
	PromptToClear      bool
	WaitForIndexing    bool
	PurgeCache         bool
	WarmCache          bool
	QueriesPerSecond   float64
	QueryConcurrency   int
	QueryDistribution  string
	QueryParetoAlpha   float64
	ActiveNamespacePct float64
	UpsertsPerSecond   int
	UpsertConcurrency  int
	UpsertBatchSize    int
	Duration           time.Duration
	QueryRetries       int
	OutputDir          string
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// We want to avoid collisions between multiple benchmark runs,
// i.e. we suffix the output directory with a timestamp
func defaultOutputDir() string {
	return fmt.Sprintf("benchmark-results-%d", time.Now().Unix())
}
