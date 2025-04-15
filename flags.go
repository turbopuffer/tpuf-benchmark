package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// Request flags, i.e. tuning the HTTP client

var apiKey = flag.String("api-key", "", "the turbopuffer API key to use")

var endpoint = flag.String(
	"endpoint",
	"",
	"the turbopuffer endpoint to use",
)

var hostHeader = flag.String(
	"host-header",
	"",
	"an optional host header to include with turbopuffer requests",
)

var allowTlsInsecure = flag.Bool(
	"allow-tls-insecure",
	false,
	"allow insecure TLS connections to the turbopuffer API",
)

// Template settings
// i.e. which query / upsert template files to use

var upsertTemplate = flag.String(
	"upsert-template",
	"templates/upsert_default.json.tmpl",
	"template file for upsert requests",
)

var documentTemplate = flag.String(
	"document-template",
	"templates/document_default.json.tmpl",
	"template file for document generation",
)

var queryTemplate = flag.String(
	"query-template",
	"templates/query_default.json.tmpl",
	"template file for query requests",
)

// Namespace settings, i.e. controlling how the benchmark is setup

var namespacePrefix = flag.String(
	"namespace-prefix",
	hostname(),
	"a unique string to prefix namespace names with. defaults to your machine hostname",
)

var namespaceCount = flag.Int(
	"namespace-count",
	10,
	"the number of namespaces to operate on. namespaces are named <namespace-prefix>_<num>",
)

var namespaceCombinedSize = flag.Int64(
	"namespace-combined-size",
	100_000,
	"combined number of documents distributed across all namespaces",
)

var namespaceSizeDistribution = flag.String(
	"namespace-size-distribution",
	"uniform",
	"distribution of document counts across namespaces. options: 'uniform', 'lognormal'",
)

var logNormalMu = flag.Float64(
	"lognormal-mu",
	0,
	"mu parameter for lognormal distribution of namespace sizes",
)

var logNormalSigma = flag.Float64(
	"lognormal-sigma",
	0.95,
	"sigma parameter for lognormal distribution of namespace sizes",
)

// Benchmark settings

var benchmarkWaitForIndexing = flag.Bool(
	"wait-for-indexing",
	true,
	"wait for namespaces to be indexed after initial high-wps upserts, before starting benchmark",
)

var benchmarkPurgeCache = flag.Bool(
	"purge-cache",
	false,
	"purge the cache before starting the benchmark",
)

var benchmarkWarmCache = flag.Bool(
	"warm-cache",
	false,
	"warm the cache before starting the benchmark",
)

var benchmarkQueriesPerSecond = flag.Float64(
	"queries-per-sec",
	3.0,
	"combined queries per second across all namespaces. see: `query-distribution`",
)

var benchmarkQueryConcurrency = flag.Int(
	"query-concurrency",
	8,
	"the number of concurrent queries to run",
)

var benchmarkQueryDistribution = flag.String(
	"query-distribution",
	"uniform",
	"distribution of queries across namespaces. options: 'uniform', 'pareto'",
)

var benchmarkQueryParetoAlpha = flag.Float64(
	"query-pareto-alpha",
	1.5,
	"alpha parameter for pareto distribution of queries",
)

var benchmarkActiveNamespacePct = flag.Float64(
	"query-active-namespace-pct",
	1.0,
	"the percentage of namespaces that will be queried. defaults to ~20%, conservative estimate from our production workloads",
)

var benchmarkUpsertsPerSecond = flag.Int(
	"upserts-per-sec",
	5,
	"combined upserts per second across all namespaces. will respect `upsert-min-batch-size` and `upsert-max-batch-size`",
)

var benchmarkUpsertConcurrency = flag.Int(
	"upsert-concurrency",
	8,
	"the number of concurrent upserts to run",
)

var upsertBatchSize = flag.Int(
	"upsert-batch-size",
	1,
	"number of documents to upsert in a single request",
)

var benchmarkDuration = flag.Duration(
	"benchmark-duration",
	time.Minute*10,
	"duration of the benchmark. if 0, will run indefinitely",
)

var outputDir = flag.String(
	"output-dir",
	defaultOutputDir(),
	"directory to write benchmark results to. if empty, won't write anything to disk",
)

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
