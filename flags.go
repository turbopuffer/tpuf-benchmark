package main

import (
	"flag"
	"os"
	"time"
)

var apiKey = flag.String("api-key", "", "the turbopuffer API key to use")

var endpoint = flag.String(
	"endpoint",
	"",
	"the turbopuffer endpoint to use",
)

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

var namespaceSizeMin = flag.Int(
	"namespace-each-size-min",
	1_000,
	"the minimum number of documents in each namespace",
)

var namespaceSizeMax = flag.Int(
	"namespace-each-size-max",
	20_000,
	"the maximum number of documents in each namespace",
)

var namespaceInitialSize = flag.String(
	"namespace-each-initial-size",
	"lognormal",
	"how to populate the initial size of each namespace. options are 'lognormal', 'min', and 'max'",
)

var overrideExisting = flag.Bool(
	"override-existing",
	false,
	"deletes any existing data, then upserts between [min, max] documents to each namespace according to a log-normal distribution",
)

var setupLognormalMu = flag.Float64(
	"setup-lognormal-mu",
	0,
	"the mu parameter for the lognormal distribution of namespace sizes during setup. to tune this, run the script to see size distribution for setup step",
)

var setupLognormalSigma = flag.Float64(
	"setup-lognormal-sigma",
	0.95,
	"the sigma parameter for the lognormal distribution of namespace sizes during setup. to tune this, run the script to see size distribution for setup step",
)

var namespaceUpsertFrequency = flag.Int(
	"namespace-each-upsert-frequency-s",
	5,
	"frequency to upsert document batches (-namespace-each-upsert-batch-size) in seconds. disabled if set to 0",
)

var namespaceUpsertBatchSize = flag.Int(
	"namespace-each-upsert-batch-size",
	50,
	"the number of documents to write to each namespace in each write batch",
)

var namespaceDistributedQps = flag.Float64(
	"namespace-distributed-qps",
	3,
	"number of queries per second to execute against the set of namespaces",
)

var namespaceQueryDistribution = flag.String(
	"namespace-query-distribution",
	"pareto",
	"how to distribute queries across namespaces. options are 'pareto' and 'uniform'",
)

var queryParetoAlpha = flag.Float64(
	"namespace-query-pareto-alpha",
	1.5,
	"the alpha parameter for the pareto distribution of query sizes",
)

var activeNamespacePct = flag.Float64(
	"namespace-active-pct",
	0.2,
	"the percentage of namespaces that will be queried. defaults to ~20%, which is a conservative estimate from our production workloads",
)

var reportInterval = flag.Duration(
	"report-interval",
	time.Second*10,
	"how often to log a report of the benchmark progress",
)

var queryHeadstart = flag.Duration(
	"query-headstart-cache",
	time.Second*3,
	"pre-warm the cache of a namespace before starting to query it",
)

var steadyStateDuration = flag.Duration(
	"steady-state-duration",
	time.Minute*10,
	"how long to run the benchmark for at steady-state. if 0, will run indefinitely",
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

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}
