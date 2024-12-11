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
	"the minimum number of documents in each namespace. populated with a lognormal distrubution",
)

var namespaceSizeMax = flag.Int(
	"namespace-each-size-max",
	20_000,
	"the maximum number of documents in each namespace. populated with a lognormal distrubution",
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

var reportInterval = flag.Duration(
	"report-interval",
	time.Second*5,
	"how often to log a report of the benchmark progress",
)

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}
