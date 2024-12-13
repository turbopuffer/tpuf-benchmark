# tpuf-benchmark

A general purpose tool for benchmarking [turbopuffer](https://turbopuffer.com) deployments across a wide variety of workloads. Modifications, extensions and new benchmarks are encouraged; we want you to push turbopuffer to it's limits. If you find any issues or performance problems, let us know so we can improve turbopuffer for your specific workload!

### Setup

Requirements:
- Go 1.23 ([install instructions](https://go.dev/doc/install))


First, compile the benchmark script:

```bash
go build . -o tpuf-benchmark
```

Then, you can run the script with the default parameters (this'll do a tiny benchmark to verify that the script is working as intended):

```bash
./tpuf-benchmark \
-api-key <API_KEY> \
-endpoint <ENDPOINT>
```

Note: If the system detects there are any existing data in the namespace(s), it won't delete the data and won't upsert any new data. Passing `-override-existing` will force the script to delete the existing data and upsert new data before beginning the benchmark.

### Example workloads

To see a list of all available configuration options, run:

```bash
./tpuf-benchmark --help
```

#### Upsert to a namespace as fast as possible

```bash
go run . \
	-api-key <API_KEY> \
	-endpoint <ENDPOINT> \
	-namespace-count 1 \
	-namespace-each-size-min 1000000 \
	-namespace-each-size-max 1000000 \
	-namespace-each-upsert-frequency-s 0 \
	-namespace-distributed-qps 0
```

#### Upsert 200 documents every second to each of 500 namespaces

```bash
go run . \
  -api-key <API_KEY> \
	-endpoint <ENDPOINT> \
	-namespace-count 500 \
	-namespace-each-size-min 0 \
	-namespace-each-size-max 1000000 \
	-namespace-each-initial-size min \
	-namespace-each-upsert-frequency-s 1 \
	-namespace-each-upsert-batch-size 200 \
	-namespace-distributed-qps 0
```

#### Upsert between 20,000 and 1M documents to each of 1000 namespaces, then start querying across the set of namespaces (20% active; pareto distribution) at a combined 100 QPS while concurrently upserting 100 documents every 20s (~5 WPS) to each namespace

```bash
go run . \
	-api-key <API_KEY> \
	-endpoint <ENDPOINT> \
	-namespace-count 1000 \
	-namespace-each-size-min 20000 \
	-namespace-each-size-max 1000000 \
	-namespace-each-upsert-frequency-s 20 \
	-namespace-each-upsert-batch-size 100 \
	-namespace-distributed-qps 100 \
	-namespace-active-pct 0.2
```
