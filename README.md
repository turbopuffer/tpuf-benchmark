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

#### 25000 small namespaces, 100 upserts per minute to each, 50 queries per second (distributed across all namespaces)

```bash
./tpuf-benchmark \
-api-key <API_KEY> \
-endpoint <ENDPOINT> \
-namespace-count 25000 \
-namespace-each-size-min 1000 \
-namespace-each-size-max 100000 \
-namespace-each-upsert-frequency-s 60 \
-namespace-each-upsert-batch-size 100 \
-namespace-distributed-qps 50
```

#### 1000 namespaces (w/ each ranging from 50k -> 1M documents), 150 upserts to each namespace every 30 seconds (~5 WPS), 20 queries per second (distributed across all namespaces)

```bash
./tpuf-benchmark \
-api-key <API_KEY> \
-endpoint <ENDPOINT> \
-namespace-count 1000 \
-namespace-each-size-min 50_000 \
-namespace-each-size-max 1_000_000 \
-namespace-each-upsert-frequency-s 30 \
-namespace-each-upsert-batch-size 150 \
-namespace-distributed-qps 20
```

#### 2000 upserts to a single namespace every 10 seconds (~200 WPS), 1 query per second

```bash
./tpuf-benchmark \
-api-key <API_KEY> \
-endpoint <ENDPOINT> \
-namespace-count 1 \
-namespace-each-size-min 100_000 \
-namespace-each-size-max 1_000_000 \
-namespace-each-upsert-frequency-s 10 \
-namespace-each-upsert-batch-size 2000 \
-namespace-distributed-qps 1
```
