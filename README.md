# tpuf-benchmark

A general purpose tool for benchmarking [turbopuffer](https://turbopuffer.com)
deployments across a wide variety of workloads. Modifications, extensions and
new benchmarks are encouraged; we want you to push turbopuffer to it's limits.
If you find any issues or performance problems, let us know so we can improve
turbopuffer for your specific workload!

### Setup

Requirements:
- Go 1.25 ([install instructions](https://go.dev/doc/install))


First, compile the benchmark script:

```bash
go build -o tpufbench ./cmd/tpufbench
```

Then, you can run the script with the default parameters (this'll do a tiny
benchmark to verify that the script is working as intended):

```bash
TURBOPUFFER_API_KEY=tpuf_myapikey REGION=gcp-us-central1 ./tpufbench run \
    ./benchmarks/website/vector-10m-hot.toml
```

### Example Usage

To see a list of all available configuration options, run:

```bash
./tpufbench run --help
```

### Reproducing website benchmarks

All benchmarks were run on a c2-standard-30 instance running in GCP
us-central1. The files definining the benchmarks and their parameters are in
`benchmarks/website`.

```bash
TURBOPUFFER_API_KEY=tpuf_myapikey REGION=gcp-us-central1 ./tpufbench run \
    ./benchmarks/website/vector-10m-hot.toml
```

You can customize these template files with additional parameters to benchmark
different variants of the workload. For example:
- `purge_cache = true` to test the performance of cold (uncached) queries
