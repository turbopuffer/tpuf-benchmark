# tpuf-benchmark

A general purpose tool for benchmarking [turbopuffer](https://turbopuffer.com)
deployments across a wide variety of workloads. Modifications, extensions and
new benchmarks are encouraged; we want you to push turbopuffer to it's limits.
If you find any issues or performance problems, let us know so we can improve
turbopuffer for your specific workload!

### Setup

Requirements:
- Go 1.25 ([install instructions](https://go.dev/doc/install))


First, compile the `tpufbench` command:

```bash
go build -o tpufbench ./cmd/tpufbench
```

Then, you can run an example benchmark:

```bash
TURBOPUFFER_API_KEY=tpuf_myapikey TURBOPUFFER_REGION=gcp-us-central1 ./tpufbench run \
    --warm-cache \
    ./benchmarks/website/vector-1m.toml
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
TURBOPUFFER_API_KEY=tpuf_myapikey TURBOPUFFER_REGION=gcp-us-central1 ./tpufbench run \
    --warm-cache \
    ./benchmarks/website/vector-1m.toml
```

You can customize these template files with additional parameters to benchmark
different variants of the workload.
