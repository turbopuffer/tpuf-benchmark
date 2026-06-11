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
TURBOPUFFER_API_KEY=tpuf_myapikey TPUFBENCH_ENDPOINT=https://gcp-us-central1.turbopuffer.com ./tpufbench run \
    --warm-cache \
    ./benchmarks/website/vector-1m.toml
```

### Example Usage

To see a list of all available configuration options, run:

```bash
./tpufbench run --help
```

### Environment variable configuration

Most `tpufbench run` flags can also be configured with environment variables. Explicit command-line flags take precedence over environment variables, and environment variables take precedence over the built-in defaults.

| Flag | Environment variable |
| --- | --- |
| `--endpoint` | `TPUFBENCH_ENDPOINT` |
| `--host-header` | `TPUFBENCH_HOST_HEADER` |
| `--allow-tls-insecure` | `TPUFBENCH_ALLOW_TLS_INSECURE` |
| `--namespace-prefix` | `TPUFBENCH_NAMESPACE_PREFIX` |
| `--namespace-setup-concurrency` | `TPUFBENCH_NAMESPACE_SETUP_CONCURRENCY` |
| `--namespace-setup-concurrency-max` | `TPUFBENCH_NAMESPACE_SETUP_CONCURRENCY_MAX` |
| `--if-nonempty` | `TPUFBENCH_IF_NONEMPTY` |
| `--output-dir` | `TPUFBENCH_OUTPUT_DIR` |
| `--warm-cache` | `TPUFBENCH_WARM_CACHE` |
| `--purge-cache` | `TPUFBENCH_PURGE_CACHE` |
| `--duration` | `TPUFBENCH_DURATION` |

`TURBOPUFFER_API_KEY` is still required for API authentication. `DATASET_CACHE_DIR` can be used to choose the local dataset cache directory.

### Reproducing website benchmarks

All benchmarks were run on a c2-standard-30 instance running in GCP
us-central1. The files definining the benchmarks and their parameters are in
`benchmarks/website`.

```bash
TURBOPUFFER_API_KEY=tpuf_myapikey TPUFBENCH_ENDPOINT=https://gcp-us-central1.turbopuffer.com ./tpufbench run \
    --warm-cache \
    ./benchmarks/website/vector-1m.toml
```

You can customize these template files with additional parameters to benchmark
different variants of the workload.
