# tpuf-benchmark

A general purpose tool for benchmarking [turbopuffer](https://turbopuffer.com) deployments across a wide variety of workloads. Modifications, extensions and new benchmarks are encouraged; we want you to push turbopuffer to it's limits. If you find any issues or performance problems, let us know so we can improve turbopuffer for your specific workload!

### Setup

Requirements:
- Go 1.23.4 ([install instructions](https://go.dev/doc/install))


First, compile the benchmark script:

```bash
go build -o tpuf-benchmark
```

Then, you can run the script with the default parameters (this'll do a tiny benchmark to verify that the script is working as intended):

```bash
./tpuf-benchmark \
-api-key <API_KEY> \
-endpoint <ENDPOINT>
```

### Example Usage

To see a list of all available configuration options, run:

```bash
./tpuf-benchmark --help
```

### Reproducing website benchmarks

All benchmarks were run on a c2-standard-30 instance running in GCP us-central1.

```bash
./tpuf-benchmark \
    -api-key <API_KEY> \
    -endpoint <ENDPOINT> \
    -namespace-count 1 \
    -namespace-combined-size 1000000 \
    -upserts-per-sec 0 \
    -queries-per-sec 3 \
    -query-template <TEMPLATE_FILE_PATH>
```

The template file defines the workload that'll be run:
- For vector search, use `templates/query_default.json.tmpl`
- For full-text (BM25) search, use `templates/query_full_text.json.tmpl`

You can customize these template files with additional parameters to benchmark
different variants of the workload. For example:
- `"consistency": {"level": "eventual"}` to test eventual consistency for queries (default is strong consistency)
- `"disable_cache": true` to test the performance of cold (uncached) queries
