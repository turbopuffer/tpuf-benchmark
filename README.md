# tpuf-benchmark

A general purpose tool for benchmarking [turbopuffer](https://turbopuffer.com) deployments across a wide variety of workloads. Modifications, extensions and new benchmarks are encouraged; we want you to push turbopuffer to it's limits. If you find any issues or performance problems, let us know so we can improve turbopuffer for your specific workload!

### Setup

Requirements:
- Go 1.23.4 ([install instructions](https://go.dev/doc/install))


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

### Example Usage

To see a list of all available configuration options, run:

```bash
./tpuf-benchmark --help
```

### Reproducing website benchmarks

All benchmarks were run on a c2-standard-30 instance running in us-central1.

By default, `benchmark-duration` is 10 minutes, and can be configured with a flag.
At the end of the benchmark, the script will aggregate the results into a final report,
which will be written to disk under `benchmark-results-[timestamp]`.

1 million, 768 dimensional vectors (sourced from [wikipedia-22-12-en-embeddings](https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings))

- for `hot` benchmarks (i.e. all data is in cache), use `templates/query_default.json.tmpl`
- for `cold` benchmarks (i.e. bypassing cache), use `templates/query_disable_cache.json.tmpl`

```bash
./tpuf-benchmark \
    -api-key <API_KEY> \
    -endpoint <ENDPOINT> \
    -namespace-count 1 \
    -namespace-combined-size 1000000 \
    -upserts-per-sec 0 \
    -queries-per-sec 3 \
    -purge-cache=false \
    -query-template <template>
```

1 million [MSMARCO](https://huggingface.co/datasets/BeIR/msmarco) text documents, using MSMARCO queries

- for `hot` benchmarks (i.e. all data is in cache), use `templates/query_full_text.json.tmpl`
- for `cold` benchmarks (i.e. bypassing cache), use `templates/query_full_text_disable_cache.json.tmpl`

```bash
./tpuf-benchmark \
    -api-key <API_KEY> \
    -endpoint <ENDPOINT> \
    -namespace-count 1 \
    -namespace-combined-size 1000000 \
    -upserts-per-sec 0 \
    -queries-per-sec 3 \
    -purge-cache=false \
    -query-template <template>
```

When running benchmarks with eventual consistency, add `"consistency": {"level": "eventual"}` to your query template.
