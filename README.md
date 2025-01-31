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
