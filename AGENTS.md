# AGENTS.md

## Cursor Cloud specific instructions

`tpuf-benchmark` (binary `tpufbench`) is a single-product Go CLI that benchmarks a
remote [turbopuffer](https://turbopuffer.com) deployment (vector/FTS/hybrid search
and upserts). It hosts no local server or database; the only backing "service" is
the external turbopuffer API reached over HTTPS. There is no monorepo, no Docker,
and no JS/Python toolchain required for the core tool (`generate_charts.py` is an
optional Python post-processing script for results).

Standard commands live in `README.md` and the `Makefile`. Quick reference:
- Lint: `go vet ./...`
- Test: `go test ./...` (offline; covers definition parsing and datasource logic)
- Build: `make build` (`go build -o tpufbench ./cmd/tpufbench`)
- List benchmarks (offline): `./tpufbench list benchmarks/`

Non-obvious notes:
- A real benchmark run requires a valid `TURBOPUFFER_API_KEY` env var plus an
  `--endpoint` (e.g. `--endpoint https://gcp-us-central1.turbopuffer.com`). The
  default endpoint `https://REGION.turbopuffer.com` is a placeholder and will not
  resolve. Without a valid key, `tpufbench run` fails early in its API sanity
  check with a `401 Unauthorized` (this is expected, not an env problem).
- `tpufbench run` starts an in-process debug/metrics server on `:6060`
  (`/metrics`, `/debug/pprof/`) only while a benchmark is running.
- The committed definitions under `benchmarks/` are large (1M–1B documents) and
  are meant to run for minutes from a cloud VM in the turbopuffer region. For a
  fast smoke test, write a small TOML using `datasource = "random"` with a tiny
  `document_count` and a short `duration`, then run it against a real endpoint.
- Datasets are cached to disk at `DATASET_CACHE_DIR` (defaults to the OS temp
  dir). Only the `random` datasource avoids network dataset downloads.
