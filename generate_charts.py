#!/usr/bin/env python3
"""Generate benchmark charts by injecting data into the Vite-built React app."""

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

MAX_DATA_POINTS = 180

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_WEB_DIST = SCRIPT_DIR / "web" / "dist" / "index.html"


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark latency charts")
    parser.add_argument("build_dir", type=Path, help="Path to the build/ directory")
    parser.add_argument(
        "--web-dist",
        type=Path,
        default=DEFAULT_WEB_DIST,
        help="Path to Vite-built index.html (default: web/dist/index.html)",
    )
    args = parser.parse_args()

    build_dir = args.build_dir.resolve()
    if not build_dir.is_dir():
        print(f"Error: {build_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    web_dist = args.web_dist.resolve()
    if not web_dist.is_file():
        print(f"Error: {web_dist} not found. Run 'cd web && npm run build' first.", file=sys.stderr)
        sys.exit(1)

    today = date.today().isoformat()
    output_dir = build_dir / today
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Find and sort date directories.
    date_dirs = find_date_dirs(build_dir)
    if not date_dirs:
        print("No date directories found in build/", file=sys.stderr)
        sys.exit(1)

    # Step 2: Collect all report data, grouped by benchmark definition.
    all_data = collect_reports(build_dir, date_dirs)

    # Step 3: Build chart data structures.
    chart_data = build_chart_data(all_data, date_dirs)

    # Step 4: Build the data payload.
    payload = {
        "meta": {
            "generated": today,
            "num_dates": len(date_dirs),
        },
        "benchmarks": chart_data,
    }

    # Step 5: Inject data into the Vite-built HTML.
    template_html = web_dist.read_text()
    data_script = f"<script>window.__TPUFBENCH_DATA__ = {json.dumps(payload)};</script>"
    html = template_html.replace("</head>", f"{data_script}\n</head>")

    # Step 6: Write output.
    output_path = output_dir / "index.html"
    output_path.write_text(html)
    print(f"Generated: {output_path}")


def find_date_dirs(build_dir):
    """Find YYYY-MM-DD directories, return sorted list limited to last 180."""
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    dirs = [
        d.name
        for d in build_dir.iterdir()
        if d.is_dir() and date_pattern.match(d.name)
    ]
    dirs.sort()
    return dirs[-MAX_DATA_POINTS:]


def collect_reports(build_dir, date_dirs):
    """
    Returns: {
        bench_name: { date_str: parsed_json, ... },
        ...
    }
    """
    data = {}
    for date_str in date_dirs:
        date_path = build_dir / date_str
        for json_file in date_path.rglob("*.json"):
            rel = json_file.relative_to(date_path)
            bench_name = str(rel.with_suffix(""))

            try:
                report = json.loads(json_file.read_text())
            except (json.JSONDecodeError, OSError) as e:
                print(f"Warning: skipping {json_file}: {e}", file=sys.stderr)
                continue

            data.setdefault(bench_name, {})[date_str] = report
    return data


def parse_latency_string(s):
    """Parse 'min=7ms, p10=10ms, ...' into {'min': 7, 'p10': 10, ...}."""
    result = {}
    for part in s.split(", "):
        key, value = part.split("=")
        result[key] = int(value.rstrip("ms"))
    return result


def get_combined(workload_data):
    """Extract the combined bucket from a workload, normalizing latencies.

    Returns (latencies_dict, combined_dict) or (None, None) if unavailable.
    """
    combined = workload_data.get("combined")
    if combined is None:
        return None, None
    latencies = combined.get("latencies")
    if latencies is None:
        return None, None
    if isinstance(latencies, str):
        latencies = parse_latency_string(latencies)
    return latencies, combined


def get_query_count_and_qps(report, workload_name):
    """Extract sample count and QPS for a workload from a report.

    QPS is computed as count / duration_secs. Returns (None, None) when the
    data is unavailable.
    """
    workload = report.get("queries", {}).get(workload_name)
    if workload is None:
        return None, None
    _, combined = get_combined(workload)
    if combined is None:
        return None, None
    count = combined.get("count")
    if count is None:
        return None, None
    duration_secs = (report.get("benchmark") or {}).get("duration_secs")
    qps = round(count / duration_secs, 2) if duration_secs else None
    return count, qps


def get_ingest_mb_per_sec(report):
    """Extract ingest+index throughput in MB/sec from a report's ingest section.

    Uses indexed_duration_secs (time from start of upsert through indexing
    completion) when available, falling back to ingest_duration_secs.
    Returns a float or None if the data is unavailable.
    """
    ingest = report.get("ingest")
    if ingest is None:
        return None
    total_bytes = ingest.get("bytes")
    duration_secs = ingest.get("indexed_duration_secs") or ingest.get("ingest_duration_secs")
    if not total_bytes or not duration_secs:
        return None
    return round(total_bytes / duration_secs / 1_000_000, 2)


def build_chart_data(all_data, date_dirs):
    """
    Returns: {
        bench_name: {
            "ingest": {
                "dates": [...],
                "mb_per_sec": [...]
            },
            "workloads": {
                workload_name: {
                    "dates": [...],
                    "p50": [...],
                    "p90": [...],
                    "p99": [...]
                },
                ...
            }
        },
        ...
    }
    """
    charts = {}
    for bench_name in sorted(all_data.keys()):
        bench_reports = all_data[bench_name]

        # Discover all workload names across all dates.
        workload_names = set()
        for report in bench_reports.values():
            workload_names.update(report.get("queries", {}).keys())

        if not workload_names:
            continue

        # Build per-benchmark ingest throughput data.
        ingest_vals = []
        for date_str in date_dirs:
            report = bench_reports.get(date_str)
            if report is None:
                ingest_vals.append(None)
            else:
                ingest_vals.append(get_ingest_mb_per_sec(report))

        # Build per-workload latency data.
        workloads = {}
        for workload_name in sorted(workload_names):
            p50_vals = []
            p90_vals = []
            p99_vals = []
            p999_vals = []
            count_vals = []
            qps_vals = []

            for date_str in date_dirs:
                report = bench_reports.get(date_str)
                if report is None:
                    p50_vals.append(None)
                    p90_vals.append(None)
                    p99_vals.append(None)
                    p999_vals.append(None)
                    count_vals.append(None)
                    qps_vals.append(None)
                    continue

                workload = report.get("queries", {}).get(workload_name)
                if workload is None:
                    p50_vals.append(None)
                    p90_vals.append(None)
                    p99_vals.append(None)
                    p999_vals.append(None)
                    count_vals.append(None)
                    qps_vals.append(None)
                    continue

                latencies, _ = get_combined(workload)
                if latencies is None:
                    p50_vals.append(None)
                    p90_vals.append(None)
                    p99_vals.append(None)
                    p999_vals.append(None)
                else:
                    p50_vals.append(latencies.get("p50"))
                    p90_vals.append(latencies.get("p90"))
                    p99_vals.append(latencies.get("p99"))
                    p999_vals.append(latencies.get("p999"))

                count, qps = get_query_count_and_qps(report, workload_name)
                count_vals.append(count)
                qps_vals.append(qps)

            workloads[workload_name] = {
                "dates": date_dirs,
                "p50": p50_vals,
                "p90": p90_vals,
                "p99": p99_vals,
                "p999": p999_vals,
                "count": count_vals,
                "qps": qps_vals,
            }

        # Extract the benchmark definition TOML from the most recent report.
        definition = None
        for date_str in reversed(date_dirs):
            report = bench_reports.get(date_str)
            if report is not None:
                definition = (report.get("benchmark") or {}).get("definition")
                if definition is not None:
                    break

        charts[bench_name] = {
            "ingest": {
                "dates": date_dirs,
                "mb_per_sec": ingest_vals,
            },
            "workloads": workloads,
            "definition": definition,
        }

    return charts


if __name__ == "__main__":
    main()
