#!/usr/bin/env python3
"""Generate the tpufbench nightlies dashboard from the build/ results tree.

Stdlib only. Reads build/<date>/<benchmark>.json and writes
build/<date>/index.html, a single self-contained page whose charts are
hand-rolled SVG. Mirrors the presentation of the private nightly dashboard in
the turbopuffer repo (ci/benchmark/generate_report.py), which in turn took it
from script/generate_ann_report.py.
"""

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

MAX_DATA_POINTS = 180

ANNOTATIONS = [
    {
        "date": "2026-04-22",
        "message": "QPS increased to 8",
        "benchmarks": ["website/fulltext-10m-cold", "website/vector-10m-cold"],
    },
]

def main():
    parser = argparse.ArgumentParser(description="Generate benchmark latency charts")
    parser.add_argument("build_dir", type=Path, help="Path to the build/ directory")
    args = parser.parse_args()

    build_dir = args.build_dir.resolve()
    if not build_dir.is_dir():
        print(f"Error: {build_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    today = date.today().isoformat()
    output_dir = build_dir / today
    output_dir.mkdir(parents=True, exist_ok=True)

    date_dirs = find_date_dirs(build_dir)
    if not date_dirs:
        print("No date directories found in build/", file=sys.stderr)
        sys.exit(1)

    all_data = collect_reports(build_dir, date_dirs)
    chart_data = build_chart_data(all_data, date_dirs)

    payload = {
        "meta": {
            "generated": today,
            "num_dates": len(date_dirs),
            "dates": date_dirs,
        },
        "annotations": ANNOTATIONS,
        "benchmarks": chart_data,
    }

    html = INDEX_TEMPLATE.replace(
        "DATA_JSON_PLACEHOLDER", json.dumps(payload, separators=(",", ":"))
    )

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

INDEX_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>tpufbench nightlies</title>
<style>
  :root {
    color-scheme: light dark;
    --surface: #fcfcfb;
    --ink: #0b0b0b;
    --ink-2: #52514e;
    --muted: #898781;
    --grid: #e1e0d9;
    --axis: #c3c2b7;
    --border: rgba(11, 11, 11, 0.10);
    --hover: rgba(11, 11, 11, 0.04);
    --code: rgba(11, 11, 11, 0.05);
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --surface: #17171a;
      --ink: #f1f1ee;
      --ink-2: #b6b5b0;
      --muted: #8b8a85;
      --grid: #2e2e32;
      --axis: #4b4b50;
      --border: rgba(255, 255, 255, 0.14);
      --hover: rgba(255, 255, 255, 0.06);
      --code: rgba(255, 255, 255, 0.07);
    }
  }
  body { font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         margin: 0 auto; max-width: 1200px; padding: 2rem 1.25rem 4rem;
         color: var(--ink); }
  h1 { font-size: 1.5rem; margin: 0 0 .25rem; }
  h2 { font-size: 1.15rem; margin: 0 0 .5rem; padding-bottom: .4rem;
       border-bottom: 1px solid var(--grid); }
  .sub { color: var(--ink-2); margin: 0 0 .35rem; }
  .sub:last-of-type { margin-bottom: 1.5rem; }
  code { background: var(--code); padding: .1em .4em; border-radius: 3px; font-size: .9em; }
  a { color: inherit; }
  footer { margin-top: 3rem; color: var(--muted); font-size: 12px; }

  nav { margin: 0 0 1.5rem; color: var(--muted); }
  nav a { color: var(--ink-2); text-decoration: none; }
  nav a:hover { color: var(--ink); text-decoration: underline; }

  .filter-row { display: flex; align-items: center; gap: 8px; margin: 0 0 1.5rem; flex-wrap: wrap; }
  .filter-label { color: var(--ink-2); }
  .range-btn { border: 1px solid var(--border); background: none; padding: 4px 10px; border-radius: 6px;
    font: inherit; font-size: 0.85em; color: var(--ink-2); cursor: pointer; }
  .range-btn:hover { background: var(--hover); }
  .range-btn[aria-pressed="true"] { background: var(--ink); border-color: var(--ink); color: var(--surface); }

  .bench { margin: 0 0 2.75rem; }
  .definition { margin: .75rem 0 0; }
  .definition summary { cursor: pointer; color: var(--ink-2); font-size: .9em; padding: 4px 0; }
  .definition summary:hover { color: var(--ink); }
  .definition pre { background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
    padding: 14px; overflow-x: auto; margin: .5rem 0 0; font-size: .82em; line-height: 1.5; }
  .definition pre code { background: none; padding: 0; font-size: inherit; }

  .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); gap: 16px; margin: 12px 0 0; }
  .chart-card { margin: 0; background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
    padding: 12px 12px 8px; min-width: 0; }
  .chart-card.chart-expanded { position: fixed; inset: 0; z-index: 100; border-radius: 0; border: none;
    padding: 24px; overflow: auto; }
  .chart-head { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; margin-bottom: 4px; }
  .chart-title { font-size: 0.95em; font-weight: 600; margin: 0; }
  .chart-right { margin-left: auto; display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; }
  .legend { display: flex; gap: 10px; font-size: 0.8em; color: var(--ink-2); flex-wrap: wrap; }
  .legend .key { display: inline-flex; align-items: center; gap: 5px; }
  .legend .key i, .viz-tooltip .tt-row i { width: 14px; height: 0; border-top: 3px solid; border-radius: 2px; display: inline-block; }
  .chart-actions { display: flex; gap: 6px; }
  .chart-btn { border: 1px solid var(--border); background: none; padding: 2px 8px; border-radius: 6px;
    font: inherit; font-size: 0.75em; color: var(--ink-2); cursor: pointer; line-height: 1.6; }
  .chart-btn:hover { background: var(--hover); }
  .chart-box { position: relative; }
  .chart-card svg { display: block; }
  .chart-card svg:focus-visible { outline: 2px solid #2a78d6; outline-offset: 2px; border-radius: 4px; }
  .chart-card svg text { font: 11px system-ui, sans-serif; fill: var(--muted); font-variant-numeric: tabular-nums; }
  .chart-card svg text.endlabel { fill: var(--ink-2); }

  .viz-tooltip { position: absolute; pointer-events: none; display: none; background: var(--surface);
    border: 1px solid var(--border); box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12); border-radius: 6px;
    padding: 6px 9px; font-size: 12px; white-space: nowrap; z-index: 10; }
  .viz-tooltip .tt-date { color: var(--muted); margin-bottom: 4px; }
  .viz-tooltip .tt-row { display: flex; align-items: center; gap: 6px; margin-top: 2px; }
  .viz-tooltip .tt-val { font-weight: 600; font-variant-numeric: tabular-nums; }
  .viz-tooltip .tt-label { color: var(--ink-2); }
  .viz-tooltip .tt-extra { margin-left: 20px; }
  .viz-tooltip .tt-note { color: var(--ink-2); margin-top: 4px; padding-top: 4px; border-top: 1px solid var(--border); }

  .mini-table-wrap { overflow-x: auto; }
  .mini-table-wrap table { border-collapse: collapse; width: 100%; font-size: 0.85em; margin-top: 8px;
    font-variant-numeric: tabular-nums; }
  .mini-table-wrap th, .mini-table-wrap td { text-align: right; padding: 4px 10px;
    border-bottom: 1px solid var(--grid); white-space: nowrap; }
  .mini-table-wrap th:first-child, .mini-table-wrap td:first-child { text-align: left; }
  .mini-table-wrap thead th { border-bottom: 2px solid var(--axis); font-weight: 600; color: var(--ink-2); }
  .note { color: var(--muted); margin: 8px 0; }
</style>
</head>
<body>
<h1>tpufbench nightlies</h1>
<p class="sub">All nightly benchmark results are from production turbopuffer in the <code>gcp-us-central1</code> region.</p>
<p class="sub" id="meta-line"></p>
<nav id="nav"></nav>
<div class="filter-row">
  <span class="filter-label">Range:</span>
  <button class="range-btn" data-days="30">30 days</button>
  <button class="range-btn" data-days="90">90 days</button>
  <button class="range-btn" data-days="365">1 year</button>
  <button class="range-btn" data-days="" aria-pressed="true">All</button>
</div>
<p class="note" id="status" hidden></p>
<div id="results"></div>
<footer>
  Generated by <code>generate_charts.py</code> from the reports under <code>build/&lt;date&gt;/</code>.
</footer>
<script>
const DATA = DATA_JSON_PLACEHOLDER;

// Categorical palette slots 1-20 (light mode), assigned to series in a fixed
// order so a given series keeps its color everywhere. Ordered so adjacent
// slots stay distinct under color-vision deficiency.
const SERIES_COLORS = [
  '#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948',
  '#00a3c4', '#a32626', '#7a9c1e', '#1c5cab', '#d2a35c', '#8626c0', '#00917b', '#9c5b16',
  '#b02e8c', '#56b4e9', '#a63a5e', '#9c6ade',
];
// Percentiles share one cool-to-hot ramp drawn from the palette above, so p50
// is the same blue on every chart on the page.
const PCTILES = [
  { key: 'p50', label: 'p50', slot: 0 },
  { key: 'p90', label: 'p90', slot: 3 },
  { key: 'p99', label: 'p99', slot: 1 },
  { key: 'p999', label: 'p99.9', slot: 9 },
];
const INGEST_SLOT = 2;
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const GAP_BREAK_MS = 7 * 86400000;

const state = { rangeDays: null };
let redraws = [];

function el(tag, attrs = {}, ...children) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') n.className = v;
    else n.setAttribute(k, v);
  }
  n.append(...children);
  return n;
}

function svg(tag, attrs = {}) {
  const n = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  return n;
}

function toId(name) { return name.replace(/[^a-zA-Z0-9]/g, '-'); }

function parseDateMs(d) {
  const [y, m, day] = d.split('-').map(Number);
  return Date.UTC(y, m - 1, day);
}

function fmtDateTick(d, withYear) {
  const [y, m, day] = d.split('-').map(Number);
  return MONTHS[m - 1] + ' ' + day + (withYear ? " '" + String(y).slice(2) : '');
}

function niceTicks(lo, hi, count) {
  const span = hi - lo;
  const step0 = span / Math.max(1, count);
  const mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const step = [1, 2, 5, 10].map(m => m * mag).find(s => span / s <= count) || 10 * mag;
  const ticks = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + step * 1e-6; v += step) ticks.push(v);
  return { step, ticks };
}

// Ticks are spread evenly in time (not run index) so uneven nightly cadence
// can't bunch labels, then thinned to a minimum pixel gap.
function pickTickIdxs(xs, maxTicks, minGap) {
  const n = xs.length;
  if (n <= 2) return [...Array(n).keys()];
  const cand = new Set([0, n - 1]);
  for (let j = 1; j < maxTicks - 1; j++) {
    const target = xs[0] + (xs[n - 1] - xs[0]) * j / (maxTicks - 1);
    let best = 0;
    for (let i = 1; i < n; i++) {
      if (Math.abs(xs[i] - target) < Math.abs(xs[best] - target)) best = i;
    }
    cand.add(best);
  }
  const out = [];
  for (const i of [...cand].sort((a, b) => a - b)) {
    if (!out.length || xs[i] - xs[out[out.length - 1]] >= minGap) out.push(i);
  }
  while (out.length > 1 && out[out.length - 1] !== n - 1 && xs[n - 1] - xs[out[out.length - 1]] < minGap) out.pop();
  if (out[out.length - 1] !== n - 1) out.push(n - 1);
  return out;
}

function seriesExtent(series, positiveOnly) {
  let lo = Infinity, hi = -Infinity;
  for (const s of series) {
    for (const v of s.values.values()) {
      if (positiveOnly && !(v > 0)) continue;
      if (v < lo) lo = v;
      if (v > hi) hi = v;
    }
  }
  return [lo, hi];
}

function drawLineChart(box, spec, height) {
  box.replaceChildren();
  const W = Math.max(box.clientWidth || 0, 300);
  const H = height || 240;
  const r2 = n => Math.round(n * 100) / 100;

  const yt = spec.ticks ? { ticks: spec.ticks } : niceTicks(spec.lo, spec.hi, 5);
  const tickTexts = yt.ticks.map(spec.fmtTick);
  const M = {
    top: 12, right: spec.endLabels ? 62 : 14, bottom: 26,
    left: Math.max(...tickTexts.map(t => t.length)) * 6.5 + 14,
  };
  const plotW = W - M.left - M.right, plotH = H - M.top - M.bottom;
  const bottom = M.top + plotH, right = M.left + plotW;

  // x domain comes from spec.xDates (shared page-wide) so every chart spans
  // the same date range regardless of its own coverage.
  const axisTs = spec.xDates.map(parseDateMs);
  let t0 = axisTs[0], t1 = axisTs[axisTs.length - 1];
  if (t0 === t1) { t0 -= 43200000; t1 += 43200000; }
  const xAt = t => r2(M.left + (t - t0) / (t1 - t0) * plotW);
  const axisXs = axisTs.map(xAt);
  const ts = spec.dates.map(parseDateMs);
  const xs = ts.map(xAt);
  // spec.lo/hi live in transformed space, so a value is mapped through fwd
  // first. fwd can be undefined (linear) and can reject a value outright
  // (log10 of a non-positive number), so every call site checks for a finite
  // result rather than letting NaN/-Infinity reach the SVG geometry.
  const fwd = spec.fwd || (v => v);
  const y = v => r2(bottom - (fwd(v) - spec.lo) / (spec.hi - spec.lo) * plotH);

  const root = svg('svg', { width: W, height: H, tabindex: 0, role: 'img', 'aria-label': spec.ariaLabel });

  yt.ticks.forEach((tv, i) => {
    const gy = y(tv);
    root.append(svg('line', { x1: M.left, x2: right, y1: gy, y2: gy, stroke: 'var(--grid)', 'stroke-width': 1 }));
    const lbl = svg('text', { x: M.left - 6, y: gy + 3.5, 'text-anchor': 'end' });
    lbl.textContent = tickTexts[i];
    root.append(lbl);
  });
  root.append(svg('line', { x1: M.left, x2: right, y1: bottom, y2: bottom, stroke: 'var(--axis)', 'stroke-width': 1 }));

  // Noteworthy events (a QPS change, a deploy) get a dashed rule, so a step in
  // the line reads as the thing that caused it rather than as a regression.
  for (const a of spec.annotations) {
    const ax = xAt(parseDateMs(a.date));
    if (!Number.isFinite(ax)) continue;
    root.append(svg('line', { x1: ax, x2: ax, y1: M.top, y2: bottom, stroke: 'var(--axis)',
                              'stroke-width': 1, 'stroke-dasharray': '5 4' }));
  }

  const withYear = spec.xDates[0].slice(0, 4) !== spec.xDates[spec.xDates.length - 1].slice(0, 4);
  const tickGap = withYear ? 78 : 62;
  for (const i of pickTickIdxs(axisXs, Math.max(2, Math.min(7, Math.floor(plotW / 80))), tickGap)) {
    const tx = Math.max(M.left + 18, Math.min(right - 18, axisXs[i]));
    const lbl = svg('text', { x: tx, y: bottom + 17, 'text-anchor': 'middle' });
    lbl.textContent = fmtDateTick(spec.xDates[i], withYear);
    root.append(lbl);
  }

  const endInfo = [];
  for (const s of spec.series) {
    const pts = [];
    spec.dates.forEach((d, i) => {
      const v = s.values.get(d);
      if (v == null) return;
      const py = y(v);
      if (Number.isFinite(py)) pts.push({ i, v, px: xs[i], py });
    });
    if (!pts.length) continue;
    // Break the line on nights a benchmark did not run and on long outages,
    // so a data gap never draws as a fake trend.
    const conn = (a, b) => b.i === a.i + 1 && ts[b.i] - ts[a.i] <= GAP_BREAK_MS;
    let d = '', prev = null;
    for (const p of pts) {
      d += (prev && conn(prev, p) ? ' L' : ' M') + p.px + ' ' + p.py;
      prev = p;
    }
    root.append(svg('path', { d: d.trim(), fill: 'none', stroke: s.color, 'stroke-width': 2, 'stroke-linejoin': 'round', 'stroke-linecap': 'round' }));
    // Dot every point when the shared date domain is sparse, and on
    // gap-isolated points so they stay visible. The threshold is on the
    // domain, not this series' own point count, so a benchmark that started
    // running recently is drawn like the one beside it.
    const sparse = spec.xDates.length <= 16;
    pts.forEach((p, j) => {
      const isolated = !(pts[j - 1] && conn(pts[j - 1], p)) && !(pts[j + 1] && conn(p, pts[j + 1]));
      if (sparse || isolated || j === pts.length - 1) {
        root.append(svg('circle', { cx: p.px, cy: p.py, r: 4, fill: s.color, stroke: 'var(--surface)', 'stroke-width': 2 }));
      }
    });
    endInfo.push({ s, end: pts[pts.length - 1] });
  }

  // Direct end labels, nudged apart with leader lines when lines converge or
  // when a series ends before the shared domain's right edge.
  if (spec.endLabels) {
    endInfo.sort((a, b) => a.end.py - b.end.py);
    let prev = -Infinity;
    for (const e of endInfo) { e.ly = Math.max(e.end.py, prev + 13); prev = e.ly; }
    let limit = bottom;
    for (let i = endInfo.length - 1; i >= 0; i--) {
      endInfo[i].ly = Math.min(endInfo[i].ly, limit);
      limit = endInfo[i].ly - 13;
    }
    for (const e of endInfo) {
      if (Math.abs(e.ly - e.end.py) > 5 || right - e.end.px > 8) {
        root.append(svg('line', { x1: e.end.px + 6, y1: e.end.py, x2: right + 6, y2: e.ly, stroke: 'var(--axis)', 'stroke-width': 1 }));
      }
      const lbl = svg('text', { x: right + 9, y: e.ly + 3.5, class: 'endlabel' });
      lbl.textContent = e.s.label;
      root.append(lbl);
    }
  }

  // Hover layer: crosshair snaps to the nearest shared axis date so every
  // chart reads the same date at the same x, one tooltip lists every series,
  // same readout on keyboard focus.
  const tip = el('div', { class: 'viz-tooltip' });
  const cross = svg('g', { 'pointer-events': 'none' });
  cross.style.display = 'none';
  root.append(cross);
  let activeIdx = null;

  function show(i) {
    activeIdx = i;
    const d = spec.xDates[i];
    cross.style.display = '';
    cross.replaceChildren(svg('line', { x1: axisXs[i], x2: axisXs[i], y1: M.top, y2: bottom, stroke: 'var(--axis)', 'stroke-width': 1 }));
    tip.replaceChildren(el('div', { class: 'tt-date' }, d));
    for (const s of spec.series) {
      const v = s.values.get(d);
      const key = el('i');
      key.style.borderTopColor = s.color;
      tip.append(el('div', { class: 'tt-row' }, key,
        el('span', { class: 'tt-val' }, v == null ? '–' : spec.fmtVal(v)),
        el('span', { class: 'tt-label' }, s.label)));
      if (v != null && Number.isFinite(y(v))) {
        cross.append(svg('circle', { cx: axisXs[i], cy: y(v), r: 4, fill: s.color, stroke: 'var(--surface)', 'stroke-width': 2 }));
      }
    }
    // Sample count and offered QPS are context for the latencies above rather
    // than series of their own, so they read as unkeyed rows under them.
    for (const x of spec.extras) {
      const v = x.values.get(d);
      if (v == null) continue;
      tip.append(el('div', { class: 'tt-row tt-extra' },
        el('span', { class: 'tt-val' }, x.fmt(v)),
        el('span', { class: 'tt-label' }, x.label)));
    }
    for (const a of spec.annByDate.get(d) || []) {
      tip.append(el('div', { class: 'tt-note' }, '\u{1F4CC} ' + a.message));
    }
    tip.style.display = 'block';
    tip.style.left = (axisXs[i] + 14 + tip.offsetWidth > W ? axisXs[i] - tip.offsetWidth - 14 : axisXs[i] + 14) + 'px';
    tip.style.top = M.top + 'px';
  }
  function hide() {
    activeIdx = null;
    cross.style.display = 'none';
    tip.style.display = 'none';
  }

  const overlay = svg('rect', { x: M.left, y: M.top, width: plotW, height: plotH, fill: 'transparent' });
  overlay.addEventListener('pointermove', ev => {
    const px = ev.clientX - root.getBoundingClientRect().left;
    let best = 0;
    for (let i = 1; i < axisXs.length; i++) {
      if (Math.abs(axisXs[i] - px) < Math.abs(axisXs[best] - px)) best = i;
    }
    show(best);
  });
  overlay.addEventListener('pointerleave', hide);
  root.addEventListener('keydown', ev => {
    if (ev.key === 'ArrowLeft' || ev.key === 'ArrowRight') {
      const cur = activeIdx == null ? spec.xDates.length - 1 : activeIdx;
      show(Math.max(0, Math.min(spec.xDates.length - 1, cur + (ev.key === 'ArrowLeft' ? -1 : 1))));
      ev.preventDefault();
    } else if (ev.key === 'Escape') {
      hide();
    }
  });
  root.addEventListener('blur', hide);
  root.append(overlay);
  box.append(root, tip);
}

function buildTable(spec) {
  const hr = el('tr', {}, el('th', {}, 'Date'));
  for (const s of spec.series) hr.append(el('th', {}, s.label));
  for (const x of spec.extras) hr.append(el('th', {}, x.label));
  const tbody = el('tbody');
  for (let i = spec.dates.length - 1; i >= 0; i--) {
    const d = spec.dates[i];
    const cells = [...spec.series, ...spec.extras].map(s => s.values.get(d));
    if (cells.every(v => v == null)) continue;
    const row = el('tr', {}, el('td', {}, d));
    spec.series.forEach(s => {
      const v = s.values.get(d);
      row.append(el('td', {}, v == null ? '–' : spec.fmtVal(v)));
    });
    spec.extras.forEach(x => {
      const v = x.values.get(d);
      row.append(el('td', {}, v == null ? '–' : x.fmt(v)));
    });
    tbody.append(row);
  }
  return el('table', {}, el('thead', {}, hr), tbody);
}

function chartCard(spec) {
  const head = el('div', { class: 'chart-head' }, el('h3', { class: 'chart-title' }, spec.title));
  const right = el('div', { class: 'chart-right' });
  const legend = el('div', { class: 'legend' });
  for (const s of spec.series) {
    const key = el('i');
    key.style.borderTopColor = s.color;
    legend.append(el('span', { class: 'key' }, key, s.label));
  }
  right.append(legend);
  const toggle = el('button', { class: 'chart-btn', type: 'button', 'aria-pressed': 'false' }, 'Table');
  const expand = el('button', { class: 'chart-btn', type: 'button', 'aria-pressed': 'false',
                                title: 'Expand' }, '⛶');
  right.append(el('div', { class: 'chart-actions' }, toggle, expand));
  head.append(right);
  const box = el('div', { class: 'chart-box' });
  const tableWrap = el('div', { class: 'mini-table-wrap' });
  tableWrap.hidden = true;
  const card = el('figure', { class: 'chart-card' }, head, box, tableWrap);
  // Expanded charts get the viewport's height rather than the card's, which is
  // the point of expanding one.
  const draw = () => {
    if (box.hidden) return;
    drawLineChart(box, spec, card.classList.contains('chart-expanded')
      ? Math.max(320, window.innerHeight - 170) : 240);
  };
  let tableBuilt = false;
  toggle.addEventListener('click', () => {
    const showTable = tableWrap.hidden;
    if (showTable && !tableBuilt) {
      tableWrap.append(buildTable(spec));
      tableBuilt = true;
    }
    tableWrap.hidden = !showTable;
    box.hidden = showTable;
    toggle.textContent = showTable ? 'Chart' : 'Table';
    toggle.setAttribute('aria-pressed', String(showTable));
    draw();
  });
  expand.addEventListener('click', () => {
    const expanded = card.classList.toggle('chart-expanded');
    expand.textContent = expanded ? '✕' : '⛶';
    expand.title = expanded ? 'Collapse' : 'Expand';
    expand.setAttribute('aria-pressed', String(expanded));
    draw();
  });
  return { card, draw };
}

// One series per label, values pulled out of the parallel date/value arrays the
// generator emits. A date with no run (or no value) is simply absent from the
// map, which drawLineChart renders as a gap.
function mkSeries(label, slot, dates, values, keep) {
  const map = new Map();
  (dates || []).forEach((d, i) => {
    const v = (values || [])[i];
    if (keep.has(d) && v != null && Number.isFinite(v)) map.set(d, v);
  });
  return map.size ? { label, color: SERIES_COLORS[slot % SERIES_COLORS.length], values: map } : null;
}

function mkExtra(label, dates, values, keep, fmt) {
  const s = mkSeries(label, 0, dates, values, keep);
  return s && { label, values: s.values, fmt };
}

// Axis bounds with headroom, so a flat series doesn't hug the frame and small
// regressions stay visible. `minPad` floors that headroom and `include` is a
// value the axis must cover whether or not the data reaches it.
function axisSpec(series, unit, { minPad = 1, include, log = false } = {}) {
  let [lo, hi] = seriesExtent(series, log);
  if (include != null) {
    lo = Math.min(lo, include);
    hi = Math.max(hi, include);
  }
  if (log) return logAxisSpec(lo, hi, unit);
  const pad = Math.max((hi - lo) * 0.1, hi * 0.02, minPad);
  lo = Math.max(0, lo - pad);
  hi += pad;
  if (hi <= lo) hi = lo + minPad;
  const dec = hi - lo < 1 ? 2 : hi - lo < 5 ? 1 : 0;
  const num = v => v.toLocaleString('en-US', { maximumFractionDigits: dec });
  return { lo, hi, fmtTick: num, fmtVal: v => num(v) + ' ' + unit };
}

// A log10 axis, used for latency. p50 and p99.9 on one chart routinely differ
// by more than an order of magnitude, and on a linear axis that pins p50 and
// p90 to the bottom gridline where a doubling is invisible. In log space a 2x
// regression is the same distance whatever percentile it happens to. Ticks land
// on 1/2/5 per decade.
function logAxisSpec(lo, hi, unit) {
  // Only positive values have a log. Fall back rather than emit NaN bounds.
  if (!(lo > 0) || !Number.isFinite(lo)) lo = hi > 0 && Number.isFinite(hi) ? hi : 1;
  if (!(hi > 0) || !Number.isFinite(hi)) hi = lo;
  if (hi < lo) { const t = lo; lo = hi; hi = t; }
  let l0 = Math.log10(lo), l1 = Math.log10(hi);
  const pad = Math.max((l1 - l0) * 0.08, 0.05);
  l0 -= pad;
  l1 += pad;
  const ticks = [];
  for (let k = Math.floor(l0); k <= Math.ceil(l1); k++) {
    for (const m of [1, 2, 5]) {
      const t = m * Math.pow(10, k);
      const lt = Math.log10(t);
      if (lt >= l0 && lt <= l1) ticks.push(t);
    }
  }
  const num = v => v.toLocaleString('en-US', { maximumFractionDigits: v < 10 ? 2 : 0 });
  return { lo: l0, hi: l1, fwd: v => Math.log10(v), ticks, fmtTick: num,
           fmtVal: v => num(v) + ' ' + unit };
}

// Put every latency chart in a benchmark on one y axis, since a row of them is
// only worth putting side by side if the heights mean the same thing.
function shareYAxis(charts) {
  const axis = axisSpec(charts.flatMap(c => c.series), charts[0].unit, charts[0].axisOpts);
  for (const chart of charts) Object.assign(chart, axis);
}

function lineSpec(title, unit, series, xDates, opts = {}) {
  if (!series.length) return null;
  const annotations = opts.annotations || [];
  const annByDate = new Map();
  for (const a of annotations) {
    if (!annByDate.has(a.date)) annByDate.set(a.date, []);
    annByDate.get(a.date).push(a);
  }
  return {
    title: title + ' (' + unit + ')',
    ariaLabel: title + ' over time. Arrow keys step through runs.',
    series, dates: xDates, xDates, endLabels: opts.endLabels ?? true, unit, axisOpts: opts,
    annotations, annByDate, extras: opts.extras || [],
    ...axisSpec(series, unit, opts),
  };
}

function annotationsFor(name, keep) {
  return (DATA.annotations || []).filter(
    a => (!a.benchmarks || a.benchmarks.includes(name)) && keep.has(a.date));
}

const intFmt = v => v.toLocaleString('en-US');
const qpsFmt = v => v.toLocaleString('en-US', { maximumFractionDigits: 2 });

function benchmarkSpecs(name, entry, xDates) {
  const keep = new Set(xDates);
  const anns = annotationsFor(name, keep);
  const latency = [];
  for (const w of Object.keys(entry.workloads || {}).sort()) {
    const wd = entry.workloads[w];
    const series = PCTILES
      .map(p => mkSeries(p.label, p.slot, wd.dates, wd[p.key], keep))
      .filter(Boolean);
    const extras = [
      mkExtra('samples', wd.dates, wd.count, keep, intFmt),
      mkExtra('req/s', wd.dates, wd.qps, keep, qpsFmt),
    ].filter(Boolean);
    const spec = lineSpec(w, 'ms', series, xDates, { log: true, annotations: anns, extras });
    if (spec) latency.push(spec);
  }
  // Latency charts share an axis; throughput is a different unit, so it keeps
  // its own.
  if (latency.length > 1) shareYAxis(latency);
  const ingest = mkSeries('MB/s', INGEST_SLOT, (entry.ingest || {}).dates,
                          (entry.ingest || {}).mb_per_sec, keep);
  const ingestSpec = ingest
    ? lineSpec('Ingest & index throughput', 'MB/s', [ingest], xDates, { annotations: anns })
    : null;
  return ingestSpec ? [...latency, ingestSpec] : latency;
}

function benchmarkNames() {
  return Object.keys(DATA.benchmarks).sort().reverse();
}

function render() {
  const container = document.getElementById('results');
  const status = document.getElementById('status');
  redraws = [];
  container.replaceChildren();

  // Every chart shares the x domain of the whole range, so the same date sits
  // at the same x in every card.
  let xDates = DATA.meta.dates || [];
  if (state.rangeDays != null && xDates.length) {
    const cutoff = parseDateMs(xDates[xDates.length - 1]) - state.rangeDays * 86400000;
    xDates = xDates.filter(d => parseDateMs(d) >= cutoff);
  }

  let sections = 0;
  if (xDates.length) {
    for (const name of benchmarkNames()) {
      const specs = benchmarkSpecs(name, DATA.benchmarks[name], xDates);
      if (!specs.length) continue;
      sections++;
      const section = el('section', { class: 'bench', id: toId(name) }, el('h2', {}, name));
      const definition = DATA.benchmarks[name].definition;
      if (definition) {
        section.append(el('details', { class: 'definition' },
          el('summary', {}, 'Benchmark definition'),
          el('pre', {}, el('code', {}, definition))));
      }
      const grid = el('div', { class: 'chart-grid' });
      for (const spec of specs) {
        const { card, draw } = chartCard(spec);
        grid.append(card);
        redraws.push(draw);
      }
      section.append(grid);
      container.append(section);
    }
  }

  status.textContent = !sections ? 'No runs in the selected range.'
    : xDates.length === 1 ? 'Only one run in range, trends appear once more nights accumulate.' : '';
  status.hidden = !status.textContent;
  redraws.forEach(f => f());
}

(function init() {
  document.getElementById('meta-line').textContent =
    'Generated: ' + DATA.meta.generated + ' · Showing data across ' + DATA.meta.num_dates + ' dates';

  const nav = document.getElementById('nav');
  benchmarkNames().forEach((name, i) => {
    if (i > 0) nav.append(' · ');
    nav.append(el('a', { href: '#' + toId(name) }, name));
  });

  document.querySelectorAll('.range-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.range-btn').forEach(b => b.setAttribute('aria-pressed', 'false'));
      btn.setAttribute('aria-pressed', 'true');
      state.rangeDays = btn.dataset.days ? Number(btn.dataset.days) : null;
      render();
    });
  });

  let resizeTimer;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => redraws.forEach(f => f()), 150);
  });

  render();
  // Sections only exist after the first render, so the browser's own jump to
  // the fragment has already missed.
  if (location.hash) {
    const target = document.getElementById(location.hash.slice(1));
    if (target) target.scrollIntoView();
  }
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
