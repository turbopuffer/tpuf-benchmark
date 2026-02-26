export interface WorkloadData {
  dates: string[];
  p50: (number | null)[];
  p90: (number | null)[];
  p99: (number | null)[];
}

export interface IngestData {
  dates: string[];
  mb_per_sec: (number | null)[];
}

export interface BenchmarkData {
  ingest: IngestData;
  workloads: Record<string, WorkloadData>;
}

export interface ChartPageMeta {
  generated: string;
  num_dates: number;
}

export interface ChartPageData {
  meta: ChartPageMeta;
  benchmarks: Record<string, BenchmarkData>;
}

declare global {
  interface Window {
    __TPUFBENCH_DATA__?: ChartPageData;
  }
}
