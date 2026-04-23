// Annotations mark noteworthy events on benchmark charts.
// Add new entries here — they will render as vertical lines with hover tooltips.
// If `benchmarks` is omitted, the annotation applies to every benchmark.

export interface Annotation {
  date: string;
  message: string;
  benchmarks?: string[];
}

export const ANNOTATIONS: Annotation[] = [
  {
    date: "2026-04-22",
    message: "QPS increased to 8",
    benchmarks: ["website/fulltext-10m-cold", "website/vector-10m-cold"],
  },
];

export function annotationsFor(benchmarkName: string): Annotation[] {
  return ANNOTATIONS.filter(
    (a) => !a.benchmarks || a.benchmarks.includes(benchmarkName),
  );
}
