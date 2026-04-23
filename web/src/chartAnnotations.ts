import type { AnnotationOptions } from "chartjs-plugin-annotation";
import { annotationsFor, type Annotation } from "./annotations";

// Build chartjs-plugin-annotation config for a benchmark, scoped to the dates
// actually plotted. Annotations whose date is outside the plotted range are
// skipped.
export function buildAnnotations(
  benchmarkName: string,
  dates: string[],
): Record<string, AnnotationOptions> {
  const dateSet = new Set(dates);
  const result: Record<string, AnnotationOptions> = {};
  annotationsFor(benchmarkName).forEach((ann, i) => {
    if (!dateSet.has(ann.date)) return;
    result[`ann-${i}`] = {
      type: "line",
      xMin: ann.date,
      xMax: ann.date,
      borderColor: "rgba(107, 114, 128, 0.8)",
      borderWidth: 2,
      borderDash: [6, 4],
    };
  });
  return result;
}

// Returns a map from date -> annotation messages that apply to the benchmark.
// Used to enrich the chart tooltip's afterBody content on hover.
export function annotationsByDate(
  benchmarkName: string,
): Map<string, Annotation[]> {
  const map = new Map<string, Annotation[]>();
  for (const ann of annotationsFor(benchmarkName)) {
    const list = map.get(ann.date) ?? [];
    list.push(ann);
    map.set(ann.date, list);
  }
  return map;
}
