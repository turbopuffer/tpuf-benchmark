import type { ChartPageData } from "./types";

export function getChartData(): ChartPageData {
  const data = window.__TPUFBENCH_DATA__;
  if (!data) {
    throw new Error(
      "No benchmark data found. Expected window.__TPUFBENCH_DATA__ to be set."
    );
  }
  return data;
}
