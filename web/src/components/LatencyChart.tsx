import { Line } from "react-chartjs-2";
import type { ChartOptions, TooltipItem } from "chart.js";
import type { WorkloadData } from "../types";
import ChartContainer from "./ChartContainer";
import { buildAnnotations, annotationsByDate } from "../chartAnnotations";

interface LatencyChartProps {
  benchmarkName: string;
  workloadName: string;
  data: WorkloadData;
}

function makeOptions(benchmarkName: string, data: WorkloadData): ChartOptions<"line"> {
  const annByDate = annotationsByDate(benchmarkName);
  return {
    responsive: true,
    plugins: {
      annotation: {
        annotations: buildAnnotations(benchmarkName, data.dates),
      },
      tooltip: {
        mode: "index",
        intersect: false,
        callbacks: {
          afterBody(items: TooltipItem<"line">[]) {
            if (items.length === 0) return "";
            const idx = items[0].dataIndex;
            const lines: string[] = [];
            const count = data.count[idx];
            if (count != null) {
              lines.push(`Samples: ${count.toLocaleString()}`);
            }
            const qps = data.qps[idx];
            if (qps != null) {
              lines.push(`QPS: ${qps}`);
            }
            const anns = annByDate.get(data.dates[idx]) ?? [];
            for (const a of anns) {
              lines.push(`\u{1F4CC} ${a.message}`);
            }
            return lines.length > 0 ? "\n" + lines.join("\n") : "";
          },
        },
      },
    },
    scales: {
      x: { title: { display: true, text: "Date" } },
      y: {
        type: "logarithmic",
        title: { display: true, text: "Latency (ms)" },
        afterBuildTicks(axis: { ticks: { value: number }[] }) {
          // Find data range from existing ticks
          const values = axis.ticks.map((t: { value: number }) => t.value).filter((v: number) => v > 0);
          if (values.length === 0) return;
          const min = Math.min(...values);
          const max = Math.max(...values);
          // Generate powers of 2 covering the range
          const ticks: { value: number }[] = [];
          let v = Math.pow(2, Math.floor(Math.log2(min)));
          while (v <= max * 1.5) {
            ticks.push({ value: v });
            v *= 2;
          }
          axis.ticks = ticks;
        },
        ticks: {
          callback(value: string | number) {
            return String(Number(value));
          },
        },
      },
    },
  };
}

export default function LatencyChart({ benchmarkName, workloadName, data }: LatencyChartProps) {
  const options = makeOptions(benchmarkName, data);

  const chartData = {
    labels: data.dates,
    datasets: [
      ...(data.p999.some((v) => v != null)
        ? [
            {
              label: "p99.9",
              data: data.p999,
              borderColor: "#dc2626",
              backgroundColor: "rgba(220,38,38,0.1)",
              borderWidth: 2,
              pointRadius: 3,
              tension: 0.1,
              spanGaps: false,
            },
          ]
        : []),
      {
        label: "p99",
        data: data.p99,
        borderColor: "#ef4444",
        backgroundColor: "rgba(239,68,68,0.1)",
        borderWidth: 2,
        pointRadius: 3,
        tension: 0.1,
        spanGaps: false,
      },
      {
        label: "p90",
        data: data.p90,
        borderColor: "#f59e0b",
        backgroundColor: "rgba(245,158,11,0.1)",
        borderWidth: 2,
        pointRadius: 3,
        tension: 0.1,
        spanGaps: false,
      },
      {
        label: "p50",
        data: data.p50,
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59,130,246,0.1)",
        borderWidth: 2,
        pointRadius: 3,
        tension: 0.1,
        spanGaps: false,
      },
    ],
  };

  return (
    <ChartContainer title={`Query Latency: ${workloadName} (combined)`}>
      <Line data={chartData} options={options} />
    </ChartContainer>
  );
}
