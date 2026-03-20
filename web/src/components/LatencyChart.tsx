import { Line } from "react-chartjs-2";
import type { ChartOptions, TooltipItem } from "chart.js";
import type { WorkloadData } from "../types";
import ChartContainer from "./ChartContainer";

interface LatencyChartProps {
  workloadName: string;
  data: WorkloadData;
}

function makeOptions(data: WorkloadData): ChartOptions<"line"> {
  return {
    responsive: true,
    plugins: {
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
            return lines.length > 0 ? "\n" + lines.join("\n") : "";
          },
        },
      },
    },
    scales: {
      x: { title: { display: true, text: "Date" } },
      y: {
        title: { display: true, text: "Latency (ms)" },
        beginAtZero: true,
      },
    },
  };
}

export default function LatencyChart({ workloadName, data }: LatencyChartProps) {
  const options = makeOptions(data);

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
