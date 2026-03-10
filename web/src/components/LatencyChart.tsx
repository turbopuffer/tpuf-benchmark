import { Line } from "react-chartjs-2";
import type { ChartOptions } from "chart.js";
import type { WorkloadData } from "../types";

interface LatencyChartProps {
  workloadName: string;
  data: WorkloadData;
}

const options: ChartOptions<"line"> = {
  responsive: true,
  plugins: {
    tooltip: { mode: "index", intersect: false },
  },
  scales: {
    x: { title: { display: true, text: "Date" } },
    y: {
      title: { display: true, text: "Latency (ms)" },
      beginAtZero: true,
    },
  },
};

export default function LatencyChart({ workloadName, data }: LatencyChartProps) {
  const chartData = {
    labels: data.dates,
    datasets: [  
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
    <div className="chart-container">
      <h3>Query Latency: {workloadName} (combined)</h3>
      <Line data={chartData} options={options} />
    </div>
  );
}
