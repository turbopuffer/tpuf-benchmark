import { Line } from "react-chartjs-2";
import type { ChartOptions } from "chart.js";
import type { IngestData } from "../types";
import ChartContainer from "./ChartContainer";

interface IngestChartProps {
  data: IngestData;
}

const options: ChartOptions<"line"> = {
  responsive: true,
  plugins: {
    tooltip: { mode: "index", intersect: false },
  },
  scales: {
    x: { title: { display: true, text: "Date" } },
    y: {
      title: { display: true, text: "MB/sec" },
      beginAtZero: true,
    },
  },
};

export default function IngestChart({ data }: IngestChartProps) {
  const chartData = {
    labels: data.dates,
    datasets: [
      {
        label: "MB/sec",
        data: data.mb_per_sec,
        borderColor: "#10b981",
        backgroundColor: "rgba(16,185,129,0.1)",
        borderWidth: 2,
        pointRadius: 3,
        tension: 0.1,
        fill: true,
        spanGaps: false,
      },
    ],
  };

  return (
    <ChartContainer title="Ingest &amp; Index Throughput">
      <Line data={chartData} options={options} />
    </ChartContainer>
  );
}
