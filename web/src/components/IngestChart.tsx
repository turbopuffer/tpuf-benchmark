import { Line } from "react-chartjs-2";
import type { ChartOptions, TooltipItem } from "chart.js";
import type { IngestData } from "../types";
import ChartContainer from "./ChartContainer";
import { buildAnnotations, annotationsByDate } from "../chartAnnotations";

interface IngestChartProps {
  benchmarkName: string;
  data: IngestData;
}

function makeOptions(benchmarkName: string, data: IngestData): ChartOptions<"line"> {
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
            const anns = annByDate.get(data.dates[idx]) ?? [];
            if (anns.length === 0) return "";
            return "\n" + anns.map((a) => `\u{1F4CC} ${a.message}`).join("\n");
          },
        },
      },
    },
    scales: {
      x: { title: { display: true, text: "Date" } },
      y: {
        title: { display: true, text: "MB/sec" },
        beginAtZero: true,
      },
    },
  };
}

export default function IngestChart({ benchmarkName, data }: IngestChartProps) {
  const options = makeOptions(benchmarkName, data);
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
