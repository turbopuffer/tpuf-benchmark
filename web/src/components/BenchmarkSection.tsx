import type { BenchmarkData } from "../types";
import LatencyChart from "./LatencyChart";
import IngestChart from "./IngestChart";

function toBenchmarkId(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, "-");
}

interface BenchmarkSectionProps {
  name: string;
  data: BenchmarkData;
}

export default function BenchmarkSection({ name, data }: BenchmarkSectionProps) {
  const workloadNames = Object.keys(data.workloads).sort();

  return (
    <div className="chart-section" id={toBenchmarkId(name)}>
      <h2>{name}</h2>
      {workloadNames.map((workloadName) => (
        <LatencyChart
          key={workloadName}
          workloadName={workloadName}
          data={data.workloads[workloadName]}
        />
      ))}
      <IngestChart data={data.ingest} />
    </div>
  );
}
