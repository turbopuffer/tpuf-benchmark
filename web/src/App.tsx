import { getChartData } from "./data";
import Nav from "./components/Nav";
import BenchmarkSection from "./components/BenchmarkSection";

function App() {
  const data = getChartData();
  const benchmarkNames = Object.keys(data.benchmarks).sort().reverse();

  return (
    <>
      <h1>tpufbench nightlies</h1>
      <p className="subtitle">
        All nightly benchmark results are from production turbopuffer in the{" "}
        <code>gcp-us-central1</code> region.
      </p>
      <p className="subtitle">
        Generated: {data.meta.generated} &middot; Showing data across{" "}
        {data.meta.num_dates} dates
      </p>
      <Nav benchmarkNames={benchmarkNames} />
      {benchmarkNames.map((name) => (
        <BenchmarkSection
          key={name}
          name={name}
          data={data.benchmarks[name]}
        />
      ))}
    </>
  );
}

export default App;
