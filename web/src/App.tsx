import { getChartData } from "./data";
import Nav from "./components/Nav";
import BenchmarkSection from "./components/BenchmarkSection";

function App() {
  const data = getChartData();
  const benchmarkNames = Object.keys(data.benchmarks).sort();

  return (
    <>
      <h1>tpufbench nightlies</h1>
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
