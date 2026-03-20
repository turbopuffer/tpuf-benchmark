import { useState, type ReactNode } from "react";

interface ChartContainerProps {
  title: string;
  children: ReactNode;
}

export default function ChartContainer({ title, children }: ChartContainerProps) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className={`chart-container${expanded ? " chart-expanded" : ""}`}>
      <div className="chart-header">
        <h3>{title}</h3>
        <button className="expand-btn" onClick={() => setExpanded(!expanded)} title={expanded ? "Collapse" : "Expand"}>
          {expanded ? "\u2715" : "\u26F6"}
        </button>
      </div>
      {children}
    </div>
  );
}
