function toBenchmarkId(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, "-");
}

interface NavProps {
  benchmarkNames: string[];
}

export default function Nav({ benchmarkNames }: NavProps) {
  return (
    <nav>
      {benchmarkNames.map((name, i) => (
        <span key={name}>
          {i > 0 && " \u00b7 "}
          <a href={`#${toBenchmarkId(name)}`}>{name}</a>
        </span>
      ))}
    </nav>
  );
}
