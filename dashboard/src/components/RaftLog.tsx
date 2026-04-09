import type { LogEntry } from "../types";

interface RaftLogProps {
  entries: LogEntry[];
}

export function RaftLog({ entries }: RaftLogProps) {
  if (entries.length === 0) {
    return (
      <div className="log-empty">
        No committed entries yet. Use the Write Tester to add some.
      </div>
    );
  }

  return (
    <>
      {entries.map((entry) => (
        <div key={`${entry.index}-${entry.term}`} className="log-entry">
          <span className="log-index">#{entry.index}</span>
          <span className="log-term">t{entry.term}</span>
          <span className="log-op">{entry.operation}</span>
          <span className="log-detail">
            {entry.key}
            {entry.value !== undefined ? ` → "${entry.value}"` : ""}
          </span>
          <span className="log-check">✓</span>
        </div>
      ))}
    </>
  );
}
