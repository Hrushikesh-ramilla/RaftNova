import { useState } from "react";
import { putKey } from "../api";

interface ChaosPanelProps {
  leaderID: string | null;
  onKillNode: (nodeId: string) => void;
  onHealAll: () => void;
  onPartitionNode: (nodeId: string) => void;
}

export function ChaosPanel({
  leaderID,
  onKillNode,
  onHealAll,
  onPartitionNode,
}: ChaosPanelProps) {
  const [flooding, setFlooding] = useState(false);
  const [floodProgress, setFloodProgress] = useState(0);
  const [floodResult, setFloodResult] = useState<string | null>(null);

  async function handleFloodWrites() {
    if (!leaderID) return;
    setFlooding(true);
    setFloodProgress(0);
    setFloodResult(null);

    const total = 100;
    const start = performance.now();
    let succeeded = 0;

    for (let i = 0; i < total; i++) {
      const res = await putKey(
        `flood:${Date.now()}-${i}`,
        `value-${i}`,
        leaderID
      );
      if (res.success) succeeded++;
      setFloodProgress(((i + 1) / total) * 100);
    }

    const elapsed = ((performance.now() - start) / 1000).toFixed(1);
    const throughput = Math.round(succeeded / parseFloat(elapsed));
    setFloodResult(
      `${succeeded}/${total} succeeded · ${elapsed}s · ~${throughput} ops/s`
    );
    setFlooding(false);
  }

  return (
    <div className="chaos-panel">
      <span className="chaos-label">Chaos Controls</span>
      <div className="chaos-buttons">
        <button
          className="chaos-btn danger"
          onClick={() => leaderID && onKillNode(leaderID)}
          disabled={!leaderID}
        >
          Kill Leader
        </button>
        <button className="chaos-btn" onClick={onHealAll}>
          Heal All
        </button>
        <button
          className="chaos-btn"
          onClick={() => onPartitionNode("node-5")}
        >
          Partition node-5
        </button>
        <button
          className="chaos-btn"
          onClick={handleFloodWrites}
          disabled={flooding || !leaderID}
        >
          {flooding ? "Flooding..." : "Flood Writes (100)"}
        </button>
      </div>

      {flooding && (
        <div className="progress-bar">
          <div
            className="progress-fill"
            style={{ width: `${floodProgress}%` }}
          />
        </div>
      )}

      {floodResult && <div className="flood-stats">{floodResult}</div>}
    </div>
  );
}
