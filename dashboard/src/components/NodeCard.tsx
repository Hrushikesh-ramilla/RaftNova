import type { NodeState } from "../types";
import { NODE_PORTS } from "../api";

interface NodeCardProps {
  nodeId: string;
  nodeState: NodeState;
  onKill: (nodeId: string) => void;
}

export function NodeCard({ nodeId, nodeState, onKill }: NodeCardProps) {
  const { status, reachable } = nodeState;
  const port = NODE_PORTS[nodeId] ?? 8080;

  const state = reachable && status ? status.state : "unreachable";
  const isLeader = state === "leader";

  const dotColor =
    state === "leader"
      ? "green"
      : state === "follower"
      ? "green"
      : state === "candidate"
      ? "amber"
      : "red";

  return (
    <div className={`node-card${isLeader ? " is-leader" : ""}`}>
      <div className="node-card-header">
        <span className="node-card-id">{nodeId}</span>
        <span className={`node-dot ${dotColor}`} />
      </div>

      <span className={`node-badge ${state}`}>{state}</span>

      <div className="node-stat">
        <span className="node-stat-label">term</span>
        <span className="node-stat-value">{status?.term ?? "—"}</span>
      </div>
      <div className="node-stat">
        <span className="node-stat-label">commit</span>
        <span className="node-stat-value">{status?.commit_index ?? "—"}</span>
      </div>
      <div className="node-stat">
        <span className="node-stat-label">applied</span>
        <span className="node-stat-value">{status?.last_applied ?? "—"}</span>
      </div>
      <div className="node-stat">
        <span className="node-stat-label">port</span>
        <span className="node-stat-value">:{port}</span>
      </div>

      {reachable && (
        <button className="kill-btn" onClick={() => onKill(nodeId)}>
          kill
        </button>
      )}
    </div>
  );
}
