import type { NodeState } from "../types";
import { NODE_IDS } from "../api";
import { NodeCard } from "./NodeCard";

interface NodeGridProps {
  nodes: Record<string, NodeState>;
  onKillNode: (nodeId: string) => void;
}

export function NodeGrid({ nodes, onKillNode }: NodeGridProps) {
  return (
    <div className="node-grid">
      {NODE_IDS.map((id) => (
        <NodeCard
          key={id}
          nodeId={id}
          nodeState={nodes[id] ?? { status: null, reachable: false, port: 8080 }}
          onKill={onKillNode}
        />
      ))}
    </div>
  );
}
