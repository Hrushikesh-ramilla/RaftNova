export interface NodeStatus {
  node_id: string;
  state: "leader" | "follower" | "candidate";
  term: number;
  commit_index: number;
  last_applied: number;
  leader_id: string;
  peers: string[];
}

export interface NodeState {
  status: NodeStatus | null;
  reachable: boolean;
  port: number;
}

export interface ClusterState {
  nodes: Record<string, NodeState>;
  leaderID: string | null;
  currentTerm: number;
  commitIndex: number;
  electionCount: number;
  lastLeaderID: string | null;
}

export interface WriteResult {
  success: boolean;
  statusCode: number;
  data: Record<string, unknown> | null;
  error: string | null;
  latencyMs: number;
  redirectedTo: string | null;
}

export interface LogEntry {
  index: number;
  term: number;
  operation: string;
  key: string;
  value?: string;
  timestamp: number;
}
