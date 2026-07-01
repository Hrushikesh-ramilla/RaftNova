import { useState, useEffect, useCallback, useRef } from "react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { fetchNodeStatus, NODE_IDS } from "./api";
import type { NodeState, LogEntry, ClusterState } from "./types";
import { MetricsBar } from "./components/MetricsBar";
import { NodeGrid } from "./components/NodeGrid";
import { TopologyGraph } from "./components/TopologyGraph";
import { RaftLog } from "./components/RaftLog";
import { WriteTester } from "./components/WriteTester";
import { ChaosPanel } from "./components/ChaosPanel";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Don't retry on failure — we want to show unreachable fast
      retry: false,
      refetchOnWindowFocus: false,
    },
  },
});

function Dashboard() {
  // Track which nodes are "killed" (client-side simulation —
  // we just stop polling them and mark them unreachable)
  const [killedNodes, setKilledNodes] = useState<Set<string>>(new Set());

  // Track which nodes are "partitioned" (same as killed, client-side)
  const [partitionedNodes, setPartitionedNodes] = useState<Set<string>>(
    new Set()
  );

  // Cluster-wide derived state
  const [clusterState, setClusterState] = useState<ClusterState>({
    nodes: {},
    leaderID: null,
    currentTerm: 0,
    commitIndex: 0,
    electionCount: 0,
    lastLeaderID: null,
  });

  // Raft log entries — synthetic, derived from commit index changes
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const prevCommitRef = useRef(0);
  const electionCountRef = useRef(0);
  const lastLeaderRef = useRef<string | null>(null);

  // Poll all 5 nodes concurrently rather than sequentially —
  // a slow/dead node would block the others if we awaited in series
  const nodeQueries = NODE_IDS.map((id) => {
    const isBlocked = killedNodes.has(id) || partitionedNodes.has(id);
    return useQuery({
      queryKey: ["node-status", id],
      queryFn: () => fetchNodeStatus(id),
      refetchInterval: isBlocked ? false : 1000,
      enabled: !isBlocked,
    });
  });

  // Derive cluster state from individual node poll results
  useEffect(() => {
    const nodes: Record<string, NodeState> = {};
    let leaderID: string | null = null;
    let maxTerm = 0;
    let maxCommit = 0;

    NODE_IDS.forEach((id, i) => {
      const query = nodeQueries[i];
      const isBlocked = killedNodes.has(id) || partitionedNodes.has(id);

      if (isBlocked) {
        nodes[id] = {
          status: clusterState.nodes[id]?.status ?? null,
          reachable: false,
          port: 8080 + i,
        };
        return;
      }

      const status = query.data ?? null;
      const reachable = status !== null;

      nodes[id] = { status, reachable, port: 8080 + i };

      if (status) {
        if (status.state === "leader") leaderID = id;
        if (status.term > maxTerm) maxTerm = status.term;
        if (status.commit_index > maxCommit) maxCommit = status.commit_index;
      }
    });

    // Count elections — detect when leader_id changes
    if (leaderID && leaderID !== lastLeaderRef.current) {
      if (lastLeaderRef.current !== null) {
        electionCountRef.current++;
      }
      lastLeaderRef.current = leaderID;
    }

    // Generate synthetic log entries when commit index advances
    if (maxCommit > prevCommitRef.current) {
      const newEntries: LogEntry[] = [];
      for (let i = prevCommitRef.current + 1; i <= maxCommit; i++) {
        newEntries.push({
          index: i,
          term: maxTerm,
          operation: "PUT",
          key: `key:${i}`,
          value: `value-${i}`,
          timestamp: Date.now(),
        });
      }
      prevCommitRef.current = maxCommit;

      setLogEntries((prev) => [...newEntries, ...prev].slice(0, 20));
    }

    setClusterState({
      nodes,
      leaderID,
      currentTerm: maxTerm,
      commitIndex: maxCommit,
      electionCount: electionCountRef.current,
      lastLeaderID: lastLeaderRef.current,
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeQueries.map((q) => q.dataUpdatedAt).join(",")]);

  const handleKillNode = useCallback((nodeId: string) => {
    setKilledNodes((prev) => new Set(prev).add(nodeId));
  }, []);

  const handleHealAll = useCallback(() => {
    setKilledNodes(new Set());
    setPartitionedNodes(new Set());
  }, []);

  const handlePartitionNode = useCallback((nodeId: string) => {
    setPartitionedNodes((prev) => new Set(prev).add(nodeId));
  }, []);

  return (
    <>
      {/* Topbar */}
      <div className="topbar">
        <div className="topbar-left">
          <span className="topbar-title">RaftNova</span>
          <span className="pulse-dot" />
          <span className="topbar-live">live</span>
        </div>
        <div className="topbar-right">
          term: {clusterState.currentTerm}
        </div>
      </div>

      {/* Main layout */}
      <div className="main-layout">
        {/* Metrics */}
        <div className="metrics-row">
          <MetricsBar
            currentTerm={clusterState.currentTerm}
            commitIndex={clusterState.commitIndex}
            electionCount={clusterState.electionCount}
          />
        </div>

        {/* Middle: Topology + Node Grid */}
        <div className="middle-row">
          <div className="panel">
            <div className="panel-header">Cluster Topology</div>
            <div className="panel-body" style={{ padding: 0 }}>
              <TopologyGraph
                nodes={clusterState.nodes}
                leaderID={clusterState.leaderID}
              />
            </div>
          </div>
          <div className="panel">
            <div className="panel-header">Nodes</div>
            <div className="panel-body" style={{ padding: 0 }}>
              <NodeGrid
                nodes={clusterState.nodes}
                onKillNode={handleKillNode}
              />
            </div>
          </div>
        </div>

        {/* Bottom: Raft Log + Write Tester & Chaos */}
        <div className="bottom-row">
          <div className="panel">
            <div className="panel-header">Raft Log</div>
            <div className="panel-body">
              <RaftLog entries={logEntries} />
            </div>
          </div>
          <div className="panel">
            <div className="panel-header">Write Tester</div>
            <div className="panel-body">
              <WriteTester leaderID={clusterState.leaderID} />
              <ChaosPanel
                leaderID={clusterState.leaderID}
                onKillNode={handleKillNode}
                onHealAll={handleHealAll}
                onPartitionNode={handlePartitionNode}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Dashboard />
    </QueryClientProvider>
  );
}
