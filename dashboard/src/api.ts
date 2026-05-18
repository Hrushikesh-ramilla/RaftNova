import type { NodeStatus, WriteResult } from "./types";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8080";

// Each node gets its own port — node-1 is :8080, node-2 is :8081, etc.
const NODE_PORTS: Record<string, number> = {
  "node-1": 8080,
  "node-2": 8081,
  "node-3": 8082,
  "node-4": 8083,
  "node-5": 8084,
};

function baseUrlForNode(nodeId: string): string {
  const port = NODE_PORTS[nodeId] ?? 8080;
  // Replace the port in the base URL
  const url = new URL(API_BASE);
  url.port = String(port);
  return url.origin;
}

// 1500ms timeout — marks node unreachable fast enough to feel live
export async function fetchNodeStatus(
  nodeId: string
): Promise<NodeStatus | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 1500);

  try {
    const res = await fetch(`${baseUrlForNode(nodeId)}/status`, {
      signal: controller.signal,
    });
    if (!res.ok) return null;
    return (await res.json()) as NodeStatus;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

export async function putKey(
  key: string,
  value: string,
  targetNodeId?: string
): Promise<WriteResult> {
  const start = performance.now();
  const base = targetNodeId
    ? baseUrlForNode(targetNodeId)
    : baseUrlForNode("node-1");

  try {
    // Don't follow redirects automatically — we want to show the redirect chain
    const res = await fetch(`${base}/kv/${encodeURIComponent(key)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
      redirect: "follow",
    });

    const latencyMs = Math.round(performance.now() - start);
    const data = await res.json().catch(() => null);

    return {
      success: res.ok,
      statusCode: res.status,
      data,
      error: res.ok ? null : `HTTP ${res.status}`,
      latencyMs,
      redirectedTo: res.redirected ? res.url : null,
    };
  } catch (err) {
    return {
      success: false,
      statusCode: 0,
      data: null,
      error: err instanceof Error ? err.message : "Unknown error",
      latencyMs: Math.round(performance.now() - start),
      redirectedTo: null,
    };
  }
}

export async function getKey(
  key: string,
  targetNodeId?: string
): Promise<WriteResult> {
  const start = performance.now();
  const base = targetNodeId
    ? baseUrlForNode(targetNodeId)
    : baseUrlForNode("node-1");

  try {
    const res = await fetch(`${base}/kv/${encodeURIComponent(key)}`, {
      redirect: "follow",
    });

    const latencyMs = Math.round(performance.now() - start);
    const data = await res.json().catch(() => null);

    return {
      success: res.ok,
      statusCode: res.status,
      data,
      error: res.ok ? null : `HTTP ${res.status}`,
      latencyMs,
      redirectedTo: res.redirected ? res.url : null,
    };
  } catch (err) {
    return {
      success: false,
      statusCode: 0,
      data: null,
      error: err instanceof Error ? err.message : "Unknown error",
      latencyMs: Math.round(performance.now() - start),
      redirectedTo: null,
    };
  }
}

export async function deleteKey(
  key: string,
  targetNodeId?: string
): Promise<WriteResult> {
  const start = performance.now();
  const base = targetNodeId
    ? baseUrlForNode(targetNodeId)
    : baseUrlForNode("node-1");

  try {
    const res = await fetch(`${base}/kv/${encodeURIComponent(key)}`, {
      method: "DELETE",
      redirect: "follow",
    });

    const latencyMs = Math.round(performance.now() - start);

    return {
      success: res.ok,
      statusCode: res.status,
      data: null,
      error: res.ok ? null : `HTTP ${res.status}`,
      latencyMs,
      redirectedTo: res.redirected ? res.url : null,
    };
  } catch (err) {
    return {
      success: false,
      statusCode: 0,
      data: null,
      error: err instanceof Error ? err.message : "Unknown error",
      latencyMs: Math.round(performance.now() - start),
      redirectedTo: null,
    };
  }
}

export const NODE_IDS = Object.keys(NODE_PORTS);
export { NODE_PORTS };
