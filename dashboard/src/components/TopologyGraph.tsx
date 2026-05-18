import { useEffect, useRef } from "react";
import * as d3 from "d3";
import type { NodeState } from "../types";
import { NODE_IDS } from "../api";

interface TopologyGraphProps {
  nodes: Record<string, NodeState>;
  leaderID: string | null;
}

// Pentagon positions — computed once for 5 nodes.
// Rotated so node-1 is at the top.
function pentagonPositions(
  cx: number,
  cy: number,
  r: number
): [number, number][] {
  return Array.from({ length: 5 }, (_, i) => {
    const angle = (2 * Math.PI * i) / 5 - Math.PI / 2;
    return [cx + r * Math.cos(angle), cy + r * Math.sin(angle)] as [
      number,
      number,
    ];
  });
}

export function TopologyGraph({ nodes, leaderID }: TopologyGraphProps) {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    const svg = d3.select(svgRef.current);
    const width = svgRef.current?.clientWidth ?? 400;
    const height = svgRef.current?.clientHeight ?? 300;
    const cx = width / 2;
    const cy = height / 2;
    const radius = Math.min(width, height) * 0.32;

    const positions = pentagonPositions(cx, cy, radius);

    // Clear and rebuild — simpler than complex update patterns for 5 nodes
    svg.selectAll("*").remove();

    const defs = svg.append("defs");

    // Heartbeat pulse dot marker
    defs
      .append("marker")
      .attr("id", "arrowhead")
      .attr("viewBox", "0 0 6 6")
      .attr("refX", 3)
      .attr("refY", 3)
      .attr("markerWidth", 4)
      .attr("markerHeight", 4)
      .append("circle")
      .attr("cx", 3)
      .attr("cy", 3)
      .attr("r", 2)
      .attr("fill", "#1D9E75");

    // Draw edges between all pairs
    for (let i = 0; i < 5; i++) {
      for (let j = i + 1; j < 5; j++) {
        svg
          .append("line")
          .attr("x1", positions[i][0])
          .attr("y1", positions[i][1])
          .attr("x2", positions[j][0])
          .attr("y2", positions[j][1])
          .attr("stroke", "rgba(0,0,0,0.08)")
          .attr("stroke-width", 0.5);
      }
    }

    // Draw heartbeat arrows from leader to each follower
    const leaderIndex = NODE_IDS.indexOf(leaderID ?? "");
    if (leaderIndex >= 0) {
      NODE_IDS.forEach((id, i) => {
        if (id === leaderID) return;
        const nodeState = nodes[id];
        if (!nodeState?.reachable) return;

        const [x1, y1] = positions[leaderIndex];
        const [x2, y2] = positions[i];

        // Animated dot traveling along the edge
        const dot = svg
          .append("circle")
          .attr("r", 2.5)
          .attr("fill", "#1D9E75")
          .attr("opacity", 0.7);

        function animateDot() {
          dot
            .attr("cx", x1)
            .attr("cy", y1)
            .attr("opacity", 0.8)
            .transition()
            .duration(1200)
            .ease(d3.easeLinear)
            .attr("cx", x2)
            .attr("cy", y2)
            .attr("opacity", 0.2)
            .on("end", () => {
              // Stagger restart so dots don't all sync up
              setTimeout(animateDot, Math.random() * 400);
            });
        }
        // Stagger initial start per follower
        setTimeout(animateDot, i * 200);
      });
    }

    // Draw nodes
    NODE_IDS.forEach((id, i) => {
      const [x, y] = positions[i];
      const nodeState = nodes[id];
      const isLeader = id === leaderID;
      const isReachable = nodeState?.reachable ?? false;

      const r = isLeader ? 28 : 22;

      const fill = isLeader
        ? "#1D9E75"
        : isReachable
        ? "#e8e7e3"
        : "#FCEBEB";

      const stroke = isLeader
        ? "#178a63"
        : isReachable
        ? "rgba(0,0,0,0.1)"
        : "#E24B4A";

      const g = svg.append("g");

      g.append("circle")
        .attr("cx", x)
        .attr("cy", y)
        .attr("r", r)
        .attr("fill", fill)
        .attr("stroke", stroke)
        .attr("stroke-width", isReachable ? 1 : 1.5)
        .attr("stroke-dasharray", isReachable ? "none" : "4,3")
        .style("transition", "all 400ms ease");

      g.append("text")
        .attr("x", x)
        .attr("y", y + 1)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "central")
        .attr("font-size", isLeader ? 11 : 10)
        .attr("font-weight", 500)
        .attr("font-family", "var(--mono)")
        .attr("fill", isLeader ? "#ffffff" : "var(--text-secondary)")
        .text(id.replace("node-", "n"));
    });
  }, [nodes, leaderID]);

  return (
    <div className="topology-container">
      <svg ref={svgRef} />
    </div>
  );
}
