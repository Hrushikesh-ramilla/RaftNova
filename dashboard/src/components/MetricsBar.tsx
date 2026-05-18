import { useEffect, useRef, useState } from "react";

interface MetricsBarProps {
  currentTerm: number;
  commitIndex: number;
  electionCount: number;
}

export function MetricsBar({
  currentTerm,
  commitIndex,
  electionCount,
}: MetricsBarProps) {
  // Track the commit delta per second for throughput calculation.
  // Using ref so we don't cause extra renders on every tick.
  const prevCommitRef = useRef(commitIndex);
  const prevTimeRef = useRef(Date.now());
  const [throughput, setThroughput] = useState(0);

  useEffect(() => {
    const now = Date.now();
    const elapsed = (now - prevTimeRef.current) / 1000;
    if (elapsed > 0.5) {
      const delta = commitIndex - prevCommitRef.current;
      setThroughput(Math.max(0, Math.round(delta / elapsed)));
      prevCommitRef.current = commitIndex;
      prevTimeRef.current = now;
    }
  }, [commitIndex]);

  return (
    <div className="metrics-bar">
      <MetricCard label="Current Term" value={currentTerm} />
      <MetricCard label="Commit Index" value={commitIndex} />
      <MetricCard label="Throughput" value={throughput} suffix=" ops/s" />
      <MetricCard label="Elections" value={electionCount} />
    </div>
  );
}

function MetricCard({
  label,
  value,
  suffix = "",
}: {
  label: string;
  value: number;
  suffix?: string;
}) {
  const [flash, setFlash] = useState(false);
  const prevValue = useRef(value);

  useEffect(() => {
    if (value !== prevValue.current) {
      setFlash(true);
      prevValue.current = value;
      const timer = setTimeout(() => setFlash(false), 300);
      return () => clearTimeout(timer);
    }
  }, [value]);

  return (
    <div className="metric-card">
      <span className="metric-label">{label}</span>
      <span className={`metric-value${flash ? " flash" : ""}`}>
        {value.toLocaleString()}
        {suffix}
      </span>
    </div>
  );
}
