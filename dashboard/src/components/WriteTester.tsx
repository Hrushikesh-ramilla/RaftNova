import { useState } from "react";
import { putKey, getKey, deleteKey } from "../api";
import type { WriteResult } from "../types";

interface WriteTesterProps {
  leaderID: string | null;
}

export function WriteTester({ leaderID }: WriteTesterProps) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [result, setResult] = useState<WriteResult | null>(null);
  const [loading, setLoading] = useState(false);

  async function handlePut() {
    if (!key || !value) return;
    setLoading(true);
    const res = await putKey(key, value, leaderID ?? undefined);
    setResult(res);
    setLoading(false);
  }

  async function handleGet() {
    if (!key) return;
    setLoading(true);
    const res = await getKey(key, leaderID ?? undefined);
    setResult(res);
    setLoading(false);
  }

  async function handleDelete() {
    if (!key) return;
    setLoading(true);
    const res = await deleteKey(key, leaderID ?? undefined);
    setResult(res);
    setLoading(false);
  }

  function formatResult(r: WriteResult): string {
    if (!r.success) {
      return `error: ${r.error} · ${r.latencyMs}ms`;
    }

    const parts: string[] = [`${r.statusCode}`];

    if (r.data) {
      if ("index" in r.data) parts.push(`index ${r.data.index}`);
      if ("term" in r.data) parts.push(`term ${r.data.term}`);
      if ("value" in r.data) parts.push(`"${r.data.value}"`);
      if ("key" in r.data && !("index" in r.data))
        parts.push(`key "${r.data.key}"`);
    }

    parts.push(`${r.latencyMs}ms`);

    const base = parts.join(" · ");
    if (r.redirectedTo) {
      return `→ redirected · ${base}`;
    }
    return base;
  }

  return (
    <div className="write-tester">
      <div className="write-inputs">
        <input
          className="write-input"
          placeholder="key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handlePut()}
        />
        <input
          className="write-input"
          placeholder="value"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handlePut()}
        />
      </div>

      <div className="write-actions">
        <button
          className="action-btn primary"
          onClick={handlePut}
          disabled={loading || !key || !value}
        >
          PUT
        </button>
        <button
          className="action-btn"
          onClick={handleGet}
          disabled={loading || !key}
        >
          GET
        </button>
        <button
          className="action-btn"
          onClick={handleDelete}
          disabled={loading || !key}
        >
          DELETE
        </button>
      </div>

      {result && (
        <div className={`write-result ${result.success ? "success" : "error"}`}>
          {formatResult(result)}
        </div>
      )}
    </div>
  );
}
