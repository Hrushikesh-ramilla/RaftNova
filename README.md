# RaftNova

A distributed, crash-consistent key-value store implementing the Raft consensus algorithm from scratch in Go. This is not a toy — it implements the full Raft paper with crash-safe durability, correctness-first invariants, and real engineering fixes for bugs that only surface under failure conditions.

## Why This Problem Is Hard

Building a correct distributed KV store — not just a fast one — requires solving several problems simultaneously:

**Split votes.** When multiple followers timeout at the same instant, they all become candidates and split the vote. No one gets a majority. The cluster is leaderless. Raft solves this with randomized election timeouts (150–300ms), but getting the distribution right matters — too tight and you get correlated timeouts, too wide and failover is slow.

**Leader crash mid-commit.** A leader accepts a write, persists it to its own WAL, replicates it to one follower, then crashes. The entry is on 2/5 nodes. Is it committed? No — but the client might have received an ack. The new leader might or might not have the entry. Raft's answer: the entry isn't committed until a majority has it, and only the leader decides when commitIndex advances. But there's a subtlety — Figure 8.

**Log divergence after partition.** Node-1 is partitioned from the rest. It accepts writes that never replicate. Meanwhile, the majority elects a new leader and makes progress. When the partition heals, node-1 has entries that conflict with the true log. Raft handles this through the AppendEntries consistency check — the leader probes backward until it finds agreement, then overwrites the divergent suffix.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for sequence diagrams, WAL format spec, and component breakdown.

## Correctness Invariants

These are enforced in code with inline comments at the enforcement site:

1. **`currentTerm` only increases** — Monotonically increasing term ensures stale leaders step down.
2. **`votedFor` persisted before any vote reply** — Prevents double-voting after crash recovery.
3. **Committed entries never deleted** — Log truncation only removes entries beyond commitIndex.
4. **Leader only commits entries from its own term** — The Figure 8 safety rule. A leader cannot commit entries from previous terms by counting replicas alone.

## Real Bugs Hit and Fixed

These are actual bugs discovered and fixed during development, in chronological order.

### Bug 1: Election timer not resetting on AppendEntries

**Commit:** `fix(raft): reset election timer on AppendEntries — was causing split votes`

**Symptom:** Under load, followers would timeout and start elections even while receiving valid heartbeats from the leader. Cluster would enter repeated election cycles.

**Root cause:** AppendEntries handler processed entries but forgot to call `resetElectionTimer()`. The timer fired even on valid heartbeat receipt.

**Fix:** One line. But finding it took two hours because the symptom (split votes) looked like a vote-counting bug.

**Lesson:** In Raft, the election timer is the heartbeat of the follower's trust in the leader. Missing a single reset breaks the entire liveness guarantee.

### Bug 2: Double-counting votes

**Commit:** `fix(raft): double-counting votes when peer sends duplicate reply`

**Symptom:** Occasionally a node would believe it had majority and become leader even with only 2 actual votes.

**Root cause:** A peer could respond to RequestVote twice (retry on timeout + delayed original). Vote counter was incremented both times.

**Fix:** Added `votesReceived map[NodeID]bool`. Only count first response per peer.

**Lesson:** Network can deliver duplicates. Idempotency is not optional in distributed systems.

### Bug 3: WAL replay not restoring votedFor

**Commit:** `fix(storage): WAL replay was not restoring votedFor — critical bug`

**Symptom:** After a crash and restart, a node could vote for two different candidates in the same term — directly violating the election safety property.

**Root cause:** WAL replay reconstructed `currentTerm` correctly but skipped `votedFor`. The node restarted as if it had never voted.

**Fix:** Add RecordVote to WAL replay path. Restore both term and votedFor atomically.

**Lesson:** Every piece of Raft state that affects safety must be persisted. Missing votedFor isn't a performance bug — it's a correctness violation that could elect two leaders.

### Bug 4: Advancing commitIndex without term check (Figure 8)

**Commit:** `fix(raft): commitIndex advance must check entry.term == currentTerm (Figure 8)`

**Symptom:** Stale reads after leader change. A key written under term 3 would return an old value after a new leader took over in term 4.

**Root cause:** Leader was committing entries from previous terms by counting matchIndex majority — which violates Raft Figure 8. An entry replicated to majority in term 3 can be overwritten by a new leader if the original leader crashes before committing it.

**Fix:** In `advanceCommitIndex()`, add: `if log.TermAt(idx) != currentTerm { continue }`. Only commit entries from the current term.

**Lesson:** Raft Figure 8 is the hardest part of the paper for a reason. The term check is not obvious and violating it allows data loss.

### Bug 5: nextIndex initialization causing missed entries after leader crash

**Commit:** `fix(raft): committed entries lost after leader crash — nextIndex init bug`

**Symptom:** `TestCommitSurvivesLeaderCrash` failed — keys written and acked by the old leader were absent after new leader election.

**Root cause:** New leader initialized `nextIndex[peer] = lastLogIndex + 1` using its own log length, which was shorter than some followers. Followers with longer logs were sent empty AppendEntries and their extra entries were truncated.

**Fix:** Leader must not truncate follower logs that are ahead. The AppendEntries truncation logic only applies to *conflicting* entries, not extra valid ones.

**Lesson:** Log length is not a proxy for log correctness. A newly elected leader must discover each follower's actual log state through the AppendEntries probe protocol, not assume all followers match its own log.

## Performance

```
Benchmark: sequential writes, 100-byte values, no pipelining

3-node cluster:   ~1,200 ops/sec   P50: 2.1ms   P99: 8.4ms
5-node cluster:   ~900 ops/sec     P50: 2.8ms   P99: 11.2ms

Latency floor explanation:
Every write pays: WAL fsync (producer) + gRPC to 2 peers + WAL fsync (peers) + majority ack
5-node adds one more peer round-trip vs 3-node — hence lower throughput.
The fsync is the dominant cost.
```

## API Reference

### Write a key

```bash
curl -X PUT http://localhost:8080/kv/mykey -d '{"value": "hello"}'
```

```json
{"key": "mykey", "value": "hello", "index": 42, "term": 3}
```

### Read a key

```bash
curl http://localhost:8080/kv/mykey
```

```json
{"key": "mykey", "value": "hello"}
```

Reads are linearizable — only the leader serves reads. Followers return `307 Temporary Redirect` to the leader.

### Delete a key

```bash
curl -X DELETE http://localhost:8080/kv/mykey
```

### Cluster status

```bash
curl http://localhost:8080/status
```

```json
{
  "node_id": "node-1",
  "state": "leader",
  "term": 4,
  "commit_index": 142,
  "last_applied": 142,
  "peers": ["node-2", "node-3", "node-4", "node-5"],
  "leader_id": "node-1"
}
```

### Health probes

```bash
curl http://localhost:8080/healthz   # liveness
curl http://localhost:8080/readyz    # readiness (checks apply lag)
```

### Prometheus metrics

```bash
curl http://localhost:8080/metrics
```

## Running

### Local (single node for testing)

```bash
go build -o raftnova ./cmd/server
./raftnova --node-id=node-1 --http-addr=:8080 --grpc-addr=:9090
```

### Docker Compose (5-node cluster)

```bash
docker-compose up --build -d

# Check cluster health
curl http://localhost:8080/status
curl http://localhost:8081/status
```

Nodes are available at:
- HTTP: ports 8080–8084
- gRPC: ports 9090–9094

### Run tests

```bash
# Unit tests
go test -v ./internal/...

# Chaos tests (correctness invariants)
go test -v -timeout 120s ./tests/chaos/...
```

## Future Work

- **Log compaction** — Snapshot state machine periodically, discard prefix of log
- **Snapshots** — Install snapshot on lagging followers instead of replaying entire log
- **Lease reads** — Read without redirect using leader lease, trading linearizability for latency
- **Dynamic membership** — Add/remove nodes at runtime via joint consensus
- **Batched writes** — Amortize fsync cost across multiple client requests
- **Read indices** — Confirm leadership via heartbeat round before serving read
