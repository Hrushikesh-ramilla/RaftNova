# Contributing to RaftNova

## Code Structure

```
cmd/server/          Entry point — flag parsing, component wiring, graceful shutdown
internal/
  raft/
    types.go         Core types: NodeID, Term, LogIndex, State, RPC arg/reply structs
    log.go           In-memory Raft log (1-indexed, sentinel at 0)
    node.go          Raft node: elections, RequestVote/AppendEntries handlers
    replication.go   Leader's log replication loop, commit pipeline, client commands
    transport.go     Transport interface (decouples core from network)
    errors.go        Sentinel errors
  storage/
    wal.go           Append-only WAL with CRC32, fsync, rotation at 64MB
  transport/
    grpc.go          gRPC client — connection pooling, lazy connect, type conversion
    server.go        gRPC server — delegates to Node handlers
  api/
    handler.go       HTTP API: PUT/GET/DELETE /kv/{key}, /status, /healthz, /readyz
  kvstore/
    store.go         KV state machine — map[string]string with Apply()
  config/
    config.go        Config from flags/env
pkg/proto/           Protobuf definitions and generated stubs
tests/chaos/         Chaos test harness with in-process cluster
```

## Building

```bash
go build ./...
```

## Testing

```bash
# Unit tests
go test -v ./internal/...

# Chaos tests (correctness invariants)
go test -v -timeout 120s ./tests/chaos/...
```

## Style

- Go standard formatting (`gofmt`)
- Single mutex per node — no nested locks
- Comments on non-obvious logic only
- Proper error wrapping: `fmt.Errorf("context: %w", err)`
- No panics in library code
