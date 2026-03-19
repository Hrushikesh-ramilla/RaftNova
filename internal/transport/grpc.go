package transport

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
	pb "github.com/hrushikesh-ramilla/raftnova/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// GRPCTransport implements raft.Transport using gRPC.
// Connections are lazily established and cached per peer.
type GRPCTransport struct {
	mu      sync.RWMutex
	conns   map[raft.NodeID]*grpc.ClientConn
	addrs   map[raft.NodeID]string // nodeID -> "host:port"
	logger  *slog.Logger
}

// NewGRPCTransport creates a new gRPC transport.
// addrs maps node IDs to their gRPC addresses (host:port).
func NewGRPCTransport(addrs map[raft.NodeID]string, logger *slog.Logger) *GRPCTransport {
	return &GRPCTransport{
		conns:  make(map[raft.NodeID]*grpc.ClientConn),
		addrs:  addrs,
		logger: logger,
	}
}

// getConn returns a cached or newly established gRPC connection to the target.
// Lazy connect with 3 retries, 100ms backoff.
func (t *GRPCTransport) getConn(target raft.NodeID) (*grpc.ClientConn, error) {
	t.mu.RLock()
	if conn, ok := t.conns[target]; ok {
		t.mu.RUnlock()
		return conn, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock.
	if conn, ok := t.conns[target]; ok {
		return conn, nil
	}

	addr, ok := t.addrs[target]
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", target)
	}

	var conn *grpc.ClientConn
	var err error

	// Retry with backoff.
	for attempt := 0; attempt < 3; attempt++ {
		conn, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             3 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		if err == nil {
			break
		}
		t.logger.Debug("gRPC connect retry",
			"peer", target,
			"attempt", attempt+1,
			"error", err,
		)
		time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		return nil, fmt.Errorf("connect to %s (%s): %w", target, addr, err)
	}

	t.conns[target] = conn
	return conn, nil
}

// SendRequestVote sends a RequestVote RPC to the target peer.
func (t *GRPCTransport) SendRequestVote(ctx context.Context, target raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}

	client := pb.NewRaftClient(conn)
	resp, err := client.RequestVote(ctx, &pb.VoteRequest{
		Term:         uint64(args.Term),
		CandidateId:  string(args.CandidateID),
		LastLogIndex: uint64(args.LastLogIndex),
		LastLogTerm:  uint64(args.LastLogTerm),
	})
	if err != nil {
		t.logger.Debug("RequestVote RPC error",
			"peer", target,
			"term", args.Term,
			"error", err,
		)
		return raft.RequestVoteReply{}, fmt.Errorf("RequestVote to %s: %w", target, err)
	}

	return raft.RequestVoteReply{
		Term:        raft.Term(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

// SendAppendEntries sends an AppendEntries RPC to the target peer.
func (t *GRPCTransport) SendAppendEntries(ctx context.Context, target raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}

	// Convert internal entries to proto.
	protoEntries := make([]*pb.LogEntryProto, len(args.Entries))
	for i, e := range args.Entries {
		protoEntries[i] = &pb.LogEntryProto{
			Term:    uint64(e.Term),
			Index:   uint64(e.Index),
			Command: e.Command,
		}
	}

	client := pb.NewRaftClient(conn)
	resp, err := client.AppendEntries(ctx, &pb.AppendRequest{
		Term:         uint64(args.Term),
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: uint64(args.PrevLogIndex),
		PrevLogTerm:  uint64(args.PrevLogTerm),
		Entries:      protoEntries,
		LeaderCommit: uint64(args.LeaderCommit),
	})
	if err != nil {
		t.logger.Debug("AppendEntries RPC error",
			"peer", target,
			"term", args.Term,
			"error", err,
		)
		return raft.AppendEntriesReply{}, fmt.Errorf("AppendEntries to %s: %w", target, err)
	}

	return raft.AppendEntriesReply{
		Term:          raft.Term(resp.Term),
		Success:       resp.Success,
		ConflictTerm:  raft.Term(resp.ConflictTerm),
		ConflictIndex: raft.LogIndex(resp.ConflictIndex),
	}, nil
}

// Close closes all cached connections.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var firstErr error
	for id, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close conn to %s: %w", id, err)
		}
	}
	t.conns = make(map[raft.NodeID]*grpc.ClientConn)
	return firstErr
}
