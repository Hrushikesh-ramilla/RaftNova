package transport

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
	pb "github.com/hrushikesh-ramilla/raftnova/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// GRPCServer wraps a gRPC server that implements the Raft service.
// It delegates all RPC handling to the Raft Node.
type GRPCServer struct {
	pb.UnimplementedRaftServer
	node   *raft.Node
	server *grpc.Server
	logger *slog.Logger
}

// NewGRPCServer creates a new gRPC server that delegates RPCs to the given node.
func NewGRPCServer(node *raft.Node, logger *slog.Logger) *GRPCServer {
	s := &GRPCServer{
		node:   node,
		logger: logger,
	}

	s.server = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterRaftServer(s.server, s)

	return s
}

// Serve starts the gRPC server on the given address.
func (s *GRPCServer) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.logger.Info("gRPC server listening", "addr", addr)
	return s.server.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *GRPCServer) Stop() {
	s.server.GracefulStop()
}

// RequestVote implements the Raft.RequestVote RPC.
func (s *GRPCServer) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	args := raft.RequestVoteArgs{
		Term:         raft.Term(req.Term),
		CandidateID:  raft.NodeID(req.CandidateId),
		LastLogIndex: raft.LogIndex(req.LastLogIndex),
		LastLogTerm:  raft.Term(req.LastLogTerm),
	}

	reply := s.node.HandleRequestVote(args)

	return &pb.VoteResponse{
		Term:        uint64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

// AppendEntries implements the Raft.AppendEntries RPC.
func (s *GRPCServer) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{
			Term:    raft.Term(e.Term),
			Index:   raft.LogIndex(e.Index),
			Command: e.Command,
		}
	}

	args := raft.AppendEntriesArgs{
		Term:         raft.Term(req.Term),
		LeaderID:     raft.NodeID(req.LeaderId),
		PrevLogIndex: raft.LogIndex(req.PrevLogIndex),
		PrevLogTerm:  raft.Term(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: raft.LogIndex(req.LeaderCommit),
	}

	reply := s.node.HandleAppendEntries(args)

	return &pb.AppendResponse{
		Term:          uint64(reply.Term),
		Success:       reply.Success,
		ConflictTerm:  uint64(reply.ConflictTerm),
		ConflictIndex: uint64(reply.ConflictIndex),
	}, nil
}
