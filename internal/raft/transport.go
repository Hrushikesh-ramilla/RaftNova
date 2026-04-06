package raft

import "context"

// Transport defines the interface for sending RPCs between Raft nodes.
// Implementations must be safe for concurrent use.
type Transport interface {
	SendRequestVote(ctx context.Context, target NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, target NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
}
