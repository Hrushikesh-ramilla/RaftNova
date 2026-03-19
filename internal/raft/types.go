package raft

import "fmt"

// NodeID uniquely identifies a node in the Raft cluster.
type NodeID string

// Term represents a Raft election term. Monotonically increasing.
type Term uint64

// LogIndex represents the position of an entry in the Raft log. 1-indexed.
type LogIndex uint64

// State represents the current role of a Raft node.
type State int

const (
	Follower  State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    Term
	Index   LogIndex
	Command []byte
}

// RequestVoteArgs contains the arguments for a RequestVote RPC.
type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

// RequestVoteReply contains the response for a RequestVote RPC.
type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

// AppendEntriesArgs contains the arguments for an AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

// AppendEntriesReply contains the response for an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    Term
	Success bool
	// Conflict hint fields for fast log rollback. When Success is false
	// and the follower's log is inconsistent, ConflictTerm is the term of
	// the conflicting entry and ConflictIndex is the first index of that term.
	ConflictTerm  Term
	ConflictIndex LogIndex
}
