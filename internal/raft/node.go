package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/hrushikesh-ramilla/raftnova/internal/storage"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// Node implements a single Raft consensus node. It manages elections,
// log replication, and state transitions. All mutable state is protected
// by a single mutex — simpler than RWMutex, eliminates deadlocks from
// bidirectional RPC callbacks.
type Node struct {
	// Identity
	id    NodeID
	peers []NodeID

	// Persistent state — persisted to WAL before any reply
	currentTerm Term
	votedFor    NodeID
	log         *RaftLog

	// Volatile state
	state       State
	commitIndex LogIndex
	lastApplied LogIndex
	leaderID    NodeID

	// Leader-only volatile state
	nextIndex  map[NodeID]LogIndex
	matchIndex map[NodeID]LogIndex

	// Election and heartbeat
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	votes          int
	votesReceived  map[NodeID]bool

	// Dependencies
	transport Transport
	wal       *storage.WAL
	logger    *slog.Logger

	// Channels
	applyCh   chan LogEntry
	stopCh    chan struct{}
	commandCh chan *commandRequest

	// Mutex — single lock for all state
	mu sync.Mutex

	// Replication cancel — used to stop replication goroutines on leader stepdown
	replicateCancel context.CancelFunc

	// Metrics
	leaderChanges    int64
	committedEntries int64
	electionStart    time.Time
}

type commandRequest struct {
	command []byte
	result  chan commandResult
}

type commandResult struct {
	index LogIndex
	term  Term
	err   error
}

// NewNode creates a new Raft node. Call Start() to begin participating in the cluster.
func NewNode(id NodeID, peers []NodeID, transport Transport, wal *storage.WAL, logger *slog.Logger) *Node {
	n := &Node{
		id:            id,
		peers:         peers,
		currentTerm:   0,
		votedFor:      "",
		log:           NewRaftLog(),
		state:         Follower,
		commitIndex:   0,
		lastApplied:   0,
		leaderID:      "",
		nextIndex:     make(map[NodeID]LogIndex),
		matchIndex:    make(map[NodeID]LogIndex),
		votesReceived: make(map[NodeID]bool),
		transport:     transport,
		wal:           wal,
		logger:        logger.With("node", string(id)),
		applyCh:       make(chan LogEntry, 256),
		stopCh:        make(chan struct{}),
		commandCh:     make(chan *commandRequest, 64),
	}
	return n
}

// Start begins the Raft node's event loops. Must call Replay() first if WAL exists.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	go n.run()
	go n.applyLoop()
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	close(n.stopCh)
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	if n.replicateCancel != nil {
		n.replicateCancel()
	}
}

// Replay restores state from the WAL. Must be called before Start().
func (n *Node) Replay() error {
	if n.wal == nil {
		return nil
	}

	term, votedFor, entries, err := n.wal.Replay()
	if err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = Term(term)
	n.votedFor = NodeID(votedFor)

	if len(entries) > 0 {
		var logEntries []LogEntry
		for _, e := range entries {
			logEntries = append(logEntries, LogEntry{
				Term:    Term(e.Term),
				Index:   LogIndex(e.Index),
				Command: e.Command,
			})
		}
		n.log.RestoreEntries(logEntries)
	}

	n.logger.Info("WAL replay complete",
		"term", n.currentTerm,
		"voted_for", n.votedFor,
		"log_length", n.log.LastIndex(),
	)
	return nil
}

// ApplyCh returns the channel on which applied (committed) entries are delivered.
func (n *Node) ApplyCh() <-chan LogEntry {
	return n.applyCh
}

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		state := n.state
		var timerCh <-chan time.Time

		switch state {
		case Follower, Candidate:
			if n.electionTimer != nil {
				timerCh = n.electionTimer.C
			}
		case Leader:
			if n.heartbeatTimer != nil {
				timerCh = n.heartbeatTimer.C
			}
		}
		n.mu.Unlock()

		if timerCh == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		select {
		case <-timerCh:
			n.mu.Lock()
			currentState := n.state
			n.mu.Unlock()

			switch currentState {
			case Follower, Candidate:
				n.startElection()
			case Leader:
				n.sendHeartbeats()
				n.mu.Lock()
				if n.heartbeatTimer != nil {
					n.heartbeatTimer.Reset(heartbeatInterval)
				}
				n.mu.Unlock()
			}
		case <-n.stopCh:
			return
		}
	}
}

// startElection transitions to Candidate, increments term, votes for self,
// and sends RequestVote to all peers concurrently.
func (n *Node) startElection() {
	n.mu.Lock()

	// INVARIANT 1: currentTerm only increases, never decreases
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.votes = 1
	n.votesReceived = map[NodeID]bool{n.id: true}
	n.electionStart = time.Now()

	// Persist term and vote before sending any RPCs.
	if n.wal != nil {
		if err := n.wal.PersistTerm(uint64(n.currentTerm)); err != nil {
			n.logger.Error("failed to persist term", "error", err)
		}
		// INVARIANT 2: votedFor is persisted to WAL before any VoteReply is sent
		if err := n.wal.PersistVote(string(n.votedFor), uint64(n.currentTerm)); err != nil {
			n.logger.Error("failed to persist vote", "error", err)
		}
	}

	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	peers := make([]NodeID, len(n.peers))
	copy(peers, n.peers)

	n.logger.Info("starting election",
		"term", term,
		"last_log_index", lastLogIndex,
		"last_log_term", lastLogTerm,
	)

	n.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Send RequestVote to all peers concurrently.
	for _, peer := range peers {
		go func(p NodeID) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			reply, err := n.transport.SendRequestVote(ctx, p, args)
			if err != nil {
				n.logger.Debug("RequestVote RPC failed",
					"peer", p,
					"term", term,
					"error", err,
				)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Stale response — ignore.
			if n.currentTerm != term || n.state != Candidate {
				return
			}

			// If reply has higher term, revert to follower.
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}

			if reply.VoteGranted {
				// Guard against double-counting — only count first vote per peer.
				if n.votesReceived[p] {
					return
				}
				n.votesReceived[p] = true
				n.votes++

				// Check if we have majority.
				majority := (len(n.peers)+1)/2 + 1
				if n.votes >= majority {
					n.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

// becomeLeaderLocked transitions the node to Leader state.
// Caller must hold n.mu.
func (n *Node) becomeLeaderLocked() {
	n.logger.Info("became leader",
		"term", n.currentTerm,
		"election_duration", time.Since(n.electionStart),
	)

	n.state = Leader
	n.leaderID = n.id
	n.leaderChanges++

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Initialize nextIndex and matchIndex for all peers.
	lastIdx := n.log.LastIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx
		n.matchIndex[peer] = 0
	}

	// Start heartbeat timer.
	n.heartbeatTimer = time.NewTimer(heartbeatInterval)

	// Start replication goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	n.replicateCancel = cancel
	for _, peer := range n.peers {
		go n.replicateLog(ctx, peer)
	}
}

// becomeFollowerLocked transitions the node to Follower state with the given term.
// Caller must hold n.mu.
func (n *Node) becomeFollowerLocked(term Term) {
	if term > n.currentTerm {
		// INVARIANT 1: currentTerm only increases, never decreases
		n.currentTerm = term
		n.votedFor = ""

		if n.wal != nil {
			if err := n.wal.PersistTerm(uint64(term)); err != nil {
				n.logger.Error("failed to persist term", "error", err)
			}
		}
	}

	wasLeader := n.state == Leader
	n.state = Follower
	n.votes = 0
	n.votesReceived = make(map[NodeID]bool)

	if wasLeader {
		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
		}
		if n.replicateCancel != nil {
			n.replicateCancel()
			n.replicateCancel = nil
		}
	}

	n.resetElectionTimerLocked()
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	// If caller's term is higher, step down.
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
		reply.Term = n.currentTerm
	}

	// Reject if term is stale.
	if args.Term < n.currentTerm {
		return reply
	}

	// Grant vote if: we haven't voted, or already voted for this candidate,
	// AND candidate's log is at least as up-to-date as ours.
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	logOK := n.isLogUpToDate(args.LastLogTerm, args.LastLogIndex)

	if canVote && logOK {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimerLocked()

		// INVARIANT 2: votedFor is persisted to WAL before any VoteReply is sent
		if n.wal != nil {
			if err := n.wal.PersistVote(string(n.votedFor), uint64(n.currentTerm)); err != nil {
				n.logger.Error("failed to persist vote", "error", err)
			}
		}

		n.logger.Info("granted vote",
			"candidate", args.CandidateID,
			"term", n.currentTerm,
		)
	}

	return reply
}

// isLogUpToDate checks if the candidate's log is at least as up-to-date as ours.
// "Up-to-date": higher last term wins; if equal term, longer log wins.
func (n *Node) isLogUpToDate(lastTerm Term, lastIndex LogIndex) bool {
	ourLastTerm := n.log.LastTerm()
	ourLastIndex := n.log.LastIndex()

	if lastTerm != ourLastTerm {
		return lastTerm > ourLastTerm
	}
	return lastIndex >= ourLastIndex
}

// HandleAppendEntries processes an incoming AppendEntries RPC (heartbeats and log replication).
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	// Reject if term is stale.
	if args.Term < n.currentTerm {
		return reply
	}

	// Valid leader — step down if needed and reset election timer.
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	if n.state != Follower {
		n.state = Follower
		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
		}
		if n.replicateCancel != nil {
			n.replicateCancel()
			n.replicateCancel = nil
		}
	}

	n.leaderID = args.LeaderID
	reply.Term = n.currentTerm

	// Reset election timer — the leader is alive.
	n.resetElectionTimerLocked()

	// Check log consistency at prevLogIndex/prevLogTerm.
	if args.PrevLogIndex > 0 {
		if !n.log.Contains(args.PrevLogIndex) {
			// Our log is too short — return conflict hint.
			reply.ConflictIndex = n.log.LastIndex() + 1
			reply.ConflictTerm = 0
			return reply
		}

		if n.log.TermAt(args.PrevLogIndex) != args.PrevLogTerm {
			// Term mismatch — return the conflicting term and the first index of that term.
			conflictTerm := n.log.TermAt(args.PrevLogIndex)
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = n.log.FindFirstIndexOfTerm(conflictTerm)
			return reply
		}
	}

	// Append new entries, truncating any conflicting suffix.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + LogIndex(i) + 1
		if n.log.Contains(idx) {
			if n.log.TermAt(idx) != entry.Term {
				// INVARIANT 3: log entries are never deleted once committed
				// truncateAfter only removes entries with index > commitIndex
				// (conflicting entries cannot be committed since they differ from leader)
				n.log.TruncateAfter(idx - 1)
				n.log.Append(args.Entries[i:]...)

				// Persist new entries to WAL.
				if n.wal != nil {
					for _, e := range args.Entries[i:] {
						if err := n.wal.PersistEntry(uint64(e.Index), uint64(e.Term), e.Command); err != nil {
							n.logger.Error("failed to persist entry", "error", err)
						}
					}
				}
				break
			}
		} else {
			// No existing entry — append all remaining.
			n.log.Append(args.Entries[i:]...)

			if n.wal != nil {
				for _, e := range args.Entries[i:] {
					if err := n.wal.PersistEntry(uint64(e.Index), uint64(e.Term), e.Command); err != nil {
						n.logger.Error("failed to persist entry", "error", err)
					}
				}
			}
			break
		}
	}

	// Update commitIndex.
	if args.LeaderCommit > n.commitIndex {
		// INVARIANT 4 (partial): commitIndex never decreases
		newCommit := args.LeaderCommit
		if lastIdx := n.log.LastIndex(); lastIdx < newCommit {
			newCommit = lastIdx
		}
		n.commitIndex = newCommit
	}

	reply.Success = true
	return reply
}

// resetElectionTimerLocked resets the election timer to a random duration.
// Caller must hold n.mu.
func (n *Node) resetElectionTimerLocked() {
	duration := electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))

	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(duration)
	} else {
		// Drain the timer channel if possible.
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(duration)
	}
}

// sendHeartbeats sends empty AppendEntries RPCs to all peers.
func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex
	peers := make([]NodeID, len(n.peers))
	copy(peers, n.peers)

	n.mu.Unlock()

	for _, peer := range peers {
		go func(p NodeID) {
			n.mu.Lock()
			prevLogIndex := n.nextIndex[p] - 1
			prevLogTerm := n.log.TermAt(prevLogIndex)
			entries := n.log.EntriesFrom(n.nextIndex[p])
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			reply, err := n.transport.SendAppendEntries(ctx, p, args)
			if err != nil {
				n.logger.Debug("heartbeat failed",
					"peer", p,
					"term", term,
					"error", err,
				)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Leader || n.currentTerm != term {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}

			if reply.Success {
				if len(entries) > 0 {
					newNextIdx := entries[len(entries)-1].Index + 1
					if newNextIdx > n.nextIndex[p] {
						n.nextIndex[p] = newNextIdx
					}
					newMatchIdx := newNextIdx - 1
					if newMatchIdx > n.matchIndex[p] {
						n.matchIndex[p] = newMatchIdx
					}
					n.advanceCommitIndexLocked()
				}
			} else {
				// Log inconsistency — use conflict hint for fast rollback.
				n.handleConflictLocked(p, reply)
			}
		}(peer)
	}
}

// handleConflictLocked adjusts nextIndex using conflict hints for fast rollback.
// Caller must hold n.mu.
func (n *Node) handleConflictLocked(peer NodeID, reply AppendEntriesReply) {
	if reply.ConflictTerm > 0 {
		// Find the last entry with ConflictTerm in our log.
		lastIdxOfTerm := LogIndex(0)
		for i := n.log.LastIndex(); i > 0; i-- {
			if n.log.TermAt(i) == reply.ConflictTerm {
				lastIdxOfTerm = i
				break
			}
		}

		if lastIdxOfTerm > 0 {
			n.nextIndex[peer] = lastIdxOfTerm + 1
		} else {
			n.nextIndex[peer] = reply.ConflictIndex
		}
	} else if reply.ConflictIndex > 0 {
		n.nextIndex[peer] = reply.ConflictIndex
	} else {
		// Slow rollback fallback.
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// Status returns the current node status for the /status endpoint.
func (n *Node) Status() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()

	peers := make([]string, len(n.peers))
	for i, p := range n.peers {
		peers[i] = string(p)
	}

	return NodeStatus{
		NodeID:      string(n.id),
		State:       n.state.String(),
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		Peers:       peers,
		LeaderID:    string(n.leaderID),
	}
}

// NodeStatus contains the node's current state for API responses.
type NodeStatus struct {
	NodeID      string   `json:"node_id"`
	State       string   `json:"state"`
	Term        Term     `json:"term"`
	CommitIndex LogIndex `json:"commit_index"`
	LastApplied LogIndex `json:"last_applied"`
	Peers       []string `json:"peers"`
	LeaderID    string   `json:"leader_id"`
}

// IsLeader returns true if this node is currently the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// LeaderID returns the current leader's ID, or empty if unknown.
func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// ID returns this node's ID.
func (n *Node) ID() NodeID {
	return n.id
}
