package raft

import (
	"context"
	"sort"
	"time"
)

// replicateLog runs as a goroutine for each peer when this node is Leader.
// It continuously sends AppendEntries RPCs to keep the peer's log in sync.
func (n *Node) replicateLog(ctx context.Context, peer NodeID) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond): // replication tick
		}

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}

		nextIdx := n.nextIndex[peer]
		prevLogIndex := nextIdx - 1
		prevLogTerm := n.log.TermAt(prevLogIndex)
		entries := n.log.EntriesFrom(nextIdx)
		term := n.currentTerm
		commitIndex := n.commitIndex

		n.mu.Unlock()

		if len(entries) == 0 {
			continue // nothing to replicate — heartbeats handle keepalive
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}

		rpcCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		reply, err := n.transport.SendAppendEntries(rpcCtx, peer, args)
		cancel()

		if err != nil {
			n.logger.Debug("replication RPC failed",
				"peer", peer,
				"term", term,
				"error", err,
			)
			continue
		}

		n.mu.Lock()

		// Stale response — our state has changed since we sent.
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		if reply.Term > n.currentTerm {
			n.becomeFollowerLocked(reply.Term)
			n.mu.Unlock()
			return
		}

		if reply.Success {
			// Update nextIndex and matchIndex for this peer.
			newNextIdx := entries[len(entries)-1].Index + 1
			if newNextIdx > n.nextIndex[peer] {
				n.nextIndex[peer] = newNextIdx
			}
			newMatchIdx := newNextIdx - 1
			if newMatchIdx > n.matchIndex[peer] {
				n.matchIndex[peer] = newMatchIdx
			}

			n.advanceCommitIndexLocked()
		} else {
			// Log inconsistency — use conflict hint for fast rollback.
			n.handleConflictLocked(peer, reply)
		}

		n.mu.Unlock()
	}
}

// advanceCommitIndexLocked finds the highest N such that:
//   - N > commitIndex
//   - log[N].term == currentTerm  (Raft Figure 8 safety — leader only commits entries from own term)
//   - a majority of matchIndex[peer] >= N
//
// INVARIANT 4: leader only commits entries from its own term
// Reference: Raft paper Figure 8 — committing entries from previous terms
// by counting replicas is unsafe. A new leader must commit an entry from
// its own term first, which implicitly commits all prior entries.
//
// Caller must hold n.mu.
func (n *Node) advanceCommitIndexLocked() {
	// Collect all matchIndex values including self.
	matches := make([]LogIndex, 0, len(n.peers)+1)
	matches = append(matches, n.log.LastIndex()) // self
	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[peer])
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	// The (n/2)th value is the highest index replicated to a majority.
	majority := (len(n.peers) + 1) / 2
	if majority >= len(matches) {
		return
	}

	candidateCommit := matches[majority]

	// INVARIANT 4: leader only commits entries from its own term
	// This is the Figure 8 safety check — we must not commit entries
	// from previous terms by counting replicas alone.
	if candidateCommit > n.commitIndex &&
		n.log.Contains(candidateCommit) &&
		n.log.TermAt(candidateCommit) == n.currentTerm {

		n.logger.Info("advancing commit index",
			"old", n.commitIndex,
			"new", candidateCommit,
			"term", n.currentTerm,
		)
		n.commitIndex = candidateCommit
		n.committedEntries = int64(candidateCommit)
	}
}

// applyLoop runs as a goroutine and delivers committed entries to applyCh.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(5 * time.Millisecond): // apply tick
		}

		n.mu.Lock()

		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			continue
		}

		// Collect entries to apply.
		entries := n.log.Entries(n.lastApplied+1, n.commitIndex)
		newLastApplied := n.commitIndex

		n.mu.Unlock()

		for _, entry := range entries {
			select {
			case n.applyCh <- entry:
			case <-n.stopCh:
				return
			}
		}

		n.mu.Lock()
		n.lastApplied = newLastApplied
		n.mu.Unlock()
	}
}

// SubmitCommand submits a command to the Raft cluster for replication.
// Appends to the leader's log directly, then waits for commit + apply.
// Returns the log index and term if accepted, or an error.
// Blocks until the entry is committed and applied, or times out (5s).
func (n *Node) SubmitCommand(command []byte) (LogIndex, Term, error) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}

	// Append entry to log.
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.log.LastIndex() + 1,
		Command: command,
	}
	n.log.Append(entry)

	// Persist to WAL before acknowledging.
	if n.wal != nil {
		if err := n.wal.PersistEntry(uint64(entry.Index), uint64(entry.Term), entry.Command); err != nil {
			n.mu.Unlock()
			return 0, 0, err
		}
	}

	index := entry.Index
	term := entry.Term

	n.mu.Unlock()

	// Wait for the entry to be applied (committed + applied) with timeout.
	// The replication goroutines will deliver to peers; heartbeats also carry entries.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return 0, 0, ErrNotLeader
		}
		if n.lastApplied >= index {
			n.mu.Unlock()
			return index, term, nil
		}
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	return 0, 0, ErrTimeout
}

