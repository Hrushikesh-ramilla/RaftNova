package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
)

// TestTransport simulates a network by calling node methods directly.
// Supports delay injection, message dropping, and partition simulation.
type TestTransport struct {
	mu    sync.RWMutex
	nodes map[raft.NodeID]*raft.Node

	// Partition simulation: if partitioned[A][B] is true, messages from A to B are dropped.
	partitioned map[raft.NodeID]map[raft.NodeID]bool

	// Delay injection: added latency to all RPCs.
	minDelay time.Duration
	maxDelay time.Duration

	// Drop rate: probability [0, 1) of dropping any message.
	dropRate float64

	logger *slog.Logger
}

// NewTestTransport creates a test transport with no partitions or delays.
func NewTestTransport(logger *slog.Logger) *TestTransport {
	return &TestTransport{
		nodes:       make(map[raft.NodeID]*raft.Node),
		partitioned: make(map[raft.NodeID]map[raft.NodeID]bool),
		logger:      logger,
	}
}

// RegisterNode adds a node to the test transport.
func (t *TestTransport) RegisterNode(id raft.NodeID, node *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

// Partition blocks all messages between nodeA and nodeB in both directions.
func (t *TestTransport) Partition(nodeA, nodeB raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.partitioned[nodeA] == nil {
		t.partitioned[nodeA] = make(map[raft.NodeID]bool)
	}
	if t.partitioned[nodeB] == nil {
		t.partitioned[nodeB] = make(map[raft.NodeID]bool)
	}
	t.partitioned[nodeA][nodeB] = true
	t.partitioned[nodeB][nodeA] = true
}

// Heal removes the partition between nodeA and nodeB.
func (t *TestTransport) Heal(nodeA, nodeB raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.partitioned[nodeA] != nil {
		delete(t.partitioned[nodeA], nodeB)
	}
	if t.partitioned[nodeB] != nil {
		delete(t.partitioned[nodeB], nodeA)
	}
}

// HealAll removes all partitions.
func (t *TestTransport) HealAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitioned = make(map[raft.NodeID]map[raft.NodeID]bool)
}

// PartitionNode isolates a single node from all others.
func (t *TestTransport) PartitionNode(nodeID raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id := range t.nodes {
		if id == nodeID {
			continue
		}
		if t.partitioned[nodeID] == nil {
			t.partitioned[nodeID] = make(map[raft.NodeID]bool)
		}
		if t.partitioned[id] == nil {
			t.partitioned[id] = make(map[raft.NodeID]bool)
		}
		t.partitioned[nodeID][id] = true
		t.partitioned[id][nodeID] = true
	}
}

// SetDelay sets a random delay range applied to all RPCs.
func (t *TestTransport) SetDelay(min, max time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.minDelay = min
	t.maxDelay = max
}

// SetDropRate sets the probability of dropping any message.
func (t *TestTransport) SetDropRate(rate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropRate = rate
}

func (t *TestTransport) isPartitioned(from, to raft.NodeID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if m, ok := t.partitioned[from]; ok {
		return m[to]
	}
	return false
}

func (t *TestTransport) shouldDrop() bool {
	t.mu.RLock()
	rate := t.dropRate
	t.mu.RUnlock()
	return rate > 0 && rand.Float64() < rate
}

func (t *TestTransport) applyDelay() {
	t.mu.RLock()
	min := t.minDelay
	max := t.maxDelay
	t.mu.RUnlock()

	if max > 0 {
		delay := min
		if max > min {
			delay += time.Duration(rand.Int63n(int64(max - min)))
		}
		time.Sleep(delay)
	}
}

// PerNodeTransport returns a Transport implementation specific to a source node.
// This allows simulating partitions per-node.
type PerNodeTransport struct {
	sourceID  raft.NodeID
	transport *TestTransport
}

// ForNode returns a Transport scoped to a specific source node.
func (t *TestTransport) ForNode(id raft.NodeID) *PerNodeTransport {
	return &PerNodeTransport{
		sourceID:  id,
		transport: t,
	}
}

// SendRequestVote sends a RequestVote to the target node via direct function call.
func (p *PerNodeTransport) SendRequestVote(ctx context.Context, target raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	if p.transport.isPartitioned(p.sourceID, target) {
		return raft.RequestVoteReply{}, fmt.Errorf("partitioned: %s -> %s", p.sourceID, target)
	}
	if p.transport.shouldDrop() {
		return raft.RequestVoteReply{}, fmt.Errorf("message dropped: %s -> %s", p.sourceID, target)
	}

	p.transport.applyDelay()

	p.transport.mu.RLock()
	node, ok := p.transport.nodes[target]
	p.transport.mu.RUnlock()

	if !ok {
		return raft.RequestVoteReply{}, fmt.Errorf("node %s not found", target)
	}

	select {
	case <-ctx.Done():
		return raft.RequestVoteReply{}, ctx.Err()
	default:
	}

	reply := node.HandleRequestVote(args)
	return reply, nil
}

// SendAppendEntries sends an AppendEntries to the target node via direct function call.
func (p *PerNodeTransport) SendAppendEntries(ctx context.Context, target raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	if p.transport.isPartitioned(p.sourceID, target) {
		return raft.AppendEntriesReply{}, fmt.Errorf("partitioned: %s -> %s", p.sourceID, target)
	}
	if p.transport.shouldDrop() {
		return raft.AppendEntriesReply{}, fmt.Errorf("message dropped: %s -> %s", p.sourceID, target)
	}

	p.transport.applyDelay()

	p.transport.mu.RLock()
	node, ok := p.transport.nodes[target]
	p.transport.mu.RUnlock()

	if !ok {
		return raft.AppendEntriesReply{}, fmt.Errorf("node %s not found", target)
	}

	select {
	case <-ctx.Done():
		return raft.AppendEntriesReply{}, ctx.Err()
	default:
	}

	reply := node.HandleAppendEntries(args)
	return reply, nil
}
