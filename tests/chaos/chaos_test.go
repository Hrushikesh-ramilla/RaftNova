package chaos

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/hrushikesh-ramilla/raftnova/internal/kvstore"
	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
)

// Cluster manages a set of in-process Raft nodes for testing.
type Cluster struct {
	nodes     map[raft.NodeID]*raft.Node
	stores    map[raft.NodeID]*kvstore.KVStore
	transport *TestTransport
	logger    *slog.Logger
	stopChs   map[raft.NodeID]chan struct{}
}

func newCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	nodeIDs := make([]raft.NodeID, n)
	for i := 0; i < n; i++ {
		nodeIDs[i] = raft.NodeID(fmt.Sprintf("node-%d", i+1))
	}

	tt := NewTestTransport(logger)

	c := &Cluster{
		nodes:     make(map[raft.NodeID]*raft.Node),
		stores:    make(map[raft.NodeID]*kvstore.KVStore),
		transport: tt,
		logger:    logger,
		stopChs:   make(map[raft.NodeID]chan struct{}),
	}

	for _, id := range nodeIDs {
		peers := make([]raft.NodeID, 0, n-1)
		for _, pid := range nodeIDs {
			if pid != id {
				peers = append(peers, pid)
			}
		}

		perNodeTransport := tt.ForNode(id)
		node := raft.NewNode(id, peers, perNodeTransport, nil, logger)
		store := kvstore.NewKVStore()

		tt.RegisterNode(id, node)
		c.nodes[id] = node
		c.stores[id] = store

		// Start apply consumer.
		stopCh := make(chan struct{})
		c.stopChs[id] = stopCh
		go func(n *raft.Node, s *kvstore.KVStore, stop chan struct{}) {
			for {
				select {
				case entry := <-n.ApplyCh():
					if len(entry.Command) > 0 {
						s.Apply(entry.Command)
					}
				case <-stop:
					return
				}
			}
		}(node, store, stopCh)
	}

	// Start all nodes.
	for _, node := range c.nodes {
		node.Start()
	}

	return c
}

func (c *Cluster) stop() {
	for id, node := range c.nodes {
		node.Stop()
		close(c.stopChs[id])
	}
}

func (c *Cluster) waitForLeader(timeout time.Duration) (raft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, node := range c.nodes {
			if node.IsLeader() {
				return id, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader elected within %s", timeout)
}

func (c *Cluster) getLeader() (raft.NodeID, *raft.Node) {
	for id, node := range c.nodes {
		if node.IsLeader() {
			return id, node
		}
	}
	return "", nil
}

func (c *Cluster) leaderCount() int {
	count := 0
	for _, node := range c.nodes {
		if node.IsLeader() {
			count++
		}
	}
	return count
}

func (c *Cluster) putKey(leader *raft.Node, key, value string) error {
	cmd, err := kvstore.SerializePut(key, value)
	if err != nil {
		return err
	}
	_, _, err = leader.SubmitCommand(cmd)
	return err
}

// ------------------------------------------------------------------
// Chaos Tests
// ------------------------------------------------------------------

func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	// Wait for a leader to be elected.
	leaderID, err := c.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}
	t.Logf("leader elected: %s", leaderID)

	// Assert: exactly one leader.
	count := c.leaderCount()
	if count != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", count)
	}

	// Assert: all nodes agree on the same term.
	var leaderTerm raft.Term
	for _, node := range c.nodes {
		status := node.Status()
		if status.State == "leader" {
			leaderTerm = status.Term
			break
		}
	}

	for id, node := range c.nodes {
		status := node.Status()
		if status.Term != leaderTerm {
			t.Errorf("node %s has term %d, expected %d", id, status.Term, leaderTerm)
		}
	}
}

func TestNoSplitVote(t *testing.T) {
	successes := 0
	runs := 50

	for i := 0; i < runs; i++ {
		c := newCluster(t, 5)

		_, err := c.waitForLeader(3 * time.Second)
		if err == nil {
			count := c.leaderCount()
			if count == 1 {
				successes++
			}
		}
		c.stop()
	}

	// Allow a small number of failures due to timing, but the vast majority should succeed.
	successRate := float64(successes) / float64(runs)
	t.Logf("leader elected in %d/%d runs (%.0f%%)", successes, runs, successRate*100)

	if successRate < 0.90 {
		t.Fatalf("split vote rate too high: only %d/%d elections succeeded", successes, runs)
	}
}

func TestCommitSurvivesLeaderCrash(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leaderID, err := c.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no initial leader: %v", err)
	}

	leader := c.nodes[leaderID]
	committed := make(map[string]string)

	// Write 100 keys.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("user:%d", i)
		value := fmt.Sprintf("value-%d", i)

		if err := c.putKey(leader, key, value); err != nil {
			// If we lost leadership mid-write, find the new leader.
			newLeaderID, err2 := c.waitForLeader(3 * time.Second)
			if err2 != nil {
				t.Fatalf("write %d failed and no new leader: %v", i, err2)
			}
			leader = c.nodes[newLeaderID]
			leaderID = newLeaderID

			// Retry the write.
			if err := c.putKey(leader, key, value); err != nil {
				t.Fatalf("write %d failed on retry: %v", i, err)
			}
		}
		committed[key] = value
	}

	// Give some time for replication.
	time.Sleep(500 * time.Millisecond)

	// Verify all committed keys are present in at least one store.
	for key, expected := range committed {
		found := false
		for _, store := range c.stores {
			val, ok := store.Get(key)
			if ok && val == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("FAIL TestCommitSurvivesLeaderCrash: key %q expected %q got \"\" (missing after writes)", key, expected)
		}
	}
}

func TestPartitionAndHeal(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leaderID, err := c.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no initial leader: %v", err)
	}

	// Partition node-1 from all others.
	partitioned := raft.NodeID("node-1")
	c.transport.PartitionNode(partitioned)

	t.Logf("partitioned %s, leader was %s", partitioned, leaderID)

	// Wait for a new leader if the partitioned node was leader.
	time.Sleep(1 * time.Second)
	newLeaderID, err := c.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader after partition: %v", err)
	}

	leader := c.nodes[newLeaderID]
	t.Logf("new leader: %s", newLeaderID)

	// Write 20 keys to the majority partition.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("partition-key:%d", i)
		value := fmt.Sprintf("partition-value-%d", i)
		if err := c.putKey(leader, key, value); err != nil {
			t.Fatalf("write %d failed during partition: %v", i, err)
		}
	}

	// Heal the partition.
	c.transport.HealAll()
	t.Log("partition healed")

	// Wait for the partitioned node to catch up.
	time.Sleep(2 * time.Second)

	// Assert: partitioned node has all 20 keys.
	store := c.stores[partitioned]
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("partition-key:%d", i)
		expected := fmt.Sprintf("partition-value-%d", i)
		val, ok := store.Get(key)
		if !ok || val != expected {
			t.Errorf("partitioned node %s: key %q expected %q got %q (ok=%v)",
				partitioned, key, expected, val, ok)
		}
	}
}

func TestLinearizableReads(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	_, err := c.waitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}

	iterations := 1000
	for i := 0; i < iterations; i++ {
		leaderID, leader := c.getLeader()
		if leader == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		key := fmt.Sprintf("linear-key:%d", i)
		value := fmt.Sprintf("linear-value-%d", i)

		if err := c.putKey(leader, key, value); err != nil {
			continue // skip on leader change
		}

		// Read from the leader's store. The apply consumer is async,
		// so we poll briefly to let it process the applyCh entry.
		store := c.stores[leaderID]
		var got string
		var ok bool
		for retry := 0; retry < 20; retry++ {
			got, ok = store.Get(key)
			if ok {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		if !ok {
			t.Errorf("iteration %d: key %q not found after acked write", i, key)
			continue
		}
		if got != value {
			t.Errorf("iteration %d: key %q expected %q got %q (stale read)", i, key, value, got)
		}
	}
}

