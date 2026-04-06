package config

import (
	"flag"
	"fmt"
	"strings"

	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
)

// Config holds the server configuration parsed from flags and environment.
type Config struct {
	NodeID   raft.NodeID
	Peers    []raft.NodeID
	HTTPAddr string
	GRPCAddr string
	DataDir  string

	// Peer address maps for transport and HTTP redirects.
	PeerGRPCAddrs map[raft.NodeID]string
	PeerHTTPAddrs map[raft.NodeID]string
}

// Parse parses configuration from command-line flags.
func Parse() (*Config, error) {
	nodeID := flag.String("node-id", "", "unique node ID (e.g., node-1)")
	peers := flag.String("peers", "", "comma-separated list of peer node IDs (e.g., node-2,node-3)")
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data", "data directory for WAL")

	// Peer address maps — format: node-id=host:port,node-id=host:port
	peerGRPC := flag.String("peer-grpc-addrs", "", "peer gRPC addresses (node-2=host:port,node-3=host:port)")
	peerHTTP := flag.String("peer-http-addrs", "", "peer HTTP addresses (node-2=host:port,node-3=host:port)")

	flag.Parse()

	if *nodeID == "" {
		return nil, fmt.Errorf("--node-id is required")
	}

	cfg := &Config{
		NodeID:        raft.NodeID(*nodeID),
		HTTPAddr:      *httpAddr,
		GRPCAddr:      *grpcAddr,
		DataDir:       *dataDir,
		PeerGRPCAddrs: make(map[raft.NodeID]string),
		PeerHTTPAddrs: make(map[raft.NodeID]string),
	}

	// Parse peer list.
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.Peers = append(cfg.Peers, raft.NodeID(p))
			}
		}
	}

	// Parse peer gRPC addresses.
	if *peerGRPC != "" {
		addrs, err := parseAddrMap(*peerGRPC)
		if err != nil {
			return nil, fmt.Errorf("parse peer-grpc-addrs: %w", err)
		}
		cfg.PeerGRPCAddrs = addrs
	}

	// Parse peer HTTP addresses.
	if *peerHTTP != "" {
		addrs, err := parseAddrMap(*peerHTTP)
		if err != nil {
			return nil, fmt.Errorf("parse peer-http-addrs: %w", err)
		}
		cfg.PeerHTTPAddrs = addrs
	}

	return cfg, nil
}

func parseAddrMap(s string) (map[raft.NodeID]string, error) {
	m := make(map[raft.NodeID]string)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address pair: %q (expected node-id=host:port)", pair)
		}
		m[raft.NodeID(strings.TrimSpace(parts[0]))] = strings.TrimSpace(parts[1])
	}
	return m, nil
}

// ParseFromEnv creates a Config from environment variables — used in Docker.
// Expected env vars: NODE_ID, PEERS, HTTP_ADDR, GRPC_ADDR, DATA_DIR
func ParseFromEnv(nodeID, peers, httpAddr, grpcAddr, dataDir, peerGRPC, peerHTTP string) (*Config, error) {
	cfg := &Config{
		NodeID:        raft.NodeID(nodeID),
		HTTPAddr:      httpAddr,
		GRPCAddr:      grpcAddr,
		DataDir:       dataDir,
		PeerGRPCAddrs: make(map[raft.NodeID]string),
		PeerHTTPAddrs: make(map[raft.NodeID]string),
	}

	if peers != "" {
		for _, p := range strings.Split(peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.Peers = append(cfg.Peers, raft.NodeID(p))
			}
		}
	}

	if peerGRPC != "" {
		addrs, err := parseAddrMap(peerGRPC)
		if err != nil {
			return nil, err
		}
		cfg.PeerGRPCAddrs = addrs
	}

	if peerHTTP != "" {
		addrs, err := parseAddrMap(peerHTTP)
		if err != nil {
			return nil, err
		}
		cfg.PeerHTTPAddrs = addrs
	}

	return cfg, nil
}
