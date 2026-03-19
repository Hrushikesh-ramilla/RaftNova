package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/hrushikesh-ramilla/raftnova/internal/api"
	"github.com/hrushikesh-ramilla/raftnova/internal/config"
	"github.com/hrushikesh-ramilla/raftnova/internal/kvstore"
	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
	"github.com/hrushikesh-ramilla/raftnova/internal/storage"
	"github.com/hrushikesh-ramilla/raftnova/internal/transport"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Structured logging with node context.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := config.Parse()
	if err != nil {
		logger.Error("failed to parse config", "error", err)
		os.Exit(1)
	}

	logger = logger.With("node_id", string(cfg.NodeID))
	logger.Info("starting RaftNova",
		"http_addr", cfg.HTTPAddr,
		"grpc_addr", cfg.GRPCAddr,
		"peers", fmt.Sprintf("%v", cfg.Peers),
	)

	// Initialize WAL.
	walDir := filepath.Join(cfg.DataDir, string(cfg.NodeID), "wal")
	wal, err := storage.NewWAL(walDir, logger)
	if err != nil {
		logger.Error("failed to create WAL", "error", err)
		os.Exit(1)
	}
	defer wal.Close()

	// Initialize transport.
	grpcTransport := transport.NewGRPCTransport(cfg.PeerGRPCAddrs, logger)
	defer grpcTransport.Close()

	// Initialize Raft node.
	node := raft.NewNode(cfg.NodeID, cfg.Peers, grpcTransport, wal, logger)

	// Replay WAL before starting.
	if err := node.Replay(); err != nil {
		logger.Error("WAL replay failed", "error", err)
		os.Exit(1)
	}

	// Initialize KV store.
	store := kvstore.NewKVStore()

	// Start apply consumer — feeds committed entries to the KV store.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case entry := <-node.ApplyCh():
				if len(entry.Command) > 0 {
					if err := store.Apply(entry.Command); err != nil {
						logger.Error("failed to apply entry",
							"index", entry.Index,
							"error", err,
						)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start Raft node.
	node.Start()

	// Start gRPC server.
	grpcServer := transport.NewGRPCServer(node, logger)
	go func() {
		if err := grpcServer.Serve(cfg.GRPCAddr); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Start HTTP server.
	mux := http.NewServeMux()
	handler := api.NewHandler(node, store, cfg.PeerHTTPAddrs, logger)
	handler.RegisterRoutes(mux)

	// Prometheus metrics endpoint.
	mux.Handle("/metrics", promhttp.Handler())

	httpServer := api.NewServer(cfg.HTTPAddr, mux)
	go func() {
		logger.Info("HTTP server listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	// Graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("shutting down", "signal", sig.String())

	// Stop in reverse order.
	cancel()
	httpServer.Shutdown(context.Background())
	grpcServer.Stop()
	node.Stop()

	logger.Info("shutdown complete")
}
