package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/hrushikesh-ramilla/raftnova/internal/kvstore"
	"github.com/hrushikesh-ramilla/raftnova/internal/raft"
)

const maxRequestBody = 1 << 20 // 1MB

// Handler implements the HTTP API for the KV store.
type Handler struct {
	node   *raft.Node
	store  *kvstore.KVStore
	logger *slog.Logger
	// peerHTTPAddrs maps node IDs to their HTTP addresses for redirects.
	peerHTTPAddrs map[raft.NodeID]string
}

// NewHandler creates a new HTTP API handler.
func NewHandler(node *raft.Node, store *kvstore.KVStore, peerHTTPAddrs map[raft.NodeID]string, logger *slog.Logger) *Handler {
	return &Handler{
		node:          node,
		store:         store,
		logger:        logger,
		peerHTTPAddrs: peerHTTPAddrs,
	}
}

// RegisterRoutes registers all HTTP routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/status", h.handleStatus)
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/readyz", h.handleReadyz)
}

// NewServer creates a configured HTTP server.
func NewServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

func (h *Handler) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from path: /kv/{key}
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, key)
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Linearizable reads: only the leader can serve reads.
	if !h.node.IsLeader() {
		h.redirectToLeader(w, r)
		return
	}

	val, ok := h.store.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": val})
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	if !h.node.IsLeader() {
		h.redirectToLeader(w, r)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}
	defer r.Body.Close()

	var req struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}

	cmd, err := kvstore.SerializePut(key, req.Value)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to serialize command"})
		return
	}

	index, term, err := h.node.SubmitCommand(cmd)
	if err != nil {
		if err == raft.ErrNotLeader {
			h.redirectToLeader(w, r)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":   key,
		"value": req.Value,
		"index": index,
		"term":  term,
	})
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	if !h.node.IsLeader() {
		h.redirectToLeader(w, r)
		return
	}

	cmd, err := kvstore.SerializeDelete(key)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to serialize command"})
		return
	}

	_, _, err = h.node.SubmitCommand(cmd)
	if err != nil {
		if err == raft.ErrNotLeader {
			h.redirectToLeader(w, r)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "key": key})
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	status := h.node.Status()
	writeJSON(w, http.StatusOK, status)
}

func (h *Handler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleReadyz(w http.ResponseWriter, r *http.Request) {
	status := h.node.Status()

	// Ready if the node is caught up — lastApplied within 10 of commitIndex.
	lag := int64(status.CommitIndex) - int64(status.LastApplied)
	if lag > 10 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready",
			"reason": fmt.Sprintf("apply lag: %d", lag),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID := h.node.LeaderID()
	if leaderID == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "no leader elected — try again later",
		})
		return
	}

	leaderAddr, ok := h.peerHTTPAddrs[leaderID]
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "leader address unknown",
		})
		return
	}

	redirectURL := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.Path)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
