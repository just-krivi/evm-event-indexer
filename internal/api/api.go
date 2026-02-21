package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"evm-event-indexer/internal/db"
)

// Store is the minimal interface the API needs from the database layer.
// Using an interface makes the handlers independently testable without Postgres.
type Store interface {
	GetChunkStats(ctx context.Context) (db.ChunkStats, error)
	QueryLogs(ctx context.Context, f db.LogFilter) ([]db.Log, error)
}

// PoolStore wraps *pgxpool.Pool to implement Store.
type PoolStore struct {
	pool *pgxpool.Pool
}

func NewPoolStore(pool *pgxpool.Pool) *PoolStore {
	return &PoolStore{pool: pool}
}

func (p *PoolStore) GetChunkStats(ctx context.Context) (db.ChunkStats, error) {
	return db.GetChunkStats(ctx, p.pool)
}

func (p *PoolStore) QueryLogs(ctx context.Context, f db.LogFilter) ([]db.Log, error) {
	return db.QueryLogs(ctx, p.pool, f)
}

// Server is the HTTP query API.
type Server struct {
	store         Store
	activeWorkers func() int64
	httpServer    *http.Server
}

// New creates a Server. addr is the listen address (e.g. ":3000").
// activeWorkers is called on each /health request to get the in_progress chunk count.
func New(addr string, store Store, activeWorkers func() int64) *Server {
	s := &Server{
		store:         store,
		activeWorkers: activeWorkers,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /logs", s.handleLogs)
	mux.HandleFunc("GET /health", s.handleHealth)

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Start binds to the configured address and serves in a background goroutine.
// Returns an error immediately if the port cannot be bound (e.g. already in use).
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.httpServer.Addr, err)
	}
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("API server error", "error", err)
		}
	}()
	slog.Info("API server started", "addr", s.httpServer.Addr)
	return nil
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// --- Response types ---

type logJSON struct {
	BlockNumber int     `json:"blockNumber"`
	BlockHash   string  `json:"blockHash"`
	TxHash      string  `json:"txHash"`
	LogIndex    int     `json:"logIndex"`
	Address     string  `json:"address"`
	Topic0      *string `json:"topic0"`
	Topic1      *string `json:"topic1"`
	Topic2      *string `json:"topic2"`
	Topic3      *string `json:"topic3"`
	Data        string  `json:"data"`
}

type logsResponse struct {
	Count  int       `json:"count"`
	Offset int       `json:"offset"`
	Limit  int       `json:"limit"`
	Logs   []logJSON `json:"logs"`
}

type workersInfo struct {
	Active int64 `json:"active"`
}

type chunksInfo struct {
	Total                  int     `json:"total"`
	Done                   int     `json:"done"`
	InProgress             int     `json:"in_progress"`
	Pending                int     `json:"pending"`
	Failed                 int     `json:"failed"`
	EstimatedCompletionPct float64 `json:"estimated_completion_pct"`
}

type healthResponse struct {
	Status  string      `json:"status"`
	Workers workersInfo `json:"workers"`
	Chunks  chunksInfo  `json:"chunks"`
}

// --- Handlers ---

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := db.LogFilter{}

	if v := q.Get("address"); v != "" {
		lower := strings.ToLower(v)
		f.Address = &lower
	}
	if v := q.Get("topic0"); v != "" {
		f.Topic0 = &v
	}
	if v := q.Get("from_block"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "invalid from_block", http.StatusBadRequest)
			return
		}
		f.FromBlock = &n
	}
	if v := q.Get("to_block"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "invalid to_block", http.StatusBadRequest)
			return
		}
		f.ToBlock = &n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		f.Offset = n
	}

	logs, err := s.store.QueryLogs(r.Context(), f)
	if err != nil {
		slog.Error("query logs error", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Reflect the effective limit in the response (db.QueryLogs clamps 0→100, >1000→1000).
	effectiveLimit := f.Limit
	if effectiveLimit <= 0 {
		effectiveLimit = 100
	}
	if effectiveLimit > 1000 {
		effectiveLimit = 1000
	}

	result := make([]logJSON, 0, len(logs))
	for _, l := range logs {
		result = append(result, logJSON{
			BlockNumber: l.BlockNumber,
			BlockHash:   l.BlockHash,
			TxHash:      l.TxHash,
			LogIndex:    l.LogIndex,
			Address:     l.Address,
			Topic0:      l.Topic0,
			Topic1:      l.Topic1,
			Topic2:      l.Topic2,
			Topic3:      l.Topic3,
			Data:        l.Data,
		})
	}

	writeJSON(w, http.StatusOK, logsResponse{
		Count:  len(logs),
		Offset: f.Offset,
		Limit:  effectiveLimit,
		Logs:   result,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetChunkStats(r.Context())
	if err != nil {
		slog.Error("health check DB error", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	var pct float64
	if stats.Total > 0 {
		pct = float64(stats.Done) / float64(stats.Total) * 100.0
	}

	writeJSON(w, http.StatusOK, healthResponse{
		Status: "ok",
		Workers: workersInfo{
			Active: s.activeWorkers(),
		},
		Chunks: chunksInfo{
			Total:                  stats.Total,
			Done:                   stats.Done,
			InProgress:             stats.InProgress,
			Pending:                stats.Pending,
			Failed:                 stats.Failed,
			EstimatedCompletionPct: pct,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("JSON encode error", "error", err)
	}
}
