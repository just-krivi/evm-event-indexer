package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"evm-event-indexer/internal/db"
)

// mockStore is an in-memory Store for testing (no Postgres required).
type mockStore struct {
	stats    db.ChunkStats
	logs     []db.Log
	statsErr error
	logsErr  error

	capturedFilter db.LogFilter // set by QueryLogs for assertion
}

func (m *mockStore) GetChunkStats(_ context.Context) (db.ChunkStats, error) {
	return m.stats, m.statsErr
}

func (m *mockStore) QueryLogs(_ context.Context, f db.LogFilter) ([]db.Log, error) {
	m.capturedFilter = f
	return m.logs, m.logsErr
}

// newTestServer builds a Server wired to the given mock store.
// configuredWorkers=4, activeWorkers always returns 2.
func newTestServer(store *mockStore) *Server {
	return New(":0", store, 4, func() int64 { return 2 })
}

func get(s *Server, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	s.httpServer.Handler.ServeHTTP(w, req)
	return w
}

// --- /health tests ---

func TestHealth_Normal(t *testing.T) {
	store := &mockStore{
		stats: db.ChunkStats{Total: 50, Done: 31, InProgress: 3, Pending: 14, Failed: 2},
	}
	w := get(newTestServer(store), "/health")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("status: got %q, want %q", resp.Status, "ok")
	}
	if resp.Workers.Configured != 4 {
		t.Errorf("configured workers: got %d, want 4", resp.Workers.Configured)
	}
	if resp.Workers.Active != 2 {
		t.Errorf("active workers: got %d, want 2", resp.Workers.Active)
	}
	if resp.Chunks.Total != 50 {
		t.Errorf("total chunks: got %d, want 50", resp.Chunks.Total)
	}
	if resp.Chunks.Done != 31 {
		t.Errorf("done chunks: got %d, want 31", resp.Chunks.Done)
	}
	if resp.Chunks.InProgress != 3 {
		t.Errorf("in_progress: got %d, want 3", resp.Chunks.InProgress)
	}
	if resp.Chunks.Pending != 14 {
		t.Errorf("pending: got %d, want 14", resp.Chunks.Pending)
	}
	if resp.Chunks.Failed != 2 {
		t.Errorf("failed: got %d, want 2", resp.Chunks.Failed)
	}
	wantPct := float64(31) / float64(50) * 100
	if resp.Chunks.EstimatedCompletionPct != wantPct {
		t.Errorf("pct: got %f, want %f", resp.Chunks.EstimatedCompletionPct, wantPct)
	}
}

func TestHealth_ZeroChunks(t *testing.T) {
	// No chunks yet — guard against division by zero.
	store := &mockStore{stats: db.ChunkStats{}}
	w := get(newTestServer(store), "/health")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Chunks.EstimatedCompletionPct != 0.0 {
		t.Errorf("expected 0 pct for zero chunks, got %f", resp.Chunks.EstimatedCompletionPct)
	}
}

func TestHealth_AllDone(t *testing.T) {
	store := &mockStore{stats: db.ChunkStats{Total: 10, Done: 10}}
	w := get(newTestServer(store), "/health")

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Chunks.EstimatedCompletionPct != 100.0 {
		t.Errorf("expected 100 pct, got %f", resp.Chunks.EstimatedCompletionPct)
	}
}

func TestHealth_DBError(t *testing.T) {
	store := &mockStore{statsErr: errors.New("db down")}
	w := get(newTestServer(store), "/health")
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

// --- /logs tests ---

func TestLogs_NoFilters(t *testing.T) {
	store := &mockStore{
		logs: []db.Log{
			{BlockNumber: 100, TxHash: "0xabc", LogIndex: 0, Address: "0xdead", Data: "0x"},
			{BlockNumber: 101, TxHash: "0xdef", LogIndex: 1, Address: "0xbeef", Data: "0x01"},
		},
	}
	w := get(newTestServer(store), "/logs")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp logsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("count: got %d, want 2", resp.Count)
	}
	if resp.Limit != 100 {
		t.Errorf("default limit: got %d, want 100", resp.Limit)
	}
	if resp.Offset != 0 {
		t.Errorf("default offset: got %d, want 0", resp.Offset)
	}
	if len(resp.Logs) != 2 {
		t.Errorf("logs len: got %d, want 2", len(resp.Logs))
	}
	// No filter fields should be set.
	if store.capturedFilter.Address != nil || store.capturedFilter.Topic0 != nil ||
		store.capturedFilter.FromBlock != nil || store.capturedFilter.ToBlock != nil {
		t.Error("expected all filter fields to be nil when no query params given")
	}
}

func TestLogs_EmptyResult(t *testing.T) {
	store := &mockStore{logs: nil}
	w := get(newTestServer(store), "/logs")

	var resp logsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected count 0, got %d", resp.Count)
	}
	if resp.Logs == nil {
		t.Error("logs field should be an empty array, not null")
	}
}

func TestLogs_AddressFilter(t *testing.T) {
	store := &mockStore{}
	get(newTestServer(store), "/logs?address=0xABCDEF1234567890")

	if store.capturedFilter.Address == nil {
		t.Fatal("address filter not set")
	}
	// Handler must lowercase the address before passing it to the store.
	if *store.capturedFilter.Address != "0xabcdef1234567890" {
		t.Errorf("address not lowercased: got %q", *store.capturedFilter.Address)
	}
}

func TestLogs_Topic0Filter(t *testing.T) {
	topic := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	store := &mockStore{}
	get(newTestServer(store), "/logs?topic0="+topic)

	if store.capturedFilter.Topic0 == nil || *store.capturedFilter.Topic0 != topic {
		t.Errorf("topic0 filter: got %v, want %q", store.capturedFilter.Topic0, topic)
	}
}

func TestLogs_BlockRangeFilter(t *testing.T) {
	store := &mockStore{}
	get(newTestServer(store), "/logs?from_block=19000000&to_block=19005000")

	if store.capturedFilter.FromBlock == nil || *store.capturedFilter.FromBlock != 19000000 {
		t.Errorf("from_block: got %v, want 19000000", store.capturedFilter.FromBlock)
	}
	if store.capturedFilter.ToBlock == nil || *store.capturedFilter.ToBlock != 19005000 {
		t.Errorf("to_block: got %v, want 19005000", store.capturedFilter.ToBlock)
	}
}

func TestLogs_LimitOffset(t *testing.T) {
	store := &mockStore{}
	w := get(newTestServer(store), "/logs?limit=50&offset=200")

	var resp logsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Limit != 50 {
		t.Errorf("limit: got %d, want 50", resp.Limit)
	}
	if resp.Offset != 200 {
		t.Errorf("offset: got %d, want 200", resp.Offset)
	}
	if store.capturedFilter.Limit != 50 {
		t.Errorf("filter limit: got %d, want 50", store.capturedFilter.Limit)
	}
	if store.capturedFilter.Offset != 200 {
		t.Errorf("filter offset: got %d, want 200", store.capturedFilter.Offset)
	}
}

func TestLogs_DBError(t *testing.T) {
	store := &mockStore{logsErr: errors.New("db timeout")}
	w := get(newTestServer(store), "/logs")
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestLogs_InvalidParams(t *testing.T) {
	cases := []string{
		"/logs?from_block=abc",
		"/logs?from_block=-1",
		"/logs?to_block=xyz",
		"/logs?to_block=-5",
		"/logs?limit=bad",
		"/logs?limit=-10",
		"/logs?offset=bad",
		"/logs?offset=-1",
	}
	for _, path := range cases {
		w := get(newTestServer(&mockStore{}), path)
		if w.Code != http.StatusBadRequest {
			t.Errorf("path %s: expected 400, got %d", path, w.Code)
		}
	}
}
