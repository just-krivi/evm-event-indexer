package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/db"
	"evm-event-indexer/internal/rpc"
)

// ============================================================
// Pure function tests — no DB or RPC required
// ============================================================

func TestHexToInt(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"0x0", 0},
		{"0x1", 1},
		{"0xa", 10},
		{"0x64", 100},
		{"0x121eac0", 19000000},
	}
	for _, tc := range cases {
		got := hexToInt(tc.in)
		if got != tc.want {
			t.Errorf("hexToInt(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestConvertLogs_Basic(t *testing.T) {
	topic := "0xsig"
	in := []rpc.Log{{
		BlockNumber: "0x64", // 100
		BlockHash:   "0xblockhash",
		TxHash:      "0xtxhash",
		LogIndex:    "0x3", // 3
		Address:     "0xABCD",
		Topics:      []string{topic},
		Data:        "0xdata",
	}}

	out := convertLogs(in)

	if len(out) != 1 {
		t.Fatalf("len: got %d, want 1", len(out))
	}
	l := out[0]
	if l.BlockNumber != 100 {
		t.Errorf("BlockNumber: got %d, want 100", l.BlockNumber)
	}
	if l.LogIndex != 3 {
		t.Errorf("LogIndex: got %d, want 3", l.LogIndex)
	}
	if l.TxHash != "0xtxhash" {
		t.Errorf("TxHash: got %q", l.TxHash)
	}
}

func TestConvertLogs_AddressLowercased(t *testing.T) {
	in := []rpc.Log{{
		BlockNumber: "0x1",
		TxHash:      "0xtx",
		LogIndex:    "0x0",
		Address:     "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
		Data:        "0x",
	}}
	out := convertLogs(in)
	want := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	if out[0].Address != want {
		t.Errorf("address: got %q, want %q", out[0].Address, want)
	}
}

func TestConvertLogs_SkipsRemovedLogs(t *testing.T) {
	in := []rpc.Log{
		{Removed: true, BlockNumber: "0x1", TxHash: "0xtx1", LogIndex: "0x0", Address: "0xa", Data: "0x"},
		{Removed: false, BlockNumber: "0x2", TxHash: "0xtx2", LogIndex: "0x0", Address: "0xb", Data: "0x"},
	}
	out := convertLogs(in)
	if len(out) != 1 {
		t.Fatalf("len: got %d, want 1 (removed log should be skipped)", len(out))
	}
	if out[0].TxHash != "0xtx2" {
		t.Errorf("wrong log kept: got TxHash=%q", out[0].TxHash)
	}
}

func TestConvertLogs_TopicsSpreading(t *testing.T) {
	in := []rpc.Log{{
		BlockNumber: "0x1",
		TxHash:      "0xtx",
		LogIndex:    "0x0",
		Address:     "0xa",
		Topics:      []string{"0xt0", "0xt1", "0xt2", "0xt3"},
		Data:        "0x",
	}}
	out := convertLogs(in)
	l := out[0]
	check := func(name string, got *string, want string) {
		t.Helper()
		if got == nil || *got != want {
			t.Errorf("%s: got %v, want %q", name, got, want)
		}
	}
	check("Topic0", l.Topic0, "0xt0")
	check("Topic1", l.Topic1, "0xt1")
	check("Topic2", l.Topic2, "0xt2")
	check("Topic3", l.Topic3, "0xt3")
}

func TestConvertLogs_EmptyTopics(t *testing.T) {
	in := []rpc.Log{{
		BlockNumber: "0x1",
		TxHash:      "0xtx",
		LogIndex:    "0x0",
		Address:     "0xa",
		Topics:      nil,
		Data:        "0x",
	}}
	out := convertLogs(in)
	l := out[0]
	if l.Topic0 != nil || l.Topic1 != nil || l.Topic2 != nil || l.Topic3 != nil {
		t.Errorf("all topics should be nil for empty topics slice: %+v", l)
	}
}

func TestConvertLogs_PartialTopics(t *testing.T) {
	in := []rpc.Log{{
		BlockNumber: "0x1",
		TxHash:      "0xtx",
		LogIndex:    "0x0",
		Address:     "0xa",
		Topics:      []string{"0xt0", "0xt1"},
		Data:        "0x",
	}}
	out := convertLogs(in)
	l := out[0]
	if l.Topic0 == nil || *l.Topic0 != "0xt0" {
		t.Errorf("Topic0: got %v", l.Topic0)
	}
	if l.Topic1 == nil || *l.Topic1 != "0xt1" {
		t.Errorf("Topic1: got %v", l.Topic1)
	}
	if l.Topic2 != nil {
		t.Errorf("Topic2 should be nil, got %v", *l.Topic2)
	}
	if l.Topic3 != nil {
		t.Errorf("Topic3 should be nil, got %v", *l.Topic3)
	}
}

// ============================================================
// Integration test helpers
// ============================================================

// setupDB connects to TEST_DATABASE_URL, migrates, and truncates all tables.
// Tests that call setupDB are skipped if TEST_DATABASE_URL is not set.
func setupDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping integration tests")
	}

	ctx := context.Background()
	pool, err := db.NewPool(ctx, url, 5)
	if err != nil {
		t.Fatalf("connect to test DB: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	if err := db.TruncateAll(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

// mockLogsResponse builds a valid eth_getLogs JSON-RPC success body.
func mockLogsResponse(logs []rpc.Log) []byte {
	result, _ := json.Marshal(logs)
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"result":  json.RawMessage(result),
	})
	return body
}

// mockRPCErrResponse builds a JSON-RPC error body.
func mockRPCErrResponse(code int, message string) []byte {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"error":   map[string]any{"code": code, "message": message},
	})
	return body
}

func workerCfg() *config.Config {
	return &config.Config{MaxAttempts: 3, WorkerIntervalMs: 0}
}

// ============================================================
// Integration tests — worker loop end-to-end
// ============================================================

func TestWorkerRun_ProcessesChunk(t *testing.T) {
	ctx, pool := setupDB(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(mockLogsResponse([]rpc.Log{{
			BlockNumber: "0x64", // 100
			BlockHash:   "0xabc",
			TxHash:      "0xtx1",
			LogIndex:    "0x0",
			Address:     "0xABCD", // mixed-case — must be stored lowercase
			Topics:      []string{"0xsig"},
			Data:        "0x",
		}}))
	}))
	defer srv.Close()

	if err := db.SeedChunks(ctx, pool, []db.Chunk{{FromBlock: 100, ToBlock: 100}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	Run(ctx, 1, workerCfg(), pool, rpc.New(srv.URL, 1))

	stats, _ := db.GetChunkStats(ctx, pool)
	if stats.Done != 1 {
		t.Errorf("chunk done: got %d, want 1", stats.Done)
	}

	logs, err := db.QueryLogs(ctx, pool, db.LogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("log count: got %d, want 1", len(logs))
	}
	if logs[0].Address != "0xabcd" {
		t.Errorf("address not lowercased: got %q, want %q", logs[0].Address, "0xabcd")
	}
}

func TestWorkerRun_SplitsChunkOnTooLarge(t *testing.T) {
	ctx, pool := setupDB(t)

	var mu sync.Mutex
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			// First call (original chunk [100,199]) → response too large.
			w.WriteHeader(http.StatusBadRequest)
			w.Write(mockRPCErrResponse(-32602, "Log response size exceeded."))
			return
		}
		// Sub-chunk calls → return empty.
		w.Write(mockLogsResponse([]rpc.Log{}))
	}))
	defer srv.Close()

	if err := db.SeedChunks(ctx, pool, []db.Chunk{{FromBlock: 100, ToBlock: 199}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	Run(ctx, 1, workerCfg(), pool, rpc.New(srv.URL, 1))

	stats, _ := db.GetChunkStats(ctx, pool)
	// Original chunk marked done (by split), both sub-chunks also done.
	if stats.Done != 3 {
		t.Errorf("done: got %d, want 3 (original + 2 sub-chunks)", stats.Done)
	}
	if stats.Pending != 0 || stats.InProgress != 0 {
		t.Errorf("unexpected leftover chunks: %+v", stats)
	}
}

func TestWorkerRun_ExitsWhenNothingToDo(t *testing.T) {
	ctx, pool := setupDB(t)

	// No chunks seeded — in multi-process mode workers wait for coordinator to seed.
	// Cancel the context to confirm the worker exits cleanly without hitting RPC.
	cancelCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(cancelCtx, 1, workerCfg(), pool, rpc.New("http://localhost:9999", 1))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}
}

func TestWorkerRun_ExitsOnContextCancel(t *testing.T) {
	ctx, pool := setupDB(t)

	// The server signals when the RPC call arrives, then blocks until the test
	// explicitly unblocks it. We use a test-controlled channel rather than
	// r.Context().Done() because the Go HTTP transport keeps the TCP connection
	// alive after client-side context cancellation, so r.Context() is never
	// cancelled — relying on it would cause srv.Close() to hang forever.
	entered := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		select {
		case <-unblock:
		case <-time.After(30 * time.Second): // safety valve
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	// Guarantee the handler can always return before srv.Close() is called.
	t.Cleanup(func() {
		select {
		case <-unblock: // already closed — nothing to do
		default:
			close(unblock)
		}
		srv.Close()
	})

	if err := db.SeedChunks(ctx, pool, []db.Chunk{{FromBlock: 100, ToBlock: 199}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		Run(cancelCtx, 1, workerCfg(), pool, rpc.New(srv.URL, 1))
	}()

	// Wait until the RPC call is in flight.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RPC call did not start in time")
	}

	// Cancel the worker's context and unblock the server handler at the same time.
	cancel()
	close(unblock)

	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}

	// Chunk is left in_progress — sweep handles it on the next run.
	stats, _ := db.GetChunkStats(ctx, pool)
	if stats.InProgress != 1 {
		t.Errorf("chunk should be in_progress after cancel: got %+v", stats)
	}
}
