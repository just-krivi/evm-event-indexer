package db

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// setup connects to TEST_DATABASE_URL, runs migrations, and truncates all tables.
// Tests are skipped automatically when TEST_DATABASE_URL is not set.
func setup(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping DB integration tests")
	}

	ctx := context.Background()

	pool, err := NewPool(ctx, url, 5)
	if err != nil {
		t.Fatalf("connect to test DB: %v", err)
	}

	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}

	if err := TruncateAll(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("truncate: %v", err)
	}

	t.Cleanup(pool.Close)
	return ctx, pool
}

// testLogs returns a deterministic set of logs covering multiple addresses,
// topic0 values, and block numbers for filter tests.
func testLogs() []Log {
	transfer := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	approval := "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
	addr1 := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" // USDC
	addr2 := "0xdac17f958d2ee523a2206206994597c13d831ec7" // USDT

	return []Log{
		{BlockNumber: 100, BlockHash: "0xh1", TxHash: "0xtx1", LogIndex: 0, Address: addr1, Topic0: &transfer, Data: "0x"},
		{BlockNumber: 100, BlockHash: "0xh1", TxHash: "0xtx1", LogIndex: 1, Address: addr2, Topic0: &approval, Data: "0x"},
		{BlockNumber: 150, BlockHash: "0xh2", TxHash: "0xtx2", LogIndex: 0, Address: addr1, Topic0: &transfer, Data: "0x"},
		{BlockNumber: 200, BlockHash: "0xh3", TxHash: "0xtx3", LogIndex: 0, Address: addr2, Topic0: &transfer, Data: "0x"},
	}
}

// seedAndClaim seeds one chunk, claims it, and returns it — convenience for log-focused tests.
func seedAndClaim(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *Chunk {
	t.Helper()
	if err := SeedChunks(ctx, pool, []Chunk{{FromBlock: 100, ToBlock: 200}}); err != nil {
		t.Fatalf("SeedChunks: %v", err)
	}
	chunk, err := ClaimChunk(ctx, pool, 1)
	if err != nil || chunk == nil {
		t.Fatalf("ClaimChunk: err=%v chunk=%v", err, chunk)
	}
	return chunk
}

// ============================================================
// SeedChunks
// ============================================================

func TestSeedChunks_Basic(t *testing.T) {
	ctx, pool := setup(t)

	chunks := []Chunk{
		{FromBlock: 100, ToBlock: 199},
		{FromBlock: 200, ToBlock: 299},
		{FromBlock: 300, ToBlock: 399},
	}
	if err := SeedChunks(ctx, pool, chunks); err != nil {
		t.Fatalf("SeedChunks: %v", err)
	}

	stats, err := GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.Total != 3 || stats.Pending != 3 {
		t.Errorf("want total=3 pending=3, got %+v", stats)
	}
}

func TestSeedChunks_Idempotent(t *testing.T) {
	ctx, pool := setup(t)

	chunks := []Chunk{
		{FromBlock: 100, ToBlock: 199},
		{FromBlock: 200, ToBlock: 299},
	}
	if err := SeedChunks(ctx, pool, chunks); err != nil {
		t.Fatalf("first SeedChunks: %v", err)
	}
	if err := SeedChunks(ctx, pool, chunks); err != nil {
		t.Fatalf("second SeedChunks: %v", err)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Total != 2 {
		t.Errorf("re-seed: want total=2, got %d", stats.Total)
	}
}

func TestSeedChunks_Empty(t *testing.T) {
	ctx, pool := setup(t)

	if err := SeedChunks(ctx, pool, nil); err != nil {
		t.Errorf("nil slice: %v", err)
	}
	if err := SeedChunks(ctx, pool, []Chunk{}); err != nil {
		t.Errorf("empty slice: %v", err)
	}
}

// ============================================================
// ClaimChunk
// ============================================================

func TestClaimChunk_NoPending(t *testing.T) {
	ctx, pool := setup(t)

	chunk, err := ClaimChunk(ctx, pool, 1)
	if err != nil {
		t.Fatalf("ClaimChunk: %v", err)
	}
	if chunk != nil {
		t.Errorf("expected nil when nothing pending, got %+v", chunk)
	}
}

func TestClaimChunk_ClaimsCorrectly(t *testing.T) {
	ctx, pool := setup(t)

	SeedChunks(ctx, pool, []Chunk{{FromBlock: 100, ToBlock: 199}})

	chunk, err := ClaimChunk(ctx, pool, 42)
	if err != nil {
		t.Fatalf("ClaimChunk: %v", err)
	}
	if chunk == nil {
		t.Fatal("expected a chunk, got nil")
	}
	if chunk.FromBlock != 100 || chunk.ToBlock != 199 {
		t.Errorf("wrong range: %d–%d", chunk.FromBlock, chunk.ToBlock)
	}
	if chunk.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1", chunk.Attempts)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.InProgress != 1 || stats.Pending != 0 {
		t.Errorf("want in_progress=1 pending=0, got %+v", stats)
	}
}

func TestClaimChunk_IncrementsAttempts(t *testing.T) {
	ctx, pool := setup(t)

	SeedChunks(ctx, pool, []Chunk{{FromBlock: 100, ToBlock: 199}})
	chunk, _ := ClaimChunk(ctx, pool, 1)

	// Manually re-queue to simulate sweep returning it to pending.
	pool.Exec(ctx, `UPDATE chunks SET status='pending', claimed_at=NULL WHERE id=$1`, chunk.ID)

	chunk2, err := ClaimChunk(ctx, pool, 1)
	if err != nil || chunk2 == nil {
		t.Fatalf("second claim: err=%v chunk=%v", err, chunk2)
	}
	if chunk2.Attempts != 2 {
		t.Errorf("attempts on second claim: got %d, want 2", chunk2.Attempts)
	}
}

func TestClaimChunk_SkipsInProgress(t *testing.T) {
	ctx, pool := setup(t)

	SeedChunks(ctx, pool, []Chunk{
		{FromBlock: 100, ToBlock: 199},
		{FromBlock: 200, ToBlock: 299},
	})
	ClaimChunk(ctx, pool, 1)
	ClaimChunk(ctx, pool, 2)

	third, err := ClaimChunk(ctx, pool, 3)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if third != nil {
		t.Errorf("expected nil when all in_progress, got %+v", third)
	}
}

// ============================================================
// MarkChunkDone
// ============================================================

func TestMarkChunkDone_NoLogs(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	if err := MarkChunkDone(ctx, pool, chunk.ID, nil); err != nil {
		t.Fatalf("MarkChunkDone: %v", err)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Done != 1 {
		t.Errorf("done: got %d, want 1", stats.Done)
	}
}

func TestMarkChunkDone_InsertsLogs(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	if err := MarkChunkDone(ctx, pool, chunk.ID, testLogs()); err != nil {
		t.Fatalf("MarkChunkDone: %v", err)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Done != 1 {
		t.Errorf("done: got %d, want 1", stats.Done)
	}

	result, err := QueryLogs(ctx, pool, LogFilter{})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 4 {
		t.Errorf("log count: got %d, want 4", len(result))
	}
}

func TestMarkChunkDone_IdempotentLogInsert(t *testing.T) {
	ctx, pool := setup(t)

	// Two chunks that could theoretically return the same log.
	SeedChunks(ctx, pool, []Chunk{
		{FromBlock: 100, ToBlock: 149},
		{FromBlock: 150, ToBlock: 199},
	})

	logs := []Log{{BlockNumber: 100, BlockHash: "0xh", TxHash: "0xtx", LogIndex: 0, Address: "0xaddr", Data: "0x"}}

	c1, _ := ClaimChunk(ctx, pool, 1)
	MarkChunkDone(ctx, pool, c1.ID, logs)

	c2, _ := ClaimChunk(ctx, pool, 1)
	if err := MarkChunkDone(ctx, pool, c2.ID, logs); err != nil {
		t.Fatalf("duplicate insert should not error: %v", err)
	}

	result, _ := QueryLogs(ctx, pool, LogFilter{Limit: 100})
	if len(result) != 1 {
		t.Errorf("expected 1 unique log, got %d", len(result))
	}
}

// ============================================================
// SplitChunk
// ============================================================

func TestSplitChunk_CreatesSubChunks(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	if err := SplitChunk(ctx, pool, chunk.ID, 100, 149, 150, 200); err != nil {
		t.Fatalf("SplitChunk: %v", err)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Done != 1 {
		t.Errorf("original done: got %d, want 1", stats.Done)
	}
	if stats.Pending != 2 {
		t.Errorf("sub-chunks pending: got %d, want 2", stats.Pending)
	}
	if stats.Total != 3 {
		t.Errorf("total: got %d, want 3", stats.Total)
	}
}

func TestSplitChunk_SubChunksStartWithZeroAttempts(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	SplitChunk(ctx, pool, chunk.ID, 100, 149, 150, 200)

	// Claim both sub-chunks and verify attempts start at 1 (first claim).
	c1, _ := ClaimChunk(ctx, pool, 1)
	c2, _ := ClaimChunk(ctx, pool, 1)
	if c1 == nil || c2 == nil {
		t.Fatal("expected two sub-chunks to be claimable")
	}
	if c1.Attempts != 1 {
		t.Errorf("sub-chunk1 attempts after claim: got %d, want 1", c1.Attempts)
	}
	if c2.Attempts != 1 {
		t.Errorf("sub-chunk2 attempts after claim: got %d, want 1", c2.Attempts)
	}
}

func TestSplitChunk_Idempotent(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	SplitChunk(ctx, pool, chunk.ID, 100, 149, 150, 200)

	// Same sub-ranges again — ON CONFLICT DO NOTHING.
	if err := SplitChunk(ctx, pool, chunk.ID, 100, 149, 150, 200); err != nil {
		t.Errorf("duplicate SplitChunk should not error: %v", err)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Total != 3 {
		t.Errorf("total after duplicate split: got %d, want 3", stats.Total)
	}
}

// ============================================================
// MarkChunkFailed
// ============================================================

func TestMarkChunkFailed(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	if err := MarkChunkFailed(ctx, pool, chunk.ID); err != nil {
		t.Fatalf("MarkChunkFailed: %v", err)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Failed != 1 {
		t.Errorf("failed: got %d, want 1", stats.Failed)
	}
	if stats.InProgress != 0 {
		t.Errorf("in_progress after fail: got %d, want 0", stats.InProgress)
	}
}

// ============================================================
// CountRemaining
// ============================================================

func TestCountRemaining(t *testing.T) {
	ctx, pool := setup(t)

	SeedChunks(ctx, pool, []Chunk{
		{FromBlock: 100, ToBlock: 199},
		{FromBlock: 200, ToBlock: 299},
		{FromBlock: 300, ToBlock: 399},
	})

	remaining, err := CountRemaining(ctx, pool)
	if err != nil {
		t.Fatalf("CountRemaining: %v", err)
	}
	if remaining != 3 {
		t.Errorf("all pending: got %d, want 3", remaining)
	}

	// Claim one, mark another done.
	c1, _ := ClaimChunk(ctx, pool, 1)
	c2, _ := ClaimChunk(ctx, pool, 1)
	MarkChunkDone(ctx, pool, c2.ID, nil)
	_ = c1 // left in_progress — still counts

	remaining, _ = CountRemaining(ctx, pool)
	// c1 in_progress + one still pending = 2.
	if remaining != 2 {
		t.Errorf("after claim+done: got %d, want 2", remaining)
	}
}

func TestCountRemaining_ZeroWhenAllDone(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, nil)

	remaining, _ := CountRemaining(ctx, pool)
	if remaining != 0 {
		t.Errorf("all done: got %d, want 0", remaining)
	}
}

// ============================================================
// GetChunkStats
// ============================================================

func TestGetChunkStats_Empty(t *testing.T) {
	ctx, pool := setup(t)

	stats, err := GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.Total != 0 {
		t.Errorf("empty DB: got total=%d, want 0", stats.Total)
	}
}

func TestGetChunkStats_AllStatuses(t *testing.T) {
	ctx, pool := setup(t)

	SeedChunks(ctx, pool, []Chunk{
		{FromBlock: 100, ToBlock: 199}, // → done
		{FromBlock: 200, ToBlock: 299}, // → in_progress
		{FromBlock: 300, ToBlock: 399}, // → failed
		{FromBlock: 400, ToBlock: 499}, // stays pending
	})

	done, _ := ClaimChunk(ctx, pool, 1)
	inProg, _ := ClaimChunk(ctx, pool, 2)
	failed, _ := ClaimChunk(ctx, pool, 3)

	MarkChunkDone(ctx, pool, done.ID, nil)
	MarkChunkFailed(ctx, pool, failed.ID)
	_ = inProg

	stats, err := GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}

	if stats.Total != 4 {
		t.Errorf("total: got %d, want 4", stats.Total)
	}
	if stats.Done != 1 {
		t.Errorf("done: got %d, want 1", stats.Done)
	}
	if stats.InProgress != 1 {
		t.Errorf("in_progress: got %d, want 1", stats.InProgress)
	}
	if stats.Pending != 1 {
		t.Errorf("pending: got %d, want 1", stats.Pending)
	}
	if stats.Failed != 1 {
		t.Errorf("failed: got %d, want 1", stats.Failed)
	}
}

// ============================================================
// SweepStale
// ============================================================

func backdateClaimedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chunkID int) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`UPDATE chunks SET claimed_at = NOW() - INTERVAL '10 minutes' WHERE id = $1`,
		chunkID,
	)
	if err != nil {
		t.Fatalf("backdate claimed_at: %v", err)
	}
}

func TestSweepStale_RequeuesWhenAttemptsRemaining(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool) // attempts=1
	backdateClaimedAt(t, ctx, pool, chunk.ID)

	// maxAttempts=3, current=1 → should requeue.
	requeued, failed, err := SweepStale(ctx, pool, 1000, 3)
	if err != nil {
		t.Fatalf("SweepStale: %v", err)
	}
	if requeued != 1 {
		t.Errorf("requeued: got %d, want 1", requeued)
	}
	if failed != 0 {
		t.Errorf("failed: got %d, want 0", failed)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Pending != 1 || stats.InProgress != 0 {
		t.Errorf("after requeue: want pending=1 in_progress=0, got %+v", stats)
	}
}

func TestSweepStale_FailsWhenAttemptsExhausted(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	backdateClaimedAt(t, ctx, pool, chunk.ID)
	// Force attempts to maxAttempts.
	pool.Exec(ctx, `UPDATE chunks SET attempts = 3 WHERE id = $1`, chunk.ID)

	requeued, failed, err := SweepStale(ctx, pool, 1000, 3)
	if err != nil {
		t.Fatalf("SweepStale: %v", err)
	}
	if requeued != 0 {
		t.Errorf("requeued: got %d, want 0", requeued)
	}
	if failed != 1 {
		t.Errorf("failed: got %d, want 1", failed)
	}

	stats, _ := GetChunkStats(ctx, pool)
	if stats.Failed != 1 {
		t.Errorf("failed status: got %d, want 1", stats.Failed)
	}
}

func TestSweepStale_IgnoresRecentChunks(t *testing.T) {
	ctx, pool := setup(t)

	seedAndClaim(t, ctx, pool) // claimed_at = NOW()

	// Large timeout — chunk just claimed, should not be touched.
	requeued, failed, err := SweepStale(ctx, pool, 3_600_000, 3)
	if err != nil {
		t.Fatalf("SweepStale: %v", err)
	}
	if requeued != 0 || failed != 0 {
		t.Errorf("should not touch recent chunk: requeued=%d failed=%d", requeued, failed)
	}
}

func TestSweepStale_NothingToSweep(t *testing.T) {
	ctx, pool := setup(t)

	requeued, failed, err := SweepStale(ctx, pool, 1000, 3)
	if err != nil {
		t.Fatalf("SweepStale on empty DB: %v", err)
	}
	if requeued != 0 || failed != 0 {
		t.Errorf("empty DB: got requeued=%d failed=%d, want 0/0", requeued, failed)
	}
}

func TestSweepStale_ResetsWorkerID(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	backdateClaimedAt(t, ctx, pool, chunk.ID)

	SweepStale(ctx, pool, 1000, 3)

	// After requeue, worker_id and claimed_at should be NULL.
	var workerID *int
	var claimedAt *string
	pool.QueryRow(ctx,
		`SELECT worker_id, claimed_at::text FROM chunks WHERE id = $1`,
		chunk.ID,
	).Scan(&workerID, &claimedAt)

	if workerID != nil {
		t.Errorf("worker_id should be NULL after requeue, got %v", *workerID)
	}
	if claimedAt != nil {
		t.Errorf("claimed_at should be NULL after requeue, got %v", *claimedAt)
	}
}

// ============================================================
// QueryLogs
// ============================================================

func TestQueryLogs_NoFilter(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	result, err := QueryLogs(ctx, pool, LogFilter{Limit: 100})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 4 {
		t.Errorf("no filter: got %d, want 4", len(result))
	}
}

func TestQueryLogs_FilterByAddress(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	addr := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	result, err := QueryLogs(ctx, pool, LogFilter{Address: &addr, Limit: 100})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("address filter: got %d, want 2", len(result))
	}
}

func TestQueryLogs_AddressIsCaseInsensitive(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	// Pass UPPERCASE — stored as lowercase, query uses LOWER($1).
	addr := "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"
	result, err := QueryLogs(ctx, pool, LogFilter{Address: &addr, Limit: 100})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("case-insensitive address: got %d, want 2", len(result))
	}
}

func TestQueryLogs_FilterByTopic0(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	topic := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	result, err := QueryLogs(ctx, pool, LogFilter{Topic0: &topic, Limit: 100})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("topic0 filter: got %d, want 3", len(result))
	}
}

func TestQueryLogs_FilterByBlockRange(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	from, to := 100, 150
	result, err := QueryLogs(ctx, pool, LogFilter{FromBlock: &from, ToBlock: &to, Limit: 100})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("block range 100–150: got %d, want 3", len(result))
	}
}

func TestQueryLogs_FilterAddressAndTopic0(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	addr := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	topic := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	result, err := QueryLogs(ctx, pool, LogFilter{Address: &addr, Topic0: &topic, Limit: 100})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("address+topic0: got %d, want 2", len(result))
	}
}

func TestQueryLogs_Pagination(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	page1, _ := QueryLogs(ctx, pool, LogFilter{Limit: 2, Offset: 0})
	page2, _ := QueryLogs(ctx, pool, LogFilter{Limit: 2, Offset: 2})

	if len(page1) != 2 {
		t.Errorf("page1: got %d, want 2", len(page1))
	}
	if len(page2) != 2 {
		t.Errorf("page2: got %d, want 2", len(page2))
	}
	// Pages must not contain the same rows.
	key := func(l Log) string { return l.TxHash + ":" + string(rune(l.LogIndex)) }
	seen := make(map[string]bool)
	for _, l := range page1 {
		seen[key(l)] = true
	}
	for _, l := range page2 {
		if seen[key(l)] {
			t.Errorf("pages overlap on %s", key(l))
		}
	}
}

func TestQueryLogs_LimitCappedAt1000(t *testing.T) {
	ctx, pool := setup(t)

	// No rows needed — just verify no error and no panic with oversized limit.
	_, err := QueryLogs(ctx, pool, LogFilter{Limit: 99999})
	if err != nil {
		t.Fatalf("oversized limit: %v", err)
	}
}

func TestQueryLogs_DefaultLimit(t *testing.T) {
	ctx, pool := setup(t)

	// Default limit (0) should be treated as 100, not return error.
	_, err := QueryLogs(ctx, pool, LogFilter{})
	if err != nil {
		t.Fatalf("default limit: %v", err)
	}
}

func TestQueryLogs_OrderedByBlockThenLogIndex(t *testing.T) {
	ctx, pool := setup(t)

	chunk := seedAndClaim(t, ctx, pool)
	MarkChunkDone(ctx, pool, chunk.ID, testLogs())

	result, _ := QueryLogs(ctx, pool, LogFilter{Limit: 100})

	for i := 1; i < len(result); i++ {
		prev, cur := result[i-1], result[i]
		if cur.BlockNumber < prev.BlockNumber {
			t.Errorf("out of order at [%d]: block %d before %d", i, cur.BlockNumber, prev.BlockNumber)
		}
		if cur.BlockNumber == prev.BlockNumber && cur.LogIndex < prev.LogIndex {
			t.Errorf("out of order at [%d]: log_index %d before %d within block %d",
				i, cur.LogIndex, prev.LogIndex, cur.BlockNumber)
		}
	}
}
