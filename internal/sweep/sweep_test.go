package sweep

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"evm-event-indexer/internal/db"
)

// TestMain starts a single Postgres container for the entire sweep test suite.
// If TEST_DATABASE_URL is already set the container is skipped.
func TestMain(m *testing.M) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		os.Exit(m.Run())
	}

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		// Docker unavailable — pure function tests still run; integration tests skip.
		os.Exit(m.Run())
	}
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		os.Exit(1)
	}

	os.Setenv("TEST_DATABASE_URL", connStr)
	os.Exit(m.Run())
}

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

// insertStaleChunk inserts an in_progress chunk whose claimed_at is already
// older than any reasonable timeout, making it immediately eligible for sweep.
func insertStaleChunk(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fromBlock, toBlock, attempts int) int {
	t.Helper()
	var id int
	err := pool.QueryRow(ctx, `
		INSERT INTO chunks (from_block, to_block, status, worker_id, attempts, claimed_at)
		VALUES ($1, $2, 'in_progress', 1, $3, NOW() - INTERVAL '1 hour')
		RETURNING id
	`, fromBlock, toBlock, attempts).Scan(&id)
	if err != nil {
		t.Fatalf("insertStaleChunk: %v", err)
	}
	return id
}

// ============================================================
// runOnce tests — integration, require DB
// ============================================================

func TestRunOnce_RequeuedBelowMaxAttempts(t *testing.T) {
	ctx, pool := setupDB(t)

	// Insert a stale chunk with 1 attempt out of 3 max — should be re-queued.
	insertStaleChunk(t, ctx, pool, 100, 199, 1)

	// claimTimeoutMs=0 makes every in_progress chunk immediately stale.
	runOnce(ctx, pool, 0, 3)

	stats, err := db.GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.Pending != 1 {
		t.Errorf("pending: got %d, want 1", stats.Pending)
	}
	if stats.InProgress != 0 {
		t.Errorf("in_progress: got %d, want 0", stats.InProgress)
	}
	if stats.Failed != 0 {
		t.Errorf("failed: got %d, want 0", stats.Failed)
	}
	if stats.Done != 0 {
		t.Errorf("failed: got %d, want 0", stats.Failed)
	}
}

func TestRunOnce_FailedAtMaxAttempts(t *testing.T) {
	ctx, pool := setupDB(t)

	// Insert a stale chunk that has already reached maxAttempts — should be failed.
	insertStaleChunk(t, ctx, pool, 200, 299, 3)

	runOnce(ctx, pool, 0, 3)

	stats, err := db.GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.Failed != 1 {
		t.Errorf("failed: got %d, want 1", stats.Failed)
	}
	if stats.Pending != 0 {
		t.Errorf("pending: got %d, want 0", stats.Pending)
	}
	if stats.InProgress != 0 {
		t.Errorf("in_progress: got %d, want 0", stats.InProgress)
	}
	if stats.Done != 0 {
		t.Errorf("failed: got %d, want 0", stats.Failed)
	}
}

func TestRunOnce_MixedAttempts(t *testing.T) {
	ctx, pool := setupDB(t)

	// One chunk below max (should be re-queued), one at max (should be failed).
	insertStaleChunk(t, ctx, pool, 100, 199, 2)
	insertStaleChunk(t, ctx, pool, 200, 299, 3)

	runOnce(ctx, pool, 0, 3)

	stats, err := db.GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.Pending != 1 {
		t.Errorf("pending: got %d, want 1", stats.Pending)
	}
	if stats.Failed != 1 {
		t.Errorf("failed: got %d, want 1", stats.Failed)
	}
	if stats.InProgress != 0 {
		t.Errorf("in_progress: got %d, want 0", stats.InProgress)
	}
}

func TestRunOnce_NoOp_NoStaleChunks(t *testing.T) {
	ctx, pool := setupDB(t)

	// Seed a chunk that is pending (not in_progress) — sweep must not touch it.
	if err := db.SeedChunks(ctx, pool, []db.Chunk{{FromBlock: 100, ToBlock: 199}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	runOnce(ctx, pool, 0, 3)

	stats, err := db.GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.Pending != 1 {
		t.Errorf("pending: got %d, want 1 (sweep should not have changed it)", stats.Pending)
	}
	if stats.Failed != 0 || stats.InProgress != 0 {
		t.Errorf("unexpected status change: %+v", stats)
	}
}

func TestRunOnce_FreshChunkNotSwept(t *testing.T) {
	ctx, pool := setupDB(t)

	// Insert an in_progress chunk with claimed_at = NOW() (fresh, not timed out).
	// Use a 1-hour timeout — this chunk should NOT be swept.
	_, err := pool.Exec(ctx, `
		INSERT INTO chunks (from_block, to_block, status, worker_id, attempts, claimed_at)
		VALUES (100, 199, 'in_progress', 1, 1, NOW())
	`)
	if err != nil {
		t.Fatalf("insert fresh chunk: %v", err)
	}

	runOnce(ctx, pool, 3_600_000, 3) // 1-hour timeout

	stats, err := db.GetChunkStats(ctx, pool)
	if err != nil {
		t.Fatalf("GetChunkStats: %v", err)
	}
	if stats.InProgress != 1 {
		t.Errorf("in_progress: got %d, want 1 (fresh chunk must not be swept)", stats.InProgress)
	}
}

// ============================================================
// Run loop tests
// ============================================================

func TestRun_ExitsOnContextCancel(t *testing.T) {
	ctx, pool := setupDB(t)

	cancelCtx, cancel := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(cancelCtx, pool, 50, 0, 3) // 50ms tick
	}()

	// Give the goroutine a moment to start, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

func TestRun_SweepsPeriodicaly(t *testing.T) {
	ctx, pool := setupDB(t)

	// Insert a stale chunk that should be swept.
	insertStaleChunk(t, ctx, pool, 100, 199, 1)

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		Run(cancelCtx, pool, 50, 0, 3) // very short interval so sweep fires quickly
	}()

	// Poll until the chunk transitions away from in_progress.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := db.GetChunkStats(ctx, pool)
		if err != nil {
			t.Fatalf("GetChunkStats: %v", err)
		}
		if stats.InProgress == 0 {
			return // sweep ran and moved the chunk
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("sweep did not process stale chunk within deadline")
}
