package sweep

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"evm-event-indexer/internal/db"
)

// Run starts the sweep loop, ticking every intervalMs until ctx is cancelled.
// Each tick re-queues timed-out in_progress chunks that have remaining attempts,
// and permanently fails those that have exhausted their attempts.
func Run(ctx context.Context, pool *pgxpool.Pool, intervalMs, claimTimeoutMs, maxAttempts int) {
	slog.Info("sweep started", "interval_ms", intervalMs)
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("sweep shutting down")
			return
		case <-ticker.C:
			runOnce(ctx, pool, claimTimeoutMs, maxAttempts)
		}
	}
}

// runOnce performs a single sweep cycle: re-queues or fails stale chunks,
// then prints a status line.
func runOnce(ctx context.Context, pool *pgxpool.Pool, claimTimeoutMs, maxAttempts int) {
	requeued, failed, err := db.SweepStale(ctx, pool, claimTimeoutMs, maxAttempts)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("sweep error", "error", err)
		}
		return
	}

	stats, err := db.GetChunkStats(ctx, pool)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("sweep stats error", "error", err)
		}
		return
	}

	fmt.Printf("[sweep] re-queued: %d | failed: %d | pending: %d | in_progress: %d | done: %d | total: %d\n",
		requeued, failed, stats.Pending, stats.InProgress, stats.Done, stats.Total)
}
