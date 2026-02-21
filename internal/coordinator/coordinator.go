package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/db"
	"evm-event-indexer/internal/sweep"
)

// Coordinator seeds chunks, runs the sweep, and polls for completion.
type Coordinator struct {
	cfg  *config.Config
	pool *pgxpool.Pool
}

// New creates a Coordinator wired to the given config and database pool.
func New(cfg *config.Config, pool *pgxpool.Pool) *Coordinator {
	return &Coordinator{cfg: cfg, pool: pool}
}

// Run orchestrates the full indexing run:
//  1. Seeds the chunks table for the configured block range.
//  2. Starts the sweep goroutine.
//  3. Polls the DB until all chunks are done or failed (or ctx is cancelled).
//  4. Prints the final summary.
func (c *Coordinator) Run(ctx context.Context) error {
	// 1. Seed chunks.
	chunks := buildChunks(c.cfg.BlockFrom, c.cfg.BlockTo, c.cfg.ChunkSize)
	if err := db.SeedChunks(ctx, c.pool, chunks); err != nil {
		return fmt.Errorf("seed chunks: %w", err)
	}
	slog.Info("seeded chunks",
		"count", len(chunks),
		"from_block", c.cfg.BlockFrom,
		"to_block", c.cfg.BlockTo,
		"chunk_size", c.cfg.ChunkSize,
	)

	start := time.Now()

	// 2. Start sweep goroutine.
	sweepCtx, cancelSweep := context.WithCancel(ctx)
	defer cancelSweep()

	var sweepWg sync.WaitGroup
	sweepWg.Add(1)
	go func() {
		defer sweepWg.Done()
		sweep.Run(sweepCtx, c.pool, c.cfg.SweepIntervalMs, c.cfg.ClaimTimeoutMs, c.cfg.MaxAttempts)
	}()

	// 3. Poll until all chunks are terminal (done or failed) or ctx is cancelled.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	slog.Info("coordinator waiting for workers to process chunks")
poll:
	for {
		select {
		case <-ctx.Done():
			slog.Info("coordinator context cancelled, stopping")
			break poll
		case <-ticker.C:
			stats, err := db.GetChunkStats(ctx, c.pool)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("failed to get chunk stats", "error", err)
				}
				continue
			}
			if stats.Total > 0 && stats.Pending == 0 && stats.InProgress == 0 {
				slog.Info("all chunks processed")
				break poll
			}
			slog.Info("progress",
				"done", stats.Done,
				"pending", stats.Pending,
				"in_progress", stats.InProgress,
				"failed", stats.Failed,
				"total", stats.Total,
			)
		}
	}

	cancelSweep()
	sweepWg.Wait()

	// 4. Print summary (use a fresh context — the run context may be cancelled).
	c.printSummary(context.Background(), start)

	return nil
}

// buildChunks divides [from, to] (inclusive) into chunks of chunkSize blocks each.
func buildChunks(from, to, chunkSize int) []db.Chunk {
	total := to - from + 1
	n := int(math.Ceil(float64(total) / float64(chunkSize)))
	chunks := make([]db.Chunk, 0, n)
	for i := 0; i < n; i++ {
		f := from + i*chunkSize
		t := f + chunkSize - 1
		if t > to {
			t = to
		}
		chunks = append(chunks, db.Chunk{FromBlock: f, ToBlock: t})
	}
	return chunks
}

func (c *Coordinator) printSummary(ctx context.Context, start time.Time) {
	elapsed := time.Since(start)

	stats, err := db.GetChunkStats(ctx, c.pool)
	if err != nil {
		slog.Error("failed to read final chunk stats", "error", err)
		return
	}

	totalLogs, err := db.CountLogs(ctx, c.pool)
	if err != nil {
		slog.Error("failed to count final logs", "error", err)
		return
	}

	totalBlocks := c.cfg.BlockTo - c.cfg.BlockFrom + 1
	elapsedSec := elapsed.Seconds()
	var blocksPerSec, logsPerSec float64
	if elapsedSec > 0 {
		blocksPerSec = float64(totalBlocks) / elapsedSec
		logsPerSec = float64(totalLogs) / elapsedSec
	}

	fmt.Println()
	fmt.Println("════════════════════════════════════════════")
	fmt.Println("  EVM Indexer — Run Complete")
	fmt.Printf("  Block range:    %d – %d\n", c.cfg.BlockFrom, c.cfg.BlockTo)
	fmt.Printf("  Total chunks:   %d\n", stats.Total)
	fmt.Printf("  Done:           %d\n", stats.Done)
	fmt.Printf("  Failed:         %d\n", stats.Failed)
	fmt.Printf("  Total logs:     %d\n", totalLogs)
	fmt.Printf("  Total blocks:   %d\n", totalBlocks)
	fmt.Printf("  Total time:      %.1fs\n", elapsedSec)
	fmt.Printf("  Avg rate:       %.1f blocks/s | %.0f logs/s\n", blocksPerSec, logsPerSec)
	fmt.Println("════════════════════════════════════════════")
}
