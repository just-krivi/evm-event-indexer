package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/db"
	"evm-event-indexer/internal/rpc"
	"evm-event-indexer/internal/sweep"
	"evm-event-indexer/internal/worker"
)

// Coordinator seeds chunks, manages worker lifecycles, and prints the final summary.
type Coordinator struct {
	cfg        *config.Config
	pool       *pgxpool.Pool
	rpc        *rpc.Client
	numWorkers atomic.Int64 // count of currently active worker goroutines
}

// New creates a Coordinator wired to the given config, database pool, and RPC client.
func New(cfg *config.Config, pool *pgxpool.Pool, rpcClient *rpc.Client) *Coordinator {
	return &Coordinator{cfg: cfg, pool: pool, rpc: rpcClient}
}

// ActiveWorkers returns the number of currently running worker goroutines.
// Used by the /health endpoint (wired in Phase 5).
func (c *Coordinator) ActiveWorkers() int64 {
	return c.numWorkers.Load()
}

// Run orchestrates the full indexing run:
//  1. Seeds the chunks table for the configured block range.
//  2. Spawns NUM_WORKERS worker goroutines.
//  3. Blocks until all workers exit (ctx cancellation or no work remaining).
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

	// Start sweep goroutine with its own cancel so it stops when workers are done.
	sweepCtx, cancelSweep := context.WithCancel(ctx)
	defer cancelSweep()

	var sweepWg sync.WaitGroup
	sweepWg.Add(1)
	go func() {
		defer sweepWg.Done()
		sweep.Run(sweepCtx, c.pool, c.cfg.SweepIntervalMs, c.cfg.ClaimTimeoutMs, c.cfg.MaxAttempts)
	}()

	// 2. Spawn workers.
	var workerWg sync.WaitGroup
	for i := 1; i <= c.cfg.NumWorkers; i++ {
		workerWg.Add(1)
		id := i
		c.numWorkers.Add(1)
		go func() {
			defer workerWg.Done()
			defer c.numWorkers.Add(-1)
			worker.Run(ctx, id, c.cfg, c.pool, c.rpc)
		}()
	}

	// 3. Block until all workers have exited, then stop sweep.
	workerWg.Wait()
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
	fmt.Printf("  Workers:        %d\n", c.cfg.NumWorkers)
	fmt.Println("════════════════════════════════════════════")
}
