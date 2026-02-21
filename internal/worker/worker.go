package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/db"
	"evm-event-indexer/internal/rpc"
)

var maxWaitMs = 500

// NewID generates a random worker ID for use in multi-process deployments.
// Purely observational — stored in chunks.worker_id for debugging.
func NewID() int {
	return rand.Intn(1_000_000) + 1
}

// Run processes chunks in a loop until the context is cancelled or no work remains.
func Run(ctx context.Context, id int, cfg *config.Config, pool *pgxpool.Pool, rpcClient *rpc.Client) {
	label := fmt.Sprintf("worker-%d", id)
	slog.Info("worker started", "worker", label)

	for {
		// Check for cancellation at the start of every iteration.
		select {
		case <-ctx.Done():
			slog.Info("worker shutting down", "worker", label)
			return
		default:
		}

		chunk, err := db.ClaimChunk(ctx, pool, id)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled while claiming — exit cleanly without logging
			}
			slog.Error("claim chunk error", "worker", label, "error", err)
			sleep(ctx, max(cfg.WorkerIntervalMs, maxWaitMs))
			continue
		}

		// Nothing to claim at this moment.
		if chunk == nil {
			stats, err := db.GetChunkStats(ctx, pool)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("get chunk stats error", "worker", label, "error", err)
				sleep(ctx, max(cfg.WorkerIntervalMs, maxWaitMs))
				continue
			}
			if stats.Total == 0 {
				// Coordinator hasn't seeded chunks yet — keep waiting.
				fmt.Printf("[%s] waiting for coordinator to seed chunks...\n", label)
				sleep(ctx, max(cfg.WorkerIntervalMs, maxWaitMs))
				continue
			}
			if stats.Pending == 0 && stats.InProgress == 0 {
				// All chunks are terminal (done or failed).
				slog.Info("all chunks processed, exiting", "worker", label)
				return
			}
			// Some chunks are in_progress held by peers — sweep will re-queue stalled ones.
			fmt.Printf("[%s] no pending chunks, waiting... (in_progress: %d)\n", label, stats.InProgress)
			sleep(ctx, max(cfg.WorkerIntervalMs, maxWaitMs))
			continue
		}

		processChunk(ctx, label, chunk, pool, rpcClient)

		// Optional throttle between successful iterations (0 = max throughput).
		if cfg.WorkerIntervalMs > 0 {
			sleep(ctx, cfg.WorkerIntervalMs)
		}
	}
}

// processChunk fetches logs for a chunk and writes them to the database.
//
// ctx is the same signal-cancelled context as the worker loop. A SIGINT will
// immediately cancel any in-flight RPC call or DB write, leaving the chunk in
// in_progress state. The sweep process detects stale in_progress chunks on the
// next run and re-queues them for fresh processing — no data is lost or corrupted.
func processChunk(ctx context.Context, label string, chunk *db.Chunk, pool *pgxpool.Pool, rpcClient *rpc.Client) {
	start := time.Now()

	rpcLogs, err := rpcClient.GetLogs(ctx, chunk.FromBlock, chunk.ToBlock)

	// Block range is too wide (too much logs)
	// We will mark chunk as done and create two new chunks with half the block range
	if errors.Is(err, rpc.ErrResponseTooLarge) {
		// Block range returns too many logs — split into two halves and re-queue.
		mid := (chunk.FromBlock + chunk.ToBlock) / 2
		if splitErr := db.SplitChunk(ctx, pool, chunk.ID, chunk.FromBlock, mid, mid+1, chunk.ToBlock); splitErr != nil {
			if ctx.Err() == nil {
				slog.Error("split chunk failed", "worker", label, "chunk_id", chunk.ID, "error", splitErr)
			}
			return
		}
		fmt.Printf("[%s] chunk #%d split | blocks %d–%d → [%d–%d] [%d–%d]\n",
			label, chunk.ID, chunk.FromBlock, chunk.ToBlock,
			chunk.FromBlock, mid, mid+1, chunk.ToBlock)
		return
	}

	if err != nil {
		if ctx.Err() == nil {
			// Only log if this wasn't a shutdown cancellation.
			slog.Error("rpc error, chunk left for sweep",
				"worker", label,
				"chunk_id", chunk.ID,
				"from", chunk.FromBlock,
				"to", chunk.ToBlock,
				"error", err,
			)
		}
		return
	}

	dbLogs := convertLogs(rpcLogs)

	if err := db.MarkChunkDone(ctx, pool, chunk.ID, dbLogs); err != nil {
		if ctx.Err() == nil {
			slog.Error("mark chunk done failed", "worker", label, "chunk_id", chunk.ID, "error", err)
		}
		return
	}

	elapsed := time.Since(start)
	elapsedSec := elapsed.Seconds()
	blockCount := float64(chunk.ToBlock - chunk.FromBlock + 1)
	var blocksPerSec, logsPerSec float64
	if elapsedSec > 0 {
		blocksPerSec = blockCount / elapsedSec
		logsPerSec = float64(len(dbLogs)) / elapsedSec
	}
	fmt.Printf("[%s] ✓ chunk #%d | blocks %d–%d | logs: %d | time: %.2fs | rate: %.1f blocks/s | %.1f logs/s | attempts: %d\n",
		label, chunk.ID, chunk.FromBlock, chunk.ToBlock,
		len(dbLogs), elapsedSec, blocksPerSec, logsPerSec, chunk.Attempts)
}

// convertLogs maps rpc.Log → db.Log, normalising hex types and lowercasing addresses.
func convertLogs(rpcLogs []rpc.Log) []db.Log {
	out := make([]db.Log, 0, len(rpcLogs))
	for _, l := range rpcLogs {
		if l.Removed {
			continue // skip logs removed by a chain reorg
		}
		entry := db.Log{
			BlockNumber: hexToInt(l.BlockNumber),
			BlockHash:   l.BlockHash,
			TxHash:      l.TxHash,
			LogIndex:    hexToInt(l.LogIndex),
			Address:     strings.ToLower(l.Address),
			Data:        l.Data,
		}
		if len(l.Topics) > 0 {
			entry.Topic0 = strPtr(l.Topics[0])
		}
		if len(l.Topics) > 1 {
			entry.Topic1 = strPtr(l.Topics[1])
		}
		if len(l.Topics) > 2 {
			entry.Topic2 = strPtr(l.Topics[2])
		}
		if len(l.Topics) > 3 {
			entry.Topic3 = strPtr(l.Topics[3])
		}
		out = append(out, entry)
	}
	return out
}

// hexToInt parses a 0x-prefixed hex string to int.
func hexToInt(h string) int {
	n, _ := strconv.ParseInt(strings.TrimPrefix(h, "0x"), 16, 64)
	return int(n)
}

func strPtr(s string) *string { return &s }

// sleep waits for the given number of milliseconds, returning early if ctx is cancelled.
func sleep(ctx context.Context, ms int) {
	if ms <= 0 {
		return
	}
	select {
	case <-time.After(time.Duration(ms) * time.Millisecond):
	case <-ctx.Done():
	}
}

// max returns the larger of a and b.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
