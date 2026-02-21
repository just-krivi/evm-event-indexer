package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"evm-event-indexer/internal/api"
	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/coordinator"
	"evm-event-indexer/internal/db"
	"evm-event-indexer/internal/rpc"
	"evm-event-indexer/internal/worker"
)

func main() {
	// Load .env if present (ignored if missing)
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// Single signal context for the whole process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, cfg.DBPoolSize)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	switch cfg.Mode {
	case "coordinator":
		runCoordinator(ctx, cfg, pool)
	case "worker":
		runWorker(ctx, cfg, pool)
	}
}

func runCoordinator(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) {
	if err := db.Migrate(ctx, pool); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
	slog.Info("migrations applied")

	if cfg.FreshStart {
		if err := db.TruncateAll(ctx, pool); err != nil {
			slog.Error("truncate failed", "error", err)
			os.Exit(1)
		}
		slog.Info("fresh start: truncated chunks and logs")
	}

	slog.Info("coordinator starting",
		"block_from", cfg.BlockFrom,
		"block_to", cfg.BlockTo,
		"chunk_size", cfg.ChunkSize,
	)

	coord := coordinator.New(cfg, pool)

	// activeWorkers reads in_progress chunk count from DB — works across process boundaries.
	activeWorkers := func() int64 {
		stats, err := db.GetChunkStats(context.Background(), pool)
		if err != nil {
			return 0
		}
		return int64(stats.InProgress)
	}

	apiServer := api.New(
		fmt.Sprintf(":%d", cfg.APIPort),
		api.NewPoolStore(pool),
		activeWorkers,
	)
	if err := apiServer.Start(); err != nil {
		slog.Error("failed to start API server", "error", err)
		os.Exit(1)
	}

	// Blocks until all chunks are terminal or ctx is cancelled.
	if err := coord.Run(ctx); err != nil {
		slog.Error("coordinator error", "error", err)
		os.Exit(1)
	}

	if ctx.Err() != nil {
		slog.Info("signal received, shutting down API server")
	} else {
		slog.Info("indexing complete, API still serving", "port", cfg.APIPort)
		<-ctx.Done()
		slog.Info("shutting down API server")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("API shutdown error", "error", err)
	}
}

func runWorker(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) {
	id := worker.NewID()
	rpcClient := rpc.New(cfg.RPCURL, cfg.MaxAttempts)

	slog.Info("worker starting", "worker_id", id)
	worker.Run(ctx, id, cfg, pool, rpcClient)
	slog.Info("worker exiting", "worker_id", id)
}
