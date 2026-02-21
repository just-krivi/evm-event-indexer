package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"evm-event-indexer/internal/api"
	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/coordinator"
	"evm-event-indexer/internal/db"
	"evm-event-indexer/internal/rpc"
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
	// Cancels on the first SIGINT/SIGTERM — workers check ctx.Done() and stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, cfg.DBPoolSize)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

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

	slog.Info("startup complete",
		"block_from", cfg.BlockFrom,
		"block_to", cfg.BlockTo,
		"num_workers", cfg.NumWorkers,
		"chunk_size", cfg.ChunkSize,
	)

	rpcClient := rpc.New(cfg.RPCURL, cfg.MaxAttempts)
	coord := coordinator.New(cfg, pool, rpcClient)

	// Start the API server before the coordinator so it's reachable immediately.
	apiServer := api.New(
		fmt.Sprintf(":%d", cfg.APIPort),
		api.NewPoolStore(pool),
		cfg.NumWorkers,
		coord.ActiveWorkers,
	)
	if err := apiServer.Start(); err != nil {
		slog.Error("failed to start API server", "error", err)
		os.Exit(1)
	}

	// Run the coordinator — blocks until all chunks are done or ctx is cancelled.
	if err := coord.Run(ctx); err != nil {
		slog.Error("coordinator error", "error", err)
		os.Exit(1)
	}

	// If the context was cancelled (signal received), shut down the API immediately.
	// If indexing completed naturally, keep the API serving for queries and wait
	// for an explicit shutdown signal.
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
