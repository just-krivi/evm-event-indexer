package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/joho/godotenv"

	"evm-event-indexer/internal/config"
	"evm-event-indexer/internal/db"
)

func main() {
	// Load .env if present (ignored if missing)
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

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

	// Coordinator, workers, sweep, and API will be wired here in later phases.
}
