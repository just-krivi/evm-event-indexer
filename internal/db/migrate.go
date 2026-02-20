package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrate runs idempotent schema migrations. Safe to call on every startup.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS chunks (
			id            SERIAL PRIMARY KEY,
			from_block    INTEGER      NOT NULL,
			to_block      INTEGER      NOT NULL,
			status        TEXT         NOT NULL DEFAULT 'pending',
			worker_id     INTEGER,
			attempts      INTEGER      NOT NULL DEFAULT 0,
			claimed_at    TIMESTAMPTZ,
			completed_at  TIMESTAMPTZ,
			created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			CONSTRAINT chunks_range_unique UNIQUE (from_block, to_block)
		)`,

		`CREATE TABLE IF NOT EXISTS logs (
			id            BIGSERIAL    PRIMARY KEY,
			block_number  INTEGER      NOT NULL,
			block_hash    TEXT         NOT NULL,
			tx_hash       TEXT         NOT NULL,
			log_index     INTEGER      NOT NULL,
			address       TEXT         NOT NULL,
			topic0        TEXT,
			topic1        TEXT,
			topic2        TEXT,
			topic3        TEXT,
			data          TEXT         NOT NULL,
			CONSTRAINT logs_unique UNIQUE (tx_hash, log_index)
		)`,

		// Covers address-only, address+topic0, address+topic0+block_number
		`CREATE INDEX IF NOT EXISTS logs_addr_topic0_block_idx
			ON logs (address, topic0, block_number)`,

		// Covers topic0-only and topic0+block_range queries
		`CREATE INDEX IF NOT EXISTS logs_topic0_block_idx
			ON logs (topic0, block_number)`,

		// Covers pure block-range queries
		`CREATE INDEX IF NOT EXISTS logs_block_number_idx
			ON logs (block_number)`,
	}

	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	return nil
}

// TruncateAll removes all data from chunks and logs, resetting sequences.
func TruncateAll(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "TRUNCATE chunks, logs RESTART IDENTITY CASCADE")
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}
