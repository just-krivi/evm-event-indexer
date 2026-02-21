package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Types ---

type Chunk struct {
	ID        int
	FromBlock int
	ToBlock   int
	Attempts  int
}

type Log struct {
	BlockNumber int
	BlockHash   string
	TxHash      string
	LogIndex    int
	Address     string
	Topic0      *string
	Topic1      *string
	Topic2      *string
	Topic3      *string
	Data        string
}

type ChunkStats struct {
	Total       int
	Done        int
	InProgress  int
	Pending     int
	Failed      int
}

type LogFilter struct {
	Address   *string
	Topic0    *string
	FromBlock *int
	ToBlock   *int
	Limit     int
	Offset    int
}

// --- Chunk operations ---

// SeedChunks bulk-inserts chunks for the given block ranges.
// Uses ON CONFLICT DO NOTHING so it is safe to re-run on resume.
func SeedChunks(ctx context.Context, pool *pgxpool.Pool, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}

	rows := make([][]any, len(chunks))
	for i, c := range chunks {
		rows[i] = []any{c.FromBlock, c.ToBlock}
	}

	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"chunks"},
		[]string{"from_block", "to_block"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		// CopyFrom doesn't support ON CONFLICT, fall back to batch insert
		return seedChunksBatch(ctx, pool, chunks)
	}
	return nil
}

func seedChunksBatch(ctx context.Context, pool *pgxpool.Pool, chunks []Chunk) error {
	batch := &pgx.Batch{}
	for _, c := range chunks {
		batch.Queue(
			`INSERT INTO chunks (from_block, to_block) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			c.FromBlock, c.ToBlock,
		)
	}
	return pool.SendBatch(ctx, batch).Close()
}

// ClaimChunk atomically claims the next pending chunk for the given worker.
// Returns nil, nil if no pending chunk is available.
func ClaimChunk(ctx context.Context, pool *pgxpool.Pool, workerID int) (*Chunk, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var chunk Chunk
	err = tx.QueryRow(ctx, `
		SELECT id, from_block, to_block, attempts
		  FROM chunks
		 WHERE status = 'pending'
		   FOR UPDATE SKIP LOCKED
		 LIMIT 1
	`).Scan(&chunk.ID, &chunk.FromBlock, &chunk.ToBlock, &chunk.Attempts)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select chunk: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE chunks
		   SET status     = 'in_progress',
		       worker_id  = $1,
		       claimed_at = NOW(),
		       attempts   = attempts + 1
		 WHERE id = $2
	`, workerID, chunk.ID)
	if err != nil {
		return nil, fmt.Errorf("claim chunk: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	chunk.Attempts++
	return &chunk, nil
}

// MarkChunkDone atomically inserts logs and marks the chunk as done.
func MarkChunkDone(ctx context.Context, pool *pgxpool.Pool, chunkID int, logs []Log) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := insertLogs(ctx, tx, logs); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE chunks
		   SET status       = 'done',
		       completed_at = NOW()
		 WHERE id = $1
	`, chunkID)
	if err != nil {
		return fmt.Errorf("mark chunk done: %w", err)
	}

	return tx.Commit(ctx)
}

// SplitChunk atomically marks the current chunk done and inserts two sub-chunks.
func SplitChunk(ctx context.Context, pool *pgxpool.Pool, chunkID, fromA, toA, fromB, toB int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE chunks SET status = 'done', completed_at = NOW() WHERE id = $1
	`, chunkID)
	if err != nil {
		return fmt.Errorf("mark original done: %w", err)
	}

	batch := &pgx.Batch{}
	batch.Queue(
		`INSERT INTO chunks (from_block, to_block, attempts) VALUES ($1, $2, 0) ON CONFLICT DO NOTHING`,
		fromA, toA,
	)
	batch.Queue(
		`INSERT INTO chunks (from_block, to_block, attempts) VALUES ($1, $2, 0) ON CONFLICT DO NOTHING`,
		fromB, toB,
	)
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert sub-chunks: %w", err)
	}

	return tx.Commit(ctx)
}

// MarkChunkFailed marks a chunk as failed without retrying.
func MarkChunkFailed(ctx context.Context, pool *pgxpool.Pool, chunkID int) error {
	_, err := pool.Exec(ctx, `
		UPDATE chunks SET status = 'failed' WHERE id = $1
	`, chunkID)
	return err
}

// CountRemaining returns the number of chunks still in pending or in_progress state.
func CountRemaining(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM chunks WHERE status IN ('pending', 'in_progress')
	`).Scan(&count)
	return count, err
}

// GetChunkStats returns a full breakdown of chunk statuses.
func GetChunkStats(ctx context.Context, pool *pgxpool.Pool) (ChunkStats, error) {
	rows, err := pool.Query(ctx, `
		SELECT status, COUNT(*) FROM chunks GROUP BY status
	`)
	if err != nil {
		return ChunkStats{}, err
	}
	defer rows.Close()

	var stats ChunkStats
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return ChunkStats{}, err
		}
		stats.Total += count
		switch status {
		case "done":
			stats.Done = count
		case "in_progress":
			stats.InProgress = count
		case "pending":
			stats.Pending = count
		case "failed":
			stats.Failed = count
		}
	}
	return stats, rows.Err()
}

// --- Sweep ---

// SweepStale re-queues or fails timed-out in_progress chunks.
// Returns the number of re-queued and failed chunks.
func SweepStale(ctx context.Context, pool *pgxpool.Pool, claimTimeoutMs, maxAttempts int) (requeued, failed int, err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Build the interval string in Go — pgx cannot encode int as SQL text for concatenation.
	interval := fmt.Sprintf("%d milliseconds", claimTimeoutMs)

	tag, err := tx.Exec(ctx, `
		UPDATE chunks
		   SET status = 'pending', worker_id = NULL, claimed_at = NULL
		 WHERE status = 'in_progress'
		   AND claimed_at < NOW() - $1::INTERVAL
		   AND attempts < $2
	`, interval, maxAttempts)
	if err != nil {
		return 0, 0, fmt.Errorf("requeue stale: %w", err)
	}
	requeued = int(tag.RowsAffected())

	tag, err = tx.Exec(ctx, `
		UPDATE chunks
		   SET status = 'failed'
		 WHERE status = 'in_progress'
		   AND claimed_at < NOW() - $1::INTERVAL
		   AND attempts >= $2
	`, interval, maxAttempts)
	if err != nil {
		return 0, 0, fmt.Errorf("fail stale: %w", err)
	}
	failed = int(tag.RowsAffected())

	return requeued, failed, tx.Commit(ctx)
}

// --- Logs ---

func insertLogs(ctx context.Context, tx pgx.Tx, logs []Log) error {
	if len(logs) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, l := range logs {
		batch.Queue(`
			INSERT INTO logs
				(block_number, block_hash, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (tx_hash, log_index) DO NOTHING
		`, l.BlockNumber, l.BlockHash, l.TxHash, l.LogIndex, l.Address,
			l.Topic0, l.Topic1, l.Topic2, l.Topic3, l.Data)
	}
	return tx.SendBatch(ctx, batch).Close()
}

// CountLogs returns the total number of rows in the logs table.
func CountLogs(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM logs`).Scan(&count)
	return count, err
}

// QueryLogs returns logs matching the given filter, with pagination.
//
// The WHERE clause is built dynamically so PostgreSQL always sees concrete
// equality/range predicates — never the "IS NULL OR ..." catch-all pattern
// that prevents index use on large tables.
func QueryLogs(ctx context.Context, pool *pgxpool.Pool, f LogFilter) ([]Log, error) {
	if f.Limit <= 0 {
		f.Limit = 100
	}
	if f.Limit > 1000 {
		f.Limit = 1000
	}

	// Build WHERE conditions and args list dynamically.
	// $1/$2 are reserved for LIMIT/OFFSET at the end.
	var conds []string
	var args []any
	next := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if f.Address != nil {
		conds = append(conds, fmt.Sprintf("address = LOWER(%s)", next(*f.Address)))
	}
	if f.Topic0 != nil {
		conds = append(conds, fmt.Sprintf("topic0 = %s", next(*f.Topic0)))
	}
	if f.FromBlock != nil {
		conds = append(conds, fmt.Sprintf("block_number >= %s", next(*f.FromBlock)))
	}
	if f.ToBlock != nil {
		conds = append(conds, fmt.Sprintf("block_number <= %s", next(*f.ToBlock)))
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	limitPlaceholder := next(f.Limit)
	offsetPlaceholder := next(f.Offset)

	query := fmt.Sprintf(`
		SELECT block_number, block_hash, tx_hash, log_index,
		       address, topic0, topic1, topic2, topic3, data
		  FROM logs
		  %s
		 ORDER BY block_number ASC, log_index ASC
		 LIMIT %s OFFSET %s
	`, where, limitPlaceholder, offsetPlaceholder)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Log
	for rows.Next() {
		var l Log
		if err := rows.Scan(
			&l.BlockNumber, &l.BlockHash, &l.TxHash, &l.LogIndex,
			&l.Address, &l.Topic0, &l.Topic1, &l.Topic2, &l.Topic3, &l.Data,
		); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}
