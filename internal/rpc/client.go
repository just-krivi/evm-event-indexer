package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

// ErrResponseTooLarge is returned when the RPC node reports more than 10 000
// results for the requested block range.  The caller should split the range
// and retry the two halves.
var ErrResponseTooLarge = errors.New("block range exceeded")

// Log is a single EVM event log as returned by eth_getLogs.
type Log struct {
	Removed     bool     `json:"removed"`     // true when the log was removed due to a chain reorg
	BlockNumber string   `json:"blockNumber"` // hex-encoded block number
	BlockHash   string   `json:"blockHash"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"` // hex-encoded position within the block
	Address     string   `json:"address"`  // EIP-55 mixed-case address; convert to lowercase before storing
	Topics      []string `json:"topics"`   // [topic0, topic1, topic2, topic3] — up to 4 entries
	Data        string   `json:"data"`
}

// Client wraps an Ethereum JSON-RPC endpoint.
type Client struct {
	url        string
	httpClient *http.Client
	maxRetries int
}

// New creates a new RPC client.
// maxRetries controls how many times a single eth_getLogs call is retried
// before returning an error to the caller.
func New(url string, maxRetries int) *Client {
	return &Client{
		url:        url,
		httpClient: &http.Client{Timeout: 35 * time.Second},
		maxRetries: maxRetries,
	}
}

// GetLogs calls eth_getLogs for [fromBlock, toBlock] (inclusive).
// It retries transient errors with exponential backoff.
// It returns ErrResponseTooLarge when the node signals the block range is too
// wide; the caller is responsible for splitting the range.
func (c *Client) GetLogs(ctx context.Context, fromBlock, toBlock int) ([]Log, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		logs, err := c.doGetLogs(ctx, fromBlock, toBlock)
		if err == nil {
			return logs, nil
		}

		// Not retryable: propagate immediately without waiting.
		if errors.Is(err, ErrResponseTooLarge) {
			return nil, err
		}

		lastErr = err

		// Don't sleep after the last attempt.
		if attempt == c.maxRetries-1 {
			break
		}

		wait := backoffDuration(attempt)
		slog.Warn("rpc: transient error, will retry",
			"err", err,
			"attempt", attempt+1,
			"max_retries", c.maxRetries,
		)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("rpc: all %d attempts failed, last error: %w", c.maxRetries, lastErr)
}

// --- internal ---

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

func (c *Client) doGetLogs(ctx context.Context, fromBlock, toBlock int) ([]Log, error) {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	payload := rpcRequest{
		JSONRPC: "2.0",
		Method:  "eth_getLogs",
		Params: []any{map[string]string{
			"fromBlock": hexBlock(fromBlock),
			"toBlock":   hexBlock(toBlock),
		}},
		ID: 1,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	// 5xx and 429 — transient; retry via backoff without reading the body
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}

	// Always read and parse the body from here on.
	// Alchemy returns HTTP 400 (not 200) when eth_getLogs exceeds the result
	// cap, with the error in the JSON-RPC body — we must parse it to detect
	// ErrResponseTooLarge before treating the response as a generic failure.
	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var rpcResp rpcResponse
	if err := json.Unmarshal(rawBody, &rpcResp); err != nil {
		// Body wasn't JSON-RPC — report the HTTP status.
		return nil, fmt.Errorf("http status %d (non-JSON body)", resp.StatusCode)
	}

	if rpcResp.Error != nil {
		if isTooBig(rpcResp.Error) {
			return nil, ErrResponseTooLarge
		}
		return nil, rpcResp.Error
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// 4xx with a JSON-RPC body but no error field — shouldn't happen, treat as failure.
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}

	var logs []Log
	if err := json.Unmarshal(rpcResp.Result, &logs); err != nil {
		return nil, fmt.Errorf("unmarshal logs: %w", err)
	}
	return logs, nil
}

// isTooBig reports whether an RPC error means "your block range is too wide —
// split it and retry each half."
//
// Several distinct Alchemy errors map to this condition:
//
//  1. Pay-As-You-Go result-count cap (-32602, HTTP 400):
//     "Log response size exceeded. You can make eth_getLogs requests with up
//     to a 2K block range … or … a cap of 10K logs in the response."
//
//  2. Free-tier block-range cap (-32600):
//     "Under the Free tier plan, you can make eth_getLogs requests with up to
//     a 10 block range."
//
//  3. "query returned more than 10000 results" Alchemy caps 10k events for
//     a block range.
//
// In those cases the correct action is identical: split the range.
// We match on message text rather than code alone because -32602 is also the
// generic "invalid params" code (e.g. malformed address) and -32600 is the
// generic "invalid request" code — code alone is not a safe discriminator.
func isTooBig(e *rpcError) bool {
	msg := strings.ToLower(e.Message)
	return strings.Contains(msg, "log response size exceeded") ||
		strings.Contains(msg, "10000 results") ||
		strings.Contains(msg, "query returned more than") ||
		strings.Contains(msg, "free tier plan") ||
		strings.Contains(msg, "with up to a 10 block range")
}

// hexBlock returns the block number as a 0x-prefixed hex string.
func hexBlock(n int) string {
	return fmt.Sprintf("0x%x", n)
}

// backoffDuration returns min(100ms × 2^attempt, 30s) with ±10% jitter.
func backoffDuration(attempt int) time.Duration {
	const (
		base    = 100 * time.Millisecond
		maxWait = 10 * time.Second
		jitter  = 0.1
	)
	exp := time.Duration(1 << attempt) // 2^attempt
	wait := base * exp
	if wait > maxWait {
		wait = maxWait
	}
	// Apply ±10% jitter: scale by a random factor in [0.9, 1.1].
	factor := 1.0 - jitter + rand.Float64()*2*jitter
	return time.Duration(float64(wait) * factor)
}
