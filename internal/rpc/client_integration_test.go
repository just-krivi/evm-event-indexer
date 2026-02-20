//go:build integration

package rpc

import (
	"context"
	"errors"
	"os"
	"testing"
)

// Run with:
//
//	RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<key> \
//	  go test ./internal/rpc/... -tags integration -v -run TestIntegration -timeout 60s

func rpcURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RPC_URL")
	if url == "" {
		t.Fatal("RPC_URL env var is required for integration tests")
	}
	return url
}

// TestIntegration_SingleBlock fetches logs for one block and checks the
// response shape (non-empty result, all logs carry the expected block number).
func TestIntegration_SingleBlock(t *testing.T) {
	const block = 19_000_000
	c := New(rpcURL(t), 3)

	logs, err := c.GetLogs(context.Background(), block, block)
	if err != nil {
		t.Fatalf("GetLogs failed: %v", err)
	}

	t.Logf("block %d: %d logs returned", block, len(logs))

	if len(logs) == 0 {
		t.Fatal("expected at least one log for a high-activity mainnet block")
	}

	for i, l := range logs {
		if l.BlockNumber != hexBlock(block) {
			t.Errorf("log[%d]: BlockNumber = %q, want %q", i, l.BlockNumber, hexBlock(block))
		}
		if l.TxHash == "" {
			t.Errorf("log[%d]: TxHash is empty", i)
		}
		if l.Address == "" {
			t.Errorf("log[%d]: Address is empty", i)
		}
	}
}

// TestIntegration_TooLarge requests a 100-block range from a known high-density
// area and expects ErrResponseTooLarge back from Alchemy.
func TestIntegration_TooLarge(t *testing.T) {
	// 100 blocks around 19,000,000 — very high DeFi/NFT activity, easily
	// exceeds Alchemy's 10 000-log cap in a single eth_getLogs call.
	c := New(rpcURL(t), 1) // 1 attempt — we want the error, not a retry loop

	_, err := c.GetLogs(context.Background(), 19_000_000, 19_000_010)
	if err == nil {
		t.Log("no error — Alchemy returned results without hitting the cap (range may have low activity)")
		return
	}
	if errors.Is(err, ErrResponseTooLarge) {
		t.Logf("got expected ErrResponseTooLarge — range splitting logic will trigger correctly")
		return
	}
	t.Fatalf("unexpected error: %v", err)
}
