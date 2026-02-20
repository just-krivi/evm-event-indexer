package rpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// helper: build a valid eth_getLogs JSON-RPC success response.
func logsResponse(logs []Log) []byte {
	result, _ := json.Marshal(logs)
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"result":  json.RawMessage(result),
	})
	return body
}

// helper: build an RPC error response.
func rpcErrResponse(code int, message string) []byte {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"error":   map[string]any{"code": code, "message": message},
	})
	return body
}

func TestGetLogs_Success(t *testing.T) {
	want := []Log{
		{
			BlockNumber: "0x1222200",
			BlockHash:   "0xabc",
			TxHash:      "0xdef",
			LogIndex:    "0x0",
			Address:     "0x1234",
			Topics:      []string{"0xsig"},
			Data:        "0x",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(logsResponse(want))
	}))
	defer srv.Close()

	c := New(srv.URL, 1)
	got, err := c.GetLogs(context.Background(), 19000000, 19000000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].TxHash != want[0].TxHash {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestGetLogs_RetriesOn5xx(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(logsResponse([]Log{}))
	}))
	defer srv.Close()

	c := New(srv.URL, 3)
	_, err := c.GetLogs(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestGetLogs_ExhaustsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := New(srv.URL, 2)
	_, err := c.GetLogs(context.Background(), 1, 2)
	if err == nil {
		t.Fatal("expected error after exhausted retries")
	}
}

func TestGetLogs_ResponseTooLarge_Alchemy_200(t *testing.T) {
	// Verify ErrResponseTooLarge is detected when the JSON-RPC error arrives
	// inside an HTTP 200 response (some proxy/provider configurations).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(rpcErrResponse(-32602, "Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range and no limit on the response size, or you can request any block range with a cap of 10K logs in the response."))
	}))
	defer srv.Close()

	c := New(srv.URL, 3)
	_, err := c.GetLogs(context.Background(), 1, 100)
	if err != ErrResponseTooLarge {
		t.Fatalf("expected ErrResponseTooLarge, got: %v", err)
	}
}

func TestGetLogs_ResponseTooLarge_Alchemy_400(t *testing.T) {
	// Alchemy returns HTTP 400 with the JSON-RPC error body in the real API.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(rpcErrResponse(-32602, "Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range and no limit on the response size, or you can request any block range with a cap of 10K logs in the response."))
	}))
	defer srv.Close()

	c := New(srv.URL, 3)
	_, err := c.GetLogs(context.Background(), 1, 100)
	if err != ErrResponseTooLarge {
		t.Fatalf("expected ErrResponseTooLarge, got: %v", err)
	}
}

func TestGetLogs_ResponseTooLarge_FreeTier(t *testing.T) {
	// Alchemy free tier returns -32600 when the block range exceeds 10 blocks.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(rpcErrResponse(-32600, "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range. Based on your parameters, this block range should work: [0x121eac0, 0x121eac9]. Upgrade to PAYG for expanded block range."))
	}))
	defer srv.Close()

	c := New(srv.URL, 1)
	_, err := c.GetLogs(context.Background(), 19_000_000, 19_000_099)
	if err != ErrResponseTooLarge {
		t.Fatalf("expected ErrResponseTooLarge, got: %v", err)
	}
}

func TestGetLogs_InvalidParams_NotTooLarge(t *testing.T) {
	// A genuine -32602 invalid-params error (e.g. malformed address) must NOT
	// be treated as ErrResponseTooLarge — the worker would loop forever.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(rpcErrResponse(-32602, "invalid argument 0: invalid hex address"))
	}))
	defer srv.Close()

	c := New(srv.URL, 1)
	_, err := c.GetLogs(context.Background(), 1, 100)
	if err == ErrResponseTooLarge {
		t.Fatal("genuine invalid-params error must not be classified as ErrResponseTooLarge")
	}
	if err == nil {
		t.Fatal("expected an error")
	}
}


func TestGetLogs_RetriesOn429(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(logsResponse([]Log{}))
	}))
	defer srv.Close()

	c := New(srv.URL, 3)
	_, err := c.GetLogs(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts (1 rate-limited + 1 success), got %d", attempts)
	}
}

func TestGetLogs_ContextCancelled(t *testing.T) {
	// Server that always returns 503 so the client would normally retry.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	c := New(srv.URL, 10) // high retry count — context should cut it short
	_, err := c.GetLogs(ctx, 1, 2)
	if err == nil {
		t.Fatal("expected error due to context cancellation")
	}
}

func TestGetLogs_NonRetriableRPCError(t *testing.T) {
	// A generic RPC error that is not "too large" should not be swallowed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(rpcErrResponse(-32600, "invalid request"))
	}))
	defer srv.Close()

	c := New(srv.URL, 3)
	_, err := c.GetLogs(context.Background(), 1, 10)
	// Should fail, but after retrying (RPC errors without the "too large"
	// sentinel are treated as transient since some providers reuse codes).
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestHexBlock(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{0, "0x0"},
		{1, "0x1"},
		{16, "0x10"},
		{19000000, "0x121eac0"},
	}
	for _, tc := range cases {
		got := hexBlock(tc.n)
		if got != tc.want {
			t.Errorf("hexBlock(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestBackoffDuration(t *testing.T) {
	const maxWait = 30 * time.Second
	for attempt := 0; attempt < 10; attempt++ {
		d := backoffDuration(attempt)
		if d <= 0 {
			t.Errorf("attempt %d: backoff duration must be positive, got %v", attempt, d)
		}
		// Allow 10% above maxWait due to jitter.
		if d > time.Duration(float64(maxWait.Nanoseconds())*1.11) {
			t.Errorf("attempt %d: backoff %v exceeds max %v", attempt, d, maxWait)
		}
	}
}

