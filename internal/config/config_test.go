package config

import (
	"testing"
)

func setEnv(t *testing.T, pairs map[string]string) {
	t.Helper()
	for k, v := range pairs {
		t.Setenv(k, v)
	}
}

func validBase(t *testing.T) {
	t.Helper()
	setEnv(t, map[string]string{
		"RPC_URL":      "https://eth-mainnet.example.com",
		"DATABASE_URL": "postgres://user:pass@localhost:5432/db",
		"BLOCK_FROM":   "19000000",
		"BLOCK_TO":     "19005000",
	})
}

func TestLoad_Defaults(t *testing.T) {
	validBase(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.NumWorkers != 2 {
		t.Errorf("NumWorkers: got %d, want 2", cfg.NumWorkers)
	}
	if cfg.ChunkSize != 100 {
		t.Errorf("ChunkSize: got %d, want 100", cfg.ChunkSize)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("MaxAttempts: got %d, want 3", cfg.MaxAttempts)
	}
	if cfg.SweepIntervalMs != 30000 {
		t.Errorf("SweepIntervalMs: got %d, want 30000", cfg.SweepIntervalMs)
	}
	if cfg.WorkerIntervalMs != 10 {
		t.Errorf("WorkerIntervalMs: got %d, want 10", cfg.WorkerIntervalMs)
	}
	if cfg.ClaimTimeoutMs != 120000 {
		t.Errorf("ClaimTimeoutMs: got %d, want 120000", cfg.ClaimTimeoutMs)
	}
	if !cfg.FreshStart {
		t.Errorf("FreshStart: got false, want true")
	}
	if cfg.APIPort != 3000 {
		t.Errorf("APIPort: got %d, want 3000", cfg.APIPort)
	}
	if cfg.DBPoolSize != 20 {
		t.Errorf("DBPoolSize: got %d, want 20", cfg.DBPoolSize)
	}
}

func TestLoad_RequiredFieldsMissing(t *testing.T) {
	tests := []struct {
		name    string
		missing string
	}{
		{"missing RPC_URL", "RPC_URL"},
		{"missing DATABASE_URL", "DATABASE_URL"},
		{"missing BLOCK_FROM", "BLOCK_FROM"},
		{"missing BLOCK_TO", "BLOCK_TO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validBase(t)
			t.Setenv(tt.missing, "")

			_, err := Load()
			if err == nil {
				t.Errorf("expected error when %s is missing", tt.missing)
			}
		})
	}
}

func TestLoad_InvalidIntegers(t *testing.T) {
	fields := []string{
		"BLOCK_FROM", "BLOCK_TO", "NUM_WORKERS", "CHUNK_SIZE",
		"MAX_ATTEMPTS", "SWEEP_INTERVAL_MS", "WORKER_INTERVAL_MS",
		"CLAIM_TIMEOUT_MS", "API_PORT", "DB_POOL_SIZE",
	}

	for _, field := range fields {
		t.Run(field+"_invalid", func(t *testing.T) {
			validBase(t)
			t.Setenv(field, "not-a-number")

			_, err := Load()
			if err == nil {
				t.Errorf("expected error for invalid %s", field)
			}
		})
	}
}

func TestLoad_InvalidBool(t *testing.T) {
	validBase(t)
	t.Setenv("FRESH_START", "maybe")

	_, err := Load()
	if err == nil {
		t.Error("expected error for invalid FRESH_START")
	}
}

func TestLoad_BoolVariants(t *testing.T) {
	trueValues := []string{"true", "1", "yes", "True", "YES"}
	falseValues := []string{"false", "0", "no", "False", "NO"}

	for _, v := range trueValues {
		t.Run("FRESH_START="+v, func(t *testing.T) {
			validBase(t)
			t.Setenv("FRESH_START", v)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !cfg.FreshStart {
				t.Errorf("expected FreshStart=true for %q", v)
			}
		})
	}

	for _, v := range falseValues {
		t.Run("FRESH_START="+v, func(t *testing.T) {
			validBase(t)
			t.Setenv("FRESH_START", v)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.FreshStart {
				t.Errorf("expected FreshStart=false for %q", v)
			}
		})
	}
}

func TestLoad_ValidationRules(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "BLOCK_TO less than BLOCK_FROM",
			env:  map[string]string{"BLOCK_FROM": "19005000", "BLOCK_TO": "19000000"},
		},
		{
			name: "NUM_WORKERS zero",
			env:  map[string]string{"NUM_WORKERS": "0"},
		},
		{
			name: "CHUNK_SIZE zero",
			env:  map[string]string{"CHUNK_SIZE": "0"},
		},
		{
			name: "MAX_ATTEMPTS zero",
			env:  map[string]string{"MAX_ATTEMPTS": "0"},
		},
		{
			name: "API_PORT out of range",
			env:  map[string]string{"API_PORT": "99999"},
		},
		{
			name: "DB_POOL_SIZE zero",
			env:  map[string]string{"DB_POOL_SIZE": "0"},
		},
		{
			name: "WORKER_INTERVAL_MS negative",
			env:  map[string]string{"WORKER_INTERVAL_MS": "-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validBase(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Errorf("expected validation error for %s", tt.name)
			}
		})
	}
}

func TestLoad_OverrideDefaults(t *testing.T) {
	validBase(t)
	setEnv(t, map[string]string{
		"NUM_WORKERS":       "8",
		"CHUNK_SIZE":        "50",
		"MAX_ATTEMPTS":      "5",
		"SWEEP_INTERVAL_MS": "60000",
		"WORKER_INTERVAL_MS": "0",
		"CLAIM_TIMEOUT_MS":  "240000",
		"FRESH_START":       "false",
		"API_PORT":          "8080",
		"DB_POOL_SIZE":      "10",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.NumWorkers != 8 {
		t.Errorf("NumWorkers: got %d, want 8", cfg.NumWorkers)
	}
	if cfg.ChunkSize != 50 {
		t.Errorf("ChunkSize: got %d, want 50", cfg.ChunkSize)
	}
	if cfg.MaxAttempts != 5 {
		t.Errorf("MaxAttempts: got %d, want 5", cfg.MaxAttempts)
	}
	if cfg.SweepIntervalMs != 60000 {
		t.Errorf("SweepIntervalMs: got %d, want 60000", cfg.SweepIntervalMs)
	}
	if cfg.WorkerIntervalMs != 0 {
		t.Errorf("WorkerIntervalMs: got %d, want 0", cfg.WorkerIntervalMs)
	}
	if cfg.ClaimTimeoutMs != 240000 {
		t.Errorf("ClaimTimeoutMs: got %d, want 240000", cfg.ClaimTimeoutMs)
	}
	if cfg.FreshStart {
		t.Errorf("FreshStart: got true, want false")
	}
	if cfg.APIPort != 8080 {
		t.Errorf("APIPort: got %d, want 8080", cfg.APIPort)
	}
	if cfg.DBPoolSize != 10 {
		t.Errorf("DBPoolSize: got %d, want 10", cfg.DBPoolSize)
	}
}
