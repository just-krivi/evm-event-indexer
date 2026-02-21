package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Mode             string // "coordinator" | "worker"
	RPCURL           string
	DatabaseURL      string
	BlockFrom        int
	BlockTo          int
	ChunkSize        int
	MaxAttempts      int
	SweepIntervalMs  int
	WorkerIntervalMs int
	ClaimTimeoutMs   int
	FreshStart       bool
	APIPort          int
	DBPoolSize       int
}

func Load() (*Config, error) {
	cfg := &Config{
		Mode:             "coordinator",
		ChunkSize:        10,
		MaxAttempts:      3,
		SweepIntervalMs:  30000,
		WorkerIntervalMs: 10,
		ClaimTimeoutMs:   120000,
		FreshStart:       true,
		APIPort:          3000,
		DBPoolSize:       20,
	}

	var errs []string

	// Required fields
	cfg.RPCURL = os.Getenv("RPC_URL")
	if cfg.RPCURL == "" {
		errs = append(errs, "RPC_URL is required")
	}

	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		errs = append(errs, "DATABASE_URL is required")
	}

	var err error
	cfg.BlockFrom, err = requireInt("BLOCK_FROM")
	if err != nil {
		errs = append(errs, err.Error())
	}

	cfg.BlockTo, err = requireInt("BLOCK_TO")
	if err != nil {
		errs = append(errs, err.Error())
	}

	// Optional fields with defaults
	if v := os.Getenv("MODE"); v != "" {
		cfg.Mode = v
	}

	if v := os.Getenv("CHUNK_SIZE"); v != "" {
		cfg.ChunkSize, err = parseInt("CHUNK_SIZE", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("MAX_ATTEMPTS"); v != "" {
		cfg.MaxAttempts, err = parseInt("MAX_ATTEMPTS", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("SWEEP_INTERVAL_MS"); v != "" {
		cfg.SweepIntervalMs, err = parseInt("SWEEP_INTERVAL_MS", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("WORKER_INTERVAL_MS"); v != "" {
		cfg.WorkerIntervalMs, err = parseInt("WORKER_INTERVAL_MS", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("CLAIM_TIMEOUT_MS"); v != "" {
		cfg.ClaimTimeoutMs, err = parseInt("CLAIM_TIMEOUT_MS", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("FRESH_START"); v != "" {
		cfg.FreshStart, err = parseBool("FRESH_START", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("API_PORT"); v != "" {
		cfg.APIPort, err = parseInt("API_PORT", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if v := os.Getenv("DB_POOL_SIZE"); v != "" {
		cfg.DBPoolSize, err = parseInt("DB_POOL_SIZE", v)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	var errs []string

	if c.Mode != "coordinator" && c.Mode != "worker" {
		errs = append(errs, fmt.Sprintf("MODE must be 'coordinator' or 'worker', got %q", c.Mode))
	}
	if c.BlockFrom < 0 {
		errs = append(errs, "BLOCK_FROM must be >= 0")
	}
	if c.BlockTo < 0 {
		errs = append(errs, "BLOCK_TO must be >= 0")
	}
	if c.BlockTo < c.BlockFrom {
		errs = append(errs, "BLOCK_TO must be >= BLOCK_FROM")
	}
	if c.ChunkSize < 1 {
		errs = append(errs, "CHUNK_SIZE must be >= 1")
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, "MAX_ATTEMPTS must be >= 1")
	}
	if c.SweepIntervalMs < 1 {
		errs = append(errs, "SWEEP_INTERVAL_MS must be >= 1")
	}
	if c.WorkerIntervalMs < 0 {
		errs = append(errs, "WORKER_INTERVAL_MS must be >= 0")
	}
	if c.ClaimTimeoutMs < 1 {
		errs = append(errs, "CLAIM_TIMEOUT_MS must be >= 1")
	}
	if c.APIPort < 1 || c.APIPort > 65535 {
		errs = append(errs, "API_PORT must be between 1 and 65535")
	}
	if c.DBPoolSize < 1 {
		errs = append(errs, "DB_POOL_SIZE must be >= 1")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func requireInt(key string) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, fmt.Errorf("%s is required", key)
	}
	return parseInt(key, v)
}

func parseInt(key, v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", key, v)
	}
	return n, nil
}

func parseBool(key, v string) (bool, error) {
	switch strings.ToLower(v) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean (true/false/yes/no/1/0), got %q", key, v)
	}
}
