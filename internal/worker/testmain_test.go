package worker

import (
	"context"
	"os"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestMain starts a single Postgres container for the whole worker test suite.
// If TEST_DATABASE_URL is already set the container is skipped
func TestMain(m *testing.M) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		os.Exit(m.Run())
	}

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		// Docker unavailable — pure function tests still run; integration tests skip.
		os.Exit(m.Run())
	}
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		os.Exit(1)
	}

	os.Setenv("TEST_DATABASE_URL", connStr)
	os.Exit(m.Run())
}
