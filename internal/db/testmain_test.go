package db

import (
	"context"
	"os"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestMain starts a single Postgres container for the entire db test suite,
// sets TEST_DATABASE_URL, then tears the container down when all tests finish.
//
// If TEST_DATABASE_URL is already set in the environment, the container is
// skipped and that URL is used instead (useful in CI with an external DB).
func TestMain(m *testing.M) {
	// If the caller already provides a DB, just run the tests.
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
		// Docker not available or other startup failure — tests will skip
		// gracefully because TEST_DATABASE_URL won't be set.
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
