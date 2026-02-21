.PHONY: build test run docker-up docker-down scale fresh-start psql

build:
	go build -o bin/indexer ./cmd/indexer

test:
	go test ./...

run:
	go run ./cmd/indexer

docker-up:
	docker compose up --build --scale worker=1

docker-down:
	docker compose down

# Run with N worker containers: make scale N=4
scale:
	docker compose up --build --scale worker=$(N)

# Fresh start with N workers (default 1): make fresh-start N=4
fresh-start:
	FRESH_START=true docker compose up --build --scale worker=$(or $(N),1)

psql:
	docker compose exec postgres psql -U indexer -d indexer
