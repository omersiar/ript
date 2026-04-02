GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
VERSION    := $(shell git describe --tags --always 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u +%Y%m%dT%H%M%SZ)
LDFLAGS    := -s -w \
	-X github.com/omersiar/ript/internal/version.Version=$(VERSION) \
	-X github.com/omersiar/ript/internal/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/omersiar/ript/internal/version.BuildTime=$(BUILD_TIME)

.PHONY: help build run test clean docker-up docker-down harness-build harness-run

help:
	@echo "Available commands:"
	@echo "  make build        	- Build the ript binary"
	@echo "  make run          	- Run ript locally"
	@echo "  make test         	- Run tests"
	@echo "  make clean        	- Clean build artifacts"
	@echo "  make docker-build 	- Build Docker image"
	@echo "  make docker-up    	- Start Docker Compose stack"
	@echo "  make docker-down  	- Stop Docker Compose stack"
	@echo "  make deps         	- Download dependencies"
	@echo "  make harness-build	- Build the test harness binary"
	@echo "  make harness-run  	- Run the test harness with default settings"

deps:
	go mod download
	go mod tidy

build: deps
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/ript ./cmd/ript

harness-build: deps
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/testharness ./cmd/testharness

run: build
	./bin/ript

harness-run: harness-build
	./bin/testharness

test:
	go test -v ./...

clean:
	rm -rf bin/
	go clean

docker-build:
	docker build \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t ript:latest .

docker-up: docker-build
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f ript

docker-shell:
	docker compose exec ript sh

.DEFAULT_GOAL := help
