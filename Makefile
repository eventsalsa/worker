.PHONY: help test test-unit test-integration test-integration-local lint fmt build check

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

check: lint test test-integration ## Run all checks (lint, unit tests, integration tests)

test: test-unit ## Run all tests

test-unit: ## Run unit tests
	go test -v -race -coverprofile=coverage.out ./...

test-integration: ## Run integration tests (uses testcontainers-go)
	go test -p 1 -v -tags=integration ./...

test-integration-local: test-integration ## Run integration tests locally (alias for test-integration)

lint: ## Run linter
	golangci-lint run --timeout=5m

fmt: ## Format code
	gofmt -w -s .
	goimports -w -local github.com/eventsalsa/projector .

build: ## Build all packages
	go build -v ./...
