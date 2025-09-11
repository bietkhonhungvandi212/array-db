# Array-DB Makefile
# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Test parameters
TEST_TIMEOUT=30s
TEST_FLAGS=-v -race -cover
TEST_COUNT=1

# Project structure
INTERNAL_PACKAGES=./internal/...
BUFFER_PACKAGE=./internal/storage/buffer
FILE_PACKAGE=./internal/storage/file
PAGE_PACKAGE=./internal/storage/page
UTILS_PACKAGE=./internal/utils

# Binary name
BINARY_NAME=arraydb
BINARY_PATH=./cmd/arraydb

.PHONY: help build clean test test-internal test-buffer test-file test-page test-utils test-verbose test-race test-coverage test-bench run deps tidy

# Default target
help:
	@echo "Available targets:"
	@echo "  build         - Build the main binary"
	@echo "  clean         - Clean build artifacts"
	@echo "  run           - Run the main application"
	@echo ""
	@echo "Testing targets:"
	@echo "  test          - Run all tests"
	@echo "  test-internal - Run only internal package tests"
	@echo "  test-buffer   - Run buffer pool tests"
	@echo "  test-file     - Run file manager tests"
	@echo "  test-page     - Run page tests"
	@echo "  test-utils    - Run utils tests"
	@echo "  test-verbose  - Run tests with verbose output"
	@echo "  test-race     - Run tests with race detection"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  test-bench    - Run benchmarks"
	@echo ""
	@echo "Utility targets:"
	@echo "  deps          - Download dependencies"
	@echo "  tidy          - Tidy up go.mod"

# Build targets
build:
	$(GOBUILD) -o $(BINARY_NAME) $(BINARY_PATH)

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html

run: build
	./$(BINARY_NAME)

# Test targets
test:
	$(GOTEST) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) ./...

test-internal:
	@echo "🧪 Running internal package tests..."
	$(GOTEST) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)

test-buffer:
	@echo "🗃️  Running buffer pool tests..."
	$(GOTEST) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(BUFFER_PACKAGE)

test-file:
	@echo "📁 Running file manager tests..."
	$(GOTEST) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(FILE_PACKAGE)

test-page:
	@echo "📄 Running page tests..."
	$(GOTEST) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(PAGE_PACKAGE)

test-utils:
	@echo "🔧 Running utils tests..."
	$(GOTEST) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(UTILS_PACKAGE)

test-verbose:
	$(GOTEST) -v -race -cover -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)

test-race:
	@echo "🏃 Running tests with race detection..."
	$(GOTEST) -race -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)

test-coverage:
	@echo "📊 Generating coverage report..."
	$(GOTEST) -coverprofile=coverage.out -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-bench:
	@echo "⚡ Running benchmarks..."
	$(GOTEST) -bench=. -benchmem $(INTERNAL_PACKAGES)

# Test with specific count (useful for catching flaky tests)
test-count:
	$(GOTEST) -count=$(TEST_COUNT) $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)

# Test specific function
test-func:
	@read -p "Enter test function name: " func; \
	$(GOTEST) -run $$func $(TEST_FLAGS) -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)

# Dependency management
deps:
	$(GOGET) -d ./...

tidy:
	$(GOMOD) tidy

# Development helpers
watch-test:
	@echo "👀 Watching for changes and running tests..."
	@while true; do \
		inotifywait -r -e modify . --include='.*\.go$$' 2>/dev/null; \
		clear; \
		echo "🔄 Running tests..."; \
		make test-internal; \
		echo "✅ Tests completed. Watching for changes..."; \
	done

# Quick commands for common workflows
quick-test: test-internal
full-test: test-coverage
dev-test: test-race

# CI/CD targets
ci-test:
	$(GOTEST) -race -cover -timeout=$(TEST_TIMEOUT) $(INTERNAL_PACKAGES)

# Format and lint (requires additional tools)
fmt:
	$(GOCMD) fmt ./...

vet:
	$(GOCMD) vet ./...

# Complete check
check: fmt vet test-internal

# Show project structure
tree:
	@echo "📁 Project structure:"
	@find . -type f -name "*.go" | grep -E "(internal|cmd)" | sort
