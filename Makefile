# Go commands
GO       := go
GOFLAGS  :=
GOBUILD  := $(GO) build $(GOFLAGS)
GOTEST   := $(GO) test $(GOFLAGS)
GOVET    := $(GO) vet
GOFMT    := $(GO) fmt
GOTOOL   := $(GO) tool
GOMOD    := $(GO) mod
PACKAGE  := github.com/alecthomas/mph

# Binary name
BINARY   := mph

.PHONY: all build clean test test-race fmt vet lint install uninstall

# Default target
all: build

# Build the project
build:
	$(GOBUILD) ./...

# Clean build artifacts
clean:
	rm -f $(BINARY)
	rm -f coverage.out

# Run tests
test:
	$(GOTEST) ./...

# Run tests with race detection
test-race:
	$(GOTEST) -race ./...

# Format code
fmt:
	$(GOFMT) ./...

# Run go vet
vet:
	$(GOVET) ./...

# Run linter if available
lint:
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found, skipping lint"; \
	fi

# Generate code coverage
coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOTOOL) cover -html=coverage.out

# Install binary
install:
	$(GO) install ./...

# Uninstall binary
uninstall:
	rm -f $(shell which $(BINARY))

# Check dependencies
deps:
	$(GOMOD) tidy
