.PHONY: build test clean run docker-build docker-run install

# Build configuration
BINARY_NAME=geckos3
DOCKER_REPO=randiltharusha/geckos3
VERSION?=1.0.0
BUILD_FLAGS=-ldflags="-s -w -X main.Version=$(VERSION)"

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	CGO_ENABLED=0 go build $(BUILD_FLAGS) -o $(BINARY_NAME)
	@echo "✓ Build complete: $(BINARY_NAME)"

# Build for multiple platforms
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o bin/$(BINARY_NAME)-linux-amd64
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o bin/$(BINARY_NAME)-linux-arm64
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o bin/$(BINARY_NAME)-darwin-amd64
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o bin/$(BINARY_NAME)-darwin-arm64
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o bin/$(BINARY_NAME)-windows-amd64.exe
	@echo "✓ Multi-platform build complete"

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...
	@echo "✓ Tests complete"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	rm -rf bin/
	rm -rf data/
	@echo "✓ Clean complete"

# Run the server
run: build
	@echo "Starting server..."
	./$(BINARY_NAME)

# Run with custom settings
run-dev:
	@echo "Starting server in development mode..."
	./$(BINARY_NAME) -data-dir=./dev-data -auth=false

# Docker commands
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_REPO):$(VERSION) .
	docker tag $(DOCKER_REPO):$(VERSION) $(DOCKER_REPO):latest
	@echo "✓ Docker image built: $(DOCKER_REPO):$(VERSION)"

docker-push: docker-build
	@echo "Pushing Docker images..."
	docker push $(DOCKER_REPO):$(VERSION)
	docker push $(DOCKER_REPO):latest
	@echo "✓ Docker images pushed to Docker Hub"

docker-run:
	@echo "Running Docker container..."
	docker run -d -p 9000:9000 -v $(PWD)/data:/data --name $(BINARY_NAME) $(DOCKER_REPO):latest
	@echo "✓ Container started"

docker-stop:
	@echo "Stopping Docker container..."
	docker stop $(BINARY_NAME) || true
	docker rm $(BINARY_NAME) || true
	@echo "✓ Container stopped"

# Install to system (Linux)
install: build
	@echo "Installing $(BINARY_NAME)..."
	sudo install -m 755 $(BINARY_NAME) /usr/local/bin/
	@echo "✓ Installed to /usr/local/bin/$(BINARY_NAME)"

# Install systemd service
install-service:
	@echo "Installing systemd service..."
	sudo cp geckos3.service /etc/systemd/system/
	sudo systemctl daemon-reload
	@echo "✓ Service installed. Enable with: sudo systemctl enable geckos3"

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	@echo "✓ Format complete"

# Check code quality
lint:
	@echo "Running linters..."
	go vet ./...
	@echo "✓ Lint complete"

# Show help
help:
	@echo "geckos3 Makefile Commands:"
	@echo "  make build          - Build the binary"
	@echo "  make build-all      - Build for multiple platforms"
	@echo "  make test           - Run tests"
	@echo "  make bench          - Run benchmarks"
	@echo "  make run            - Build and run the server"
	@echo "  make run-dev        - Run in development mode (no auth)"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make docker-build   - Build Docker image"
	@echo "  make docker-push    - Build and push to Docker Hub"
	@echo "  make docker-run     - Run Docker container"
	@echo "  make docker-stop    - Stop Docker container"
	@echo "  make install        - Install to /usr/local/bin"
	@echo "  make install-service- Install systemd service"
	@echo "  make fmt            - Format code"
	@echo "  make lint           - Run linters"