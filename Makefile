# Makefile for Deployer Service

include ../../../app.mk

# Deployer-specific variables
DEPLOYER_IMAGE_NAME ?= menta2l/deployer-service
DEPLOYER_IMAGE_TAG ?= $(VERSION)
DOCKER_REGISTRY ?=

# Build the server binary
.PHONY: build-server
build-server:
	@echo "Building Deployer server..."
	@go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o ./bin/deployer-server ./cmd/server

# Build Docker image for Deployer service
.PHONY: docker
docker:
	@echo "Building Docker image $(DEPLOYER_IMAGE_NAME):$(DEPLOYER_IMAGE_TAG)..."
	@docker build \
		-t $(DEPLOYER_IMAGE_NAME):$(DEPLOYER_IMAGE_TAG) \
		-t $(DEPLOYER_IMAGE_NAME):latest \
		--build-arg APP_VERSION=$(VERSION) \
		-f ./Dockerfile \
		../../../

# Build Docker image with custom registry
.PHONY: docker-tag
docker-tag: docker
	@docker tag $(DEPLOYER_IMAGE_NAME):$(DEPLOYER_IMAGE_TAG) $(DEPLOYER_IMAGE_NAME):$(DEPLOYER_IMAGE_TAG)
	@docker tag $(DEPLOYER_IMAGE_NAME):latest $(DEPLOYER_IMAGE_NAME):latest

# Push Docker image to registry
.PHONY: docker-push
docker-push: docker-tag
	@docker push $(DEPLOYER_IMAGE_NAME):$(DEPLOYER_IMAGE_TAG)
	@docker push $(DEPLOYER_IMAGE_NAME):latest

# Build multi-platform Docker image
.PHONY: docker-buildx
docker-buildx:
	@echo "Building multi-platform Docker image..."
	@docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-t $(DEPLOYER_IMAGE_NAME):$(DEPLOYER_IMAGE_TAG) \
		-t $(DEPLOYER_IMAGE_NAME):latest \
		--build-arg APP_VERSION=$(VERSION) \
		-f ./Dockerfile \
		../../../

# Run the server locally
.PHONY: run-server
run-server:
	@go run ./cmd/server -c ./configs

# Generate ent schema
.PHONY: ent
ent:
ifneq ("$(wildcard ./internal/data/ent)","")
	@ent generate \
		--feature sql/modifier \
		--feature sql/upsert \
		--feature sql/lock \
		./internal/data/ent/schema
endif

# Run tests
.PHONY: test
test:
	@go test -v ./...

# Run tests with coverage
.PHONY: test-cover
test-cover:
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean build artifacts
.PHONY: clean
clean:
	@rm -rf ./bin
	@rm -f coverage.out coverage.html
	@echo "Clean complete!"

# Show Deployer-specific help
.PHONY: deployer-help
deployer-help:
	@echo ""
	@echo "Deployer Service Makefile"
	@echo "========================="
	@echo ""
	@echo "Build targets:"
	@echo "  build-server   Build the server binary"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker         Build Docker image"
	@echo "  docker-tag     Build and tag for registry (set DOCKER_REGISTRY)"
	@echo "  docker-push    Build, tag, and push to registry"
	@echo "  docker-buildx  Build multi-platform image (amd64/arm64)"
	@echo ""
	@echo "Run targets:"
	@echo "  run-server     Run the server locally"
	@echo ""
	@echo "Test targets:"
	@echo "  test           Run tests"
	@echo "  test-cover     Run tests with coverage report"
	@echo ""
	@echo "Other targets:"
	@echo "  ent            Generate ent schema code"
	@echo "  wire           Generate wire dependency injection"
	@echo "  clean          Remove build artifacts"
	@echo ""
