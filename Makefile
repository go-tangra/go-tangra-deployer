GO        ?= go
COVER_OUT := coverage.out

.PHONY: lint vuln test test-integration cover generate buf-lint ui-build ui-test e2e

lint:
	$(GO) vet ./...
	$(GO) vet -tags integration ./...

vuln:
	./scripts/vulncheck.sh

test:
	$(GO) test -race -count=1 ./...

# TimescaleDB and Valkey come from testcontainers; the suites skip without Docker.
test-integration:
	$(GO) test -count=1 -tags integration ./internal/app/ ./internal/repo/repodb/ ./internal/stream/valkeykv/

cover:
	$(GO) test -count=1 -coverprofile=$(COVER_OUT) ./...
	$(GO) tool cover -func=$(COVER_OUT) | tail -1

generate:
	buf generate

buf-lint:
	buf lint

ui-build:
	cd ui && npm ci && npm run build

ui-test:
	cd ui && npm run lint && npm run test:unit

e2e:
	cd ui && npx playwright test
