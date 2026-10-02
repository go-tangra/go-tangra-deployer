GO        ?= go
COVER_OUT := coverage.out

.PHONY: lint vuln test test-integration cover cover-033 fuzz generate buf-lint ui-build ui-test e2e

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

# Feature 033: the descriptor validator (internal/provider) and the
# inventory-agent provider are security packages and stay at 100 % statement
# coverage.
COVER_033_PKGS := ./internal/provider/ ./internal/providers/inventoryagent/
cover-033:
	@for p in $(COVER_033_PKGS); do \
	  out=$$($(GO) test -count=1 -cover $$p | grep -oE 'coverage: [0-9.]+%' | grep -oE '[0-9.]+'); \
	  echo "$$p $$out%"; \
	  if [ "$$out" != "100.0" ]; then echo "cover-033: $$p below 100 %" >&2; exit 1; fi; \
	done

FUZZTIME ?= 30s
fuzz:
	$(GO) test -run='^$$' -fuzz='^FuzzValidateInput$$' -fuzztime=$(FUZZTIME) ./internal/provider/
	$(GO) test -run='^$$' -fuzz='^FuzzInventoryAgentConfig$$' -fuzztime=$(FUZZTIME) ./internal/providers/inventoryagent/
	$(GO) test -run='^$$' -fuzz='^FuzzCertIDFrom$$' -fuzztime=$(FUZZTIME) ./internal/events/

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
