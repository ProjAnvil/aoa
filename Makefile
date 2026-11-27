GO ?= go
COVER_THRESHOLD ?= 90
COVER_FILE ?= coverage.out

.PHONY: build test race cover cover-check fuzz vet fmt check clean tidy

build:
	$(GO) build ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

cover:
	$(GO) test -coverprofile=$(COVER_FILE) ./...
	$(GO) tool cover -func=$(COVER_FILE)

# Fails when total statement coverage drops below COVER_THRESHOLD.
cover-check: cover
	@total=$$($(GO) tool cover -func=$(COVER_FILE) | awk '/^total:/ {gsub(/%/,"",$$3); print $$3}'); \
	echo "total coverage: $$total% (threshold $(COVER_THRESHOLD)%)"; \
	awk -v c="$$total" -v t="$(COVER_THRESHOLD)" 'BEGIN { exit !(c+0 >= t+0) }'

fuzz:
	$(GO) test ./internal/ingress -run='^$$' -fuzz=FuzzCloudEventsDecoder -fuzztime=5s

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

check: tidy fmt vet race cover-check

clean:
	rm -f $(COVER_FILE)
