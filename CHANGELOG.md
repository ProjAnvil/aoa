# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.1] - 2026-11-27

Patch release. It fixes the packaging defect in v0.1.0 and raises the test gate.

### Fixed

- Restore `internal/memory/` in the published source. The `.gitignore` rule
  `memory/` matched a directory named `memory` at any depth. It also matched
  `internal/memory/`, so the initial commit skipped the whole package. The
  v0.1.0 tag and its source archives contain no `internal/memory/` files.
  `cmd/aoad` and `internal/agent` import that package, so a build from the
  v0.1.0 tag fails.
- Change the rule to `/memory/`. It now ignores only the runtime memory
  directory at the repository root.

### Changed

- Raise the coverage gate from 65% to 90%.
- Add dense unit test suites across all eleven packages.
- Make `run(ctx)`, `dispatch(...)`, `outboxHandler()`, and the NATS / Kafka
  listeners injectable for tests.
- Add `miniredis/v2` as a test-only dependency.
- Update both README files to report 94.7% coverage.

### Verification

- `make check`: tidy, fmt, vet, `-race`, and coverage gate PASS.
- Total statement coverage: 94.7% (threshold 90%).
- `make fuzz`: PASS.
- All eight compose files pass `docker compose config`.

### Upgrade notes

- v0.1.0 source archives are incomplete. Use v0.1.1.
- No API or configuration changes. Environment variable names are unchanged.
- The v0.1.0 tag stays untouched. This release does not rewrite history.

## [v0.1.0] - 2026-11-27

First minor release. Go implementation of the five always-on agent primitives
from [Building always-on agents](https://claude.com/resources/articles/building-always-on-agents),
built on [ProjAnvil/langchain-golang](https://github.com/ProjAnvil/langchain-golang).

### Added

- Durable event inbox with `(source, id)` dedup tombstones, single-active-worker
  lease, lease fence tokens, and transactional `Claim` / `Ack` / `Fail`.
- Durable timers (follow-up and periodic) with a scheduler loop.
- File-based memory: `instructions.md`, `index.md`, `reported.md`,
  `progress/<task>.md`, with path containment and symlink resolution.
- Permission boundary: rules classifier, human-in-the-loop suspension via
  `Options.Resume`, and `SafeProxy` credential injection with HTTPS and a host
  allowlist.
- Rate-limited outbox dispatcher with idempotency keys and a webhook sink.
- Cloud-native ingress adapters for CloudEvents / Knative Eventing, Dapr
  pub/sub, raw webhooks, Kubernetes AdmissionReview, NATS JetStream, Kafka,
  and Watermill (Redis Streams). Adapter, Strategy, Factory, Chain of
  Responsibility, Decorator, and Responder patterns.
- Structured JSON logging with W3C `traceparent` correlation, payload
  redaction, `/metrics`, `/healthz`, `/readyz`, access logs, and panic recovery.
- HMAC request signatures bound to method, path, timestamp, and body;
  single-transaction approval resolution; reserved approval kind on external
  event sources.
- Eight OrbStack compose groups under `examples/`, including an Ollama
  `hello world` and a `prebuilt.CreateReactAgent` `hello world`.
- Test suite: unit, race, coverage gate (65%), and CloudEvents decoder fuzzing.
- MIT license and bilingual README.

### Security

- Reject external `approval` events and injected resume values.
- Bind signatures to method/path/timestamp and enforce a 300-second window.
- Force HTTPS, reject credentials in URLs, disable redirects, and cap
  response bodies in `SafeProxy`.
- Keep failure text in `last_error` instead of appending it to model prompts.

### Verification

- `make check`: tidy, fmt, vet, `-race`, and coverage gate pass.
- Total statement coverage: 65.7%.
- `make fuzz`: 586,431 executions, PASS.
- All eight compose files pass `docker compose config`.
- Both Ollama and LangGraph hello-world examples print `Hello, world!` on
  OrbStack; seven event examples produce an outbox delivery.

### Known Issues

- The v0.1.0 tag and its source archives omit `internal/memory/`. Builds from
  that tag fail. Use v0.1.1.

[Unreleased]: https://github.com/ProjAnvil/aoa/compare/v0.1.1...HEAD
[v0.1.1]: https://github.com/ProjAnvil/aoa/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/ProjAnvil/aoa/releases/tag/v0.1.0
