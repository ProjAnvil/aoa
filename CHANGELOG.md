# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[Unreleased]: https://github.com/ProjAnvil/aoa/compare/v0.1.0...HEAD
[v0.1.0]: https://github.com/ProjAnvil/aoa/releases/tag/v0.1.0
