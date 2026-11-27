# aoa — always-on agent

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8.svg)](https://go.dev/)
[![Coverage](https://img.shields.io/badge/coverage-90%25%2B-brightgreen.svg)](Makefile)

[中文文档 / Chinese README](README.zh-CN.md)

`aoa` is a Go implementation of the architecture described in Anthropic's
[**Building always-on agents**](https://claude.com/resources/articles/building-always-on-agents)
(William Hu, 13 Nov 2026). The article's core claim: an always-on agent is
*not* actually always on. It sleeps, wakes on an event or timer, runs one short
`run`, persists state, and goes back to sleep. `aoa` implements that state
machine as durable tables plus three stateless background loops.

## Built on langchain-golang

The agent runtime is
[**ProjAnvil/langchain-golang**](https://github.com/ProjAnvil/langchain-golang),
a Go port of LangGraph. `aoa` uses its:

| Component | Use in aoa |
| --- | --- |
| `langgraph/checkpoint.Saver` | durable session checkpoints in SQLite |
| `langgraph/types.Interrupt` + `Options.Resume` | human-in-the-loop suspension |
| `langgraph/prebuilt.CreateReactAgent` | one short tool-using run |
| `langchain/chatmodels` registry | `ollama:...`, `anthropic:...`, `openai:...` |
| `partners/ollama` | local model client, no API key |

The control plane (inbox, timers, outbox, approvals, leases) is `aoa` code;
`langchain-golang` supplies the agent execution engine, not the always-on
primitives.

## Five primitives

| Primitive | Where | Mechanism |
| --- | --- | --- |
| Event inbox | `internal/ingress/`, `internal/store/` | persist then ACK; `dedup` tombstones; lease enforces one active worker; batch events into one run |
| Timers | `internal/store/`, `internal/runtime/scheduler.go` | durable follow-up / periodic rows; scheduler writes inbox events |
| Memory | `internal/memory/` | `instructions.md`, `index.md`, `reported.md`, `progress/<task>.md` |
| Permissions | `internal/permission/`, `internal/agent/` | classifier, HITL suspension, `SafeProxy` credential injection |
| Outbox | `internal/store/`, `internal/runtime/dispatcher.go` | rate-limited delivery; approval cards bypass the limit |

A run has exactly three endings: queue one message, do nothing, or suspend
waiting for approval. The session stays checkpointed; the worker immediately
moves to the next batch.

## Cloud-native adapters

One `CanonicalEvent` flows into one `Sink`. Adapters are
Strategy/Factory/Chain-of-Responsibility/Decorator implementations in
`internal/ingress/`.

| Ecosystem | Upstream repository | aoa adapter | Endpoint / config |
| --- | --- | --- | --- |
| CloudEvents | [cloudevents/spec](https://github.com/cloudevents/spec) · [cloudevents/sdk-go](https://github.com/cloudevents/sdk-go) | `cloudevents.go` | `POST /events` |
| Knative Eventing | [knative/eventing](https://github.com/knative/eventing) | `cloudevents.go` | Broker + Trigger → `/events` |
| Dapr pub/sub | [dapr/dapr](https://github.com/dapr/dapr) | `dapr.go` | `/dapr/subscribe`, `POST /dapr/events/<topic>` |
| Kubernetes | [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes) · [kubernetes/client-go](https://github.com/kubernetes/client-go) | `kubernetes.go` | `POST /k8s/events`, AdmissionReview v1 |
| NATS JetStream | [nats-io/nats.go](https://github.com/nats-io/nats.go) · [nats-io/nats-server](https://github.com/nats-io/nats-server) | `nats.go` | `AOA_BROKERS=nats` |
| Kafka | [segmentio/kafka-go](https://github.com/segmentio/kafka-go) · [redpanda-data/redpanda](https://github.com/redpanda-data/redpanda) | `kafka.go` | `AOA_BROKERS=kafka` |
| Watermill | [ThreeDotsLabs/watermill](https://github.com/ThreeDotsLabs/watermill) | `watermill.go`, `watermill_redis.go` | `AOA_BROKERS=watermill` |
| Redis Streams | [redis/go-redis](https://github.com/redis/go-redis) | `watermill_redis.go` | `AOA_WATERMILL_REDIS` |
| Ollama | [ollama/ollama](https://github.com/ollama/ollama) | `partners/ollama` | `AOA_MODEL=ollama:qwen2.5:0.5b` |

Enable HTTP adapters with `AOA_ADAPTERS=cloudevents,dapr,webhook,kubernetes`;
enable broker listeners with `AOA_BROKERS=nats,kafka`.

## Quick start

```bash
go mod tidy
make test
make check

go run ./cmd/aoad
```

Send a CloudEvent:

```bash
curl -X POST localhost:8080/events \
  -H 'Content-Type: application/cloudevents+json' \
  -d '{"specversion":"1.0","id":"alert-4821","source":"monitoring","type":"com.projanvil.aoa.webhook","datacontenttype":"application/json","data":{"session_id":"s1","payload":"P95 > 800ms"}}'
```

Key environment variables: `AOA_DB`, `AOA_CKPT_DB`, `AOA_MEMORY_DIR`,
`AOA_ADDR`, `AOA_MODEL`, `AOA_LOG_FORMAT`, `AOA_LOG_LEVEL`,
`AOA_SIGNATURE_KEY`, `AOA_PROXY_TOKEN`, `AOA_PROXY_ALLOWLIST`,
`AOA_OUTBOX_WEBHOOK`, `AOA_ADAPTERS`, `AOA_BROKERS`.

Signed clients use `ingress.SignatureHeaderValue` for `X-AOA-Signature` and
send `X-AOA-Timestamp`.

## OrbStack examples

Eight compose groups, each named `aoa_example_<name>`: `ollama`,
`langchain-golang`, `cloudevents`, `dapr`, `kubernetes`, `nats`, `kafka`,
`watermill`.

```bash
make -C examples hello          # Ollama chat client prints hello world
make -C examples hello-agent    # CreateReactAgent + checkpointer prints hello world
make -C examples validate       # validate all eight compose files
```

`.env.template` and `.env` live once at `examples/`; `run.sh` injects
`--env-file` for every group. See [examples/README.md](examples/README.md).

The `qwen2.5:0.5b` model (~397 MB) is pulled once into the shared
`aoa_shared_ollama_models` volume.

## Observability

`internal/telemetry/` provides JSON `log/slog` output, W3C `traceparent`
correlation, payload redaction, atomic counters, `/metrics`, access logs, and
panic recovery. Every log line carries `component`, `trace_id`, `session_id`,
and `event_id`.

`GET /healthz`, `GET /readyz`, `GET /metrics`.

## Testing

| Command | Coverage |
| --- | --- |
| `make test` | unit tests |
| `make race` | `-race` |
| `make cover` | `coverage.out` + per-function report |
| `make cover-check` | fails below 90% total |
| `make fuzz` | CloudEvents decoder fuzz |
| `make check` | tidy, fmt, vet, race, cover-check |

Current total coverage: **94.7%**.

## Security boundary

- HMAC binds method, path, timestamp, and body. `/approvals` is signed too.
- External events cannot inject approval kind or resume values.
- Approval resolution and inbox write share one transaction.
- `SafeProxy` enforces HTTPS, a host allowlist, no redirects, and body limits.
- Lease fence tokens reject stale worker writes. `Claim` runs in one transaction.
- Memory paths resolve symlinks; `instructions.md` is operator-owned.

## Constraints

1. Side effects must be idempotent. Ack is the last step of a run.
2. `maxScale=1`. Exactly one active worker is guaranteed by the lease.
3. SQLite is a starting point. Multi-replica deployments need Postgres.
4. The Go port does not implement `interrupt_mode="per_call"`.
5. Checkpoints and the control plane are separate databases; `suspend`
   cannot be one cross-database transaction.
6. DNS rebinding needs an egress network policy in addition to `SafeProxy`.
7. `dedup` tombstones have a retention window.

## License

[MIT](LICENSE) © 2026 ProjAnvil
