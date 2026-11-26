# OrbStack compose examples

Each directory is one container group. The project name is fixed as
`aoa_example_<name>` via the top-level Compose `name:` key.

The `builder` service compiles `aoad` and `aoa-probe`. It mounts the repo
root read-only at `/work/aoa` and `langchain_golang` at
`/work/langchain_port/langchain_golang`, matching the `replace` path in
`go.mod`. Builds use `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`; the static
binaries run directly in `alpine:3.20`.

## Prerequisites

- OrbStack 2.x, Docker 29+, Compose v2.
- Apple Silicon (`arm64`), 64 GB RAM.
- `langchain_port/langchain_golang` at `../../../langchain_port/langchain_golang`.

## Model

All groups share the `aoa_shared_ollama_models` volume. `qwen2.5:0.5b` is
about 397 MB and is pulled once. `pull-model` calls `/api/pull`.

The request body sets `"insecure": true`. OrbStack DNS resolves the Ollama R2
CDN into `198.18.0.0/15` (a proxy-reserved range). Ollama 0.34.2+ rejects
redirects resolving to non-public IPs; `insecure` relaxes that check. Use it
only for local examples; production images need an egress policy.

`examples/run.sh` rewrites `127.0.0.1` in the host `HTTPS_PROXY` to
`host.docker.internal` and exports `AOA_EXAMPLE_PROXY`. OrbStack containers
cannot reach a host loopback proxy by default; without it the pull fails with
`dial tcp 198.18.0.78:80: connect: no route to host`.

**Never use `docker compose down -v`**: it deletes the shared model volume.
Use `make -C examples down-<name>`.

## Two hello-world variants

| Variant | Group | Command | Code path |
| --- | --- | --- | --- |
| Ollama | `aoa_example_ollama` | `make -C examples hello` | `chatmodels.Resolve(ollama)` → `ChatModel.Invoke`, one `/api/chat` call |
| Real langchain-golang | `aoa_example_langchain_golang` | `make -C examples hello-agent` | `prebuilt.CreateReactAgent` + `MemorySaver` + `InvokeWithStateOptions(ThreadID)` |

Both print:

```
HELLO WORLD from langchain-golang ollama client
model=qwen2.5:0.5b
reply=Hello, world!
```

```
HELLO WORLD from langchain-golang create_react_agent
model=qwen2.5:0.5b
reply=Hello, world!
```

The `langchain-golang` group also runs the full `aoad` service with
`AOA_MODEL=ollama:qwen2.5:0.5b`: a CloudEvent enters the inbox and the worker
runs one short turn with the real model.

## Where `.env` lives

**One shared file at `examples/`, not one per group.**

```
examples/.env.template   # committed, documented defaults
examples/.env            # generated locally, gitignored
```

Eight groups share the same model, proxy, logging, and credential settings.
Eight copies would drift. Group-specific values (adapter names, broker
addresses, topics) stay in each `compose.yaml` `environment:` block.

`examples/run.sh` injects `--env-file examples/.env` for every group. It
creates `.env` from the template on first use and appends only missing keys
later, preserving operator edits. All Makefile targets go through this
wrapper; do not run bare `docker compose -f examples/<name>/compose.yaml up`,
because it will not read the shared `.env`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AOA_MODEL` | `offline` | model for the seven event examples; set `ollama:qwen2.5:0.5b` to use the real model |
| `AOA_LANGGOLANG_MODEL` | `ollama:qwen2.5:0.5b` | only the langchain-golang group, decoupled from `AOA_MODEL` |
| `OLLAMA_MODEL` | `qwen2.5:0.5b` | `/api/pull`, `hello`, `agent-hello` |
| `AOA_LOG_FORMAT` / `AOA_LOG_LEVEL` | `text` / `info` | structured logging |
| `AOA_PROXY_TOKEN` | `example-token` | `SafeProxy` requires a non-empty token |
| `AOA_SIGNATURE_KEY` | empty | examples are unsigned; production must set it |
| `AOA_EXAMPLE_PROXY` | derived by run.sh | override the host proxy rewrite |

## Eight groups

| Directory | Group | Protocol | Event source |
| --- | --- | --- | --- |
| `ollama` | `aoa_example_ollama` | Ollama `/api/chat` | direct client call |
| `langchain-golang` | `aoa_example_langchain_golang` | CloudEvents + `CreateReactAgent` | `agent-hello` / `emit` |
| `cloudevents` | `aoa_example_cloudevents` | CloudEvents 1.0 structured HTTP | `emit` plays a Knative Trigger |
| `dapr` | `aoa_example_dapr` | Dapr pub/sub envelope + `/dapr/subscribe` | `emit --mode dapr` |
| `kubernetes` | `aoa_example_kubernetes` | AdmissionReview v1 webhook | `emit --mode admission` |
| `nats` | `aoa_example_nats` | NATS JetStream durable | `publish-nats` |
| `kafka` | `aoa_example_kafka` | Kafka consumer group (Redpanda) | `publish-kafka` |
| `watermill` | `aoa_example_watermill` | Watermill `message.Subscriber` + Redis Streams | `publish-redis` |

Each event group also runs a `sink` service. `AOA_OUTBOX_WEBHOOK` points at it;
delivered messages appear in the logs.

```bash
make -C examples up-cloudevents
make -C examples logs-cloudevents
make -C examples down-cloudevents
make -C examples validate
```

`AOA_MODEL` defaults to `offline`, so event runs are deterministic. Edit
`examples/.env` to switch to the real model. The `langchain-golang` group is
unaffected by that value.

## Extra scenarios

`aoa-probe scenario <name> --target http://aoad:8080/events`:

| Scenario | Demonstrates | Expected |
| --- | --- | --- |
| `dedup` | same `source_id` twice | 202 then 200, one inbox row |
| `memory` | two events with the same `session_id` | same thread across two runs, checkpoint reused |
| `timer` | model writes a timer | `timers` gets a follow-up row, scheduler wakes it |
| `approval` | rollback request | approval card in the outbox, run suspends without blocking |

`dedup` and `memory` are deterministic. `timer` and `approval` depend on tool
calling, so the `offline` model is the reliable choice.

Run one manually against a live group:

```bash
./examples/run.sh -f examples/cloudevents/compose.yaml run --rm emit \
  /app/aoa-probe scenario dedup --target http://aoad:8080/events
```

## Notes

- `AOA_SIGNATURE_KEY` is empty in examples. Production must set it; the HMAC
  covers method, path, timestamp, and body.
- `AOA_PROXY_TOKEN=example-token` is a placeholder. `SafeProxy` requires a
  non-empty value; examples make no outbound calls.
- Container ports are not published to the host. Read `logs`.
- The first build downloads Go modules; `aoa_shared_gomodcache` is shared.
