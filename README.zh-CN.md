# aoa — 常驻智能体

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8.svg)](https://go.dev/)
[![Coverage](https://img.shields.io/badge/coverage-90%25%2B-brightgreen.svg)](Makefile)

[English README](README.md)

`aoa` 是 Anthropic [**Building always-on agents**](https://claude.com/resources/articles/building-always-on-agents)（William Hu，2026-11-13）所述架构的 Go 实现。原文的核心判断：常驻智能体并不真的常驻。它休眠，被事件或定时器唤醒，跑一次短 `run`，持久化状态，再回到休眠。`aoa` 把这套状态机落成几张持久表和三个无状态后台循环。

## 基于 langchain-golang

执行引擎是 [**ProjAnvil/langchain-golang**](https://github.com/ProjAnvil/langchain-golang)，LangGraph 的 Go 移植。`aoa` 用到：

| 组件 | 用途 |
| --- | --- |
| `langgraph/checkpoint.Saver` | SQLite 中的持久 session checkpoint |
| `langgraph/types.Interrupt` + `Options.Resume` | 人工审批挂起与恢复 |
| `langgraph/prebuilt.CreateReactAgent` | 一次短的工具调用 run |
| `langchain/chatmodels` 注册表 | `ollama:...`、`anthropic:...`、`openai:...` |
| `partners/ollama` | 本地模型客户端，无需 API key |

控制面（inbox、timers、outbox、approvals、leases）是 `aoa` 自己的代码；`langchain-golang` 提供的是执行引擎，不是常驻原语。

## 五个原语

| 原语 | 文件 | 机制 |
| --- | --- | --- |
| 事件收件箱 | `internal/ingress/`、`internal/store/` | 落库后立即 ACK；`dedup` 墓碑去重；租约保证单活跃 worker；同批事件合成一次 run |
| 定时器 | `internal/store/`、`internal/runtime/scheduler.go` | 持久的 follow-up / periodic 行，scheduler 到期写 inbox |
| 记忆 | `internal/memory/` | `instructions.md`、`index.md`、`reported.md`、`progress/<task>.md` |
| 权限 | `internal/permission/`、`internal/agent/` | 分类器、HITL 挂起、`SafeProxy` 凭证注入 |
| 发件箱 | `internal/store/`、`internal/runtime/dispatcher.go` | 限速投递；审批卡不限速 |

一次 run 只有三种结局：写一条 outbox、什么都不写、挂起等审批。session 已 checkpoint，worker 立刻处理下一批。

## 云原生适配

所有入站消息归一为 `CanonicalEvent`，交给唯一 `Sink`。适配层在 `internal/ingress/`，用 Strategy / Factory / 责任链 / Decorator 组织。

| 生态 | 上游仓库 | aoa 适配器 | 端点 / 配置 |
| --- | --- | --- | --- |
| CloudEvents | [cloudevents/spec](https://github.com/cloudevents/spec) · [cloudevents/sdk-go](https://github.com/cloudevents/sdk-go) | `cloudevents.go` | `POST /events` |
| Knative Eventing | [knative/eventing](https://github.com/knative/eventing) | `cloudevents.go` | Broker + Trigger → `/events` |
| Dapr pub/sub | [dapr/dapr](https://github.com/dapr/dapr) | `dapr.go` | `/dapr/subscribe`、`POST /dapr/events/<topic>` |
| Kubernetes | [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes) · [kubernetes/client-go](https://github.com/kubernetes/client-go) | `kubernetes.go` | `POST /k8s/events`，AdmissionReview v1 |
| NATS JetStream | [nats-io/nats.go](https://github.com/nats-io/nats.go) · [nats-io/nats-server](https://github.com/nats-io/nats-server) | `nats.go` | `AOA_BROKERS=nats` |
| Kafka | [segmentio/kafka-go](https://github.com/segmentio/kafka-go) · [redpanda-data/redpanda](https://github.com/redpanda-data/redpanda) | `kafka.go` | `AOA_BROKERS=kafka` |
| Watermill | [ThreeDotsLabs/watermill](https://github.com/ThreeDotsLabs/watermill) | `watermill.go`、`watermill_redis.go` | `AOA_BROKERS=watermill` |
| Redis Streams | [redis/go-redis](https://github.com/redis/go-redis) | `watermill_redis.go` | `AOA_WATERMILL_REDIS` |
| Ollama | [ollama/ollama](https://github.com/ollama/ollama) | `partners/ollama` | `AOA_MODEL=ollama:qwen2.5:0.5b` |

`AOA_ADAPTERS=cloudevents,dapr,webhook,kubernetes` 启用 HTTP 适配器；`AOA_BROKERS=nats,kafka` 启用 broker listener。

## 运行

```bash
go mod tidy
make test
make check

go run ./cmd/aoad
```

发送 CloudEvent：

```bash
curl -X POST localhost:8080/events \
  -H 'Content-Type: application/cloudevents+json' \
  -d '{"specversion":"1.0","id":"alert-4821","source":"monitoring","type":"com.projanvil.aoa.webhook","datacontenttype":"application/json","data":{"session_id":"s1","payload":"P95 > 800ms"}}'
```

主要环境变量：`AOA_DB`、`AOA_CKPT_DB`、`AOA_MEMORY_DIR`、`AOA_ADDR`、`AOA_MODEL`、`AOA_LOG_FORMAT`、`AOA_LOG_LEVEL`、`AOA_SIGNATURE_KEY`、`AOA_PROXY_TOKEN`、`AOA_PROXY_ALLOWLIST`、`AOA_OUTBOX_WEBHOOK`、`AOA_ADAPTERS`、`AOA_BROKERS`。

签名客户端用 `ingress.SignatureHeaderValue` 计算 `X-AOA-Signature`，并传 `X-AOA-Timestamp`。

## OrbStack 示例

八个 container group，各自 `aoa_example_<name>`：`ollama`、`langchain-golang`、`cloudevents`、`dapr`、`kubernetes`、`nats`、`kafka`、`watermill`。

```bash
make -C examples hello          # Ollama 客户端输出 hello world
make -C examples hello-agent    # CreateReactAgent + checkpointer 输出 hello world
make -C examples validate       # 校验八个 compose 文件
```

`.env.template` 与 `.env` 只在 `examples/` 根放一份，`run.sh` 给所有 group 注入 `--env-file`。详见 [examples/README.md](examples/README.md)。

模型 `qwen2.5:0.5b` 约 397 MB，共享卷 `aoa_shared_ollama_models` 只下载一次。

## 可观测性

`internal/telemetry/` 提供 JSON `log/slog`、W3C `traceparent` 关联、payload 脱敏、原子计数、`/metrics`、访问日志和 panic 恢复。每条日志带 `component`、`trace_id`、`session_id`、`event_id`。

`GET /healthz`、`GET /readyz`、`GET /metrics`。

## 测试

| 命令 | 内容 |
| --- | --- |
| `make test` | 单元测试 |
| `make race` | `-race` |
| `make cover` | `coverage.out` 与逐函数报告 |
| `make cover-check` | 总覆盖率低于 90% 失败 |
| `make fuzz` | CloudEvents 解码器 fuzz |
| `make check` | tidy、fmt、vet、race、cover-check |

当前总覆盖率 **90.9%**。

## 安全边界

- HMAC 绑定 method、path、timestamp 与 body。`/approvals` 同样签名。
- 外部事件不能注入 approval kind 或 resume 值。
- 审批状态变更与 inbox 写入在同一事务。
- `SafeProxy` 强制 HTTPS、host allowlist、禁止重定向、限制响应体。
- 租约 fence token 拒绝旧 worker 写入；`Claim` 单事务。
- memory 路径解析 symlink；`instructions.md` 由运维持有。

## 约束

1. 副作用必须幂等。Ack 是 run 的最后一步。
2. `maxScale=1`。租约保证只有一个活跃 worker。
3. SQLite 是起步实现。多副本要换 Postgres。
4. Go 移植未实现 `interrupt_mode="per_call"`。
5. checkpoint 与控制面是两个数据库，`suspend` 无法跨库事务。
6. DNS rebinding 需要 egress 网络策略配合 `SafeProxy`。
7. `dedup` 墓碑有保留窗口。

## 许可证

[MIT](LICENSE) © 2026 ProjAnvil
