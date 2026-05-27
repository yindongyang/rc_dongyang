# rc_dongyang —— 内部 HTTP 通知中转服务（MVP）

> AI Coding 作业：API 通知系统设计与实现
> 关注点：**工程判断与取舍**，而非"功能齐全"。代码刻意保持最小可运行（MVP），可用 `go run` 一键启动。

---

## 1. 我对问题的理解

### 1.1 业务本质
公司内多个业务系统在关键事件发生时，需要"通知"外部供应商 HTTP API（CRM、广告系统、库存系统等）。
业务系统**只关心送达，不关心返回值**。难点不在协议，在于：

- 外部 API **不可控**（慢、抖动、长时间宕机、限流）
- 各供应商的 URL / Header / Body 格式**各不相同**
- 业务方分布在多个系统，需要一个统一的"靠谱的"投递通道

### 1.2 抽象出的核心问题
> 把一个"业务事件"安全、可靠、可观测地变成"一次（最终被外部接受的）HTTP 调用"。

本质上这是一个**带重试与持久化的异步任务投递系统**，而非"通信中间件"。
认清这一点很重要——它决定了我**不会**把它做成"消息总线"或"工作流引擎"。

---

## 2. 系统边界

### 2.1 我做的事（In-Scope）
- 接收业务方提交的通知请求（统一内部协议：URL + Method + Header + Body + 业务幂等 key）
- **持久化**任务，**异步**投递
- 失败重试（指数退避 + jitter）、最大重试次数、死信
- 基于 `(biz_system, biz_id)` 唯一索引的**入口去重**
- 提供任务状态查询接口（业务说不关心，但出问题排查必备）
- 提供死信人工重投接口（最简实现）
- 基本可观测性：结构化日志 + 关键计数器

### 2.2 我明确不做的事（Out-of-Scope）

| 不做 | 理由 |
|---|---|
| 解析外部 API 业务返回值 | 需求明确不关心；解析意味耦合每个供应商协议，复杂度爆炸 |
| 严格 exactly-once | HTTP 不可控，"调用成功但响应丢失"不可区分；at-least-once + 幂等 key 是工业界共识（Stripe / SQS 同款） |
| 模板渲染 / 参数变换 | 业务方自己拼好 Body 再丢过来，避免成为"什么都管的中台" |
| 调度编排（DAG / 依赖通知） | 那是工作流引擎的职责 |
| 持有外部 API 凭证 | 业务方自己塞 Header；本服务零凭据，降低安全面 |
| 多租户 / 限流隔离 | MVP 假设内部可信调用；演进时再加 |
| 复杂熔断器框架（Hystrix/Sentinel） | 简单"vendor 维度连续失败短路"已够 |
| 消息队列 | DB 当队列在 < 1k QPS 完全够用，少一个组件少一份故障源（详见 §5）|

---

## 3. 整体架构

```
┌──────────────┐  POST /api/v1/notifications  ┌─────────────────┐
│ 业务系统 A   │ ───────────────────────────▶ │                 │
├──────────────┤                              │   Notify API    │
│ 业务系统 B   │ ───────────────────────────▶ │   (HTTP Server) │
└──────────────┘                              │                 │
                                              └────────┬────────┘
                                                       │ 写入任务（事务 + 唯一索引去重）
                                                       ▼
                                              ┌─────────────────┐
                                              │     MYSQL      │  ← 任务表 / 死信表
                                              └────────┬────────┘
                                                       ▲ 抢占（乐观锁）
                                                       │
                                              ┌────────┴────────┐    HTTP 调用
                                              │     Worker      │ ─────────────▶ 外部供应商 API
                                              │  (单进程协程池) │ ◀───── 5xx/超时 → 退避重试
                                              └─────────────────┘         4xx → 死信
```

**只有 3 个核心组件：API、DB、Worker**。API 与 Worker 在 MVP 里是**同一个进程**的两个 goroutine 池（也支持拆分进程，见 `cmd/`）。

---

## 4. 核心设计

### 4.1 投递语义：**at-least-once + 业务幂等**
- 我**只承诺至少一次**。重试一定会带来重复送达可能。
- "幂等"的责任**不在本服务**：通过把业务方提交的 `idempotency_key` 透传到外部 Header（如 `Idempotency-Key`），由外部供应商或业务方自己去重。
- 这是工业界标准做法（Stripe API、AWS SQS 标准队列）。

### 4.2 任务状态机
```
   pending ─▶ running ─┬─▶ success
                       │
                       ├─▶ pending (退避中, 仍未达上限)
                       │
                       └─▶ dead_letter (达上限 / 4xx 不可重试)
```

字段（精简）：
```
id, biz_system, biz_id, idempotency_key,
url, method, headers(JSON), body,
status, attempts, max_attempts,
next_retry_at, last_error, last_status_code,
created_at, updated_at
唯一索引: (biz_system, biz_id)
普通索引: (status, next_retry_at)
```

### 4.3 重试与退避
- **指数退避 + jitter**：`base * 2^(attempt-1) + rand([0, base))`，封顶 1h
- 默认 `max_attempts = 8`（约覆盖几小时窗口），可按 vendor 配置
- **HTTP 4xx 不重试**（除 408 / 429）—— 重试无意义，直接死信
- **5xx / 网络错误 / 超时 / 408 / 429** → 重试
- **429 优先尊重 `Retry-After`**

### 4.4 Worker 抢占（无需分布式锁）
MYSQL 没有 `SKIP LOCKED`，用乐观锁：
```sql
UPDATE notifications
SET status='running', lease_until=?, attempts=attempts+1, updated_at=?
WHERE id=? AND status='pending' AND next_retry_at<=?
```
`UPDATE ... WHERE` 影响行数 = 1 才算抢到。

**可见性超时（lease）**：抢到任务后立刻把 `next_retry_at` 推到 `now + lease_timeout`，防 Worker 崩溃任务卡住。看门狗定期把 `running` 且 `lease_until < now` 的任务回滚为 `pending`。

### 4.5 防雪崩 / 惊群
- 退避必加 jitter
- vendor 短路：连续失败 N 次的 host，挂起 M 分钟（内存级，简单实现）
- 单 HTTP 调用强制超时（默认 10s），不允许 worker 被慢响应吃满

### 4.6 优雅停机
SIGTERM → 不再拉新任务 → 等 in-flight 完成或超时（默认 15s）→ 退出。

---

## 5. 关键工程决策与取舍

| 决策 | 选择 | 拒绝的方案 | 理由 |
|---|---|---|---|
| 队列 | **DB 当队列** | Kafka / RocketMQ / Redis Streams | MVP 流量未知；DB 有事务可调试；引 MQ 反而带来"DB 与 MQ 状态不一致"的新难题 |
| 幂等 | **DB 唯一索引** | Redis SETNX | 写入低频，多一个组件多一个故障点 |
| 去重责任 | **交给外部/业务方** | 服务内做 exactly-once | HTTP 上做不到真 exactly-once，假装能做就是骗人 |
| 重试触发 | **DB 轮询 + next_retry_at 索引** | 时间轮 / 延迟队列 | 索引拉取在 MVP 量级（万级 pending）足够 |
| 调度并发 | **协程池 + DB 抢占** | 一致性 hash 分片 | 抢占模式天然支持 worker 水平扩容 |
| 内部协议 | **HTTP + JSON** | gRPC | 业务方语言栈不统一；性能远不是瓶颈 |
| 配置 | **YAML + 环境变量** | 配置中心 | 一台机器一个文件够用 |
| 可观测性 | **结构化日志 + 内存计数器** | 全套 OpenTelemetry | MVP 不上链路追踪；预留接口将来接 Otel |
| 框架 | **标准库 net/http** | gin / echo | 路由就 5 个，没必要 |

---

## 6. 演进路径

```
v1（当前 MVP）：API + MYSQL + 单进程 Worker
   │ 流量 < 几百 QPS、单地域
   ▼
v2：MySQL + 多进程 Worker（DB 抢占天然水平扩展）+ vendor 维度限流/熔断
   │ 流量 1k~10k QPS、多业务线
   ▼
v3：引入 Kafka（接收→存储 解耦）；按 vendor 分 partition 实现物理隔离
   │ 流量 > 10k QPS / 多团队 / SLA 分级
   ▼
v4：跨地域双活；归档冷数据到对象存储；接入统一可观测性栈（Otel + Prom + Loki）
```

> **强调：v1 → v2 平滑（不改架构，只是换 DB + 多副本）；v2 → v3 才需要重构**。
> 这是有意识地把"重构成本"推迟到收益明显时才付。

---

## 7. 快速开始

```bash
cd rc_dongyang
go mod tidy
go run ./cmd/server   # 默认监听 :8080，MYSQL 文件 ./data/notify.db
```

**提交一个通知**（外部 API 用 httpbin 模拟）：
```bash
curl -X POST http://127.0.0.1:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{
    "biz_system": "subscription",
    "biz_id": "order_1001",
    "url": "https://httpbin.org/status/200",
    "method": "POST",
    "headers": {"X-Trace": "demo"},
    "body": "{\"user_id\":42,\"event\":\"paid\"}",
    "max_attempts": 5
  }'
```

**查询状态**：
```bash
curl http://127.0.0.1:8080/api/v1/notifications/{id}
```

**重投死信**：
```bash
curl -X POST http://127.0.0.1:8080/admin/notifications/{id}/requeue
```

模拟外部失败（httpbin 返回 500）观察重试：
```bash
curl -X POST http://127.0.0.1:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{
    "biz_system":"crm","biz_id":"x1",
    "url":"https://httpbin.org/status/500","method":"POST",
    "body":"{}","max_attempts":3
  }'
```

---

## 8. AI 使用说明
见 [`docs/ai-usage.md`](docs/ai-usage.md)。

## 9. 目录结构
```
rc_dongyang/
├── README.md
├── go.mod
├── docs/
│   └── ai-usage.md
├── config.yaml
├── cmd/
│   └── server/main.go        # API + Worker 同进程入口
├── internal/
│   ├── config/               # 配置加载
│   ├── model/                # 任务模型与状态机
│   ├── store/                # 持久化（MYSQL 实现）
│   ├── api/                  # HTTP handler
│   ├── sender/               # HTTP 客户端（含退避策略）
│   └── worker/               # 调度与抢占
└── migrations/
    └── 001_init.sql
```
