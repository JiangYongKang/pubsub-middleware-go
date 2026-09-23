# 本地发布订阅中间件（Go）

一套可插拔本地传输的发布/订阅中间件，用进程内传输模拟"客户端 ⇄ 代理"之间的
消息收发，使**持久订阅、按序投递、断线重连补投、慢消费者背压**在各个入口下
都有一致且可解释的结论。

- 无第三方依赖，仅使用标准库。
- 传输层为接口（`Transport`/`Conn`），内置内存实现 `LocalTransport`，
  可按客户端模拟链路故障（`Disconnect`）与恢复（`Restore`）。

## 快速开始

```go
tr := pubsub.NewLocalTransport()
broker := pubsub.NewBroker(tr)
go broker.Serve(ctx)

c := pubsub.NewClient("client-1", tr)
_ = c.Connect(ctx)

// 持久订阅：durableID 相同即同一订阅，断连重连后状态延续
_ = c.Subscribe(ctx, "durable-orders", "order.>", func(ctx context.Context, m *pubsub.Message) error {
    // 业务处理；返回 nil 后客户端自动 ACK
    fmt.Println(m.Topic, string(m.Payload), "attempt=", m.Attempt)
    return nil
})

receipt, err := c.Publish(ctx, "order.created", []byte(`{"id":1}`))
// receipt.Outcomes 给出该消息对每个匹配订阅的处置结论
```

## 一、投递语义

### 持久订阅

- 订阅以 `durableID` 标识。连接断开只令订阅**离线**，其状态（待发队列 +
  已投递未确认集合）在代理侧完整保留；客户端用同一 `durableID` 重新接入即恢复。
- 每个订阅维护一个单调递增的 `seq`，投递严格按 `seq` 升序：
  **同一订阅内消息有序**；不同订阅相互独立，不保证全局顺序。

### 至少一次投递 + 消费端幂等去重

- 消息在收到 ACK 前一直被代理记账（在途或队列）。ACK 丢失或链路中断都会触发重投，
  因此语义是**至少一次（at-least-once）**。
- 客户端以 `(subscriptionID, messageID)` 做消费端去重（有界去重环，默认 4096）：
  重投消息在进入业务 `Handler` 之前就被识别，日志打印 `DUP-DROP` 并仅补一个幂等 ACK，
  **业务处理器对同一条消息只执行一次**。
- 帧中 `Attempt` 字段：首次投递为 1，重投为 2、3…，业务可据此区分首投与重投。
- ACK 是幂等的：重复 ACK、未知 seq 的 ACK 都被安全忽略。

### 断线重连与补投规则

1. 客户端检测到连接失效后自动重连（带退避），链路恢复后自动重发全部订阅帧；
2. 代理把断连时所有"在途未确认"消息按 `seq` 升序归还到队列前部；
3. 重新 `attach` 后，投递循环被唤醒，未确认消息连同积压消息一起按原顺序补投；
4. 已被客户端处理但 ACK 丢失的消息，补投时被去重丢弃，不会重复处理。

连接归属通过"订阅只认当前附着连接"保证无竞态：旧连接迟到的失效事件不会把
已重连的订阅再次置为离线。

## 二、慢消费者与背压

发布方**绝不**因某个订阅消费慢而阻塞，慢订阅也**不影响其他订阅方**。背压在各层的处置：

| 层级 | 行为 |
|------|------|
| 代理发布路径 | 入队是一次内存操作，立即返回；每个订阅独立投递 goroutine |
| 传输发送 | `Conn.Send` **非阻塞**，对端缓冲满立即失败，不拖住发送方 |
| 代理投递 | 只有"真正写进对端连接"成功，消息才出队转入在途；缓冲满则消息**留在队列**稍后重试，不丢不乱序 |
| 订阅积压 | 队列达到 `capacity` 上限后，按订阅配置的 `OverflowPolicy` 处置 |

两种确定的溢出策略，结论通过发布回执 `DeliveryOutcome` 明确区分（失败 vs 降级）：

- `OverflowRejectNewest`（默认，**失败**）：新消息不入队，回执
  `State=Rejected, Reason=backlog_full_reject_newest`。
- `OverflowDropOldest`（**降级**）：丢弃该订阅最旧的未发送消息为新消息腾位，回执
  `State=Degraded, Reason=slow_consumer_drop_oldest`，被丢弃的是确定性的最旧消息。

每个订阅容量与策略可配：

```go
broker := pubsub.NewBroker(tr,
    pubsub.WithBrokerCapacity(1000),
    pubsub.WithBrokerOverflowPolicy(pubsub.OverflowDropOldest))
```

其他常见结论：无匹配订阅为 `NoSubscriber`；订阅已关闭为 `Closed`。

## 三、消息格式与版本兼容

帧为自定义二进制格式：`魔数 | 版本 | 类型 | 标志 | 尝试次数 | 长度前缀字段 |
投递结果区 | TLV 扩展区 | CRC32 校验和`。

解码结论 `DecodeResult` 是**确定且可区分**的，两类情形不混淆：

**必须拒绝（`Decision=Reject`，各自独立的 `Reason`）：**

| Reason | 含义 |
|--------|------|
| `frame_too_short` | 帧不完整，短于固定头 |
| `bad_magic` | 魔数错误，非本协议数据 |
| `unsupported_version` | 版本超出可兼容范围（过新或过旧） |
| `checksum_mismatch` | CRC32 不一致，帧已损坏 |
| `incomplete_field` | 长度前缀越界，字段被截断 |
| `unknown_frame_type` | 无法解释的帧类型 |

**可安全忽略（`Decision=Process`）：**

- `unknown_field_ignored`：帧合法，仅携带新版本新增的未知 TLV 扩展字段。
  解码器跳过未知字段、正常读取全部已知字段，并在 `IgnoredFields` 中记录被忽略的 tag。

兼容性规则：

- 协议字段**只增不改**：新版本只能在 TLV 扩展区追加字段，不得改动既有字段语义；
- `0x0000–0x00FF` 为应用层保留 tag（如投递序号 `seq=0x0001`），属已知字段；
- 因此 **v1 时代写出的历史消息，v2 解码器仍可原样读出**（有专门测试覆盖）；
- 代理/客户端收到损坏或不兼容帧时记录判定依据并关闭该连接，绝不把半损坏数据
  误判为正常消息；收到带未知字段的合法帧则继续处理。

## 四、可解释性与判定依据

- 所有关键动作都有结构化日志（可注入 `WithBrokerLogger` / `WithClientLogger`）：
  `CREATED/RESUMED/OFFLINE/QUEUED/DELIVER/ACK/DUP-DROP/REJECT/DEGRADED/PEER-BUFFER-FULL` 等。
- 失败与降级原因都以 `Reason` 字符串贯穿：传输错误、积压处置、帧解码拒绝。
- 单测日志会实时打印**投递流水**与每一步**判定依据**（见测试输出）。

## 五、验证方法

```bash
go test ./...              # 全部单元测试与端到端场景
go test -race ./...        # 竞态检测
go test -v -run TestE2E    # 观察端到端投递流水与判定依据
```

测试覆盖：

| 测试 | 覆盖点 |
|------|--------|
| `TestFrameDecodeRejectReasons` | 6 类必须拒绝的损坏/不兼容帧，原因逐一可区分 |
| `TestFrameDecodeUnknownFieldsSafelyIgnored` | 未知 TLV 字段被安全忽略且已知字段完好 |
| `TestFrameDecodeOlderShapeReadable` | v1 历史消息在新版本下仍可读 |
| `TestMatchTopic` | 主题过滤 `*` / `>` 规则 |
| `TestOverflowRejectNewest` / `TestOverflowDropOldest` | 两种背压策略、失败与降级原因可区分、顺序确定 |
| `TestOfflineInflightReturnedInOrder` | 在途消息断连后按原顺序归还、重连保序补投 |
| `TestE2EReconnectRedeliveryInOrder` | 断连补投：消息不丢、同订阅有序、重复被去重 |
| `TestE2EDuplicateIdentifiedAndDropped` | ACK 丢失后重投被识别并安全丢弃，业务只执行一次 |
| `TestE2ESlowConsumerBackpressureIsolation` | 慢订阅不阻塞发布、不影响快订阅；拒绝原因可区分 |
| `TestE2EDropOldestDegradationReason` | 离线积压降级丢最旧，重连只收到保留的最新消息 |

## 主要类型

- `Broker`：代理，维护持久订阅、按序投递、ACK 清理、重连补投。
- `Client`：客户端，自动重连/重订阅、消费端去重、自动 ACK、带原因的发布回执。
- `Transport` / `Conn`：可插拔传输接口；`LocalTransport` 为内存实现并支持故障注入。
- `Frame` / `DecodeResult`：二进制帧与确定可区分的解码结论。
- `DeliveryOutcome` / `Reason`：每条消息对每个订阅的处置结论与原因。
