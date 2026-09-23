# 本地发布订阅中间件：投递语义、背压与重连

本包（`pubsub`）实现了一套**本地**发布订阅中间件。客户端（订阅方/发布方）
与代理（Broker）之间不经过真实网络，而是经由**可插拔的本地传输**
（`Transport` / `Conn`）收发**带版本的二进制帧**，从而在同一进程内
确定性地复现订阅、发布、确认、断连与重连。

- 传输可替换：`Transport` 是接口，默认实现 `NewLocalTransport` 提供成对的逻辑连接。
- 编解码可替换：`Codec` 是接口，默认实现 `NewCodec`（魔数 + 版本 + TLV 字段 + CRC16）。

## 1. 投递语义

### 1.1 持久订阅
- 订阅以 `Subscribe(id, pattern, handler)` 建立。`id` 是订阅方的持久身份，
  `pattern` 是其主题过滤规则，服务端将其持久化。
- 主题过滤为 MQTT 风格层级匹配：`+` 匹配单层，`#` 匹配其后任意层级（仅可置于末尾）。
- 每个订阅在服务端维护一个队列，模型为：

  ```
  单个 inflight 槽（任一时刻最多一条在途，保证顺序） + FIFO pending 队列
  积压 backlog = (inflight != nil) + len(pending)
  ```

### 1.2 确认与“ effectively‑once ”处理
- 业务处理器返回 `nil` 即 ACK，返回非 nil 即 NACK。
- Broker 任一时刻只向一个订阅投递**一条在途消息**，必须等该消息被 ACK/NACK
  或连接断开，才会投递下一条，因此**同一订阅严格保序**。
- 至少一次（at-least-once）投递：未确认消息在断连/NACK 后会重投。
- 消费端去重：客户端持有有界 LRU 缓存（messageID）。重投的消息若已处理过，
  **不会再次进入业务处理器**，而是记录 `duplicate_skipped` 并自动补发 ACK。
  因此在“消息 ID 唯一 + 处理器对已处理 ID 幂等”的前提下，业务效果为
  **不重复、不丢失**（effectively-once）。
- 重复/迟到的 ACK 在服务端幂等处理（已结算/已丢弃），安全忽略，不报错。

### 1.3 重连补投（按原过滤规则、保序）
- 断连时：在途消息被放回 pending **队头**（保留 `attempt` 序号），
  pending 中其余消息原样保留；服务端订阅状态不清除。
- 以相同 `id` 重新接入时，Broker 识别为同一持久订阅，恢复连接并：
  1. 先补投之前未确认的消息（`Redelivered=true`，`attempt` 递增）；
  2. 再投递断连期间按**原有 pattern** 过滤后入队的新消息；
  3. 顺序为“未确认旧消息 → 新消息”，严格有序。
- 不匹配该订阅过滤规则的消息（即使在断连期间发布）永不投递给该订阅。

## 2. 慢消费者与背压

- **发布方不被阻塞**：`Publish` 只做匹配与入队判定，立即返回
  `PublishResult`，按订阅逐项给出 `TargetResult{SubID, Reason, DroppedID}`。
- **订阅相互隔离**：单个订阅积压达上限只影响它自己，其他订阅照常接收与消费。
- 容量上限 `BrokerConfig.SubQueueCapacity` 同时计入 inflight 与 pending。
  超限时按 `BrokerConfig.Overflow` 采用**确定、可解释**的策略：

  | 策略 | 处置 | 原因码（失败 vs 降级，严格可区分） |
  |------|------|-----------------------------------|
  | `OverflowRejectNew`（默认） | 拒绝该新消息进入该订阅 | `queue_full_rejected`（**失败**） |
  | `OverflowDropOldest` | 丢弃该订阅 pending 队头最旧的**未在途**消息，为新消息腾位 | `dropped_oldest`（**降级**，并返回被丢弃的 `DroppedID`） |

  - 降级丢弃**绝不摘除正在投递的 inflight 消息**（避免与在途投递竞争）。
    若积压仅由 inflight 构成、无 pending 可丢，则退化为 `queue_full_rejected`。
  - 背压原因逐订阅、逐消息记录到流水（`reject_queue_full` / `drop_oldest`），
    失败与降级不会相互混淆。

## 3. 消息格式与版本兼容

帧格式（大端）：

```
magic(1)=0xA7 | version(1) | kind(1) | flags(1) | bodyLen(4) | crc16(2) | body(TLV...)
```

- body 为 TLV 字段序列：`tag(2) | len(2) | value(len)`。
- 版本策略：
  - `CurrentWire = 2`，`MinSupportedWire = 1`。
  - **向后可读**：V1 历史帧缺少 V2 的 `PublishedAt` 字段，解码器按缺省处理，消息仍可读。
  - **前向兼容**：遇到不认识的 TLV 标签（未来版本新增字段）时**安全跳过**，
    在 `Frame.UnknownTags` 留痕并记录 `frame_ignored`，绝不因此拒绝消息。
  - 新增字段只分配新标签、不复用旧标签。
  - 高于支持范围的版本直接拒绝。

“可安全忽略”与“必须拒绝”的结论严格区分，原因码互不相同：

| 情形 | 结论 | 原因码 |
|------|------|--------|
| 帧短于固定头 / body 长度越界 | 必须拒绝 | `frame_truncated` |
| 魔数错误（非本协议帧） | 必须拒绝 | `frame_bad_magic` |
| 版本高于支持范围 | 必须拒绝 | `frame_unsupported` |
| CRC16 不匹配（损坏） | 必须拒绝 | `frame_checksum` |
| 无法识别的帧类型 | 必须拒绝 | `frame_unknown_kind` |
| 未知 TLV 字段标签 | **可安全忽略**（跳过并留痕） | `frame_unknown_tag` |
| 空帧体（心跳/保活） | **可安全忽略** | 解码为 `FrameHeartbeat`，无错误 |

错误统一为 `*DecisionError`，携带 `Reason` 与 `Ignore` 标志；
`pubsub.IsIgnorable(err)` 可直接区分“可忽略”与“必须拒绝”。

## 4. 可解释的判定流水

Broker 与 Client 都提供 `Journal() []Event`，并可通过
`WithBrokerLogger(w)` / `WithClientLogger(w)` 实时镜像到任意 `io.Writer`
（测试中接到 `t.Log`）。每条流水含时间、事件类型、订阅、消息、主题与判定依据，
例如：`subscribed / enqueued / delivered / redeliver / acked / nacked /
reject_queue_full / drop_oldest / duplicate_skipped / frame_ignored / frame_rejected /
disconnected / reconnected`。

## 5. 验证方法

```bash
# 全量测试（-race 检测并发正确性）
go test -race ./... -v

# 仅关注某类场景
go test -race ./pubsub/ -run TestReconnectRedeliveryInOrder -v   # 重连补投 + 保序 + 去重
go test -race ./pubsub/ -run TestDuplicateDeliveryRecognized -v  # 重复投递识别
go test -race ./pubsub/ -run TestSlowConsumer -v                 # 慢消费者两种背压策略
go test -race ./pubsub/ -run TestCodec -v                        # 版本兼容 / 未知字段 / 损坏帧
```

测试覆盖与断言：
- `TestReconnectRedeliveryInOrder`：断连后 m1（ACK 丢失）重投、断连期间 m2 入队、
  不匹配的 mOther 永不投递；业务处理 m1、m2 **各恰好一次且有序**；
  服务端有 `redeliver` 流水、客户端有 `duplicate_skipped` 流水。
- `TestDuplicateDeliveryRecognized`：重投被识别，业务处理器仅运行一次。
- `TestSlowConsumerRejectNew`：慢订阅饱和后 2 条被 `queue_full_rejected` 拒绝，
  发布方不被阻塞，快订阅仍收到全部消息且不重复。
- `TestSlowConsumerDropOldest`：超限时丢弃 pending 队头 d1（`dropped_oldest` +
  `DroppedID`），其余消息恰好一次且有序；失败/降级原因码可区分。
- `TestCodecBackwardCompatV1` / `TestCodecUnknownTagIgnored` /
  `TestCodecCorruptFramesRejected`：历史版本可读、未知字段安全忽略、
  各类损坏帧以可区分原因被拒绝、空帧可安全忽略。

运行 `-v` 时可在测试日志中看到完整投递流水与每条判定依据。
