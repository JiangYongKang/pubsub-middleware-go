# pubsub-middleware-go

一套**本地**发布订阅中间件：用**可插拔的本地传输**模拟客户端与代理之间的消息收发，
让订阅、投递、背压与重连在各个入口（发布、投递、确认、断连/重连、帧解码）下
都得到**一致且可解释**的结论。

## 特性

- **持久订阅 + 投递语义**：断连后以相同订阅 ID 重新接入，未确认消息按**原有主题过滤规则**
  补投，同一订阅严格保序；消费端识别重复投递并安全丢弃（自动确认），业务处理不重复、不丢失。
- **慢消费者背压**：单个订阅消费慢不会阻塞发布方，也不影响其他订阅；积压超限时按
  `RejectNew`（拒绝，原因 `queue_full_rejected`）或 `DropOldest`（降级丢弃最旧未在途消息，
  原因 `dropped_oldest` 并携带被丢弃 ID）处置，失败与降级原因严格可区分。
- **格式与版本兼容**：魔数 + 版本 + TLV + CRC16 帧格式；V1 历史消息 V2 仍可读，
  未知字段安全跳过并留痕；不完整/损坏/错误版本/错误魔数等帧以各自确定原因被拒绝；
  “可安全忽略”与“必须拒绝”绝不混淆。
- **可解释流水**：Broker/Client 均提供 `Journal()`，可镜像到 `io.Writer`（单测中打印投递流水与判定依据）。

## 快速开始

```go
tr, sw := pubsub.NewLocalTransport(32)                       // 可插拔本地传输
br := pubsub.NewBroker(sw, pubsub.NewCodec(), pubsub.BrokerConfig{
    SubQueueCapacity: 16,
    Overflow:         pubsub.OverflowRejectNew,
})
br.Start(ctx); defer br.Close()

c := pubsub.NewClient(tr, pubsub.NewCodec(), pubsub.WithClientBroker(br))
c.Subscribe(ctx, "orders-1", "orders/+", func(ctx context.Context, d pubsub.Delivery) error {
    // 处理 d.Message；返回 nil 为 ACK，返回 error 为 NACK（稍后按序重投）
    return nil
})

r, _ := c.Publish(ctx, pubsub.Message{ID: "m1", Topic: "orders/42"})
// r.Targets[i].Reason: ok / queue_full_rejected / dropped_oldest ...

c.Disconnect()  // 模拟短暂断连（服务端保留持久订阅与未确认消息）
c.Reconnect(ctx) // 同 ID 重新接入，按原过滤规则补投未确认消息
```

主题过滤：`+` 匹配单层，`#` 匹配其后任意层级（仅末尾）。

## 目录

- `pubsub/` 中间件实现
  - `transport.go` 可插拔本地传输（逻辑连接对、断连/重连）
  - `codec.go` 带版本/CRC 的 TLV 帧编解码（旧版可读、未知字段忽略、损坏拒绝）
  - `broker.go` 代理：持久订阅、顺序投递、重连补投、背压裁决、判定流水
  - `client.go` 订阅方：收发帧、消费端去重、自动确认、NACK、断连/重连
  - `filter.go` 主题过滤
  - `errors.go` / `types.go` 可区分原因码与类型
- `docs/DELIVERY_SEMANTICS.md` 投递语义、背压与重连规则、验证方法（详细）

## 验证

```bash
go test -race ./... -v
```

覆盖：重连补投保序、重复消息识别（不重复处理）、慢消费者两种背压策略（发布不阻塞、
订阅隔离、原因可区分）、V1/V2 兼容、未知字段忽略、损坏帧分类拒绝。`-v` 日志打印完整投递流水。
