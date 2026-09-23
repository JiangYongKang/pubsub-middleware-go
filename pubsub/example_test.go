package pubsub_test

import (
	"context"
	"fmt"
	"time"

	pubsub "github.com/highcumontoa/pubsub-middleware-go/pubsub"
)

// 演示：经可插拔本地传输完成订阅、发布、断连与重连补投。
func Example() {
	ctx := context.Background()

	// 1) 一对配套的“客户端传输 + 服务端交换中枢”，以及代理。
	tr, sw := pubsub.NewLocalTransport(32)
	br := pubsub.NewBroker(sw, pubsub.NewCodec(), pubsub.BrokerConfig{
		SubQueueCapacity: 16,
		Overflow:         pubsub.OverflowRejectNew,
	})
	_ = br.Start(ctx)
	defer br.Close()

	// 2) 订阅方：id 是持久身份，pattern 是过滤规则。
	c := pubsub.NewClient(tr, pubsub.NewCodec(), pubsub.WithClientBroker(br))
	defer c.Close()
	_ = c.Subscribe(ctx, "orders-1", "orders/+", func(ctx context.Context, d pubsub.Delivery) error {
		fmt.Printf("process %s (redelivered=%v attempt=%d)\n", d.Message.ID, d.Redelivered, d.DeliveryAttempt)
		return nil // 返回 nil 即 ACK；返回 error 即 NACK（稍后按序重投）
	})
	time.Sleep(20 * time.Millisecond)

	// 3) 发布：按订阅逐项给出确定性结论。
	r, _ := c.Publish(ctx, pubsub.Message{ID: "m-1", Topic: "orders/42", Payload: []byte("hi")})
	for _, t := range r.Targets {
		fmt.Printf("sub %s -> %s\n", t.SubID, t.Reason)
	}
	time.Sleep(30 * time.Millisecond)

	// 4) 短暂断连后以相同 id 重新接入：未确认消息按原规则补投。
	_ = c.Disconnect()
	time.Sleep(10 * time.Millisecond)
	_ = c.Reconnect(ctx)
	time.Sleep(30 * time.Millisecond)
}
