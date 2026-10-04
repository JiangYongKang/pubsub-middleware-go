package pubsub

import (
	"context"
	"testing"
	"time"
)

// 场景二：新旧版本灰度混跑。
//   - "新版本"发布方在帧上附带只认识新字段的扩展（TLV tag 0x01xx），
//     只认识早期字段的代理/订阅方必须安全忽略、正常收发、不断连；
//   - "新版本"代理下发的投递帧同样附带未知扩展，老客户端照常消费；
//   - 早期格式（v1）写出的发布帧，新代理必须照常路由（历史消息可读）；
//   - 真正损坏的帧必须被明确拒绝并断开，与"可安全忽略"严格区分。
func TestE2EGrayMixedVersionsInterop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// 代理在每个 Deliver 帧上附带"新版本字段"：模拟灰度中的新版本代理。
	sys := newTestSystem(t, ctx,
		WithBrokerDeliverExtensions(map[string][]byte{"0100": []byte("broker-side-new-field")}))

	// 只认识早期字段的订阅方。
	sub := sys.newClient(t, "legacy-sub")
	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := make(chan string, 16)
	if err := sub.Subscribe(ctx, "d1", "order.>", func(ctx context.Context, m *Message) error {
		got <- string(m.Payload)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 灰度中的新版本发布方：每个发布帧附带两个"未来字段"。
	pub := sys.newClient(t, "new-pub")
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	newFields := map[string][]byte{
		"0101": []byte("trace-id-xyz"),
		"0102": []byte("schema-v3"),
	}
	const n = 5
	for i := 0; i < n; i++ {
		r, err := pub.PublishExt(ctx, "order.created", []byte{byte('0' + i)}, newFields)
		if err != nil {
			t.Fatalf("new-version publish %d failed: %v (old side must tolerate unknown fields)", i, err)
		}
		for _, o := range r.Outcomes {
			if o.SubscriptionID == "d1" && o.State != DeliveryStateQueued {
				t.Fatalf("publish %d outcome=%+v", i, o)
			}
		}
	}
	// 代理侧必须留下"安全忽略"的判定依据，而不是拒绝。
	sys.logs.waitFor(t, "client=new-pub frame type=3 PROCESS with ignored future fields")

	// 早期格式（v1）写出的发布帧：新代理必须照常路由。
	legacy, err := sys.tr.Dial("legacy-node")
	if err != nil {
		t.Fatal(err)
	}
	v1Frame := Frame{Type: FramePublish, MessageID: "legacy-m1", Topic: "order.created", Payload: []byte("from-v1")}
	rawV1, err := v1Frame.EncodeAs(FrameV1)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Send(rawV1); err != nil {
		t.Fatal(err)
	}

	// 订阅方必须按序收到全部 6 条，内容原样（未知字段不影响已知内容）。
	want := []string{"0", "1", "2", "3", "4", "from-v1"}
	for i, w := range want {
		select {
		case g := <-got:
			if g != w {
				t.Fatalf("message %d = %q, want %q (content or order changed across versions)", i, g, w)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("message %d (%q) not delivered across version boundary", i, w)
		}
	}
	// 客户端侧也必须留下"忽略新版本投递字段"的判定依据。
	sys.logs.waitFor(t, "client legacy-sub] frame type=4 PROCESS with ignored future fields")

	// 灰度流量全程不得出现拒绝或断连。
	if c := sys.logs.count("client=new-pub REJECT"); c != 0 {
		t.Fatalf("new-version publisher saw %d rejects (unknown fields must be ignorable)", c)
	}
	if c := sys.logs.count("OFFLINE"); c != 0 {
		t.Fatalf("gray traffic caused %d disconnects", c)
	}

	// 对照组一：带未知字段的合法帧——安全忽略，连接继续可用。
	oldNode, err := sys.tr.Dial("old-node")
	if err != nil {
		t.Fatal(err)
	}
	fut := Frame{Type: FrameSubscribe, SubscriptionID: "old-durable", Filter: "order.>"}
	fut.SetExt("01ff", []byte("field-from-the-future"))
	rawFut, err := fut.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := oldNode.Send(rawFut); err != nil {
		t.Fatal(err)
	}
	sys.logs.waitFor(t, "client=old-node frame type=1 PROCESS with ignored future fields")
	resp, err := oldNode.Recv() // 连接未被断开：能收到订阅确认
	if err != nil {
		t.Fatalf("old node connection must stay alive after unknown fields: %v", err)
	}
	dr, err := Decode(resp)
	if err != nil || dr.Frame.Type != FrameSubscribed {
		t.Fatalf("old node expected Subscribed, got decision=%v err=%v", dr.Decision, err)
	}
	t.Logf("DECISION: unknown future fields ignored, old node keeps working (got Subscribed)")

	// 对照组二：真正损坏的帧——明确拒绝并断开，与"可忽略"严格区分。
	rogue, err := sys.tr.Dial("rogue")
	if err != nil {
		t.Fatal(err)
	}
	corrupt, err := (Frame{Type: FrameSubscribe, SubscriptionID: "x", Filter: "a.>"}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	corrupt[len(corrupt)-1] ^= 0xFF // 破坏校验和
	if err := rogue.Send(corrupt); err != nil {
		t.Fatal(err)
	}
	sys.logs.waitFor(t, "client=rogue REJECT frame decision=reject reason=checksum_mismatch")
	if _, err := rogue.Recv(); err == nil {
		t.Fatal("corrupted frame must terminate the connection")
	}
	t.Logf("DECISION: corrupted frame rejected with checksum_mismatch and connection closed (distinct from ignorable)")
}
