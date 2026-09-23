package pubsub

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitFor 等待条件成立（确定性轮询，避免脆弱的固定 sleep）。
func waitFor(deadline time.Duration, fn func() bool) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

func newSystem(t *testing.T, cap_ int, policy OverflowPolicy) (Broker, *localClient) {
	t.Helper()
	tr, sw := NewLocalTransport(16)
	br := NewBroker(sw, NewCodec(), BrokerConfig{SubQueueCapacity: cap_, Overflow: policy}, WithBrokerLogger(testLogger{t}))
	if err := br.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := NewClient(tr, NewCodec(), WithClientBroker(br), WithClientLogger(testLogger{t})).(*localClient)
	return br, c
}

type testLogger struct{ t *testing.T }

func (l testLogger) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// 场景一：短暂断连后重新接入，未确认消息按原过滤规则补投，同一订阅顺序保持；
// 重投在消费端被识别为重复并安全丢弃，业务处理“不重复、不丢失”。
func TestReconnectRedeliveryInOrder(t *testing.T) {
	br, c := newSystem(t, 16, OverflowRejectNew)
	defer br.Close()
	defer c.Close()
	ctx := context.Background()

	var mu sync.Mutex
	processed := []string{}
	c.swallowAck = map[string]int{"m1": 1} // 确定性模拟：m1 的首次 ACK 在网络中丢失

	err := c.Subscribe(ctx, "s1", "orders/+", func(ctx context.Context, d Delivery) error {
		mu.Lock()
		processed = append(processed, d.Message.ID)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !waitFor(time.Second, func() bool { return journalHas(br.Journal(), EvSubscribed, "") }) {
		t.Fatal("subscription not established")
	}

	pub := func(id, topic string) {
		t.Helper()
		r, err := c.Publish(ctx, Message{ID: id, Topic: topic, Payload: []byte(id)})
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range r.Targets {
			if x.Reason != ReasonOK {
				t.Fatalf("publish %s unexpected reason %s", id, x.Reason)
			}
		}
	}

	pub("m1", "orders/1")
	if !waitFor(time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count(processed, "m1") == 1
	}) {
		t.Fatal("m1 not processed once before disconnect")
	}

	// 短暂断连；期间发布命中的 m2 与不命中的 mOther。
	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(time.Second, func() bool { return journalHas(br.Journal(), EvDisconnected, "") }) {
		t.Fatal("broker did not observe disconnect")
	}
	pub("m2", "orders/2")
	pub("mOther", "payments/9")

	if err := c.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}

	// 收敛：m1（重投后被去重自动确认，业务不重复处理）与 m2（恰好一次）。
	if !waitFor(2*time.Second, func() bool {
		return journalHas(c.Journal(), EvDuplicate, "m1") &&
			journalHas(br.Journal(), EvAcked, "m2")
	}) {
		mu.Lock()
		t.Fatalf("did not converge: processed=%v broker=%v client=%v",
			processed, kinds(br.Journal()), kinds(c.Journal()))
		mu.Unlock()
	}
	// 再留一个窗口确认不会有额外的重复处理。
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count(processed, "m1") != 1 || count(processed, "m2") != 1 {
		t.Fatalf("each matched message processed exactly once: %v", processed)
	}
	if count(processed, "mOther") != 0 {
		t.Fatalf("non-matching message must never reach subscriber: %v", processed)
	}
	if indexOf(processed, "m1") > indexOf(processed, "m2") {
		t.Fatalf("order violated (redelivered m1 before m2): %v", processed)
	}
	if !journalHas(br.Journal(), EvRedeliver, "m1") {
		t.Fatal("broker journal must record redeliver(m1)")
	}
	t.Logf("business processing trace (no dup, no loss, ordered): %v", processed)
}

// 场景二：重复投递被消费端识别；业务处理器只运行一次。
func TestDuplicateDeliveryRecognized(t *testing.T) {
	br, c := newSystem(t, 8, OverflowRejectNew)
	defer br.Close()
	defer c.Close()
	ctx := context.Background()

	var mu sync.Mutex
	hits := 0
	c.swallowAck = map[string]int{"d1": 1}

	if err := c.Subscribe(ctx, "s2", "t/+", func(ctx context.Context, d Delivery) error {
		mu.Lock()
		hits++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(time.Second, func() bool { return journalHas(br.Journal(), EvSubscribed, "") })

	if _, err := c.Publish(ctx, Message{ID: "d1", Topic: "t/x"}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return hits == 1
	}) {
		t.Fatal("first delivery not processed")
	}
	_ = c.Disconnect()
	waitFor(time.Second, func() bool { return journalHas(br.Journal(), EvDisconnected, "") })
	_ = c.Reconnect(ctx)

	if !waitFor(2*time.Second, func() bool { return journalHas(c.Journal(), EvDuplicate, "d1") }) {
		t.Fatal("duplicate redelivery not recognized at consumer")
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("handler must run exactly once, ran %d", hits)
	}
	if !journalHas(br.Journal(), EvRedeliver, "d1") {
		t.Fatal("broker must record redeliver(d1)")
	}
}

func count(ss []string, v string) int {
	n := 0
	for _, s := range ss {
		if s == v {
			n++
		}
	}
	return n
}
func indexOf(ss []string, v string) int {
	for i, s := range ss {
		if s == v {
			return i
		}
	}
	return -1
}
func journalHas(evs []Event, kind, msgID string) bool {
	for _, e := range evs {
		if e.Kind == kind && (msgID == "" || e.MsgID == msgID) {
			return true
		}
	}
	return false
}
func kinds(evs []Event) []string {
	out := []string{}
	for _, e := range evs {
		out = append(out, e.Kind+":"+e.MsgID)
	}
	return out
}
