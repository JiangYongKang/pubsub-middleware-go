package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestE2EBasicDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sys := newTestSystem(t, ctx)

	c := sys.newClient(t, "pub1")
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	got := make(chan string, 4)
	if err := c.Subscribe(ctx, "d1", "a.b", func(ctx context.Context, m *Message) error {
		got <- m.MessageID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	r, err := c.Publish(ctx, "a.b", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range r.Outcomes {
		if o.State != DeliveryStateQueued {
			t.Fatalf("outcome=%+v", o)
		}
	}
	select {
	case <-got:
	case <-ctx.Done():
		t.Fatal("message not delivered")
	}
}

// 场景一：短暂断连后的未确认消息必须重投，且同一订阅内顺序不变、消息不丢。
// 构造：m1 处理后自动 ACK；m2 进入处理但阻塞（在途未确认），此时链路中断；
// m3 已在发送途中或队列中。恢复后未确认消息按原 seq 顺序补投，
// 重复投递被去重丢弃；最终 m1,m2,m3 按业务顺序各处理一次。
func TestE2EReconnectRedeliveryInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sys := newTestSystem(t, ctx)

	c := sys.newClient(t, "sub1")
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	var mu sync.Mutex
	var order []string
	m2InFlight := make(chan struct{})
	proceed := make(chan struct{})
	m2Once := int32(0)
	handler := func(ctx context.Context, m *Message) error {
		mu.Lock()
		order = append(order, string(m.Payload))
		n := len(order)
		mu.Unlock()
		t.Logf("HANDLER processing #%d payload=%s attempt=%d", n, string(m.Payload), m.Attempt)
		// m2 首次投递时卡住，使它（以及尚未处理的 m3）在代理侧处于在途/队列未确认状态。
		if string(m.Payload) == "m2" && m.Attempt == 1 && atomic.CompareAndSwapInt32(&m2Once, 0, 1) {
			close(m2InFlight)
			t.Logf("HANDLER: m2 in-flight, holding unacked until fault cycle completes")
			<-proceed
			t.Logf("HANDLER: m2 released and completed")
		}
		return nil
	}
	if err := c.Subscribe(ctx, "d1", "a.>", handler); err != nil {
		t.Fatal(err)
	}

	publish := func(payload string) {
		t.Helper()
		if _, err := c.Publish(ctx, "a.b", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	publish("m1")
	publish("m2")
	publish("m3")

	// 等 m2 确实在途未确认后制造链路故障。
	<-m2InFlight
	t.Logf("FAULT: disconnect while m2/m3 are unacked")
	sys.tr.Disconnect("sub1")
	sys.logs.waitFor(t, "OFFLINE reason=conn_closed")
	go func() {
		time.Sleep(150 * time.Millisecond)
		t.Logf("FAULT: restore link")
		sys.tr.Restore("sub1")
	}()
	sys.logs.waitFor(t, "RESUMED")
	// 恢复后代理必须按 seq 补投未确认消息（attempt=2）。
	sys.logs.waitForN(t, "attempt=2", 1)
	// 此时再放行被卡住的 m2：旧投递与补投在同一个串行分发循环上对撞，
	// 去重保证业务只执行一次，且 m2 永远先于 m3 完成。
	close(proceed)

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		uniq := uniqueInOrder(order)
		mu.Unlock()
		if len(uniq) == 3 {
			if uniq[0] != "m1" || uniq[1] != "m2" || uniq[2] != "m3" {
				t.Fatalf("processing order=%v, want [m1 m2 m3]", uniq)
			}
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timeout, processed=%v (message lost or redelivery stalled)", order)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// 补投流水必须存在，且按 seq 1/2/3 的升序出现（取 attempt=2 的投递行）。
	redelivered := sys.logs.deliverySeqsWithAttempt(2)
	if len(redelivered) == 0 {
		t.Fatal("no attempt=2 redelivery found in delivery flow")
	}
	for i := 1; i < len(redelivered); i++ {
		if redelivered[i] <= redelivered[i-1] {
			t.Fatalf("redelivery seq order=%v not strictly increasing", redelivered)
		}
	}
	t.Logf("DECISION: redelivery seqs=%v in original order, no loss, duplicates deduped", redelivered)
}

// 场景二：重复投递必须被消费端识别并安全丢弃，同一消息业务只处理一次。
// 构造：handler 处理完消息但让其 ACK 丢失（发送 ACK 前断连），
// 恢复后代理重投该消息，客户端必须 DUP-DROP，Handler 不得再次执行。
func TestE2EDuplicateIdentifiedAndDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sys := newTestSystem(t, ctx)

	c := sys.newClient(t, "sub2")
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	var count int32
	faultOnce := int32(0)
	handler := func(ctx context.Context, m *Message) error {
		atomic.AddInt32(&count, 1)
		t.Logf("HANDLER business processing msg=%s (count=%d) attempt=%d",
			m.MessageID, atomic.LoadInt32(&count), m.Attempt)
		// 处理成功后、ACK 发出前断链：ACK 丢失，代理侧保持在途，恢复后重投。
		if atomic.CompareAndSwapInt32(&faultOnce, 0, 1) {
			t.Logf("FAULT: disconnect after processing but before ACK")
			sys.tr.Disconnect("sub2")
			go func() {
				time.Sleep(150 * time.Millisecond)
				sys.tr.Restore("sub2")
			}()
			time.Sleep(50 * time.Millisecond) // 确保 ACK 发送落在死链路上而丢失
		}
		return nil
	}
	if err := c.Subscribe(ctx, "d2", "a.b", handler); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, "a.b", []byte("only")); err != nil {
		t.Fatal(err)
	}

	sys.logs.waitFor(t, "OFFLINE reason=conn_closed")
	sys.logs.waitFor(t, "RESUMED")
	// 重投必须发生（attempt=2），随后被去重逻辑丢弃。
	sys.logs.waitFor(t, "attempt=2")
	sys.logs.waitFor(t, "DUP-DROP")

	// 给潜在重复处理留出窗口后断言业务只执行了一次。
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Fatalf("business handler ran %d times, want exactly 1 (duplicate not dropped)", got)
	}
}

// 场景三：慢消费者背压。s1 消费极慢、积压超容量 -> 回执出现可区分的失败原因；
// s2 正常消费不受影响；发布方调用全程快速返回，绝不被慢订阅阻塞。
func TestE2ESlowConsumerBackpressureIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const cap = 3
	sys := newTestSystem(t, ctx, WithBrokerCapacity(cap), WithBrokerOverflowPolicy(OverflowRejectNewest))

	slow := sys.newClient(t, "slow", WithClientDeliverBuffer(0))
	if err := slow.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slow.Close() })

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	if err := slow.Subscribe(ctx, "s-slow", "t.>", func(ctx context.Context, m *Message) error {
		started <- struct{}{}
		<-release // 处理极慢：阻塞直到测试末尾
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	fast := sys.newClient(t, "fast")
	if err := fast.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fast.Close() })
	fastGot := make(chan string, 16)
	if err := fast.Subscribe(ctx, "s-fast", "t.>", func(ctx context.Context, m *Message) error {
		fastGot <- m.MessageID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	pub := sys.newClient(t, "pub")
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// 先发一条预热消息并等它进入慢处理器（在途未确认，占住容量中的 1 个）。
	if _, err := pub.Publish(ctx, "t.x", []byte("warm")); err != nil {
		t.Fatal(err)
	}
	<-started

	// 逐条发布并等待"快订阅"收到（从不等慢订阅）。这样快订阅始终跟得上、
	// 慢订阅持续积压：若某条对慢订阅被拒绝而对快订阅入队，即证明二者互不影响。
	var sawReject bool
	start := time.Now()
	total := cap + 3
	for i := 0; i < total; i++ {
		r, err := pub.Publish(ctx, "t.x", []byte{byte('a' + i)})
		if err != nil {
			t.Fatalf("publish %d blocked/failed: %v", i, err)
		}
		var slowState, fastState DeliveryState
		for _, o := range r.Outcomes {
			switch o.SubscriptionID {
			case "s-slow":
				slowState = o.State
				if o.State == DeliveryStateRejected && o.Reason == ReasonBacklogFullRejectNewest {
					sawReject = true
				}
			case "s-fast":
				fastState = o.State
			}
		}
		// 关键隔离断言：慢订阅被拒绝的同一条消息，快订阅必须正常入队。
		if slowState == DeliveryStateRejected && fastState != DeliveryStateQueued {
			t.Fatalf("isolation broken on msg %d: slow=%d fast=%d", i, slowState, fastState)
		}
		// 快订阅必须很快收到本条（warm + total 共 total+1 条）。
		select {
		case <-fastGot:
		case <-time.After(2 * time.Second):
			t.Fatalf("fast subscriber did not receive message %d (isolation broken)", i)
		}
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("publisher blocked %v by slow consumer", elapsed)
	}
	if !sawReject {
		t.Fatal("expected at least one reject-newest outcome with distinguishable reason")
	}
	close(release)
}

// 场景四：DropOldest 降级策略下，离线订阅积压超容时丢弃最旧消息，
// 新消息正常入队，回执给出与"拒绝"可区分的降级原因；重连后只收到保留下来的消息。
func TestE2EDropOldestDegradationReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const cap = 2
	sys := newTestSystem(t, ctx, WithBrokerCapacity(cap), WithBrokerOverflowPolicy(OverflowDropOldest))

	sub := sys.newClient(t, "offline-sub")
	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	var mu sync.Mutex
	var got []string
	if err := sub.Subscribe(ctx, "s-off", "t.>", func(ctx context.Context, m *Message) error {
		mu.Lock()
		got = append(got, string(m.Payload))
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sys.logs.waitFor(t, "SUBSCRIBED")

	// 订阅建立后立即断链并保持离线，使消息在代理侧积压。
	sys.tr.Disconnect("offline-sub")
	sys.logs.waitFor(t, "OFFLINE reason=conn_closed")

	pub := sys.newClient(t, "pub-off")
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	var sawDegraded bool
	for i := 0; i < cap+2; i++ {
		r, err := pub.Publish(ctx, "t.x", []byte{byte('0' + i)})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range r.Outcomes {
			if o.SubscriptionID == "s-off" &&
				o.State == DeliveryStateDegraded && o.Reason == ReasonSlowConsumerDropOldest {
				sawDegraded = true
			}
			if o.SubscriptionID == "s-off" && o.State == DeliveryStateRejected {
				t.Fatalf("drop-oldest policy must not reject, got %+v", o)
			}
		}
	}
	if !sawDegraded {
		t.Fatal("expected degraded/drop-oldest outcome with distinguishable reason")
	}

	sys.tr.Restore("offline-sub")
	sys.logs.waitFor(t, "RESUMED")

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == cap {
			mu.Lock()
			// 容量为 2，共发 4 条（0..3）：最旧两条被丢，保留最新 "2","3"。
			if got[0] != "2" || got[1] != "3" {
				t.Fatalf("retained=%v, want [2 3]", got)
			}
			mu.Unlock()
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timeout got=%v", got)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func uniqueInOrder(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
