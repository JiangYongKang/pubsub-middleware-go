package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 场景一：单个客户端在一条连接上同时收发——
// 一路订阅消费很慢（积压到容量上限、触发拒绝），同连接上还要持续发布心跳与聚合结果。
// 要求：
//   - 发布调用全程快速返回，并拿到明确结论（queued / rejected / no_subscriber）；
//   - 同一客户端的另一路订阅（gw-ctl）不被拖慢；
//   - 别的客户端（other）照常收到全部消息，包括被慢订阅拒绝的那条；
//   - 同样的流程重复两轮，每一轮每个订阅得到的结论序列完全一致。
//
// 确定性来自在途窗口：慢订阅在代理侧最多占 window 条在途，客户端本地缓冲（默认 128）
// 远大于窗口，接收泵永不被堵死，PublishAck 因此始终能穿过同一条连接。
func TestE2ESingleConnectionDuplexSlowSubAndPublishing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const (
		capacity = 4
		window   = 4
		rounds   = 2
	)
	sys := newTestSystem(t, ctx,
		WithBrokerCapacity(capacity),
		WithBrokerOverflowPolicy(OverflowRejectNewest),
		WithBrokerMaxInFlight(window))

	gw := sys.newClient(t, "gw")
	if err := gw.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gw.Close() })

	// 慢订阅：处理器卡在闸门上，由测试逐轮放行。
	var gate atomic.Value // 存 chan struct{}，关闭即放行当前轮
	gate.Store(make(chan struct{}))
	if err := gw.Subscribe(ctx, "gw-slow", "data.>", func(ctx context.Context, m *Message) error {
		<-gate.Load().(chan struct{})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 同一客户端的另一路订阅：必须全程不受影响。
	ctlGot := make(chan string, 8)
	if err := gw.Subscribe(ctx, "gw-ctl", "ctl.>", func(ctx context.Context, m *Message) error {
		ctlGot <- m.MessageID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 另一个客户端：独立连接，订阅同一数据主题，必须收到全部消息。
	other := sys.newClient(t, "other")
	if err := other.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	var otherMu sync.Mutex
	var otherGot []string
	if err := other.Subscribe(ctx, "other-sub", "data.>", func(ctx context.Context, m *Message) error {
		otherMu.Lock()
		otherGot = append(otherGot, m.MessageID)
		otherMu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	otherExpect := 0
	// 等别的客户端收到第 n 条：证明慢订阅的积压从不拖慢其他客户端。
	waitOther := func(n int) {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			otherMu.Lock()
			got := len(otherGot)
			otherMu.Unlock()
			if got >= n {
				return
			}
			select {
			case <-deadline:
				otherMu.Lock()
				t.Fatalf("other client stuck at %d messages, want %d (slow subscriber affected others)", len(otherGot), n)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	slowStateOf := func(r *PublishReceipt) DeliveryOutcome {
		t.Helper()
		for _, o := range r.Outcomes {
			if o.SubscriptionID == "gw-slow" {
				return o
			}
		}
		t.Fatalf("receipt missing gw-slow outcome: %+v", r.Outcomes)
		return DeliveryOutcome{}
	}
	publishHeartbeat := func() {
		t.Helper()
		r, err := gw.Publish(ctx, "hb.beat", []byte("ping"))
		if err != nil {
			t.Fatalf("heartbeat publish blocked/failed: %v", err)
		}
		if len(r.Outcomes) != 1 || r.Outcomes[0].State != DeliveryStateNoSubscriber {
			t.Fatalf("heartbeat outcome=%+v, want no_subscriber", r.Outcomes)
		}
	}

	var roundsStates [][]DeliveryState
	for round := 0; round < rounds; round++ {
		t.Logf("=== ROUND %d: slow subscription backlogged, publishing continues on same conn ===", round)
		var states []DeliveryState
		start := time.Now()

		// 阶段 1：发满在途窗口（4 条投递完成后：在途=window、队列仍为空）。
		for i := 0; i < window; i++ {
			r, err := gw.Publish(ctx, "data.x", []byte{byte('a' + round*16 + i)})
			if err != nil {
				t.Fatalf("round %d publish %d: %v", round, i, err)
			}
			states = append(states, slowStateOf(r).State)
			publishHeartbeat()
			otherExpect++
			waitOther(otherExpect)
		}
		// 同步点：本轮 4 条全部转入在途（此前各轮的 8 条已确认）。
		// 此刻在途=window 且队列为空，后续入队必然只进队列，结论确定。
		sys.logs.waitForN(t, "subscription=gw-slow DELIVER seq=", round*(window+capacity)+window)

		// 阶段 2：再发 capacity 条，全部正常入队（队列 1..4）。
		for i := 0; i < capacity; i++ {
			r, err := gw.Publish(ctx, "data.x", []byte{byte('A' + round*16 + i)})
			if err != nil {
				t.Fatalf("round %d backlog publish %d: %v", round, i, err)
			}
			states = append(states, slowStateOf(r).State)
			publishHeartbeat()
			otherExpect++
			waitOther(otherExpect)
		}

		// 阶段 3：积压触顶，下一条必须被拒绝且原因可区分。
		r, err := gw.Publish(ctx, "data.x", []byte("overflow"))
		if err != nil {
			t.Fatalf("round %d overflow publish: %v", round, err)
		}
		out := slowStateOf(r)
		if out.State != DeliveryStateRejected || out.Reason != ReasonBacklogFullRejectNewest {
			t.Fatalf("round %d overflow outcome=%+v, want rejected/%s", round, out, ReasonBacklogFullRejectNewest)
		}
		states = append(states, out.State)
		otherExpect++
		waitOther(otherExpect)
		// 积压期间投递循环必然因窗口满而暂停下发（等 ACK 释放额度）。
		sys.logs.waitFor(t, "WINDOW-FULL")

		// 慢订阅积压期间，同一客户端的另一路订阅必须照常收到消息。
		cr, err := gw.Publish(ctx, "ctl.reload", []byte("cfg"))
		if err != nil {
			t.Fatalf("ctl publish: %v", err)
		}
		for _, o := range cr.Outcomes {
			if o.SubscriptionID == "gw-ctl" && o.State != DeliveryStateQueued {
				t.Fatalf("gw-ctl outcome=%+v", o)
			}
		}
		select {
		case <-ctlGot:
			t.Logf("DECISION round %d: gw-ctl (same client, other subscription) delivered during backlog", round)
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: same-client fast subscription was slowed by backlog", round)
		}

		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("round %d: publishing took %v while slow subscription backlogged (must stay fast)", round, elapsed)
		}
		roundsStates = append(roundsStates, states)
		t.Logf("DECISION round %d: gw-slow outcome sequence=%v", round, states)

		// 放行慢订阅，等积压全部处理完再装下一轮的闸门（否则后续消息会卡在新闸门上）。
		close(gate.Load().(chan struct{}))
		sys.logs.waitForN(t, "HANDLER OK sub=gw-slow", (round+1)*(window+capacity))
		gate.Store(make(chan struct{}))
	}

	// 反复执行结论必须一致：两轮的状态序列完全相同。
	want := []DeliveryState{
		DeliveryStateQueued, DeliveryStateQueued, DeliveryStateQueued, DeliveryStateQueued, // 在途窗口
		DeliveryStateQueued, DeliveryStateQueued, DeliveryStateQueued, DeliveryStateQueued, // 队列积压
		DeliveryStateRejected, // 触顶拒绝
	}
	for round := range roundsStates {
		if len(roundsStates[round]) != len(want) {
			t.Fatalf("round %d states=%v, want %v", round, roundsStates[round], want)
		}
		for i := range want {
			if roundsStates[round][i] != want[i] {
				t.Fatalf("round %d states=%v, want %v (non-deterministic outcome)", round, roundsStates[round], want)
			}
		}
	}

	// 别的客户端不被拖慢：每轮 9 条数据消息（含被 gw-slow 拒绝的那条）全部及时收到
	//（发布过程中已逐条 waitOther 验证，这里复核总数）。
	otherMu.Lock()
	totalOther := len(otherGot)
	otherMu.Unlock()
	if totalOther != rounds*(window+capacity+1) {
		t.Fatalf("other client received %d messages, want %d", totalOther, rounds*(window+capacity+1))
	}

	// 慢订阅最终恰好处理每轮入队的 8 条：被拒绝的消息绝不会事后冒出来。
	time.Sleep(300 * time.Millisecond)
	if got := sys.logs.count("HANDLER OK sub=gw-slow"); got != rounds*(window+capacity) {
		t.Fatalf("gw-slow processed %d messages, want %d (rejected message must stay rejected)", got, rounds*(window+capacity))
	}
	t.Logf("DECISION: two rounds identical, publisher never blocked, other client received all %d messages",
		rounds*(window+capacity+1))
}
