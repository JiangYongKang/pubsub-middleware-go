package pubsub

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mixedResult 是一轮混合负载实验的确定性结论。
type mixedResult struct {
	states   []DeliveryState // gw-slow 对 9 条数据消息的处置序列
	reasons  []Reason        // 对应的原因
	retained []string        // 慢订阅最终被业务处理的消息（按处理顺序）
	ctlGot   []string        // 同客户端快订阅在积压期间收到的消息
	otherN   int             // 别的客户端收到的数据消息数
}

// 场景三：混合负载下的容量取舍。
// 同一连接上慢订阅积压 + 持续发布（心跳/聚合），同时新旧版本数据穿插
// （发布帧带未来扩展字段、代理投递帧带未来扩展字段、v1 早期格式帧直接注入）。
// 积压触顶时按配置策略给出确定结论；同样的负载跑两遍，结论必须逐位一致。
func TestE2EMixedLoadCapacityDeterminism(t *testing.T) {
	for _, policy := range []OverflowPolicy{OverflowRejectNewest, OverflowDropOldest} {
		name := "RejectNewest"
		if policy == OverflowDropOldest {
			name = "DropOldest"
		}
		t.Run(name, func(t *testing.T) {
			run1 := runMixedLoad(t, policy)
			run2 := runMixedLoad(t, policy)

			if !reflect.DeepEqual(run1.states, run2.states) ||
				!reflect.DeepEqual(run1.reasons, run2.reasons) ||
				!reflect.DeepEqual(run1.retained, run2.retained) {
				t.Fatalf("same load must give identical conclusions:\n run1=%+v\n run2=%+v", run1, run2)
			}

			// 前 8 条（在途窗口 4 + 队列 4）一律入队；第 9 条触顶，结论由策略决定。
			for i := 0; i < 8; i++ {
				if run1.states[i] != DeliveryStateQueued {
					t.Fatalf("message %d state=%v, want queued", i, run1.states[i])
				}
			}
			switch policy {
			case OverflowRejectNewest:
				if run1.states[8] != DeliveryStateRejected || run1.reasons[8] != ReasonBacklogFullRejectNewest {
					t.Fatalf("overflow outcome=%v/%v, want rejected/%s",
						run1.states[8], run1.reasons[8], ReasonBacklogFullRejectNewest)
				}
				// 拒绝策略：前 8 条全部保留，第 9 条（z）不出现。
				want := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
				if !reflect.DeepEqual(run1.retained, want) {
					t.Fatalf("reject-newest retained=%v, want %v", run1.retained, want)
				}
			case OverflowDropOldest:
				if run1.states[8] != DeliveryStateDegraded || run1.reasons[8] != ReasonSlowConsumerDropOldest {
					t.Fatalf("overflow outcome=%v/%v, want degraded/%s",
						run1.states[8], run1.reasons[8], ReasonSlowConsumerDropOldest)
				}
				// 降级策略：确定性地丢弃最旧的未发送消息 e（不是随机、不是最新），
				// 新消息 z 入队；在途的 a-d 不受影响。
				want := []string{"a", "b", "c", "d", "f", "g", "h", "z"}
				if !reflect.DeepEqual(run1.retained, want) {
					t.Fatalf("drop-oldest retained=%v, want %v (must deterministically drop oldest)", run1.retained, want)
				}
			}

			// 被处置的消息不牵连别人：同客户端快订阅收到 v1 历史帧，
			// 别的客户端收全 9 条数据消息。
			if len(run1.ctlGot) != 2 {
				t.Fatalf("fast subscription got %v during backlog, want 2 legacy v1 messages", run1.ctlGot)
			}
			if run1.otherN != 9 {
				t.Fatalf("other client received %d messages, want 9", run1.otherN)
			}
			t.Logf("DECISION policy=%s: states=%v reasons=%v retained=%v (identical across runs)",
				name, run1.states, run1.reasons, run1.retained)
		})
	}
}

// runMixedLoad 以给定溢出策略执行一轮混合负载实验并返回确定性结论。
func runMixedLoad(t *testing.T, policy OverflowPolicy) mixedResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const (
		capacity = 4
		window   = 4
	)
	sys := newTestSystem(t, ctx,
		WithBrokerCapacity(capacity),
		WithBrokerOverflowPolicy(policy),
		WithBrokerMaxInFlight(window),
		// 代理下发的投递帧附带"新版本字段"：新旧版本数据在投递方向穿插。
		WithBrokerDeliverExtensions(map[string][]byte{"0100": []byte("new-broker-field")}))

	gw := sys.newClient(t, "gw")
	if err := gw.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gw.Close() })

	var gate atomic.Value
	gate.Store(make(chan struct{}))
	var mu sync.Mutex
	var processed []string
	if err := gw.Subscribe(ctx, "gw-slow", "data.>", func(ctx context.Context, m *Message) error {
		<-gate.Load().(chan struct{})
		mu.Lock()
		processed = append(processed, string(m.Payload))
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var ctlMu sync.Mutex
	var ctlGot []string
	if err := gw.Subscribe(ctx, "gw-ctl", "ctl.>", func(ctx context.Context, m *Message) error {
		ctlMu.Lock()
		ctlGot = append(ctlGot, string(m.Payload))
		ctlMu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	other := sys.newClient(t, "other")
	if err := other.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	otherCh := make(chan string, 16)
	if err := other.Subscribe(ctx, "other-sub", "data.>", func(ctx context.Context, m *Message) error {
		otherCh <- string(m.Payload)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// v1 早期格式帧注入通道：模拟灰度中只懂旧格式的节点持续上报。
	legacy, err := sys.tr.Dial("legacy-feed")
	if err != nil {
		t.Fatal(err)
	}
	publishV1 := func(id, topic, payload string) {
		t.Helper()
		raw, err := (Frame{Type: FramePublish, MessageID: id, Topic: topic, Payload: []byte(payload)}).EncodeAs(FrameV1)
		if err != nil {
			t.Fatal(err)
		}
		if err := legacy.Send(raw); err != nil {
			t.Fatal(err)
		}
		if _, err := legacy.Recv(); err != nil { // 读走 PublishAck，保持链路干净
			t.Fatal(err)
		}
	}

	res := mixedResult{}
	publishData := func(i int, payload string) {
		t.Helper()
		var r *PublishReceipt
		var err error
		// 新旧版本数据穿插：偶数序号的发布携带"未来扩展字段"。
		if i%2 == 0 {
			r, err = gw.PublishExt(ctx, "data.x", []byte(payload),
				map[string][]byte{"0101": []byte(fmt.Sprintf("trace-%d", i))})
		} else {
			r, err = gw.Publish(ctx, "data.x", []byte(payload))
		}
		if err != nil {
			t.Fatalf("publish %s: %v", payload, err)
		}
		for _, o := range r.Outcomes {
			if o.SubscriptionID == "gw-slow" {
				res.states = append(res.states, o.State)
				res.reasons = append(res.reasons, o.Reason)
			}
		}
		// 心跳与数据穿插：发布侧必须始终拿得到回执。
		hr, err := gw.Publish(ctx, "hb.beat", []byte("ping"))
		if err != nil {
			t.Fatalf("heartbeat during backlog: %v", err)
		}
		if len(hr.Outcomes) != 1 || hr.Outcomes[0].State != DeliveryStateNoSubscriber {
			t.Fatalf("heartbeat outcome=%+v", hr.Outcomes)
		}
		// 别的客户端必须及时收到本条（不被慢订阅拖慢）。
		select {
		case <-otherCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("other client slowed by mixed-load backlog at %s", payload)
		}
	}

	// 阶段 1：发满在途窗口（a-d）。
	for i, p := range []string{"a", "b", "c", "d"} {
		publishData(i, p)
	}
	sys.logs.waitForN(t, "subscription=gw-slow DELIVER seq=", window)
	// 阶段 2：队列积满（e-h）；期间注入 v1 旧格式帧到快订阅。
	for i, p := range []string{"e", "f", "g", "h"} {
		publishData(4+i, p)
		if i == 1 {
			publishV1("legacy-hb-1", "ctl.v1", "v1-report-1")
		}
	}
	publishV1("legacy-hb-2", "ctl.v1", "v1-report-2")
	// 阶段 3：触顶处置（z）。
	publishData(8, "z")
	sys.logs.waitFor(t, "WINDOW-FULL")

	// 新旧版本穿插的判定依据必须出现在流水里。
	sys.logs.waitFor(t, "client=gw frame type=3 PROCESS with ignored future fields")
	sys.logs.waitFor(t, "client gw] frame type=4 PROCESS with ignored future fields")

	// 放行慢订阅，收集最终被处理的消息序列。
	close(gate.Load().(chan struct{}))
	sys.logs.waitForN(t, "HANDLER OK sub=gw-slow", 8)
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := len(processed)
		mu.Unlock()
		if n == 8 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("slow subscription processed %d messages, want 8", n)
		case <-time.After(10 * time.Millisecond):
		}
	}
	mu.Lock()
	res.retained = append([]string(nil), processed...)
	mu.Unlock()
	ctlMu.Lock()
	res.ctlGot = append([]string(nil), ctlGot...)
	ctlMu.Unlock()
	res.otherN = 9 // 每条数据发布都已逐条等到 other 收到
	return res
}
