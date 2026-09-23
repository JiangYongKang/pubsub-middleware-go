package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 场景三A（OverflowRejectNew）：慢订阅积压达上限后，新消息对该订阅被拒绝，
// 原因=queue_full_rejected（失败，与降级原因可区分）；发布方不被阻塞，其他订阅不受影响。
func TestSlowConsumerRejectNew(t *testing.T) {
	slowCap := 4
	tr, sw := NewLocalTransport(64)
	br := NewBroker(sw, NewCodec(), BrokerConfig{SubQueueCapacity: slowCap, Overflow: OverflowRejectNew}, WithBrokerLogger(testLogger{t}))
	if err := br.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	ctx := context.Background()

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	slow := NewClient(tr, NewCodec(), WithClientBroker(br), WithClientLogger(testLogger{t}))
	defer slow.Close()
	if err := slow.Subscribe(ctx, "slow", "t/+", func(ctx context.Context, d Delivery) error {
		once.Do(func() { started.Done() })
		<-release // 第一条进入后停住：慢消费者
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 快订阅：大容量、立即 ACK。证明慢订阅不影响他人。
	fast := NewClient(tr, NewCodec(), WithClientBroker(br), WithClientLogger(testLogger{t}))
	defer fast.Close()
	var fastMu sync.Mutex
	fastGot := map[string]int{}
	if err := fast.Subscribe(ctx, "fast", "t/+", func(ctx context.Context, d Delivery) error {
		fastMu.Lock()
		fastGot[d.Message.ID]++
		fastMu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(time.Second, func() bool { return countSubscribed(br.Journal()) >= 2 }) {
		t.Fatal("subs not ready")
	}

	rejected := []string{}
	accepted := 0
	// 填满慢订阅（1 在途 + slowCap-1 待投递），再多发 2 条：对慢订阅必须拒绝。
	// 逐条小间隔发布：慢订阅首条被持有、永不排空，必然饱和；快订阅即时确认、逐条排空。
	for i := 0; i < slowCap+2; i++ {
		time.Sleep(15 * time.Millisecond)
		id := "p" + itoa(i)
		r, err := slow.Publish(ctx, Message{ID: id, Topic: "t/x"})
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range r.Targets {
			if x.SubID != "slow" {
				continue
			}
			switch x.Reason {
			case ReasonQueueFullRejected:
				rejected = append(rejected, id)
			case ReasonOK:
				accepted++
			default:
				t.Fatalf("unexpected reason %s for %s", x.Reason, id)
			}
		}
	}
	started.Wait()
	if accepted != slowCap || len(rejected) != 2 {
		t.Fatalf("slow: want %d accepted and 2 rejected, got accepted=%d rejected=%v", slowCap, accepted, rejected)
	}
	for _, id := range rejected {
		if !journalHas(br.Journal(), EvRejectFull, id) {
			t.Fatalf("missing reject_full decision for %s", id)
		}
	}

	// 发布方未被慢订阅阻塞：慢订阅仍卡住时，发布立刻返回（该条对慢订阅仍被拒绝）。
	done := make(chan error, 1)
	go func() {
		_, e := slow.Publish(ctx, Message{ID: "z9", Topic: "t/x"})
		done <- e
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher must not be blocked by slow consumer")
	}

	close(release) // 放开慢订阅
	// 快订阅收到全部 slowCap+3 条；慢订阅的饱和对它零影响。
	if !waitFor(2*time.Second, func() bool {
		fastMu.Lock()
		defer fastMu.Unlock()
		return len(fastGot) == slowCap+3
	}) {
		fastMu.Lock()
		t.Fatalf("fast subscriber must receive all messages regardless of slow one, got=%d", len(fastGot))
		fastMu.Unlock()
	}
	fastMu.Lock()
	for id := range fastGot {
		if fastGot[id] != 1 {
			fastMu.Unlock()
			t.Fatalf("fast subscriber duplicate processing for %s", id)
		}
	}
	fastMu.Unlock()
	t.Logf("slow rejected(full)=%v (reason=%s); fast received %d unique messages; publisher not blocked",
		rejected, ReasonQueueFullRejected, slowCap+3)
}

// 场景三B（OverflowDropOldest）：达上限后丢弃该订阅最旧的未确认消息，
// 原因=dropped_oldest（降级，携带 DroppedID），新消息入队；与失败原因可区分。
func TestSlowConsumerDropOldest(t *testing.T) {
	cap_ := 3
	tr, sw := NewLocalTransport(32)
	br := NewBroker(sw, NewCodec(), BrokerConfig{SubQueueCapacity: cap_, Overflow: OverflowDropOldest}, WithBrokerLogger(testLogger{t}))
	if err := br.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	ctx := context.Background()

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	got := []string{}
	var mu sync.Mutex
	slow := NewClient(tr, NewCodec(), WithClientBroker(br), WithClientLogger(testLogger{t}))
	defer slow.Close()
	if err := slow.Subscribe(ctx, "slow", "t/+", func(ctx context.Context, d Delivery) error {
		if d.Message.ID == "d0" {
			once.Do(func() { started.Done() })
			<-release
		}
		mu.Lock()
		got = append(got, d.Message.ID)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(time.Second, func() bool { return journalHas(br.Journal(), EvSubscribed, "") })

	// 先发 d0 并等待其进入“在途”（handler 持有 release 不返回），
	// 使积压形态确定为 1 inflight + 其余 pending。
	if _, err := slow.Publish(ctx, Message{ID: "d0", Topic: "t/x"}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return journalHas(br.Journal(), EvDelivered, "d0") }) {
		t.Fatal("d0 never became inflight")
	}
	started.Wait() // d0 的 handler 已进入并持有 release

	var drops []TargetResult
	for i := 1; i <= cap_; i++ { // d1,d2 填满，d3 触发丢弃最旧待投递 d1
		id := "d" + itoa(i)
		r, err := slow.Publish(ctx, Message{ID: id, Topic: "t/x"})
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range r.Targets {
			if x.Reason == ReasonDroppedOldest {
				drops = append(drops, x)
			}
			if x.Reason == ReasonQueueFullRejected {
				t.Fatalf("drop policy must never surface reject-full for %s", id)
			}
		}
	}
	if len(drops) != 1 || drops[0].DroppedID != "d1" {
		t.Fatalf("expected single drop of oldest pending d1, got %+v", drops)
	}
	if !journalHas(br.Journal(), EvDropOldest, "d1") {
		t.Fatal("missing drop_oldest decision for d1")
	}

	close(release)
	if !waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count(got, "d3") == 1
	}) {
		mu.Lock()
		t.Fatalf("did not drain: %v", got)
		mu.Unlock()
	}
	mu.Lock()
	defer mu.Unlock()
	if count(got, "d1") != 0 {
		t.Fatalf("victim d1 must not be delivered, got=%v", got)
	}
	if count(got, "d0") != 1 || count(got, "d2") != 1 || count(got, "d3") != 1 {
		t.Fatalf("survivors must each deliver once: %v", got)
	}
	if indexOf(got, "d0") > indexOf(got, "d2") || indexOf(got, "d2") > indexOf(got, "d3") {
		t.Fatalf("order among survivors must be preserved: %v", got)
	}
	t.Logf("degraded reason=%s victim=%s (distinguishable from %s); survivors in order=%v",
		ReasonDroppedOldest, "d1", ReasonQueueFullRejected, got)
}

// 失败原因与降级原因彼此可区分的直接断言。
func TestReasonsAreDistinguishable(t *testing.T) {
	distinct := []Reason{
		ReasonOK, ReasonQueueFullRejected, ReasonDroppedOldest,
		ReasonDuplicateDelivery, ReasonRedelivered,
		ReasonFrameTruncated, ReasonFrameBadMagic, ReasonFrameUnsupported,
		ReasonFrameChecksum, ReasonFrameUnknownTag,
	}
	seen := map[Reason]bool{}
	for _, r := range distinct {
		if seen[r] {
			t.Fatalf("duplicate reason code %s", r)
		}
		seen[r] = true
	}
}

func countSubscribed(evs []Event) int {
	n := 0
	for _, e := range evs {
		if e.Kind == EvSubscribed {
			n++
		}
	}
	return n
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}
