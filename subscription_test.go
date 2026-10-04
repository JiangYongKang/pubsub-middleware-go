package pubsub

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.>", "a.b.c", true},
		{"a.>", "a", true}, // '>' 匹配零个或多个后续节
		{">", "anything.here", true},
		{"a.>.>", "a.b.c", false}, // 非法规则：'>' 必须在末尾
		{"", "a", false},
		{"a.b", "a", false},
		{"a.b", "a.b.c", false},
	}
	for _, c := range cases {
		if got := MatchTopic(c.filter, c.topic); got != c.want {
			t.Errorf("MatchTopic(%q,%q)=%v want %v", c.filter, c.topic, got, c.want)
		}
	}
}

// RejectNewest：积压达到容量后新消息失败，原因可区分，且消息不入队、顺序不乱。
func TestOverflowRejectNewest(t *testing.T) {
	s := newSubscription("s1", "c1", "t", 3, OverflowRejectNewest, 64)
	s.attach(&recordingConn{})
	var rejected []string
	for i := 0; i < 5; i++ {
		o := s.enqueue(msgID(i), "t", nil)
		if o.State == DeliveryStateRejected {
			if o.Reason != ReasonBacklogFullRejectNewest {
				t.Fatalf("reason=%s", o.Reason)
			}
			rejected = append(rejected, msgID(i))
		}
	}
	if s.backlog() != 3 {
		t.Fatalf("backlog=%d want 3", s.backlog())
	}
	if len(rejected) != 2 {
		t.Fatalf("rejected=%v want 2 messages", rejected)
	}
	// 前三条按序取出。
	for i := 0; i < 3; i++ {
		m, attempt := takeNext(s)
		if m == nil || attempt != 1 || m.messageID != msgID(i) {
			t.Fatalf("take %d = %+v attempt=%d", i, m, attempt)
		}
	}
	if m, _ := takeNext(s); m != nil {
		t.Fatal("queue must be empty")
	}
}

// DropOldest：积压达到容量后丢弃最旧未发送消息，新消息入队，降级原因可区分。
func TestOverflowDropOldest(t *testing.T) {
	s := newSubscription("s1", "c1", "t", 3, OverflowDropOldest, 64)
	s.attach(&recordingConn{})
	var degraded []string
	for i := 0; i < 5; i++ {
		o := s.enqueue(msgID(i), "t", nil)
		if o.State == DeliveryStateDegraded {
			if o.Reason != ReasonSlowConsumerDropOldest {
				t.Fatalf("reason=%s", o.Reason)
			}
			degraded = append(degraded, msgID(i))
		} else if o.State != DeliveryStateQueued {
			t.Fatalf("msg %d state=%d", i, o.State)
		}
	}
	if s.backlog() != 3 {
		t.Fatalf("backlog=%d want 3", s.backlog())
	}
	if len(degraded) != 2 || degraded[0] != msgID(3) || degraded[1] != msgID(4) {
		t.Fatalf("degraded=%v", degraded)
	}
	// 容量内剩余的必须是最新三条 m2,m3,m4，且严格按序。
	for i, want := range []int{2, 3, 4} {
		m, _ := takeNext(s)
		if m == nil || m.messageID != msgID(want) {
			t.Fatalf("take %d = %+v want m%d", i, m, want)
		}
	}
}

// 断连后在途消息按原顺序归还队首：先投递 m1,m2（未确认），m3 在队列中，
// 断连后恢复，补投顺序必须仍是 m1,m2,m3。
func TestOfflineInflightReturnedInOrder(t *testing.T) {
	s := newSubscription("s1", "c1", "t", 10, OverflowRejectNewest, 64)
	for i := 0; i < 3; i++ {
		if o := s.enqueue(msgID(i), "t", nil); o.State != DeliveryStateQueued {
			t.Fatalf("enqueue %d: %+v", i, o)
		}
	}
	fakeConn := &recordingConn{}
	s.attach(fakeConn)
	first, _ := takeNext(s)
	second, _ := takeNext(s)
	if first.messageID != msgID(0) || second.messageID != msgID(1) {
		t.Fatal("initial order broken")
	}
	// m2 已确认，m1 未确认：断连后只有 m1 回到队首，且排在队列中的 m3 之前。
	s.ack(second.seq)
	if !s.goOffline(fakeConn) {
		t.Fatal("goOffline must apply to attached conn")
	}
	if m, _ := takeNext(s); m != nil {
		t.Fatal("must not deliver while offline")
	}
	newConn := &recordingConn{}
	s.attach(newConn)
	var order []string
	for {
		m, attempt := takeNext(s)
		if m == nil {
			break
		}
		if m.messageID == msgID(0) && attempt != 2 {
			t.Fatalf("redelivered m1 attempt=%d want 2", attempt)
		}
		order = append(order, m.messageID)
	}
	if len(order) != 2 || order[0] != msgID(0) || order[1] != msgID(2) {
		t.Fatalf("redelivery order=%v want [m0 m2]", order)
	}
	// 旧连接在重连后再失效不得影响新连接（无竞态）。
	if s.goOffline(fakeConn) {
		t.Fatal("stale conn must not detach resumed subscription")
	}
}

// ACK 幂等：重复/未知确认不产生错误状态。
func TestAckIdempotent(t *testing.T) {
	s := newSubscription("s1", "c1", "t", 3, OverflowRejectNewest, 64)
	s.attach(&recordingConn{})
	o := s.enqueue("m", "t", nil)
	if o.State != DeliveryStateQueued {
		t.Fatal(o.Reason)
	}
	m, _ := takeNext(s)
	s.ack(m.seq)
	s.ack(m.seq)
	s.ack(99999)
}

func msgID(i int) string {
	return "m" + string(rune('0'+i))
}

// takeNext 是测试辅助：模拟"非阻塞发送成功后"的出队转在途。
func takeNext(s *subscription) (*queuedMessage, uint16) {
	m := s.head()
	if m == nil {
		return nil, 0
	}
	return m, s.markInflight(m)
}

type recordingConn struct{ closed bool }

func (r *recordingConn) Send([]byte) error     { return nil }
func (r *recordingConn) Recv() ([]byte, error) { return nil, nil }
func (r *recordingConn) Close() error          { r.closed = true; return nil }
func (r *recordingConn) Done() <-chan struct{} { return nil }
func (r *recordingConn) ClientID() string      { return "recording" }
