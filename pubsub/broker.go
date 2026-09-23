package pubsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	defaultSubQueueCap = 128

	// 事件 Kind 常量，单测日志据此打印投递流水与判定依据。
	EvSubscribed   = "subscribed"
	EvReconnected  = "reconnected"
	EvEnqueued     = "enqueued"
	EvDelivered    = "delivered"
	EvAcked        = "acked"
	EvNacked       = "nacked"
	EvRedeliver    = "redeliver"
	EvRejectFull   = "reject_queue_full"
	EvDropOldest   = "drop_oldest"
	EvDisconnected = "disconnected"
	EvFrameIgnored = "frame_ignored"
	EvFrameReject  = "frame_rejected"
	EvDuplicate    = "duplicate_skipped"
)

// Broker 是本地代理：终结传输服务侧、维护持久订阅状态、
// 执行过滤、顺序投递、重连补投与背压策略，并记录可解释的判定流水。
type Broker interface {
	Start(ctx context.Context) error
	Close() error
	// Publish 经由代理发布一条消息；返回各匹配订阅的确定性处置结论。
	Publish(ctx context.Context, m Message) (*PublishResult, error)
	// Journal 返回判定流水快照（投递/确认/重投/背压/丢弃）。
	Journal() []Event
}

// BrokerOption 配置 Broker。
type BrokerOption func(*localBroker)

// WithBrokerLogger 将流水同时写入给定 Writer（便于单测打印投递流水）。
func WithBrokerLogger(w io.Writer) BrokerOption {
	return func(b *localBroker) { b.log.out = w }
}

// NewBroker 在给定本地交换中枢上创建代理。
func NewBroker(sw *localSwitch, codec Codec, cfg BrokerConfig, opts ...BrokerOption) Broker {
	if cfg.SubQueueCapacity <= 0 {
		cfg.SubQueueCapacity = defaultSubQueueCap
	}
	b := &localBroker{sw: sw, codec: codec, cfg: cfg, subs: make(map[string]*subscription)}
	for _, o := range opts {
		o(b)
	}
	return b
}

// outEntry 是某订阅队列中的一个消息条目。
type outEntry struct {
	msg     Message
	attempt int // 首次投递为 1
}

// subscription 是一个持久订阅方的服务端状态（断连后保留）。
//
// 队列模型：单个 inflight 槽（任一时点最多一条在途，保证顺序）+ FIFO pending。
// 积压 = (inflight!=nil) + len(pending)，容量上限同时约束两者。
// 降级丢弃只允许作用于 pending，绝不摘除正在投递的 inflight。
type subscription struct {
	id      string
	pattern string
	filter  Filter

	pending  []*outEntry // 待投递（含重投）FIFO
	inflight *outEntry   // 已投递未确认的唯一条目；nil 表示无在途

	conn   Conn
	online bool
	gen    int64 // 连接代号：每次（重新）接入递增，泵只服务自己那一代
	wake   chan struct{}
}

func (s *subscription) backlog() int {
	n := len(s.pending)
	if s.inflight != nil {
		n++
	}
	return n
}

// localBroker 是 Broker 的进程内实现。
type localBroker struct {
	sw    *localSwitch
	codec Codec
	cfg   BrokerConfig
	log   eventLog

	mu     sync.Mutex
	subs   map[string]*subscription
	closed bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (b *localBroker) Start(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.wg.Add(1)
	go b.acceptLoop(b.ctx)
	return nil
}

func (b *localBroker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := append([]*subscription(nil), mapValues(b.subs)...)
	b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	for _, s := range subs {
		b.mu.Lock()
		c := s.conn
		b.mu.Unlock()
		if c != nil {
			_ = c.Close()
		}
	}
	return nil
}

func mapValues(m map[string]*subscription) []*subscription {
	out := make([]*subscription, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	return out
}

// acceptLoop 接收传输上新建立（或重连）的逻辑连接。
func (b *localBroker) acceptLoop(ctx context.Context) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.sw.acceptWait():
			for _, c := range b.sw.acceptAll() {
				conns := c
				b.wg.Add(1)
				go b.serveConn(ctx, conns)
			}
		}
	}
}

// serveConn 处理一条连接上的所有入帧（订阅/确认/拒绝），连接断开则复位投递。
func (b *localBroker) serveConn(ctx context.Context, c Conn) {
	defer b.wg.Done()
	var sub *subscription
	defer func() {
		if sub != nil {
			b.handleDisconnect(sub)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.Done():
			return
		case raw, ok := <-c.Receive():
			if !ok {
				return
			}
			f, err := b.codec.Decode(raw)
			if err != nil {
				de := AsDecision(err)
				reason := ""
				if de != nil {
					reason = string(de.Reason)
				}
				// 损坏/不支持：必须拒绝且结论可区分；记录流水，不影响其他连接。
				b.log.record(EvFrameReject, c.ID(), "", "", fmt.Sprintf("decode reject reason=%s err=%v", reason, err))
				continue
			}
			if len(f.UnknownTags) > 0 {
				b.log.record(EvFrameIgnored, c.ID(), "", "", fmt.Sprintf("unknown tags safely skipped=%v", f.UnknownTags))
			}
			switch f.Kind {
			case FrameSubscribe:
				sub = b.handleSubscribe(c, f)
			case FrameAck:
				b.handleAck(c.ID(), f)
			case FrameNack:
				b.handleNack(c.ID(), f)
			case FrameHeartbeat:
				// 保活：可安全忽略。
			default:
				b.log.record(EvFrameReject, c.ID(), "", "", "unsolicited frame kind on server side")
			}
		}
	}
}

// handleSubscribe 建立或重新接入一个持久订阅。
func (b *localBroker) handleSubscribe(c Conn, f *Frame) *subscription {
	id := c.ID()
	pattern := f.Pattern
	b.mu.Lock()
	s, ok := b.subs[id]
	if !ok {
		s = &subscription{id: id, pattern: pattern, filter: NewFilter(pattern), wake: make(chan struct{}, 1)}
		b.subs[id] = s
		b.log.record(EvSubscribed, id, "", "", fmt.Sprintf("pattern=%q durable created", pattern))
	} else {
		b.log.record(EvReconnected, id, "", "", fmt.Sprintf("pattern=%q resume; backlog=%d", s.pattern, s.backlog()))
	}
	s.conn = c
	s.online = true
	s.gen++
	gen := s.gen
	b.wg.Add(1)
	go b.pump(b.ctx, s, gen)
	b.mu.Unlock()
	b.signal(s)
	return s
}

// handleDisconnect 在连接断开时把在途消息退回 pending 队头（保序补投）。
func (b *localBroker) handleDisconnect(s *subscription) {
	b.mu.Lock()
	c := s.conn
	if c != nil {
		_ = c.Close()
	}
	reset := 0
	if s.inflight != nil {
		// 回到队头：其 attempt 保留，重投时 Redelivered=true；顺序仍先于其余 pending。
		s.pending = append([]*outEntry{s.inflight}, s.pending...)
		s.inflight = nil
		reset = 1
	}
	s.online = false
	s.conn = nil
	b.signalLocked(s)
	b.mu.Unlock()
	b.log.record(EvDisconnected, s.id, "", "",
		fmt.Sprintf("inflight returned to head=%d; durable retained=%d", reset, len(s.pending)))
}

// handleAck 确认在途消息：仅当该 id 仍是当前 inflight 时移除。
// 重复 ACK、迟到 ACK（已被降级丢弃或已结算）一律幂等、安全忽略。
func (b *localBroker) handleAck(subID string, f *Frame) {
	b.mu.Lock()
	s, ok := b.subs[subID]
	if !ok {
		b.mu.Unlock()
		b.log.record(EvAcked, subID, f.AckID, "", "reject ack: subscription unknown")
		return
	}
	if s.inflight != nil && s.inflight.msg.ID == f.AckID {
		s.inflight = nil
		b.mu.Unlock()
		b.log.record(EvAcked, subID, f.AckID, "", "inflight settled and removed")
		b.signal(s)
		return
	}
	// 也可能在 pending 中（例如旧连接的迟到 ACK，对应消息尚未重投）——
	// pending 消息未确认，ACK 不应将其移除，故仅当不在任何队列才算重复并忽略。
	if findPending(s.pending, f.AckID) != nil {
		b.mu.Unlock()
		b.log.record(EvAcked, subID, f.AckID, "", "late ack for not-yet-redelivered message; ignored (remains pending)")
		return
	}
	b.mu.Unlock()
	b.log.record(EvAcked, subID, f.AckID, "", "idempotent ack: already settled/dropped, ignored")
}

// handleNack 拒绝在途消息：退回 pending 队头，稍后按原顺序重投。
func (b *localBroker) handleNack(subID string, f *Frame) {
	b.mu.Lock()
	s, ok := b.subs[subID]
	if !ok {
		b.mu.Unlock()
		return
	}
	if s.inflight != nil && s.inflight.msg.ID == f.AckID {
		s.pending = append([]*outEntry{s.inflight}, s.pending...)
		s.inflight = nil
	}
	b.mu.Unlock()
	b.log.record(EvNacked, subID, f.AckID, "", "returned to head; will redeliver in order")
	b.signal(s)
}

// pump 是每个订阅、每代连接一个的顺序投递泵：任一时点最多一条在途。
func (b *localBroker) pump(ctx context.Context, s *subscription, gen int64) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		b.mu.Lock()
		if b.closed || s.gen != gen {
			b.mu.Unlock()
			return
		}
		conn := s.conn
		var head *outEntry
		if s.online && s.inflight == nil && len(s.pending) > 0 {
			head = s.pending[0]
			s.inflight = head
			s.pending = s.pending[1:]
		}
		if head == nil {
			w := s.wake
			var done <-chan struct{}
			if conn != nil {
				done = conn.Done()
			}
			curGen := s.gen
			b.mu.Unlock()
			if curGen != gen {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-done:
				continue // 连接断开：由 serveConn 复位 + 新连接的泵接管
			case <-w:
				continue
			}
		}
		head.attempt++
		attempt := head.attempt
		redelivered := attempt > 1
		m := head.msg
		b.mu.Unlock()

		raw, err := b.codec.Encode(&Frame{
			Kind: FrameDeliver, SubID: s.id, Attempt: attempt,
			Redelivered: redelivered, Message: &m,
		})
		if err != nil {
			b.returnToHead(s, head)
			b.log.record(EvFrameReject, s.id, m.ID, m.Topic, "encode failed: "+err.Error())
			if sleepCancelled(ctx, 20*time.Millisecond) {
				return
			}
			b.signal(s)
			continue
		}
		kind, note := EvDelivered, fmt.Sprintf("attempt=%d in-order", attempt)
		if redelivered {
			kind, note = EvRedeliver, fmt.Sprintf("attempt=%d redelivered after disconnect/nack in-order", attempt)
		}
		b.log.record(kind, s.id, m.ID, m.Topic, note)

		// 非阻塞发送：连接缓冲满不卡死泵；失败则退回队头稍后重试，不丢消息。
		if conn == nil || conn.Send(raw) != nil {
			b.returnToHead(s, head)
			b.log.record(EvFrameReject, s.id, m.ID, m.Topic, "transport not ready; returned to pending head")
			if sleepCancelled(ctx, 10*time.Millisecond) {
				return
			}
			b.signal(s)
			continue
		}

		// 顺序闸门：等待该在途消息被 ACK/NACK 或连接关闭，才投递下一条。
		select {
		case <-ctx.Done():
			return
		case <-conn.Done():
			return
		case <-s.wake:
		}
	}
}

// returnToHead 在发送失败时把尚未真正在途的条目安全放回队头。
func (b *localBroker) returnToHead(s *subscription, e *outEntry) {
	b.mu.Lock()
	if s.inflight == e {
		s.inflight = nil
		s.pending = append([]*outEntry{e}, s.pending...)
		e.attempt--
	}
	b.mu.Unlock()
}

func sleepCancelled(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}

// Publish 向所有匹配的持久订阅入队；发布方不被慢订阅阻塞，也不彼此影响。
func (b *localBroker) Publish(ctx context.Context, m Message) (*PublishResult, error) {
	if m.ID == "" {
		return nil, errors.New("pubsub: message id required")
	}
	if m.PublishedAt.IsZero() {
		m.PublishedAt = time.Now()
	}
	res := &PublishResult{MessageID: m.ID}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errors.New("pubsub: broker closed")
	}
	matched := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		// 过滤规则在订阅时固定；匹配只取决于消息主题与该订阅 pattern。
		if s.filter.Match(m.Topic) {
			matched = append(matched, s)
		}
	}
	for _, s := range matched {
		tr := TargetResult{SubID: s.id}
		if s.backlog() >= b.cfg.SubQueueCapacity {
			switch b.cfg.Overflow {
			case OverflowDropOldest:
				// 只丢弃 pending 队头（最旧的“未在途未确认”消息）；
				// 若积压仅由 inflight 构成（无 pending 可丢），退化为拒绝，原因仍可区分。
				if len(s.pending) > 0 {
					victim := s.pending[0]
					s.pending = s.pending[1:]
					tr.Reason = ReasonDroppedOldest
					tr.DroppedID = victim.msg.ID
					b.log.record(EvDropOldest, s.id, victim.msg.ID, victim.msg.Topic,
						fmt.Sprintf("backlog=%d cap=%d drop oldest pending to admit %s", s.backlog()+1, b.cfg.SubQueueCapacity, m.ID))
				} else {
					tr.Reason = ReasonQueueFullRejected
					b.log.record(EvRejectFull, s.id, m.ID, m.Topic,
						"full by inflight only; no pending to drop, reject")
				}
			default:
				tr.Reason = ReasonQueueFullRejected
				b.log.record(EvRejectFull, s.id, m.ID, m.Topic,
					fmt.Sprintf("backlog=%d cap=%d reject policy; publisher not blocked", s.backlog(), b.cfg.SubQueueCapacity))
			}
		}
		if tr.Reason == "" || tr.Reason == ReasonDroppedOldest {
			s.pending = append(s.pending, &outEntry{msg: m})
			if tr.Reason == "" {
				tr.Reason = ReasonOK
				res.Accepted++
			}
			b.log.record(EvEnqueued, s.id, m.ID, m.Topic,
				fmt.Sprintf("backlog=%d filter=%q", s.backlog(), s.pattern))
		}
		res.Targets = append(res.Targets, tr)
		b.signalLocked(s)
	}
	b.mu.Unlock()
	return res, nil
}

// ---- 小工具 ----

func (b *localBroker) signal(s *subscription) {
	b.mu.Lock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	b.mu.Unlock()
}

func (b *localBroker) signalLocked(s *subscription) {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func findPending(q []*outEntry, id string) *outEntry {
	for _, e := range q {
		if e.msg.ID == id {
			return e
		}
	}
	return nil
}

// serverConn 为传输服务侧连接的别名。
type serverConn = Conn

func (b *localBroker) Journal() []Event { return b.log.snapshot() }

const defaultJournalCap = 4096

// eventLog 为有界判定流水（环形覆盖最旧条目），可选镜像到 io.Writer，
// 便于单测打印完整投递流水与每条判定依据。
type eventLog struct {
	mu  sync.Mutex
	evs []Event
	out io.Writer
	max int
}

func (l *eventLog) record(kind, subID, msgID, topic, detail string) {
	ev := Event{Time: time.Now(), Kind: kind, SubID: subID, MsgID: msgID, Topic: topic, Detail: detail}
	l.mu.Lock()
	if l.max <= 0 {
		l.max = defaultJournalCap
	}
	if len(l.evs) >= l.max {
		l.evs = l.evs[1:]
	}
	l.evs = append(l.evs, ev)
	w := l.out
	l.mu.Unlock()
	if w != nil {
		fmt.Fprintf(w, "[%s] %-18s sub=%-6s msg=%-6s topic=%-10s %s\n",
			ev.Time.Format("15:04:05.000"), ev.Kind, ev.SubID, ev.MsgID, ev.Topic, ev.Detail)
	}
}

func (l *eventLog) snapshot() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.evs))
	copy(out, l.evs)
	return out
}
