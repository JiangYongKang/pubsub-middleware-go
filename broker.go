package pubsub

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// connRef 持有某客户端当前最新连接，供订阅投递循环在重连后取用。
type connRef struct {
	mu sync.Mutex
	c  Conn
}

func (r *connRef) load() Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.c
}

func (r *connRef) store(c Conn) {
	r.mu.Lock()
	r.c = c
	r.mu.Unlock()
}

// Broker 是本地代理：终结传输连接，维护持久订阅，
// 负责按序投递、ACK 清理、断连保留与重连补投。
//
// 关键不变量：
//   - 订阅以 durableID 标识，连接断开只令其离线，状态（队列 + 在途）保留；
//   - 每个订阅的投递循环独立运行，慢消费者只拖慢它自己，不影响发布方与其他订阅；
//   - 同一订阅内消息严格按 seq 升序投递，重连补投也不例外。
type Broker struct {
	mu        sync.Mutex
	subs      map[string]*subscription
	transport Transport
	capacity  int
	policy    OverflowPolicy
	logf      func(format string, args ...any)
	closed    bool
	closeCh   chan struct{}
	pubSeq    uint64
	wg        sync.WaitGroup
	conns     map[string]*connRef // clientID -> 当前连接（重连后指向新连接）
	liveConns map[Conn]struct{}   // 所有在途连接，Close 时统一关闭
}

// BrokerOption 配置代理。
type BrokerOption func(*Broker)

// WithBrokerCapacity 设置每个持久订阅的积压容量上限（默认 64）。
func WithBrokerCapacity(n int) BrokerOption {
	return func(b *Broker) {
		if n > 0 {
			b.capacity = n
		}
	}
}

// WithBrokerOverflowPolicy 设置积压超限时的处置策略（默认拒绝新消息）。
func WithBrokerOverflowPolicy(p OverflowPolicy) BrokerOption {
	return func(b *Broker) { b.policy = p }
}

// WithBrokerLogger 注入日志函数（测试中打印投递流水与判定依据）。
func WithBrokerLogger(f func(format string, args ...any)) BrokerOption {
	return func(b *Broker) { b.logf = f }
}

// NewBroker 创建代理。
func NewBroker(t Transport, opts ...BrokerOption) *Broker {
	b := &Broker{
		subs:      map[string]*subscription{},
		transport: t,
		capacity:  64,
		policy:    OverflowRejectNewest,
		logf:      func(string, ...any) {},
		closeCh:   make(chan struct{}),
		conns:     map[string]*connRef{},
		liveConns: map[Conn]struct{}{},
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

func (b *Broker) log(format string, args ...any) {
	b.logf("[broker] "+format, args...)
}

// Serve 接收并处理连接，直到 transport 关闭或 ctx 取消。
func (b *Broker) Serve(ctx context.Context) error {
	go func() {
		select {
		case <-ctx.Done():
			b.Close()
		case <-b.closeCh:
		}
	}()
	connCh := make(chan Conn, 4)
	acceptErr := make(chan error, 1)
	go func() {
		for {
			conn, err := b.transport.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			select {
			case connCh <- conn:
			case <-b.closeCh:
				_ = conn.Close()
				return
			}
		}
	}()
	for {
		select {
		case <-b.closeCh:
			b.wg.Wait()
			return nil
		case err := <-acceptErr:
			b.mu.Lock()
			closed := b.closed
			b.mu.Unlock()
			if ctx.Err() != nil || closed {
				b.wg.Wait()
				return nil
			}
			b.wg.Wait()
			return err
		case conn := <-connCh:
			b.wg.Add(1)
			go func(c Conn) {
				defer b.wg.Done()
				b.serveConn(ctx, c)
			}(conn)
		}
	}
}

// serveConn 处理单条连接上的命令，并在断开时让该连接上的全部订阅转离线。
func (b *Broker) serveConn(ctx context.Context, conn Conn) {
	clientID := conn.ClientID()
	if !b.trackConn(conn) {
		// Close 已发生但连接刚被 Accept：直接关闭退出，不做任何订阅处理。
		_ = conn.Close()
		return
	}
	b.bindConn(clientID, conn)
	var attached []*subscription
	detachAll := func(reason string) {
		for _, s := range attached {
			if s.goOffline(conn) {
				b.log("client=%s subscription=%s OFFLINE reason=%s (backlog=%d, inflight returned for redelivery)",
					clientID, s.id, reason, s.backlog())
			} else {
				b.log("client=%s subscription=%s stale-conn detach skipped (already resumed on new conn)",
					clientID, s.id)
			}
		}
		attached = nil
		b.untrackConn(conn)
		b.dropConn(clientID, conn)
	}
	defer detachAll("conn_closed")

	for {
		raw, err := conn.Recv()
		if err != nil {
			b.log("client=%s recv ended: %v", clientID, err)
			return
		}
		dr, derr := Decode(raw)
		if derr != nil {
			// 损坏/不兼容帧：结论必须拒绝。代理记录判定依据后关闭此连接，
			// 避免将半损坏流误判为正常消息。
			b.log("client=%s REJECT frame decision=%s reason=%s err=%v",
				clientID, dr.Decision, dr.Reason, derr)
			_ = conn.Close()
			return
		}
		f := dr.Frame
		if dr.Reason == ReasonUnknownFieldIgnored {
			b.log("client=%s frame type=%d PROCESS with ignored future fields tags=%v (forward compatible)",
				clientID, f.Type, dr.IgnoredFields)
		}

		switch f.Type {
		case FrameSubscribe:
			b.handleSubscribe(conn, clientID, &f, &attached)
		case FramePublish:
			b.handlePublish(conn, clientID, &f)
		case FrameAck:
			b.handleAck(clientID, &f)
		default:
			b.log("client=%s unexpected frame type=%d on this direction, ignore", clientID, f.Type)
		}
	}
}

// handleSubscribe 建立或恢复一个持久订阅，并为其启动（或复用）按序投递循环。
func (b *Broker) handleSubscribe(conn Conn, clientID string, f *Frame, attached *[]*subscription) {
	s, created := b.getOrCreateSubscription(f.SubscriptionID, clientID, f.Filter)
	// 恢复时以最新订阅请求中的过滤规则为准。
	s.mu.Lock()
	s.filter = f.Filter
	s.mu.Unlock()
	// 在当前连接上为该订阅打开独占数据通道：投递走数据通道，
	// 与发布回执等控制帧及其他订阅的投递互不阻塞。
	lane, err := conn.OpenLane(s.id)
	if err != nil {
		b.log("client=%s subscription=%s open lane failed: %v", clientID, s.id, err)
		return
	}
	// 绑定到当前连接：先附着再唤醒投递循环，确保补投走新连接与新数据通道。
	s.attach(conn, lane)
	*attached = append(*attached, s)
	if !created {
		b.log("client=%s subscription=%s RESUMED filter=%q backlog=%d", clientID, s.id, f.Filter, s.backlog())
	} else {
		b.log("client=%s subscription=%s CREATED filter=%q capacity=%d policy=%d",
			clientID, s.id, f.Filter, s.capacity, s.policy)
		b.wg.Add(1)
		go func(s *subscription) {
			defer b.wg.Done()
			b.deliverLoop(s)
		}(s)
	}
	ack := Frame{Type: FrameSubscribed, SubscriptionID: s.id, Filter: s.filter}
	b.sendFrame(conn, clientID, &ack)
}

// getOrCreateSubscription 返回持久订阅（重连用同一 durableID 即恢复同一状态）。
func (b *Broker) getOrCreateSubscription(id, clientID, filter string) (*subscription, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.subs[id]; ok {
		return s, false
	}
	s := newSubscription(id, clientID, filter, b.capacity, b.policy)
	b.subs[id] = s
	return s, true
}

func (b *Broker) bindConn(clientID string, conn Conn) {
	b.mu.Lock()
	ref := b.conns[clientID]
	if ref == nil {
		ref = &connRef{}
		b.conns[clientID] = ref
	}
	ref.store(conn)
	b.mu.Unlock()
}

func (b *Broker) trackConn(conn Conn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.liveConns[conn] = struct{}{}
	return true
}

func (b *Broker) untrackConn(conn Conn) {
	b.mu.Lock()
	delete(b.liveConns, conn)
	b.mu.Unlock()
}

func (b *Broker) dropConn(clientID string, conn Conn) {
	b.mu.Lock()
	ref := b.conns[clientID]
	b.mu.Unlock()
	if ref != nil && ref.load() == conn {
		ref.store(nil)
	}
}

// deliverLoop 每个订阅一个 goroutine：仅在在线且有积压时取队首投递，
// 绝不阻塞发布路径。两种发送失败分开处理：
//   - 对端接收缓冲满（慢消费者）：消息归还队首，稍后重试，不丢不乱序；
//   - 连接断开：消息留在在途集合，由 detachAll 统一归还，等重连补投。
func (b *Broker) deliverLoop(s *subscription) {
	// 初始为"已停止"的定时器；需要重试时 Reset。
	retry := time.NewTimer(time.Hour)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	hasRetry := false
	scheduleRetry := func() {
		if !hasRetry {
			retry.Reset(5 * time.Millisecond)
			hasRetry = true
		}
	}
	clearRetry := func() {
		if hasRetry {
			if !retry.Stop() {
				select {
				case <-retry.C:
				default:
				}
			}
			hasRetry = false
		}
	}
	for {
		select {
		case <-b.closeCh:
			return
		case <-s.notifyCh():
		case <-retry.C:
		}
		clearRetry()
		for {
			lane := s.currentLane()
			if lane == nil {
				break // 已离线，等待重连 attach 的唤醒
			}
			msg := s.head()
			if msg == nil {
				break
			}
			// attempt 取下一条在途将记录的次数（markInflight 会 +1）；
			// 这里先按"即将成为的尝试次数"填入帧。
			nextAttempt := s.previewNextAttempt(msg)
			df := Frame{
				Type:           FrameDeliver,
				SubscriptionID: s.id,
				MessageID:      msg.messageID,
				Topic:          msg.topic,
				Payload:        msg.payload,
				Attempt:        nextAttempt,
			}
			df.PutSeq(msg.seq)
			// 仅投递到本订阅独占的数据通道：只有真正写进对端，才把消息移出队列
			// 转入在途。该通道满只说明这一个订阅消费慢，不触碰控制通道与其他订阅。
			raw, err := df.Encode()
			if err != nil {
				b.log("subscription=%s encode failed seq=%d: %v", s.id, msg.seq, err)
				break
			}
			if err := lane.Send(raw); err != nil {
				if isPeerBufferFull(err) {
					// 慢消费者：消息仍在队列里（未转在途、未丢失、顺序不变），稍后重试。
					b.log("subscription=%s PEER-BUFFER-FULL seq=%d msg=%s (publisher & control lane unaffected, backlog=%d, retry)",
						s.id, msg.seq, msg.messageID, s.backlog())
					scheduleRetry()
					break
				}
				b.log("subscription=%s DELIVER FAILED seq=%d msg=%s err=%v (will redeliver after resume)",
					s.id, msg.seq, msg.messageID, err)
				break
			}
			attempt := s.markInflight(msg)
			if attempt == 0 {
				// 发送期间订阅状态变化（如离线）：消息可能已被 detach 归还，
				// 本次帧成为多余投递（消费端去重会安全丢弃），停止本轮。
				break
			}
			b.log("subscription=%s DELIVER seq=%d msg=%s topic=%s attempt=%d (backlog=%d)",
				s.id, msg.seq, msg.messageID, msg.topic, attempt, s.backlog())
		}
	}
}

func isPeerBufferFull(err error) bool {
	var e *Error
	if asBrokerError(err, &e) {
		return e.Reason == ReasonBacklogFullRejectNewest
	}
	return false
}

func asBrokerError(err error, target **Error) bool {
	for err != nil {
		if e, ok := err.(*Error); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
			continue
		}
		return false
	}
	return false
}

// handleAck 幂等确认。
func (b *Broker) handleAck(clientID string, f *Frame) {
	s, ok := b.lookupSubscription(f.SubscriptionID)
	if !ok {
		b.log("client=%s ACK unknown subscription=%s msg=%s ignore", clientID, f.SubscriptionID, f.MessageID)
		return
	}
	seq, hasSeq := f.Seq()
	if !hasSeq {
		b.log("client=%s ACK without seq subscription=%s msg=%s ignore", clientID, s.id, f.MessageID)
		return
	}
	s.ack(seq)
	b.log("subscription=%s ACK seq=%d msg=%s from=%s", s.id, seq, f.MessageID, clientID)
}

func (b *Broker) lookupSubscription(id string) (*subscription, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.subs[id]
	return s, ok
}

// handlePublish 将消息路由到全部主题匹配的持久订阅，并把每个订阅的
// 入队 / 失败（拒绝）/ 降级（丢旧）结论通过 PublishAck 回送给发布方。
func (b *Broker) handlePublish(conn Conn, clientID string, f *Frame) {
	messageID := f.MessageID
	if messageID == "" {
		b.mu.Lock()
		b.pubSeq++
		messageID = "m" + strconv.FormatUint(b.pubSeq, 10)
		b.mu.Unlock()
	}
	b.mu.Lock()
	snap := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		snap = append(snap, s)
	}
	b.mu.Unlock()

	outcomes := make([]DeliveryOutcome, 0, len(snap))
	matched := 0
	for _, s := range snap {
		s.mu.Lock()
		filter := s.filter
		s.mu.Unlock()
		if !MatchTopic(filter, f.Topic) {
			continue
		}
		matched++
		out := s.enqueue(messageID, f.Topic, f.Payload)
		outcomes = append(outcomes, out)
		switch out.State {
		case DeliveryStateQueued:
			b.log("PUBLISH msg=%s topic=%s -> subscription=%s QUEUED (backlog=%d)",
				messageID, f.Topic, s.id, s.backlog())
		case DeliveryStateRejected:
			b.log("PUBLISH msg=%s topic=%s -> subscription=%s REJECT reason=%s (publisher NOT blocked, other subs unaffected)",
				messageID, f.Topic, s.id, out.Reason)
		case DeliveryStateDegraded:
			b.log("PUBLISH msg=%s topic=%s -> subscription=%s DEGRADED reason=%s (oldest pending dropped, msg=%s enqueued)",
				messageID, f.Topic, s.id, out.Reason, messageID)
		case DeliveryStateClosed:
			b.log("PUBLISH msg=%s topic=%s -> subscription=%s CLOSED reason=%s",
				messageID, f.Topic, s.id, out.Reason)
		}
	}
	if matched == 0 {
		outcomes = append(outcomes, DeliveryOutcome{State: DeliveryStateNoSubscriber, Reason: ReasonNone})
		b.log("PUBLISH msg=%s topic=%s NO SUBSCRIBER", messageID, f.Topic)
	}

	ack := Frame{
		Type:      FramePublishAck,
		MessageID: messageID,
		Outcomes:  outcomes,
	}
	if err := b.sendFrame(conn, clientID, &ack); err != nil {
		b.log("PUBLISH ack to=%s msg=%s failed: %v", clientID, messageID, err)
	}
}

// sendFrame 非阻塞语义的发送：单次帧写入失败即返回，绝不阻塞发布路径。
func (b *Broker) sendFrame(conn Conn, clientID string, f *Frame) error {
	raw, err := f.Encode()
	if err != nil {
		return err
	}
	if err := conn.Send(raw); err != nil {
		return err
	}
	return nil
}

// Close 停止代理并关闭全部在途连接（持久订阅状态随对象释放）。
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.closeCh)
	for _, s := range b.subs {
		s.markClosed()
	}
	live := make([]Conn, 0, len(b.liveConns))
	for c := range b.liveConns {
		live = append(live, c)
	}
	b.mu.Unlock()
	for _, c := range live {
		_ = c.Close()
	}
	return nil
}
