package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Message 是投递给消费端的消息。Attempt>1 表示这是一次重投。
type Message struct {
	SubscriptionID string
	MessageID      string
	Topic          string
	Payload        []byte
	Attempt        int
}

// Handler 处理一条消息；返回 nil 后客户端自动发送 ACK。
// 重复投递在进入 Handler 之前就会被识别并安全丢弃（自动 ACK），Handler 不会被重复调用。
type Handler func(ctx context.Context, msg *Message) error

// PublishReceipt 是发布回执：消息 ID 及对每个匹配订阅的处置结论。
type PublishReceipt struct {
	MessageID string
	Outcomes  []DeliveryOutcome
}

// 去重环最大保留的已处理消息 ID 数。重投消息由 (subscriptionID, messageID) 识别。
const defaultDedupSize = 4096

// 每个订阅本地投递通道的默认容量；测试可调小以更快把背压传导回代理队列。
const defaultDeliverBuffer = 128

type subReg struct {
	durableID string
	filter    string
	handler   Handler
}

// Client 是发布/订阅客户端：断线自动重连、重连后以同一持久订阅 ID 恢复，
// 并对重投消息做消费端去重，保证同一消息只被业务处理一次。
type Client struct {
	id          string
	transport   Transport
	logf        func(format string, args ...any)
	dedupSize   int
	deliverSize int

	mu        sync.Mutex
	closed    bool
	conn      Conn
	subs      []subReg
	seen      map[string]struct{}      // 键 subscriptionID + "/" + messageID
	seenRing  []string                 // LRU 风格的环，超出容量后淘汰最旧记录
	controlCh chan *Frame              // PublishAck/Subscribed 等控制帧，与投递路径相互独立
	ackCh     chan *Frame              // 待发送 ACK
	receipts  map[string]chan *Frame   // 键 messageID
	handlers  map[string]Handler       // durableID -> 业务处理器
	deliverCh map[string]chan *Frame   // durableID -> 该订阅独占的投递通道
	subReqCh  chan subReg              // 订阅请求统一由会话循环串行发送
	subAcks   map[string]chan struct{} // durableID -> SUBSCRIBED 回执通知
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// ClientOption 配置客户端。
type ClientOption func(*Client)

// WithClientLogger 注入日志函数。
func WithClientLogger(f func(format string, args ...any)) ClientOption {
	return func(c *Client) { c.logf = f }
}

// WithClientDedupSize 设置消费端去重环容量（默认 4096）。
func WithClientDedupSize(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.dedupSize = n
		}
	}
}

// WithClientDeliverBuffer 设置每个订阅本地投递通道容量（默认 128）。
// 调小可让消费速度更快地把背压传导回代理；正常使用无需修改。
func WithClientDeliverBuffer(n int) ClientOption {
	return func(c *Client) {
		if n >= 0 {
			c.deliverSize = n
		}
	}
}

// NewClient 创建客户端。
func NewClient(id string, t Transport, opts ...ClientOption) *Client {
	c := &Client{
		id:          id,
		transport:   t,
		logf:        func(string, ...any) {},
		dedupSize:   defaultDedupSize,
		deliverSize: defaultDeliverBuffer,
		seen:        map[string]struct{}{},
		controlCh:   make(chan *Frame, 64),
		ackCh:       make(chan *Frame, 128),
		receipts:    map[string]chan *Frame{},
		handlers:    map[string]Handler{},
		deliverCh:   map[string]chan *Frame{},
		subReqCh:    make(chan subReg, 32),
		subAcks:     map[string]chan struct{}{},
		closeCh:     make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) log(format string, args ...any) {
	c.logf("[client "+c.id+"] "+format, args...)
}

func newMessageID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "msg-" + hex.EncodeToString(b[:])
}

// Connect 建立连接并启动收发与自动重连循环。
func (c *Client) Connect(ctx context.Context) error {
	conn, err := c.dialLoop(ctx)
	if err != nil {
		return err
	}
	c.setConn(conn)
	c.wg.Add(3)
	go c.runSessionLoop(ctx, conn)
	go c.runSendLoop(ctx)
	go c.runDispatchLoop(ctx)
	return nil
}

func (c *Client) setConn(conn Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

func (c *Client) getConn() Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// dialLoop 持续尝试拨号直到成功、ctx 取消或客户端关闭（用于模拟断网恢复后的自动重连）。
func (c *Client) dialLoop(ctx context.Context) (Conn, error) {
	backoff := 10 * time.Millisecond
	for {
		conn, err := c.transport.Dial(c.id)
		if err == nil {
			return conn, nil
		}
		c.log("dial failed reason=%v, retry in %v", err, backoff)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closeCh:
			return nil, errors.New("client closed")
		case <-time.After(backoff):
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

// runSessionLoop 是每条连接的唯一管理者，串行完成三件事：
//  1. 从当前连接泵帧到 recvCh；
//  2. 连接建立后重放全部持久订阅（初次与重连路径完全一致）；
//  3. 处理 Subscribe 调用发来的订阅请求。
//
// 所有订阅帧都经由同一循环写入连接，因此不存在重复发送竞态；
// 连接断开后自动重连并再次注册，持久订阅在代理侧的状态不变。
func (c *Client) runSessionLoop(ctx context.Context, conn Conn) {
	defer c.wg.Done()
	pumpDone := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.pumpConn(ctx, conn)
		close(pumpDone)
	}()
	c.resendAllSubs(conn)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		case <-pumpDone:
			// 当前连接断开：等待链路恢复后重连，订阅状态由代理侧保留。
			c.log("connection DOWN, reconnecting (durable subscriptions retained broker-side)")
			nc, err := c.dialLoop(ctx)
			if err != nil {
				return
			}
			c.setConn(nc)
			c.log("reconnected, re-attaching durable subscriptions")
			conn = nc
			pumpDone = make(chan struct{})
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.pumpConn(ctx, conn)
				close(pumpDone)
			}()
			c.resendAllSubs(conn)
		case reg := <-c.subReqCh:
			c.mu.Lock()
			_, exists := c.handlers[reg.durableID]
			if !exists {
				c.subs = append(c.subs, reg)
				c.handlers[reg.durableID] = reg.handler
			}
			notify := c.subAcks[reg.durableID]
			c.mu.Unlock()
			if !exists {
				// 每个持久订阅一个独立处理 goroutine（首次订阅时建立一次，
				// 重连只重发订阅帧，不会再建）。
				c.startSubWorker(reg)
			}
			f := Frame{Type: FrameSubscribe, SubscriptionID: reg.durableID, Filter: reg.filter}
			if err := c.writeOn(conn, &f); err != nil {
				c.log("subscribe send failed sub=%s: %v (will be retried on reconnect)", reg.durableID, err)
			}
			if notify != nil {
				select {
				case <-notify:
				case <-ctx.Done():
				case <-c.closeCh:
				case <-time.After(2 * time.Second):
				}
				c.mu.Lock()
				delete(c.subAcks, reg.durableID)
				c.mu.Unlock()
			}
		}
	}
}

// resendAllSubs 在连接建立（或重连建立）后重放全部订阅；写失败不影响后续重连重试。
func (c *Client) resendAllSubs(conn Conn) {
	c.mu.Lock()
	subs := append([]subReg(nil), c.subs...)
	c.mu.Unlock()
	for _, s := range subs {
		f := Frame{Type: FrameSubscribe, SubscriptionID: s.durableID, Filter: s.filter}
		if err := c.writeOn(conn, &f); err != nil {
			c.log("resubscribe failed sub=%s: %v", s.durableID, err)
			return
		}
		c.log("SUBSCRIBE (re)sent durable=%s filter=%q", s.durableID, s.filter)
	}
}

func (c *Client) pumpConn(ctx context.Context, conn Conn) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-c.closeCh:
			_ = conn.Close()
		case <-stop:
		case <-ctx.Done():
			_ = conn.Close()
		}
	}()
	for {
		raw, err := conn.Recv()
		if err != nil {
			select {
			case <-c.closeCh:
				return
			default:
			}
			c.log("recv ended: %v", err)
			return
		}
		dr, derr := Decode(raw)
		if derr != nil {
			c.log("REJECT frame decision=%s reason=%s err=%v", dr.Decision, dr.Reason, derr)
			_ = conn.Close()
			return
		}
		f := dr.Frame
		if dr.Reason == ReasonUnknownFieldIgnored {
			c.log("frame type=%d PROCESS with ignored future fields tags=%v", f.Type, dr.IgnoredFields)
		}
		// 投递帧直接进入该订阅的独占通道：慢消费者会在此处自然形成背压，
		// 但只影响它自己；控制帧走独立 controlCh，不被投递阻塞。
		if f.Type == FrameDeliver {
			c.routeDeliver(&f)
			continue
		}
		select {
		case c.controlCh <- &f:
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		}
	}
}

// runSendLoop 是唯一向当前连接写业务帧的地方：ACK 串行发送，避免与 Publish 竞争写。
func (c *Client) runSendLoop(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		case f := <-c.ackCh:
			conn := c.getConn()
			if conn == nil {
				continue
			}
			if err := c.writeOn(conn, f); err != nil {
				c.log("ack send failed sub=%s msg=%s: %v (redelivery expected, dedup will make it safe)",
					f.SubscriptionID, f.MessageID, err)
			}
		}
	}
}

func (c *Client) writeOn(conn Conn, f *Frame) error {
	raw, err := f.Encode()
	if err != nil {
		return err
	}
	return conn.Send(raw)
}

// runDispatchLoop 只处理控制帧（发布回执、订阅确认），与投递路径完全隔离：
// 慢订阅只会阻塞它自己的接收泵与处理 worker，绝不影响发布回执的收取和
// Publish 调用对失败/降级原因的接收。
func (c *Client) runDispatchLoop(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		case f := <-c.controlCh:
			switch f.Type {
			case FramePublishAck:
				c.log("PUBLISH-ACK msg=%s outcomes=%s", f.MessageID, formatOutcomes(f.Outcomes))
				c.mu.Lock()
				ch := c.receipts[f.MessageID]
				c.mu.Unlock()
				if ch != nil {
					select {
					case ch <- f:
					default:
					}
				}
			case FrameSubscribed:
				c.log("SUBSCRIBED durable=%s filter=%q", f.SubscriptionID, f.Filter)
				c.mu.Lock()
				ch := c.subAcks[f.SubscriptionID]
				c.mu.Unlock()
				if ch != nil {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
			}
		}
	}
}

// routeDeliver 把投递放入订阅独占通道。慢消费者会阻塞在这里，从而把背压
// 沿传输传导回代理（代理非阻塞发送失败后保留消息重试）；它只阻塞本连接的
// 接收泵，不影响其他客户端，也不影响本客户端 PublishAck 所在的控制通道。
func (c *Client) routeDeliver(f *Frame) {
	c.mu.Lock()
	ch := c.deliverCh[f.SubscriptionID]
	c.mu.Unlock()
	if ch == nil {
		c.log("DELIVER sub=%s msg=%s no local subscription, skip", f.SubscriptionID, f.MessageID)
		return
	}
	select {
	case ch <- f:
	case <-c.closeCh:
	}
}

// startSubWorker 为一个持久订阅启动独立处理 goroutine：
// 串行处理本订阅消息（保证同订阅顺序），与其他订阅完全隔离。
func (c *Client) startSubWorker(reg subReg) {
	ch := make(chan *Frame, c.deliverSize)
	c.mu.Lock()
	c.deliverCh[reg.durableID] = ch
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.closeCh:
				return
			case f := <-ch:
				c.processOneDelivery(f, reg.handler)
			}
		}
	}()
}

// processOneDelivery 处理单条投递：先消费端去重，再调用 Handler，成功后自动 ACK。
// 重复投递被识别后仅补 ACK，绝不二次调用业务 Handler。
func (c *Client) processOneDelivery(f *Frame, h Handler) {
	ctx := context.Background()
	key := f.SubscriptionID + "/" + f.MessageID
	seq, _ := f.Seq()
	if !c.noteSeen(key) {
		// 重投且已成功处理过：安全丢弃，仅补一个幂等 ACK，杜绝重复处理。
		c.log("DELIVER DUP-DROP sub=%s seq=%d msg=%s attempt=%d -> auto ACK (already processed)",
			f.SubscriptionID, seq, f.MessageID, f.Attempt)
		c.queueAck(f)
		return
	}

	msg := &Message{
		SubscriptionID: f.SubscriptionID,
		MessageID:      f.MessageID,
		Topic:          f.Topic,
		Payload:        f.Payload,
		Attempt:        int(f.Attempt),
	}
	c.log("DELIVER RECEIVE sub=%s seq=%d msg=%s attempt=%d topic=%s",
		f.SubscriptionID, seq, f.MessageID, f.Attempt, f.Topic)
	if err := h(ctx, msg); err != nil {
		// 处理失败：回退去重标记并保持未确认，代理重连补投时会再次投递。
		c.forgetSeen(key)
		c.log("HANDLER ERROR sub=%s msg=%s err=%v (left unacked for redelivery)",
			f.SubscriptionID, f.MessageID, err)
		return
	}
	c.log("HANDLER OK sub=%s msg=%s -> ACK seq=%d", f.SubscriptionID, f.MessageID, seq)
	c.queueAck(f)
}

func formatOutcomes(oo []DeliveryOutcome) string {
	out := ""
	for i, o := range oo {
		if i > 0 {
			out += "; "
		}
		if o.SubscriptionID != "" {
			out += o.SubscriptionID + "="
		}
		out += deliveryStateName(o.State)
		if o.Reason != ReasonNone {
			out += "(" + string(o.Reason) + ")"
		}
	}
	return out
}

func deliveryStateName(s DeliveryState) string {
	switch s {
	case DeliveryStateQueued:
		return "queued"
	case DeliveryStateNoSubscriber:
		return "no_subscriber"
	case DeliveryStateRejected:
		return "rejected"
	case DeliveryStateDegraded:
		return "degraded"
	case DeliveryStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

func (c *Client) queueAck(f *Frame) {
	ack := &Frame{Type: FrameAck, SubscriptionID: f.SubscriptionID, MessageID: f.MessageID}
	if seq, ok := f.Seq(); ok {
		ack.PutSeq(seq)
	}
	select {
	case c.ackCh <- ack:
	default: // 极端情况下由重投兜底
	}
}

// Subscribe 建立（或恢复）一个持久订阅；durableID 相同即同一订阅。
// 请求交由会话循环串行发送，并等待代理的 SUBSCRIBED 确认（确认在重连后也会再次到来）。
func (c *Client) Subscribe(ctx context.Context, durableID, filter string, h Handler) error {
	notify := make(chan struct{}, 2)
	c.mu.Lock()
	if _, dup := c.handlers[durableID]; dup {
		c.mu.Unlock()
		return errors.New("duplicate subscription id: " + durableID)
	}
	c.subAcks[durableID] = notify
	c.mu.Unlock()

	select {
	case c.subReqCh <- subReg{durableID: durableID, filter: filter, handler: h}:
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.subAcks, durableID)
		c.mu.Unlock()
		return ctx.Err()
	}
	select {
	case <-notify:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Publish 发布一条消息，并等待代理回送带每个订阅失败/降级原因的回执。
// 发布本身不被慢消费者阻塞：积压处置在代理侧立即完成，结论随回执返回。
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) (*PublishReceipt, error) {
	conn := c.getConn()
	if conn == nil {
		return nil, errors.New("client not connected")
	}
	id := newMessageID()
	f := Frame{Type: FramePublish, MessageID: id, Topic: topic, Payload: payload}

	ch := make(chan *Frame, 1)
	c.mu.Lock()
	c.receipts[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.receipts, id)
		c.mu.Unlock()
	}()

	if err := c.writeOn(conn, &f); err != nil {
		return nil, err
	}
	c.log("PUBLISH msg=%s topic=%s", id, topic)
	select {
	case rf := <-ch:
		return &PublishReceipt{MessageID: rf.MessageID, Outcomes: rf.Outcomes}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// forgetSeen 回退一条尚未真正处理成功的去重记录，使后续重投能再次进入 Handler。
func (c *Client) forgetSeen(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[key]; !ok {
		return
	}
	delete(c.seen, key)
	for i, k := range c.seenRing {
		if k == key {
			c.seenRing = append(c.seenRing[:i], c.seenRing[i+1:]...)
			break
		}
	}
}

// Close 关闭客户端。
func (c *Client) noteSeen(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[key]; ok {
		return false
	}
	c.seen[key] = struct{}{}
	c.seenRing = append(c.seenRing, key)
	if len(c.seenRing) > c.dedupSize {
		oldest := c.seenRing[0]
		c.seenRing = c.seenRing[1:]
		delete(c.seen, oldest)
	}
	return true
}

// Close 关闭客户端：停止全部后台循环并关闭连接，返回后不再产生日志。
func (c *Client) Close() error {
	var conn Conn
	c.closeOnce.Do(func() {
		close(c.closeCh)
		c.mu.Lock()
		c.closed = true
		conn = c.conn
		c.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
	c.wg.Wait()
	return nil
}
