package pubsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Handler 处理一条投递。返回 nil 表示确认（ACK）；
// 返回非 nil 表示拒绝（NACK），该消息保持未确认并稍后/重连后按原顺序重投。
type Handler func(ctx context.Context, d Delivery) error

// Client 是订阅方客户端：建立/重建传输连接，
// 在消费端识别重复投递并安全丢弃（自动确认，绝不重复处理）。
type Client interface {
	io.Closer
	// Subscribe 以订阅方 id 和过滤规则建立持久订阅（重连同名 id 即重新接入）。
	Subscribe(ctx context.Context, id, pattern string, h Handler) error
	// Publish 经代理发布一条消息（返回每个匹配订阅的确定性处置结论）。
	Publish(ctx context.Context, m Message) (*PublishResult, error)
	// Reconnect 模拟短暂断连后以相同 id 重新接入，触发未确认消息按原过滤规则补投。
	Reconnect(ctx context.Context) error
	// Disconnect 模拟网络断开（不取消服务端持久订阅）。
	Disconnect() error
	// Journal 返回客户端侧判定流水（重复识别/确认/重连）。
	Journal() []Event
}

// ClientOption 配置 Client。
type ClientOption func(*localClient)

// WithClientBroker 让客户端经由本地代理发布（发布结论直接来自代理）。
func WithClientBroker(br Broker) ClientOption {
	return func(c *localClient) { c.br = br }
}

// WithClientLogger 将客户端流水同时写入给定 Writer。
func WithClientLogger(w io.Writer) ClientOption {
	return func(c *localClient) { c.log.out = w }
}

// defaultClientDedupeWindow 为消费端有界去重缓存默认条目数。
const defaultClientDedupeWindow = 256

// WithDedupeWindow 设置消费端有界去重缓存条目数（默认 256）。
func WithDedupeWindow(n int) ClientOption {
	return func(c *localClient) {
		if n > 0 {
			c.seenCap = n
		}
	}
}

// NewClient 在给定本地传输上创建订阅方客户端。
func NewClient(tr Transport, codec Codec, opts ...ClientOption) Client {
	c := &localClient{tr: tr, codec: codec, seen: make(map[string]int), seenCap: defaultClientDedupeWindow}
	for _, o := range opts {
		o(c)
	}
	return c
}

type localClient struct {
	tr    Transport
	codec Codec
	br    Broker
	log   eventLog

	mu      sync.Mutex
	id      string
	pattern string
	handler Handler
	conn    Conn
	closed  bool
	// seen 为有界 LRU 去重缓存：messageID -> 最近投递序号。
	seen     map[string]int
	seenKeys []string
	seenCap  int
	seq      int

	recvCancel context.CancelFunc
	recvWG     sync.WaitGroup

	// swallowAck 为非空时，按计数丢弃对某消息 ID 的 ACK（确定性模拟
	// “断连窗口内首次确认丢失”，重连后计数耗尽即恢复送达）。
	swallowAck map[string]int
}

func (c *localClient) Subscribe(ctx context.Context, id, pattern string, h Handler) error {
	if id == "" {
		return errors.New("pubsub: subscriber id required")
	}
	if h == nil {
		return errors.New("pubsub: handler required")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("pubsub: client closed")
	}
	c.id, c.pattern, c.handler = id, pattern, h
	c.mu.Unlock()
	return c.connect(ctx)
}

func (c *localClient) connect(ctx context.Context) error {
	c.mu.Lock()
	id, pattern := c.id, c.pattern
	old := c.conn
	c.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	conn, err := c.tr.Dial(ctx, id)
	if err != nil {
		return err
	}
	// 发送订阅帧；同一 id 重连即让代理按原过滤规则恢复并补投。
	sub := &Frame{Kind: FrameSubscribe, SubID: id, Pattern: pattern}
	raw, err := c.codec.Encode(sub)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := conn.Send(raw); err != nil {
		_ = conn.Close()
		return err
	}
	rctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.conn = conn
	c.recvCancel = cancel
	c.mu.Unlock()
	c.recvWG.Add(1)
	go c.receiveLoop(rctx, conn)
	return nil
}

func (c *localClient) receiveLoop(ctx context.Context, conn Conn) {
	defer c.recvWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-conn.Done():
			return
		case raw, ok := <-conn.Receive():
			if !ok {
				return
			}
			f, err := c.codec.Decode(raw)
			if err != nil {
				// 帧级别结论确定可区分：可忽略则忽略，必须拒绝则记录并丢弃该帧。
				de := AsDecision(err)
				reason := ""
				if de != nil {
					reason = string(de.Reason)
				}
				if IsIgnorable(err) {
					c.log.record(EvFrameIgnored, c.id, "", "", "ignorable frame: "+reason)
				} else {
					c.log.record(EvFrameReject, c.id, "", "", "corrupt frame discarded: "+reason)
				}
				continue
			}
			if len(f.UnknownTags) > 0 {
				c.log.record(EvFrameIgnored, c.id, "", "", fmt.Sprintf("unknown tags safely skipped=%v", f.UnknownTags))
			}
			if f.Kind == FrameHeartbeat {
				continue
			}
			if f.Kind != FrameDeliver || f.Message == nil {
				c.log.record(EvFrameReject, c.id, "", "", "unexpected frame kind on client")
				continue
			}
			c.deliver(ctx, conn, f)
		}
	}
}

func (c *localClient) deliver(ctx context.Context, conn Conn, f *Frame) {
	m := *f.Message
	d := Delivery{Message: m, Redelivered: f.Redelivered, DeliveryAttempt: f.Attempt}

	c.mu.Lock()
	h := c.handler
	dup := c.isDuplicate(m.ID, f.Attempt)
	c.mu.Unlock()

	if dup {
		// 消费端识别为重复投递：绝不交给业务处理；自动确认以终止重投，安全丢弃。
		c.log.record(EvDuplicate, c.id, m.ID, m.Topic,
			fmt.Sprintf("duplicate redelivery detected attempt=%d -> auto-ACK, handler skipped", f.Attempt))
		c.sendSettlement(conn, FrameAck, m.ID)
		return
	}

	err := h(ctx, d)
	if err == nil {
		c.log.record(EvAcked, c.id, m.ID, m.Topic, fmt.Sprintf("handler ok attempt=%d -> ACK", f.Attempt))
		c.sendSettlement(conn, FrameAck, m.ID)
		return
	}
	// NACK：消息回到未确认，稍后按原顺序重投；业务侧本帧不重复处理。
	c.log.record(EvNacked, c.id, m.ID, m.Topic, fmt.Sprintf("handler error=%v attempt=%d -> NACK redeliver-later", err, f.Attempt))
	c.sendSettlement(conn, FrameNack, m.ID)
}

func (c *localClient) sendSettlement(conn Conn, kind FrameKind, msgID string) {
	c.mu.Lock()
	drop := false
	if kind == FrameAck && c.swallowAck != nil && c.swallowAck[msgID] > 0 {
		c.swallowAck[msgID]--
		drop = true
	}
	c.mu.Unlock()
	if drop {
		c.log.record(EvFrameReject, c.id, msgID, "", "ACK deliberately dropped (simulated lost acknowledgement)")
		return
	}
	raw, err := c.codec.Encode(&Frame{Kind: kind, SubID: c.id, AckID: msgID})
	if err != nil {
		c.log.record(EvFrameReject, c.id, msgID, "", "settlement encode failed: "+err.Error())
		return
	}
	if err := conn.Send(raw); err != nil {
		c.log.record(EvFrameReject, c.id, msgID, "", "settlement send failed: "+err.Error())
	}
}

// isDuplicate 实现有界 LRU 去重：同一消息 ID 只在“首次见到的投递序号”处理一次。
func (c *localClient) isDuplicate(id string, attempt int) bool {
	if _, seen := c.seen[id]; seen {
		return true
	}
	c.seq++
	c.seen[id] = attempt
	c.seenKeys = append(c.seenKeys, id)
	// 超出窗口：淘汰最早记录的消息 ID（有界、确定）。
	if len(c.seenKeys) > c.seenCap {
		victim := c.seenKeys[0]
		c.seenKeys = c.seenKeys[1:]
		delete(c.seen, victim)
	}
	return false
}

func (c *localClient) Publish(ctx context.Context, m Message) (*PublishResult, error) {
	if c.br == nil {
		return nil, errors.New("pubsub: client has no broker for publishing; use WithClientBroker")
	}
	return c.br.Publish(ctx, m)
}

func (c *localClient) Reconnect(ctx context.Context) error {
	c.mu.Lock()
	if c.id == "" {
		c.mu.Unlock()
		return errors.New("pubsub: not subscribed")
	}
	if c.recvCancel != nil {
		c.recvCancel()
	}
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	c.recvWG.Wait()
	c.log.record(EvReconnected, c.id, "", "", "client reconnecting with same id; broker will redeliver unacked in order")
	return c.connect(ctx)
}

func (c *localClient) Disconnect() error {
	c.mu.Lock()
	conn := c.conn
	if c.recvCancel != nil {
		c.recvCancel()
	}
	c.mu.Unlock()
	c.log.record(EvDisconnected, c.id, "", "", "client disconnected (durable subscription retained)")
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *localClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	if c.recvCancel != nil {
		c.recvCancel()
	}
	c.mu.Unlock()
	c.recvWG.Wait()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *localClient) Journal() []Event { return c.log.snapshot() }
