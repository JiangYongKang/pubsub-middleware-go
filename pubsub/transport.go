package pubsub

import (
	"context"
	"errors"
	"sync"
)

// Transport 是可插拔的本地传输，模拟客户端与代理之间的链路。
// 同一条传输上可承载多条逻辑连接（Conn），连接断开可重建，
// 以模拟订阅方短暂断连后重新接入。
type Transport interface {
	// Dial 建立一条到代理的逻辑连接。id 用于重连时标识同一持久订阅方。
	Dial(ctx context.Context, id string) (Conn, error)
}

// ErrTransportClosed 在传输/交换中枢已关闭时返回。
var ErrTransportClosed = errors.New("pubsub: transport closed")

// ErrConnClosed 在向已关闭连接发送时返回。
var ErrConnClosed = errors.New("pubsub: connection closed")

// Conn 是传输上的一条逻辑连接。
type Conn interface {
	ID() string
	// Send 非阻塞地向对端投递一个原始帧字节；对端缓冲满时返回错误（不阻塞调用方）。
	Send(b []byte) error
	// Receive 返回接收到的原始帧字节通道；连接关闭后通道关闭。
	Receive() <-chan []byte
	// Done 在连接关闭时关闭。
	Done() <-chan struct{}
	// Close 关闭连接（模拟断连）。
	Close() error
}

// conn 是成对连接中的一端。两端共享 close 同步。
type conn struct {
	id   string
	dir  string // "client" / "server"
	in   chan []byte
	pair *connPair
}

func (c *conn) ID() string             { return c.id }
func (c *conn) Receive() <-chan []byte { return c.in }
func (c *conn) Done() <-chan struct{}  { return c.pair.done }
func (c *conn) Send(b []byte) error {
	if c.pair.isClosed() {
		return ErrConnClosed
	}
	// 拷贝一份，避免调用方复用底层数组造成串扰。
	cp := append([]byte(nil), b...)
	// 非阻塞：慢的对端不应反压/卡死发送方（背压由 Broker 订阅队列负责裁决）。
	select {
	case c.pair.peer(c).in <- cp:
		return nil
	default:
		return errors.New("pubsub: peer receive buffer full")
	}
}
func (c *conn) Close() error { c.pair.close(); return nil }

// connPair 把一条逻辑连接的客户端侧与服务端侧绑在一起。
type connPair struct {
	client *conn
	server *conn
	done   chan struct{}
	once   sync.Once
	mu     sync.RWMutex
	closed bool
}

func (p *connPair) peer(c *conn) *conn {
	if c.dir == "client" {
		return p.server
	}
	return p.client
}
func (p *connPair) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}
func (p *connPair) close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.done)
	})
}

// localSwitch 是传输的服务端交换中枢：收集已拨入、等待代理接受的连接。
type localSwitch struct {
	buffer int

	mu      sync.Mutex
	pending []serverConn
	signal  chan struct{}
	closed  bool
}

func (s *localSwitch) push(c serverConn) {
	s.mu.Lock()
	s.pending = append(s.pending, c)
	sig := s.signal
	s.mu.Unlock()
	select {
	case sig <- struct{}{}:
	default:
	}
}

// acceptWait 返回一个在有新连接或中枢关闭时触发的信号通道。
func (s *localSwitch) acceptWait() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.signal
}

// acceptAll 取出并清空当前待接受的连接（FIFO）。
func (s *localSwitch) acceptAll() []serverConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.pending
	s.pending = nil
	return out
}

func (s *localSwitch) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// localTransport 是进程内默认传输。
type localTransport struct {
	sw *localSwitch
}

// NewLocalTransport 创建配套的客户端传输与服务端交换中枢。
// buffer 为单方向每连接的帧缓冲大小。
func NewLocalTransport(buffer int) (Transport, *localSwitch) {
	sw := &localSwitch{
		buffer: buffer,
		signal: make(chan struct{}, 1),
	}
	return &localTransport{sw: sw}, sw
}

func (t *localTransport) Dial(ctx context.Context, id string) (Conn, error) {
	if t.sw.isClosed() {
		return nil, ErrTransportClosed
	}
	if id == "" {
		id = "anon"
	}
	buf := t.sw.buffer
	if buf <= 0 {
		buf = 16
	}
	p := &connPair{done: make(chan struct{})}
	cc := &conn{id: id, dir: "client", in: make(chan []byte, buf), pair: p}
	sc := &conn{id: id, dir: "server", in: make(chan []byte, buf), pair: p}
	p.client, p.server = cc, sc
	t.sw.push(sc)
	return cc, nil
}
