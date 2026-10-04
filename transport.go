package pubsub

import (
	"errors"
	"sync"
)

// Conn 是一条面向消息的、可插拔的客户端-代理连接。
// 每次 Send/Recv 处理一整个帧（消息边界由实现保证）。
type Conn interface {
	// Send 发送一帧。连接已断开时返回携带 ReasonTransportClosed 的错误。
	Send(frame []byte) error
	// Recv 阻塞等待下一帧；连接断开后返回携带 ReasonTransportClosed 的错误。
	Recv() ([]byte, error)
	Close() error
	// Done 在连接断开（含被模拟的网络故障）时关闭。
	Done() <-chan struct{}
	ClientID() string
}

// Transport 抽象客户端与代理之间的传输。
// 客户端 Dial，代理 Accept；可替换为内存实现或其他本地模拟实现。
type Transport interface {
	Dial(clientID string) (Conn, error)
	Accept() (Conn, error)
	Close() error
}

// localConnBuffer 为每方向 1：足以让握手/控制帧穿过，又不会用大缓冲掩盖
// 慢消费者背压——真正的容量上限由代理侧每订阅积压队列（capacity）决定。
const localConnBuffer = 1

// localConnState 是一条逻辑链路的共享状态：任一端 Close 都会关闭 done，
// 另一端随后的 Send/Recv 立即得到 ReasonTransportClosed（与真实连接一致）。
type localConnState struct {
	done chan struct{}
	once sync.Once
}

func (s *localConnState) close() { s.once.Do(func() { close(s.done) }) }

// localConn 是 LocalTransport 上的一端。
// Send 写入本端 out，对端从其 in 读出，两条有向通道共同组成一条逻辑链路。
type localConn struct {
	id     string
	in     chan []byte // 对端 Send 写入、本端 Recv 读出
	out    chan []byte // 本端 Send 写入、对端 Recv 读出
	mu     sync.Mutex
	closed bool
	state  *localConnState
}

func (c *localConn) Send(frame []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return &Error{Op: "conn.send", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	}
	c.mu.Unlock()
	cp := append([]byte(nil), frame...)
	// 非阻塞：接收方（消费端）来不及取时立即失败而不是拖住发送方（发布隔离）。
	// 未发出的消息仍由代理保留（在途/队列），稍后重投，不丢失。
	select {
	case c.out <- cp:
		return nil
	case <-c.state.done:
		return &Error{Op: "conn.send", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	default:
		return &Error{Op: "conn.send", Reason: ReasonBacklogFullRejectNewest,
			Err: errors.New("peer receive buffer full (slow consumer); send not blocking")}
	}
}

func (c *localConn) Recv() ([]byte, error) {
	select {
	case b, ok := <-c.in:
		if !ok {
			return nil, &Error{Op: "conn.recv", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
		}
		return b, nil
	case <-c.state.done:
		return nil, &Error{Op: "conn.recv", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	}
}

func (c *localConn) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.state.close()
	}
	c.mu.Unlock()
	return nil
}

func (c *localConn) Done() <-chan struct{} { return c.state.done }

func (c *localConn) ClientID() string { return c.id }

// localTransport 的内部状态。
type localTransport struct {
	mu       sync.Mutex
	acceptCh chan Conn
	closed   bool
	offline  map[string]bool   // 被模拟断网、恢复前不可重新接入的客户端
	hooks    map[string]func() // 当前在途连接的"立即失效"回调（按客户端注册）
}

// LocalTransport 是基于内存通道的本地传输，
// 并可针对单个客户端模拟断开（Disconnect）与恢复（Restore）。
type LocalTransport struct {
	impl *localTransport
}

// NewLocalTransport 创建本地传输。
func NewLocalTransport() *LocalTransport {
	return &LocalTransport{impl: &localTransport{
		acceptCh: make(chan Conn, 64),
		offline:  map[string]bool{},
		hooks:    map[string]func(){},
	}}
}

// Dial 建立一条到代理的链路；若该客户端正处于模拟断网期或传输已关闭，则失败。
func (t *LocalTransport) Dial(clientID string) (Conn, error) {
	t.impl.mu.Lock()
	if t.impl.closed {
		t.impl.mu.Unlock()
		return nil, &Error{Op: "transport.dial", Reason: ReasonTransportClosed, Err: errors.New("transport closed")}
	}
	if t.impl.offline[clientID] {
		t.impl.mu.Unlock()
		return nil, &Error{Op: "transport.dial", Reason: ReasonTransportClosed, Err: errors.New("client link is down")}
	}
	accept := t.impl.acceptCh
	t.impl.mu.Unlock()

	clientToBroker := make(chan []byte, localConnBuffer)
	brokerToClient := make(chan []byte, localConnBuffer)
	// 两端共享一个链路状态：任一端关闭，另一端立即可感知。
	state := &localConnState{done: make(chan struct{})}
	// 客户端端 out = clientToBroker，其 in = brokerToClient；代理端反之。
	clientSide := &localConn{id: clientID, in: brokerToClient, out: clientToBroker, state: state}
	brokerSide := &localConn{id: clientID, in: clientToBroker, out: brokerToClient, state: state}

	// 断网时同时掐断两端：模拟"客户端与代理之间链路中断"，
	// 任一端随后的 Send/Recv 立即得到 ReasonTransportClosed。
	fault := func() {
		clientSide.Close()
		brokerSide.Close()
	}
	t.impl.mu.Lock()
	t.impl.hooks[clientID] = fault
	t.impl.mu.Unlock()

	accept <- brokerSide
	return clientSide, nil
}

// Accept 等待下一条客户端链路。
func (t *LocalTransport) Accept() (Conn, error) {
	conn, ok := <-t.impl.acceptCh
	if !ok {
		return nil, &Error{Op: "transport.accept", Reason: ReasonTransportClosed, Err: errors.New("transport closed")}
	}
	return conn, nil
}

// Close 关闭传输，之后 Dial/Accept 均失败。
func (t *LocalTransport) Close() error {
	t.impl.mu.Lock()
	if !t.impl.closed {
		t.impl.closed = true
		close(t.impl.acceptCh)
	}
	t.impl.mu.Unlock()
	return nil
}

// Disconnect 模拟 clientID 这条链路发生网络故障：现有连接两端同时失效，
// 且在 Restore 之前无法重新 Dial。
func (t *LocalTransport) Disconnect(clientID string) {
	t.impl.mu.Lock()
	t.impl.offline[clientID] = true
	h := t.impl.hooks[clientID]
	t.impl.mu.Unlock()
	if h != nil {
		h()
	}
}

// Restore 恢复链路，使该客户端可以重新 Dial 接入（持久订阅状态在代理侧仍然保留）。
func (t *LocalTransport) Restore(clientID string) {
	t.impl.mu.Lock()
	delete(t.impl.offline, clientID)
	t.impl.mu.Unlock()
}
