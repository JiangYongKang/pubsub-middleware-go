package pubsub

import (
	"errors"
	"sync"
)

// Conn 是一条面向消息的、可插拔的客户端-代理连接。
// 每次 Send/Recv 处理一整个帧（消息边界由实现保证）。
//
// 连接内部复用两类相互独立的逻辑通道（lane）：
//   - 默认通道（Send/Recv）：承载控制帧——订阅请求、发布请求/发布回执、ACK。
//     它与任何订阅的数据投递都不共享缓冲，因此慢消费者不可能卡住发布回执；
//   - 命名数据通道（OpenLane）：每个持久订阅一条，独立缓冲、独立背压。
//     某个订阅消费慢只会堵满它自己的数据通道，不影响同连接的其他订阅，
//     也不影响默认控制通道。
type Conn interface {
	// Send 经由默认（控制）通道发送一帧。连接已断开时返回携带
	// ReasonTransportClosed 的错误；对端控制缓冲满时立即失败、绝不阻塞。
	Send(frame []byte) error
	// Recv 阻塞等待默认（控制）通道上的下一帧；连接断开后返回携带
	// ReasonTransportClosed 的错误。
	Recv() ([]byte, error)
	// OpenLane 打开（或复用）一条按 name 标识的命名数据通道。
	// 连接两端以相同 name 打开即得到同一条点对点通道；重复打开返回同一实例。
	// 约定以订阅 durableID 作为 name。
	OpenLane(name string) (Lane, error)
	Close() error
	// Done 在连接断开（含被模拟的网络故障）时关闭。
	Done() <-chan struct{}
	ClientID() string
}

// Lane 是连接上一条点对点的命名数据通道：承载某一个持久订阅的投递帧。
// 每条 Lane 独立缓冲：慢订阅只会堵满自己的 Lane，与控制通道及其他 Lane 互不牵连。
type Lane interface {
	Name() string
	// Send 非阻塞发送一帧；对端来不及取（该订阅消费慢）时立即失败，
	// 返回携带 ReasonBacklogFullRejectNewest 的错误，消息仍留在代理队列稍后重试。
	Send(frame []byte) error
	// Recv 阻塞等待下一帧；所属连接断开后返回携带 ReasonTransportClosed 的错误。
	Recv() ([]byte, error)
}

// Transport 抽象客户端与代理之间的传输。
// 客户端 Dial，代理 Accept；可替换为内存实现或其他本地模拟实现。
type Transport interface {
	Dial(clientID string) (Conn, error)
	Accept() (Conn, error)
	Close() error
}

// localConnBuffer 为每条命名数据通道的单方向缓冲：容量 1，足以让投递流水线
// 不空转，又不会用大缓冲掩盖慢消费者背压——真正的容量上限由代理侧每订阅
// 积压队列（capacity）决定，且数据通道按订阅独立，互不牵连。
const localConnBuffer = 1

// 默认控制通道缓冲：足以吸收控制帧突发（订阅、发布回执、ACK），
// 使发布回执在任何慢订阅场景下都能立刻穿过连接。
const localControlBuffer = 16

// localConn 是 LocalTransport 上的一端。
// Send 写入本端 out（默认控制通道），对端从其 in 读出。
// 命名数据通道（laneSet）由连接两端共享、按订阅独立建槽。
type localConn struct {
	id     string
	shared *localLaneSet
	in     chan []byte // 默认控制通道：对端 Send 写入、本端 Recv 读出
	out    chan []byte // 默认控制通道：本端 Send 写入、对端 Recv 读出
	mu     sync.Mutex
	closed bool
	done   chan struct{}
}

// localLane 是一条命名数据通道的一端。
type localLane struct {
	name string
	ch   chan []byte
	done <-chan struct{}
}

func (l *localLane) Name() string { return l.name }

func (l *localLane) Send(frame []byte) error {
	cp := append([]byte(nil), frame...)
	// 非阻塞：该订阅消费端来不及取时立即失败而不是拖住投递 goroutine
	// （发布隔离与订阅间隔离）。未发出的消息仍由代理保留，稍后重投。
	select {
	case l.ch <- cp:
		return nil
	case <-l.done:
		return &Error{Op: "lane.send", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	default:
		return &Error{Op: "lane.send", Reason: ReasonBacklogFullRejectNewest,
			Err: errors.New("subscriber buffer full (slow consumer); send not blocking")}
	}
}

func (l *localLane) Recv() ([]byte, error) {
	select {
	case b, ok := <-l.ch:
		if !ok {
			return nil, &Error{Op: "lane.recv", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
		}
		return b, nil
	case <-l.done:
		return nil, &Error{Op: "lane.recv", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	}
}

// localLaneSet 由同一条物理链路的两端共享：每条命名数据通道对应一对有向通道，
// 首次以该 name 打开时原子建立。客户端与代理共享同一个 set、同一个 name，
// 因此无论谁先打开，解析到的必然是同一组通道（各取相反方向），不会各建一份。
type localLaneSet struct {
	mu      sync.Mutex
	lanes   map[string]*lanePair
	done    chan struct{}
	closeMu sync.Mutex
	closed  bool
}

type lanePair struct {
	// 命名数据通道当前只承载一个方向：代理 -> 客户端的投递帧。
	// 代理侧 lane.Send 与客户端侧 lane.Recv 落在同一条 brokerToClient 上；
	// clientToBroker 预留，当前不启用。
	clientToBroker chan []byte
	brokerToClient chan []byte
}

func laneKey(name string) string { return "lane:" + name }

func newLocalLaneSet(done chan struct{}) *localLaneSet {
	return &localLaneSet{lanes: map[string]*lanePair{}, done: done}
}

// shutdown 幂等关闭共享 done（断网时连接两端各自 Close，只允许真正关闭一次）。
func (s *localLaneSet) shutdown() {
	s.closeMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.done)
	}
	s.closeMu.Unlock()
}

// open 返回 name 数据通道在本端的访问口（代理写 / 客户端读同一条单向通道）；
// 首次打开原子建立通道对，两端解析到的必然是同一组通道。
func (s *localLaneSet) open(name string) *localLane {
	s.mu.Lock()
	p, ok := s.lanes[laneKey(name)]
	if !ok {
		p = &lanePair{
			clientToBroker: make(chan []byte, localConnBuffer),
			brokerToClient: make(chan []byte, localConnBuffer),
		}
		s.lanes[laneKey(name)] = p
	}
	s.mu.Unlock()
	// 数据通道单向承载代理->客户端投递：两端都挂在 brokerToClient 上
	// （代理写、客户端读）；另一方向通道保留未用。
	return &localLane{name: name, ch: p.brokerToClient, done: s.done}
}

func (c *localConn) OpenLane(name string) (Lane, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, &Error{Op: "conn.open_lane", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	}
	c.mu.Unlock()
	return c.shared.open(name), nil
}

func (c *localConn) Send(frame []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return &Error{Op: "conn.send", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	}
	c.mu.Unlock()
	cp := append([]byte(nil), frame...)
	// 默认控制通道，非阻塞：极端情况下控制缓冲也满时立即失败而不是拖住发送方。
	// 控制通道与所有数据通道缓冲独立，正常慢消费者背压绝不会传导到这里。
	select {
	case c.out <- cp:
		return nil
	case <-c.done:
		return &Error{Op: "conn.send", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	default:
		return &Error{Op: "conn.send", Reason: ReasonBacklogFullRejectNewest,
			Err: errors.New("peer control buffer full; send not blocking")}
	}
}

func (c *localConn) Recv() ([]byte, error) {
	select {
	case b, ok := <-c.in:
		if !ok {
			return nil, &Error{Op: "conn.recv", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
		}
		return b, nil
	case <-c.done:
		return nil, &Error{Op: "conn.recv", Reason: ReasonTransportClosed, Err: errors.New("connection closed")}
	}
}

func (c *localConn) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		// done 为连接两端共享：经由 laneSet 幂等关闭，两端同时 Close 也安全。
		c.shared.shutdown()
	}
	c.mu.Unlock()
	return nil
}

func (c *localConn) Done() <-chan struct{} { return c.done }

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

	// 默认控制通道两端共享同一 done：断网时两端 localConn.Close 都会触发，
	// 幂等安全（close 受 localConn.mu 保护）。命名数据通道也监听该 done。
	done := make(chan struct{})
	lanes := newLocalLaneSet(done)
	clientToBroker := make(chan []byte, localControlBuffer)
	brokerToClient := make(chan []byte, localControlBuffer)
	// 客户端端 out = clientToBroker，其 in = brokerToClient；代理端反之。
	clientSide := &localConn{
		id: clientID, shared: lanes,
		in: brokerToClient, out: clientToBroker, done: done,
	}
	brokerSide := &localConn{
		id: clientID, shared: lanes,
		in: clientToBroker, out: brokerToClient, done: done,
	}

	// 断网时只需关闭共享 done：默认控制通道两端的 Recv 与全部数据通道同时失效，
	// 模拟"客户端与代理之间链路中断"；Send 侧通过各自 closed 标志与 done 感知。
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
