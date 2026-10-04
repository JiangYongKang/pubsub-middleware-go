package pubsub

import (
	"strings"
	"sync"
)

// OverflowPolicy 积压超过容量上限时的确定策略。
type OverflowPolicy uint8

const (
	// OverflowRejectNewest：失败策略——新消息不进入该订阅队列，
	// 发布回执中给出 ReasonBacklogFullRejectNewest。
	OverflowRejectNewest OverflowPolicy = iota
	// OverflowDropOldest：降级策略——丢弃最旧的未发送消息以腾挪容量，
	// 发布回执中给出 ReasonSlowConsumerDropOldest。
	OverflowDropOldest
)

// queuedMessage 是订阅队列中的一条消息，seq 为该订阅内单调递增序号，
// 顺序完全由 seq 决定，重连补投时同样按 seq 升序，保证同一订阅内顺序不变。
type queuedMessage struct {
	seq       uint64
	messageID string
	topic     string
	payload   []byte
}

// subscription 是一个持久订阅的代理端状态：
// 未发送队列 + 在途（已投递未确认）集合；断连后状态保留，重连后按序补投。
type subscription struct {
	id       string
	clientID string
	filter   string
	capacity int
	policy   OverflowPolicy

	mu       sync.Mutex
	online   bool
	attached Conn // 当前附着的连接；只有它能令本订阅离线
	lane     Lane // 当前附着连接上本订阅独占的数据通道（断连后随连接失效）
	nextSeq  uint64
	queue    []*queuedMessage // 待发送（含断连期间从在途归还的消息），按 seq 升序
	inflight map[uint64]*queuedMessage
	attempts map[uint64]uint16 // 每条消息的投递尝试次数
	notify   chan struct{}     // 队列有新消息 / 重新上线 / 关闭时唤醒投递循环
	closed   bool
}

func newSubscription(id, clientID, filter string, capacity int, policy OverflowPolicy) *subscription {
	return &subscription{
		id:       id,
		clientID: clientID,
		filter:   filter,
		capacity: capacity,
		policy:   policy,
		inflight: map[uint64]*queuedMessage{},
		attempts: map[uint64]uint16{},
		notify:   make(chan struct{}, 1),
	}
}

// MatchTopic 判断主题是否匹配过滤规则。
// 主题与规则均以 '.' 分节：'*' 匹配任意单个节，'>' 匹配零个或多个后续节，
// 且 '>' 只允许出现在规则末尾（如 "a.>.>" 这类非法规则一律视为不匹配）。
func MatchTopic(filter, topic string) bool {
	if filter == "" {
		return false
	}
	f := strings.Split(filter, ".")
	t := strings.Split(topic, ".")
	for i, seg := range f {
		if seg == ">" {
			if i != len(f)-1 {
				return false // '>' 必须是最后一个节
			}
			return true // 后续所有节（含零个）均匹配
		}
		if i >= len(t) {
			return false // 规则比主题长
		}
		if seg != "*" && seg != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// signal 非阻塞地唤醒投递循环。
func (s *subscription) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// enqueue 将一条新消息按序放入队列，并给出确定的处置结论：
//   - 正常：DeliveryStateQueued
//   - 积压满 + RejectNewest：DeliveryStateRejected（失败，消息不入队）
//   - 积压满 + DropOldest：DeliveryStateDegraded（降级，丢弃最旧未发送消息后入队）
//   - 订阅关闭：DeliveryStateClosed
func (s *subscription) enqueue(messageID, topic string, payload []byte) DeliveryOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := DeliveryOutcome{SubscriptionID: s.id}
	if s.closed {
		out.State = DeliveryStateClosed
		out.Reason = ReasonSubscriptionClosed
		return out
	}

	if len(s.queue) >= s.capacity {
		switch s.policy {
		case OverflowRejectNewest:
			out.State = DeliveryStateRejected
			out.Reason = ReasonBacklogFullRejectNewest
			return out
		case OverflowDropOldest:
			// 队首即该订阅最旧的未发送消息；丢弃它为新消息腾出一个位置。
			s.queue = s.queue[1:]
			out.State = DeliveryStateDegraded
			out.Reason = ReasonSlowConsumerDropOldest
		default:
			out.State = DeliveryStateRejected
			out.Reason = ReasonBacklogFullRejectNewest
			return out
		}
	} else {
		out.State = DeliveryStateQueued
	}

	s.nextSeq++
	msg := &queuedMessage{
		seq:       s.nextSeq,
		messageID: messageID,
		topic:     topic,
		payload:   append([]byte(nil), payload...),
	}
	s.queue = append(s.queue, msg)
	s.attempts[msg.seq] = 0
	s.signal()
	return out
}

// head 在不改变队列的前提下查看队首消息（在线、非空时）。
func (s *subscription) head() *queuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.online || s.closed || len(s.queue) == 0 {
		return nil
	}
	return s.queue[0]
}

// previewNextAttempt 给出 msg 若此刻转入在途，将使用的尝试次数（不改变状态）。
func (s *subscription) previewNextAttempt(msg *queuedMessage) uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[msg.seq] + 1
}

// markInflight 在"消息已成功写入对端连接"后调用：队首出队并转入在途集合，
// 投递尝试次数 +1。必须与 head() 指向同一队首（调用方需保证其间无其他改动）。
func (s *subscription) markInflight(msg *queuedMessage) uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 || s.queue[0].seq != msg.seq {
		return 0 // 状态已变（例如离线归还），调用方放弃本次投递
	}
	s.queue = s.queue[1:]
	s.inflight[msg.seq] = msg
	s.attempts[msg.seq]++
	return s.attempts[msg.seq]
}

// ack 确认一条消息，从在途集合移除并清理尝试计数。
// 对已被丢弃/已确认/未知序号的确认一律幂等忽略。
func (s *subscription) ack(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, seq)
	delete(s.attempts, seq)
}

// goOffline 在 conn 仍是该订阅的附着连接时令其离线：
// 所有在途消息按 seq 升序归位到队列前部，保持原顺序等待重连补投。
// 若订阅已重连并附着到更新的连接，则旧连接的失效不产生任何影响（无竞态）。
func (s *subscription) goOffline(conn Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attached != nil && s.attached != conn {
		return false // 已被更新的连接接管
	}
	s.online = false
	s.attached = nil
	s.lane = nil
	if len(s.inflight) > 0 {
		returned := make([]*queuedMessage, 0, len(s.inflight))
		for _, m := range s.inflight {
			returned = append(returned, m)
		}
		// 按 seq 升序排序（插入排序，在途消息数量通常极少）。
		for i := 1; i < len(returned); i++ {
			for j := i; j > 0 && returned[j-1].seq > returned[j].seq; j-- {
				returned[j-1], returned[j] = returned[j], returned[j-1]
			}
		}
		s.inflight = map[uint64]*queuedMessage{}
		s.queue = append(returned, s.queue...) // 在途消息序号一定早于队列中消息
	}
	return true
}

// attach 绑定连接与其上本订阅独占的数据通道并标记上线，然后唤醒投递循环触发（补）投递。
func (s *subscription) attach(conn Conn, lane Lane) {
	s.mu.Lock()
	s.attached = conn
	s.lane = lane
	s.online = true
	s.mu.Unlock()
	s.signal()
}

// notifyCh 返回固定的唤醒通道：入队、attach、在途归还、关闭都会非阻塞发信号。
// 投递循环采用"被唤醒 -> 取空队列 -> 再等待"模式，无忙循环、无唤醒丢失。
func (s *subscription) notifyCh() <-chan struct{} { return s.notify }

// currentLane 返回当前附着连接上本订阅独占的数据通道（离线/无通道时为 nil）。
func (s *subscription) currentLane() Lane {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lane
}

// markClosed 关闭订阅并唤醒等待者。
func (s *subscription) markClosed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.online = false
	s.signal()
}

// backlog 返回积压长度（供测试/日志观察）。
func (s *subscription) backlog() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}
