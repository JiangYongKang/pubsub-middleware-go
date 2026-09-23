// Package pubsub 提供本地发布订阅中间件：
// 经由可插拔的本地传输（Transport）模拟客户端与代理（Broker）之间的消息收发，
// 支持持久订阅、重连补投、重复识别与慢消费者背压。
package pubsub

import "time"

// 线上协议版本。历史版本必须仍可解码；高于支持范围的版本必须拒绝。
const (
	WireV1 uint8 = 1
	WireV2 uint8 = 2
	// MinSupportedWire 为仍可读取的最低历史版本。
	MinSupportedWire = WireV1
	// CurrentWire 为当前编码版本。
	CurrentWire = WireV2
)

// Message 是发布/订阅之间流动的应用消息。
type Message struct {
	ID          string
	Topic       string
	Payload     []byte
	PublishedAt time.Time
}

// Delivery 是投递给消费端的消息（可能是重投）。
type Delivery struct {
	Message Message
	// Redelivered 为 true 表示此前已投递过但未被确认。
	Redelivered bool
	// DeliveryAttempt 为投递序号，首次为 1，重投逐次加 1。
	DeliveryAttempt int
}

// Filter 描述一个主题过滤规则。
type Filter interface {
	// Pattern 返回过滤规则原文。
	Pattern() string
	// Match 判断给定主题是否命中规则。
	Match(topic string) bool
}

// OverflowPolicy 描述订阅积压达到容量上限时的处置策略。
type OverflowPolicy int

const (
	// OverflowRejectNew：新消息不再进入该订阅，发布得到可区分的拒绝原因。
	// 发布方不被阻塞，其他订阅不受影响。
	OverflowRejectNew OverflowPolicy = iota
	// OverflowDropOldest：丢弃该订阅积压中最旧的未确认消息，为新消息腾位；
	// 被丢弃消息有独立、可区分的降级原因并可被观测。
	OverflowDropOldest
)

// BrokerConfig 是 Broker 的配置。
type BrokerConfig struct {
	// SubQueueCapacity 为每个持久订阅“待投递/未确认”消息的积压容量上限。
	SubQueueCapacity int
	// Overflow 为积压超限时的处置策略。
	Overflow OverflowPolicy
}

// TargetResult 是发布结果中“单个订阅”的处置结论。
type TargetResult struct {
	SubID  string
	Reason Reason
	// DroppedID 仅在降级丢弃（DropOldest）时给出被丢弃的消息 ID。
	DroppedID string
}

// PublishResult 是一次发布对所有匹配订阅的处置结论汇总。
type PublishResult struct {
	MessageID string
	// Accepted 为正常入队的订阅数。
	Accepted int
	Targets  []TargetResult
}

// Event 是一条可解释的判定流水（投递/确认/重投/降级/解码判定）。
type Event struct {
	Time   time.Time
	Kind   string
	SubID  string
	MsgID  string
	Topic  string
	Detail string
}
