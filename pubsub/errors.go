package pubsub

import "errors"

// Reason 是可区分、可解释的处置结论原因码。
// 失败（拒绝）与降级（丢弃/重投/忽略）使用不同命名段，绝不混用。
type Reason string

const (
	ReasonOK                  Reason = "ok"                   // 正常接受/投递/确认
	ReasonAlreadyAcked        Reason = "already_acked"        // 重复确认：安全忽略（幂等）
	ReasonUnknownSubscription Reason = "unknown_subscription" // 必须拒绝：订阅不存在
	ReasonQueueFullRejected   Reason = "queue_full_rejected"  // 失败（背压）：订阅积压满，拒绝新消息
	ReasonDroppedOldest       Reason = "dropped_oldest"       // 降级（背压）：丢弃最旧未确认消息
	ReasonDuplicateDelivery   Reason = "duplicate_delivery"   // 降级：消费端识别到重复投递，安全丢弃并自动确认
	ReasonRedelivered         Reason = "redelivered"          // 信息：断连后按原过滤规则补投

	// 帧解码结论：可安全忽略 与 必须拒绝 严格区分。
	ReasonFrameTruncated   Reason = "frame_truncated"    // 必须拒绝：帧不完整（长度不足/越界）
	ReasonFrameBadMagic    Reason = "frame_bad_magic"    // 必须拒绝：魔数不符，非本协议帧
	ReasonFrameUnsupported Reason = "frame_unsupported"  // 必须拒绝：版本高于支持范围
	ReasonFrameChecksum    Reason = "frame_checksum"     // 必须拒绝：校验和不匹配，帧损坏
	ReasonFrameUnknownTag  Reason = "frame_unknown_tag"  // 可安全忽略：遇到未知字段标签，跳过该字段
	ReasonFrameUnknownKind Reason = "frame_unknown_kind" // 必须拒绝：无法识别的帧类型
	ReasonEmptyPayload     Reason = "empty_payload"      // 可安全忽略：空帧体（心跳/保活）
)

// DecisionError 携带确定性原因码的错误。
// Ignore 为 true 表示该结论属于“可安全忽略”（如未知字段、空帧/心跳），
// 与必须拒绝的错误严格区分，调用方不得把二者混为一谈。
type DecisionError struct {
	Reason Reason
	Op     string
	Msg    string
	Ignore bool
}

func (e *DecisionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return string(e.Reason) + ": " + e.Msg
	}
	return string(e.Reason)
}

// AsDecision 从错误中提取 *DecisionError；不存在则返回 nil。
func AsDecision(err error) *DecisionError {
	var de *DecisionError
	if errors.As(err, &de) {
		return de
	}
	return nil
}

// IsIgnorable 报告错误是否为“可安全忽略”的确定性结论。
func IsIgnorable(err error) bool {
	if de := AsDecision(err); de != nil {
		return de.Ignore
	}
	return false
}
