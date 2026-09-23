package pubsub

import "fmt"

// Reason 以可区分的字符串说明每一种判定结论，
// 使"安全忽略"与"必须拒绝"、"失败"与"降级"在日志与测试中均可解释。
type Reason string

const (
	ReasonNone Reason = ""

	// ---- 帧解码：必须拒绝类 ----
	ReasonFrameTooShort      Reason = "frame_too_short"     // 帧不完整（短于固定头）
	ReasonBadMagic           Reason = "bad_magic"           // 魔数错误，非本协议帧
	ReasonUnsupportedVersion Reason = "unsupported_version" // 版本超出可兼容范围，必须拒绝
	ReasonChecksumMismatch   Reason = "checksum_mismatch"   // 校验和不一致，帧已损坏
	ReasonIncompleteField    Reason = "incomplete_field"    // 字段长度越界，帧被截断
	ReasonUnknownFrameType   Reason = "unknown_frame_type"  // 无法解释的帧类型，必须拒绝

	// ---- 帧解码：可安全忽略类 ----
	ReasonUnknownFieldIgnored Reason = "unknown_field_ignored" // 遇到新版本的未知字段，跳过已知部分继续处理

	// ---- 投递：失败 / 降级类 ----
	ReasonBacklogFullRejectNewest Reason = "backlog_full_reject_newest" // 失败：积压满，按策略拒绝新消息
	ReasonSlowConsumerDropOldest  Reason = "slow_consumer_drop_oldest"  // 降级：积压满，按策略丢弃最旧未确认消息
	ReasonSubscriptionClosed      Reason = "subscription_closed"        // 订阅已关闭
	ReasonTransportClosed         Reason = "transport_closed"           // 传输已关闭
)

// Error 携带操作名、判定原因与底层错误，便于调用方按 Reason 区分处理。
type Error struct {
	Op     string
	Reason Reason
	Err    error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("pubsub %s: %s: %v", e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("pubsub %s: %s", e.Op, e.Reason)
}

func (e *Error) Unwrap() error { return e.Err }
