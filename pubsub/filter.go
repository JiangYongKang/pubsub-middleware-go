package pubsub

import "strings"

// topicFilter 支持 MQTT 风格的层级匹配：
//   - 以 "/" 分隔层级；
//   - "+" 匹配任意单层；
//   - "#" 匹配其位置及之后的任意层级，仅允许出现在最后一层；
//   - 其余层级为精确匹配。
//
// 过滤规则在订阅时确定，重连补投沿用同一规则（服务端持久化 pattern）。
type topicFilter struct {
	pattern string
	levels  []string
}

// NewFilter 依据模式串构造主题过滤规则。
func NewFilter(pattern string) Filter {
	p := strings.TrimSpace(pattern)
	if p == "" {
		p = "#"
	}
	return &topicFilter{pattern: p, levels: strings.Split(p, "/")}
}

func (f *topicFilter) Pattern() string { return f.pattern }

func (f *topicFilter) Match(topic string) bool {
	tl := strings.Split(topic, "/")
	pl := f.levels
	for i, p := range pl {
		if p == "#" {
			// '#' 仅合法于末层：匹配剩余任意层级（含零层由前置层决定）。
			return i == len(pl)-1
		}
		if i >= len(tl) {
			return false
		}
		if p != "+" && p != tl[i] {
			return false
		}
	}
	// 模式已耗尽，主题也必须恰好耗尽（'+' 不跨层）。
	return len(tl) == len(pl)
}
