package pubsub

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
)

// 协议版本：解码器可读取 [FrameMinSupportedVersion, FrameCurrentVersion]。
// 新版本只能新增（TLV 扩展字段），不得改变既有字段语义；
// 因此 v1 时代写入的历史消息，v2 解码器仍可原样读出。
const (
	FrameV1                  uint8 = 1
	FrameV2                  uint8 = 2
	FrameCurrentVersion      uint8 = FrameV2
	FrameMinSupportedVersion uint8 = FrameV1
)

// FrameType 帧类型。
type FrameType uint8

const (
	FrameUnknown    FrameType = 0
	FrameSubscribe  FrameType = 1 // 客户端 -> 代理：（重新）建立持久订阅
	FrameSubscribed FrameType = 2 // 代理 -> 客户端：订阅确认
	FramePublish    FrameType = 3 // 客户端 -> 代理：发布消息
	FrameDeliver    FrameType = 4 // 代理 -> 客户端：投递消息
	FrameAck        FrameType = 5 // 客户端 -> 代理：确认消息
	FramePublishAck FrameType = 6 // 代理 -> 客户端：发布处理结果（含每个订阅的失败/降级原因）
)

// Frame 是传输层上的一帧。Extensions 承载新版本新增的未知可选字段，
// 旧版本解码器遇到不认识的 TLV 时将其收集并安全跳过。
type Frame struct {
	Version        uint8
	Type           FrameType
	Flags          uint8
	Attempt        uint16 // 投递尝试次数：1 为首次投递，>1 表示重投
	SubscriptionID string
	MessageID      string
	Topic          string
	Filter         string
	Payload        []byte
	Outcomes       []DeliveryOutcome
	Extensions     map[string][]byte // 未知字段（键为 TLV tag 的十六进制），仅用于可解释性记录
}

// DeliveryState 单条消息对单个订阅的处置结论。
type DeliveryState uint8

const (
	DeliveryStateQueued       DeliveryState = 1 // 已入队，将按序投递
	DeliveryStateNoSubscriber DeliveryState = 2 // 无匹配订阅
	DeliveryStateRejected     DeliveryState = 3 // 失败：积压满，拒绝新消息
	DeliveryStateDegraded     DeliveryState = 4 // 降级：为腾挪容量丢弃了最旧消息
	DeliveryStateClosed       DeliveryState = 5 // 订阅已关闭
)

// DeliveryOutcome 描述消息对某一个订阅的处置结果与原因。
type DeliveryOutcome struct {
	SubscriptionID string
	State          DeliveryState
	Reason         Reason
}

// DecodeDecision 解码结论：处理 / 拒绝。
type DecodeDecision uint8

const (
	DecodeProcess DecodeDecision = iota // 可处理（可能附带"已忽略未知字段"的信息性原因）
	DecodeReject                        // 必须拒绝
)

// DecodeResult 给出确定且可区分的解码结论。
type DecodeResult struct {
	Frame         Frame
	Decision      DecodeDecision
	Reason        Reason // 信息性忽略原因，或拒绝原因
	IgnoredFields []uint16
}

// 线上格式（大端）：
//
//	 magic(2)=0x5053 | version(1) | type(1) | flags(1) | attempt(2)
//		subscriptionID : u16len + bytes
//		messageID      : u16len + bytes
//		filter         : u16len + bytes
//		topic          : u16len + bytes
//		payload        : u32len + bytes
//		outcomes       : u16count + [ subID:u16+bytes | state:u8 | reason:u16+bytes ]...
//		extensions     : u16count + [ tag:u16 | u16len + bytes ]...   （前向兼容区）
//		checksum       : u32  CRC32-IEEE，覆盖其之前的全部字节
const (
	frameMagic0    byte = 0x50
	frameMagic1    byte = 0x53
	frameHeaderL        = 2 + 1 + 1 + 1 + 2 // magic+version+type+flags+attempt
	frameChecksumL      = 4
	frameMinL           = frameHeaderL + frameChecksumL
)

var crcTable = crc32.IEEETable

func appendU16String(b []byte, s string) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	b = append(b, l[:]...)
	return append(b, s...)
}

func readU16String(data []byte, pos int) (string, int, bool) {
	if pos+2 > len(data) {
		return "", pos, false
	}
	n := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+n > len(data) {
		return "", pos, false
	}
	return string(data[pos : pos+n]), pos + n, true
}

// Encode 按当前协议版本序列化一帧（空字段省略为零长度）。
func (f Frame) Encode() ([]byte, error) {
	return f.EncodeForVersion(FrameCurrentVersion)
}

// EncodeForVersion 按指定协议版本序列化一帧，用于灰度混排与历史数据自检：
//   - FrameV1：早期形态——固定头 + 固定长度前缀字段 + payload + checksum，
//     没有 outcomes/extensions 区。当前版本解码器必须能原样读回。
//   - FrameCurrentVersion：当前形态（含投递结果区与 TLV 扩展区）。
//
// 协议字段"只增不改"：旧形态是新形态在所有追加区为空时的严格前缀。
func (f Frame) EncodeForVersion(version uint8) ([]byte, error) {
	if version != FrameV1 && version != FrameCurrentVersion {
		return nil, &Error{Op: "frame.encode", Reason: ReasonUnsupportedVersion,
			Err: fmt.Errorf("cannot encode version %d", version)}
	}
	b := make([]byte, 0, frameHeaderL+128)
	b = append(b, frameMagic0, frameMagic1)
	b = append(b, version, byte(f.Type), f.Flags)
	var a [2]byte
	binary.BigEndian.PutUint16(a[:], f.Attempt)
	b = append(b, a[:]...)

	b = appendU16String(b, f.SubscriptionID)
	b = appendU16String(b, f.MessageID)
	b = appendU16String(b, f.Filter)
	b = appendU16String(b, f.Topic)

	var l4 [4]byte
	binary.BigEndian.PutUint32(l4[:], uint32(len(f.Payload)))
	b = append(b, l4[:]...)
	b = append(b, f.Payload...)

	if version == FrameV1 {
		// 早期形态：payload 后直接以 checksum 结尾，不含 outcomes/extensions。
		// 即使帧上带了扩展，也不写入 V1——V1 本来就不认识这些字段；
		// 需要自检新字段请使用当前版本编码。
		sum := crc32.Checksum(b, crcTable)
		binary.BigEndian.PutUint32(l4[:], sum)
		b = append(b, l4[:]...)
		return b, nil
	}

	binary.BigEndian.PutUint16(a[:], uint16(len(f.Outcomes)))
	b = append(b, a[:]...)
	for _, o := range f.Outcomes {
		b = appendU16String(b, o.SubscriptionID)
		b = append(b, byte(o.State))
		b = appendU16String(b, string(o.Reason))
	}

	// 扩展区：以 TLV 形式写入应用扩展 / 新版本字段。
	// 旧版本解码器对不认识的 tag 会安全跳过，因此这里新增字段不破坏历史可读性。
	extKeys := make([]string, 0, len(f.Extensions))
	for k := range f.Extensions {
		extKeys = append(extKeys, k)
	}
	sort.Strings(extKeys)
	binary.BigEndian.PutUint16(a[:], uint16(len(extKeys)))
	b = append(b, a[:]...)
	for _, k := range extKeys {
		tag, err := strconv.ParseUint(k, 16, 16)
		if err != nil {
			return nil, &Error{Op: "frame.encode", Reason: ReasonIncompleteField, Err: err}
		}
		v := f.Extensions[k]
		binary.BigEndian.PutUint16(a[:], uint16(tag))
		b = append(b, a[:]...)
		binary.BigEndian.PutUint16(a[:], uint16(len(v)))
		b = append(b, a[:]...)
		b = append(b, v...)
	}

	sum := crc32.Checksum(b, crcTable)
	binary.BigEndian.PutUint32(l4[:], sum)
	b = append(b, l4[:]...)
	return b, nil
}

func reject(reason Reason) (DecodeResult, error) {
	return DecodeResult{Decision: DecodeReject, Reason: reason},
		&Error{Op: "frame.decode", Reason: reason}
}

// Decode 解析一帧。结论确定且可区分：
//   - 帧过短 / 魔数错误 / 版本不可兼容 / 校验失败 / 字段截断 / 帧类型未知：DecodeReject；
//   - 帧合法但携带未知 TLV 扩展字段：DecodeProcess，Reason=ReasonUnknownFieldIgnored，
//     未知字段收集在 IgnoredFields 与 Frame.Extensions 中，可安全忽略。
func Decode(data []byte) (DecodeResult, error) {
	if len(data) < frameMinL {
		return reject(ReasonFrameTooShort)
	}
	if data[0] != frameMagic0 || data[1] != frameMagic1 {
		return reject(ReasonBadMagic)
	}
	version := data[2]
	if version < FrameMinSupportedVersion || version > FrameCurrentVersion {
		return reject(ReasonUnsupportedVersion)
	}
	body, trailer := data[:len(data)-frameChecksumL], data[len(data)-frameChecksumL:]
	if binary.BigEndian.Uint32(trailer) != crc32.Checksum(body, crcTable) {
		return reject(ReasonChecksumMismatch)
	}

	ft := FrameType(data[3])
	if ft < FrameSubscribe || ft > FramePublishAck {
		return reject(ReasonUnknownFrameType)
	}

	f := Frame{Version: version, Type: ft, Flags: data[4]}
	res := DecodeResult{Frame: f, Decision: DecodeProcess}
	f.Attempt = binary.BigEndian.Uint16(data[5:7])
	pos := frameHeaderL
	ok := true
	readStr := func(dst *string) {
		if !ok {
			return
		}
		*dst, pos, ok = readU16String(data, pos)
	}
	readStr(&f.SubscriptionID)
	readStr(&f.MessageID)
	readStr(&f.Filter)
	readStr(&f.Topic)
	if ok {
		if pos+4 > len(data)-frameChecksumL {
			ok = false
		} else {
			n := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			if pos+n > len(data)-frameChecksumL {
				ok = false
			} else {
				f.Payload = append([]byte(nil), data[pos:pos+n]...)
				pos += n
			}
		}
	}
	end := len(data) - frameChecksumL
	if version == FrameV1 {
		// V1 的布局在 payload 后直接以 checksum 结尾，没有 outcomes/extensions 区。
		// 字段只增不改，因此 V1 历史消息在 V2 解码器下无需任何附加区即可读。
		if !ok || pos != end {
			return reject(ReasonIncompleteField)
		}
		res.Frame = f
		return res, nil
	}
	if ok && pos < end {
		if pos+2 > end {
			ok = false
		} else {
			n := int(binary.BigEndian.Uint16(data[pos:]))
			pos += 2
			for i := 0; i < n; i++ {
				var o DeliveryOutcome
				readStr(&o.SubscriptionID)
				if !ok || pos+1 > end {
					ok = false
					break
				}
				o.State = DeliveryState(data[pos])
				pos++
				var reason string
				readStr(&reason)
				o.Reason = Reason(reason)
				f.Outcomes = append(f.Outcomes, o)
			}
		}
	}
	// pos==end 表示该帧由更旧的编码器写出（无 outcomes/extensions 区），
	// 属于字段"只增不改"的合法历史形态：按空值处理，继续可读。
	if ok && pos < end {
		if pos+2 > end {
			ok = false
		} else {
			n := int(binary.BigEndian.Uint16(data[pos:]))
			pos += 2
			var ignored []uint16
			for i := 0; i < n; i++ {
				if pos+4 > end {
					ok = false
					break
				}
				tag := binary.BigEndian.Uint16(data[pos:])
				vlen := int(binary.BigEndian.Uint16(data[pos+2:]))
				pos += 4
				if pos+vlen > end {
					ok = false
					break
				}
				val := append([]byte(nil), data[pos:pos+vlen]...)
				pos += vlen
				if f.Extensions == nil {
					f.Extensions = map[string][]byte{}
				}
				key := unknownFieldKey(tag)
				f.Extensions[key] = val
				// 0x0000-0x00FF 为应用层保留 tag（如 seq），属于已知字段；
				// 其余 tag 来自更新版本，属于可安全忽略的未知字段。
				if tag > 0x00ff {
					ignored = append(ignored, tag)
				}
			}
			res.IgnoredFields = ignored
		}
	}
	if !ok || pos != end {
		return reject(ReasonIncompleteField)
	}

	res.Frame = f
	if len(res.IgnoredFields) > 0 {
		res.Reason = ReasonUnknownFieldIgnored
	}
	return res, nil
}

func unknownFieldKey(tag uint16) string {
	return fmt.Sprintf("%04x", tag)
}

// 应用层（代理/客户端之间）在 Extensions 中约定的键名。
// 键即 4 位十六进制 TLV tag 的字符串形式：
//   - 0x00xx 段为应用层保留 tag；
//   - 其他段预留给协议未来版本，解码器对任何未知 tag 一律安全跳过。
const (
	// ExtKeySeq 携带投递帧在该订阅内的单调序号，ACK 时原样回带。
	ExtKeySeq = "0001"
)

// SetExt 写入一个应用层扩展键值（随帧序列化传输）。
func (f *Frame) SetExt(key string, value []byte) {
	if f.Extensions == nil {
		f.Extensions = map[string][]byte{}
	}
	f.Extensions[key] = value
}

// GetExt 读取应用层扩展键值，不存在时返回 nil,false。
func (f *Frame) GetExt(key string) ([]byte, bool) {
	v, ok := f.Extensions[key]
	return v, ok
}

// PutSeq / Seq 以十进制字符串存取订阅内序号。
func (f *Frame) PutSeq(seq uint64) { f.SetExt(ExtKeySeq, []byte(fmt.Sprintf("%d", seq))) }

func (f *Frame) Seq() (uint64, bool) {
	v, ok := f.GetExt(ExtKeySeq)
	if !ok {
		return 0, false
	}
	n, err := parseUint(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseUint(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty uint")
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("bad uint %q", b)
		}
		n = n*10 + uint64(c-'0')
	}
	return n, nil
}

// Encode 按当前协议版本序列化一帧（空字段省略为零长度）。
