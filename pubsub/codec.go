package pubsub

import (
	"encoding/binary"
	"fmt"
	"time"
)

// FrameKind 区分帧的用途。
type FrameKind uint8

const (
	FrameUnknown   FrameKind = 0
	FramePublish   FrameKind = 1
	FrameDeliver   FrameKind = 2
	FrameSubscribe FrameKind = 3
	FrameAck       FrameKind = 4
	FrameNack      FrameKind = 5
	FrameHeartbeat FrameKind = 6
)

// TLV 字段标签。新增字段一律分配新标签、不复用旧标签，
// 旧解码器遇到未知标签时安全跳过，从而保证历史消息在新旧版本间仍可读。
const (
	tagMsgID       uint16 = 1
	tagTopic       uint16 = 2
	tagPayload     uint16 = 3
	tagSubID       uint16 = 4
	tagPattern     uint16 = 5
	tagAckID       uint16 = 6
	tagPublishedAt uint16 = 7 // 仅 V2+：V1 历史帧不含该字段，按零值/接收时刻处理
	tagRedelivered uint16 = 8
	tagAttempt     uint16 = 9
)

// 帧固定头：magic(1) version(1) kind(1) flags(1) bodyLen(4) crc16(2) = 10 字节。
const (
	frameMagic  byte   = 0xA7
	frameHdrLen int    = 10
	flagEmpty   uint8  = 0x01 // 空帧体（心跳/保活）
	crcPoly     uint16 = 0x1021
	crcInit     uint16 = 0xFFFF
)

// Frame 是经传输收发的、带版本的帧。
type Frame struct {
	Version uint8
	Kind    FrameKind
	// UnknownTags 记录解码时遇到、已被安全跳过的未知字段标签（可解释的忽略依据）。
	UnknownTags []uint16
	Message     *Message
	SubID       string
	Pattern     string
	AckID       string
	Redelivered bool
	Attempt     int
}

// Codec 抽象帧的编码/解码，便于替换实现（如注入损坏帧测试）。
type Codec interface {
	Encode(f *Frame) ([]byte, error)
	// Decode 对损坏/旧版/未知字段给出确定、可区分的结论：
	// 可忽略情形（未知字段、空帧/心跳）返回 nil 并在帧上留痕；
	// 必须拒绝情形返回带 ReasonFrame* 的 *DecisionError。
	Decode(b []byte) (*Frame, error)
}

// wireCodec 为默认的 TLV + CRC16 本地帧编解码器。
type wireCodec struct{ version uint8 }

// NewCodec 返回当前版本的线格式编解码器。
func NewCodec() Codec { return &wireCodec{version: CurrentWire} }

func decErr(reason Reason, msg string) error {
	return &DecisionError{Reason: reason, Msg: msg}
}

func (c *wireCodec) Encode(f *Frame) ([]byte, error) {
	ver := f.Version
	if ver == 0 {
		ver = c.version
	}
	if ver < MinSupportedWire {
		return nil, decErr(ReasonFrameUnsupported, fmt.Sprintf("version %d too old to encode", ver))
	}
	body := encodeBody(f, ver)
	flags := uint8(0)
	if len(body) == 0 {
		flags |= flagEmpty
	}
	out := make([]byte, frameHdrLen+len(body))
	out[0] = frameMagic
	out[1] = ver
	out[2] = byte(f.Kind)
	out[3] = flags
	binary.BigEndian.PutUint32(out[4:8], uint32(len(body)))
	crc := crc16(body)
	binary.BigEndian.PutUint16(out[8:10], crc)
	copy(out[frameHdrLen:], body)
	return out, nil
}

func (c *wireCodec) Decode(b []byte) (*Frame, error) {
	// 结论顺序确定：先看结构完整性，再看魔数/版本，最后看 CRC，
	// 使“不完整帧 / 非本协议帧 / 不支持版本 / 损坏帧”彼此可区分。
	if len(b) < frameHdrLen {
		return nil, decErr(ReasonFrameTruncated, fmt.Sprintf("frame shorter than header: %d bytes", len(b)))
	}
	if b[0] != frameMagic {
		return nil, decErr(ReasonFrameBadMagic, fmt.Sprintf("bad magic 0x%02x", b[0]))
	}
	ver := b[1]
	if ver > CurrentWire {
		return nil, decErr(ReasonFrameUnsupported, fmt.Sprintf("version %d > supported %d", ver, CurrentWire))
	}
	if ver < MinSupportedWire {
		return nil, decErr(ReasonFrameUnsupported, fmt.Sprintf("version %d < min %d", ver, MinSupportedWire))
	}
	kind := FrameKind(b[2])
	flags := b[3]
	bodyLen := int(binary.BigEndian.Uint32(b[4:8]))
	if len(b) < frameHdrLen+bodyLen {
		return nil, decErr(ReasonFrameTruncated, fmt.Sprintf("declared body %d but got %d bytes", bodyLen, len(b)-frameHdrLen))
	}
	body := b[frameHdrLen : frameHdrLen+bodyLen]
	wantCRC := binary.BigEndian.Uint16(b[8:10])
	if got := crc16(body); got != wantCRC {
		return nil, decErr(ReasonFrameChecksum, fmt.Sprintf("crc mismatch: want %04x got %04x", wantCRC, got))
	}
	if flags&flagEmpty != 0 || len(body) == 0 {
		// 空帧体（心跳/保活）：可安全忽略。
		return &Frame{Version: ver, Kind: FrameHeartbeat}, nil
	}

	if kind == FrameUnknown || kind > FrameHeartbeat {
		return nil, decErr(ReasonFrameUnknownKind, fmt.Sprintf("unknown frame kind %d", kind))
	}
	f := &Frame{Version: ver, Kind: kind}
	if err := decodeBody(body, f); err != nil {
		return nil, err
	}
	return f, nil
}

func encodeBody(f *Frame, ver uint8) []byte {
	var b tlvBuilder
	if f.Message != nil {
		b.str(tagMsgID, f.Message.ID)
		b.str(tagTopic, f.Message.Topic)
		b.bytes(tagPayload, f.Message.Payload)
		// 历史兼容：V1 不写 PublishedAt；V1 解码器遇到也没有该标签。
		if ver >= WireV2 && !f.Message.PublishedAt.IsZero() {
			b.int64(tagPublishedAt, f.Message.PublishedAt.UnixNano())
		}
	}
	b.str(tagSubID, f.SubID)
	b.str(tagPattern, f.Pattern)
	b.str(tagAckID, f.AckID)
	if f.Redelivered {
		b.uint8v(tagRedelivered, 1)
	}
	if f.Attempt > 0 {
		b.varintv(tagAttempt, uint64(f.Attempt))
	}
	return b.buf
}

func decodeBody(body []byte, f *Frame) error {
	for off := 0; off < len(body); {
		if off+4 > len(body) {
			return decErr(ReasonFrameTruncated, "TLV header truncated")
		}
		tag := binary.BigEndian.Uint16(body[off : off+2])
		l := int(binary.BigEndian.Uint16(body[off+2 : off+4]))
		off += 4
		if off+l > len(body) {
			return decErr(ReasonFrameTruncated, fmt.Sprintf("TLV tag %d value exceeds body", tag))
		}
		v := body[off : off+l]
		off += l
		switch tag {
		case tagMsgID:
			f.ensureMsg().ID = string(v)
		case tagTopic:
			f.ensureMsg().Topic = string(v)
		case tagPayload:
			f.ensureMsg().Payload = append([]byte(nil), v...)
		case tagPublishedAt:
			if len(v) != 8 {
				return decErr(ReasonFrameTruncated, "published-at not 8 bytes")
			}
			// V2 字段；V1 帧无此标签，保持零值（接收方可按接收时刻解释），历史消息仍可读。
			f.ensureMsg().PublishedAt = time.Unix(0, int64(binary.BigEndian.Uint64(v))).UTC()
		case tagSubID:
			f.SubID = string(v)
		case tagPattern:
			f.Pattern = string(v)
		case tagAckID:
			f.AckID = string(v)
		case tagRedelivered:
			f.Redelivered = len(v) > 0 && v[0] != 0
		case tagAttempt:
			n, _ := binary.Uvarint(v)
			f.Attempt = int(n)
		default:
			// 未知字段：前向兼容，安全跳过并留痕，绝不因此拒绝整条消息。
			f.UnknownTags = append(f.UnknownTags, tag)
		}
	}
	return nil
}

func (f *Frame) ensureMsg() *Message {
	if f.Message == nil {
		f.Message = &Message{}
	}
	return f.Message
}

// ---- 简单 TLV 构建 ----

type tlvBuilder struct{ buf []byte }

func (t *tlvBuilder) put(tag uint16, v []byte) {
	var hdr [4]byte
	binary.BigEndian.PutUint16(hdr[0:2], tag)
	binary.BigEndian.PutUint16(hdr[2:4], uint16(len(v)))
	t.buf = append(t.buf, hdr[:]...)
	t.buf = append(t.buf, v...)
}
func (t *tlvBuilder) str(tag uint16, s string) {
	if s == "" {
		return
	}
	t.put(tag, []byte(s))
}
func (t *tlvBuilder) bytes(tag uint16, v []byte) { t.put(tag, v) }
func (t *tlvBuilder) int64(tag uint16, n int64) {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(n))
	t.put(tag, v[:])
}
func (t *tlvBuilder) uint8v(tag uint16, n byte) { t.put(tag, []byte{n}) }
func (t *tlvBuilder) varintv(tag uint16, n uint64) {
	var buf [10]byte
	i := binary.PutUvarint(buf[:], n)
	t.put(tag, buf[:i])
}

// ---- CRC16/CCITT-FALSE ----

func crc16(data []byte) uint16 {
	crc := crcInit
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ crcPoly
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
