package pubsub

import (
	"testing"
	"time"
)

func TestCodecRoundtripV2(t *testing.T) {
	c := NewCodec()
	f := &Frame{Kind: FrameDeliver, SubID: "s1", Attempt: 3, Redelivered: true,
		Message: &Message{ID: "m1", Topic: "a/b", Payload: []byte("hello"), PublishedAt: time.Unix(123, 0)}}
	b, err := c.Encode(f)
	if err != nil {
		t.Fatal(err)
	}
	g, err := c.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if g.Message.ID != "m1" || g.Message.Topic != "a/b" || string(g.Message.Payload) != "hello" ||
		g.Attempt != 3 || !g.Redelivered || g.Version != WireV2 {
		t.Fatalf("decoded mismatch: %+v", g)
	}
	if !g.Message.PublishedAt.Equal(time.Unix(123, 0)) {
		t.Fatalf("PublishedAt mismatch: %v", g.Message.PublishedAt)
	}
}

// 历史版本兼容：V1 帧不含 PublishedAt，V2 解码器仍应可读，字段留零值。
func TestCodecBackwardCompatV1(t *testing.T) {
	c := NewCodec()
	old := &Frame{Version: WireV1, Kind: FramePublish,
		Message: &Message{ID: "old1", Topic: "t/1", Payload: []byte("legacy")}}
	b, err := c.Encode(old)
	if err != nil {
		t.Fatal(err)
	}
	g, err := c.Decode(b)
	if err != nil {
		t.Fatalf("historical V1 message must remain readable: %v", err)
	}
	if g.Version != WireV1 || g.Message.ID != "old1" || !g.Message.PublishedAt.IsZero() {
		t.Fatalf("V1 decode mismatch: %+v", g)
	}
}

// 前向兼容：未知字段标签必须安全跳过（可忽略），消息本体仍可读。
func TestCodecUnknownTagIgnored(t *testing.T) {
	c := NewCodec()
	body := buildBodyWithUnknownTag(t)
	raw := buildFrame(t, WireV2, FrameDeliver, body)
	g, err := c.Decode(raw)
	if err != nil {
		t.Fatalf("unknown tag must be safely ignored, got reject: %v", err)
	}
	if g.Message.ID != "m9" || len(g.UnknownTags) != 1 || g.UnknownTags[0] != 60000 {
		t.Fatalf("expected unknown tag recorded and body readable, got %+v tags=%v", g.Message, g.UnknownTags)
	}
}

// 各类损坏/非法帧必须被拒绝，且原因码彼此可区分；空帧可安全忽略。
func TestCodecCorruptFramesRejected(t *testing.T) {
	c := NewCodec()
	good := buildFrame(t, WireV2, FrameDeliver, buildBodyWithID(t, "x"))

	cases := []struct {
		name   string
		raw    []byte
		reason Reason
		ignore bool
	}{
		{"truncated header", []byte{frameMagic, 2, 1}, ReasonFrameTruncated, false},
		{"bad magic", mutate(good, 0, 0x01), ReasonFrameBadMagic, false},
		{"unsupported version", mutate(good, 1, CurrentWire+1), ReasonFrameUnsupported, false},
		{"bad crc", mutate(good, len(good)-1, good[len(good)-1]^0xFF), ReasonFrameChecksum, false},
		{"declared length exceeds", truncLen(good), ReasonFrameTruncated, false},
		{"unknown kind", buildFrame(t, WireV2, FrameKind(99), buildBodyWithID(t, "q")), ReasonFrameUnknownKind, false},
		{"heartbeat empty", buildEmptyFrame(t), ReasonEmptyPayload, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := c.Decode(tc.raw)
			if tc.ignore {
				// 空帧：返回一帧心跳、无错误，即“可安全忽略”。
				if err != nil || f == nil || f.Kind != FrameHeartbeat {
					t.Fatalf("expected safe heartbeat, f=%v err=%v", f, err)
				}
				return
			}
			de := AsDecision(err)
			if de == nil {
				t.Fatalf("expected DecisionError(%s), got f=%v err=%v", tc.reason, f, err)
			}
			if de.Reason != tc.reason {
				t.Fatalf("reason mismatch: want %s got %s", tc.reason, de.Reason)
			}
			if IsIgnorable(err) != tc.ignore {
				t.Fatalf("ignore flag mismatch for %s", tc.reason)
			}
		})
	}
}

// ---- 帧构造辅助（直接走编码内部，便于注入损坏） ----

func buildFrame(t *testing.T, ver uint8, kind FrameKind, body []byte) []byte {
	t.Helper()
	out := make([]byte, frameHdrLen+len(body))
	out[0] = frameMagic
	out[1] = ver
	out[2] = byte(kind)
	out[3] = 0
	putU32(out[4:8], uint32(len(body)))
	putU16(out[8:10], crc16(body))
	copy(out[frameHdrLen:], body)
	return out
}

func buildEmptyFrame(t *testing.T) []byte {
	out := make([]byte, frameHdrLen)
	out[0] = frameMagic
	out[1] = WireV2
	out[2] = byte(FrameHeartbeat)
	out[3] = flagEmpty
	putU16(out[8:10], crc16(nil))
	return out
}

func buildBodyWithID(t *testing.T, id string) []byte {
	var b tlvBuilder
	b.str(tagMsgID, id)
	return b.buf
}

func buildBodyWithUnknownTag(t *testing.T) []byte {
	var b tlvBuilder
	b.str(tagMsgID, "m9")
	b.put(60000, []byte{0xDE, 0xAD}) // 未来版本才认识的标签
	return b.buf
}

func mutate(b []byte, idx int, v byte) []byte {
	cp := append([]byte(nil), b...)
	cp[idx] = v
	return cp
}

func truncLen(b []byte) []byte {
	cp := append([]byte(nil), b...)
	putU32(cp[4:8], uint32(len(b)*2)) // 声称的 body 远超实际
	return cp[:len(b)-1]
}

func putU16(b []byte, v uint16) { b[0] = byte(v >> 8); b[1] = byte(v) }
func putU32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
