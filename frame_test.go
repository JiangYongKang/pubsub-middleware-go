package pubsub

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

// 判定依据必须可区分：每个拒绝原因都有独立用例。
func TestFrameDecodeRejectReasons(t *testing.T) {
	good := Frame{
		Type:           FramePublish,
		SubscriptionID: "s1",
		MessageID:      "m1",
		Topic:          "a.b",
		Payload:        []byte("hello"),
	}
	raw, err := good.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(b []byte) []byte
		want   Reason
	}{
		{"frame_too_short", func(b []byte) []byte { return b[:3] }, ReasonFrameTooShort},
		{"bad_magic", func(b []byte) []byte { b[0] ^= 0xFF; return b }, ReasonBadMagic},
		{"unsupported_version_new", func(b []byte) []byte { b[2] = FrameCurrentVersion + 10; return fixChecksum(b) }, ReasonUnsupportedVersion},
		{"unsupported_version_old", func(b []byte) []byte { b[2] = FrameMinSupportedVersion - 1; return fixChecksum(b) }, ReasonUnsupportedVersion},
		{"checksum_mismatch", func(b []byte) []byte { b[len(b)-1] ^= 0xFF; return b }, ReasonChecksumMismatch},
		{"unknown_frame_type", func(b []byte) []byte { b[3] = 99; return fixChecksum(b) }, ReasonUnknownFrameType},
		{"incomplete_field_truncated", func(b []byte) []byte {
			// 把 payload 长度前缀改大，使其越过帧尾。
			pos := frameHeaderL + 2 + len("s1") + 2 + len("m1") + 2 + 2 + len("a.b")
			binary.BigEndian.PutUint32(b[pos:], uint32(len("hello"))+100)
			return fixChecksum(b)
		}, ReasonIncompleteField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.mutate(append([]byte(nil), raw...))
			res, err := Decode(data)
			if err == nil {
				t.Fatalf("expected reject error, got decision=%v", res.Decision)
			}
			if res.Decision != DecodeReject || res.Reason != tc.want {
				t.Fatalf("decision=%v reason=%q, want reject/%q", res.Decision, res.Reason, tc.want)
			}
			var pe *Error
			if !asError(err, &pe) || pe.Reason != tc.want {
				t.Fatalf("error reason=%v, want %q", err, tc.want)
			}
		})
	}
}

func asError(err error, target **Error) bool {
	for err != nil {
		if e, ok := err.(*Error); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
			continue
		}
		return false
	}
	return false
}

// fixChecksum 重算 CRC32（用于"内容被合法改动"的损坏用例，使失败原因精确落在目标字段上）。
func fixChecksum(b []byte) []byte {
	sum := crc32.Checksum(b[:len(b)-4], crc32.IEEETable)
	binary.BigEndian.PutUint32(b[len(b)-4:], sum)
	return b
}

// 新版本新增的未知 TLV 字段必须可被安全忽略，结论为 Process 且原因可解释。
func TestFrameDecodeUnknownFieldsSafelyIgnored(t *testing.T) {
	f := goodFrame()
	// 0x0100 属于协议未来字段段（非应用保留段 0x00xx）。
	f.Extensions = map[string][]byte{"0100": []byte("new")}
	raw, err := f.Encode()
	if err != nil {
		t.Fatal(err)
	}

	res, err := Decode(raw)
	if err != nil {
		t.Fatalf("future frame must be processable: %v", err)
	}
	if res.Decision != DecodeProcess {
		t.Fatalf("decision=%v, want process", res.Decision)
	}
	if res.Reason != ReasonUnknownFieldIgnored {
		t.Fatalf("reason=%q, want %q", res.Reason, ReasonUnknownFieldIgnored)
	}
	if len(res.IgnoredFields) != 1 || res.IgnoredFields[0] != 0x0100 {
		t.Fatalf("ignored=%v", res.IgnoredFields)
	}
	if res.Frame.MessageID != "m1" || string(res.Frame.Payload) != "hello" {
		t.Fatalf("known fields corrupted: %+v", res.Frame)
	}
}

// 应用层保留 tag（seq=0x0001）是已知字段，不应被标记为"未知字段"。
func TestFrameDecodeAppReservedTagKnown(t *testing.T) {
	f := goodFrame()
	f.Type = FrameDeliver
	f.PutSeq(42)
	raw, err := f.Encode()
	if err != nil {
		t.Fatal(err)
	}
	res, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if res.Reason != ReasonNone || len(res.IgnoredFields) != 0 {
		t.Fatalf("reason=%q ignored=%v", res.Reason, res.IgnoredFields)
	}
	seq, ok := res.Frame.Seq()
	if !ok || seq != 42 {
		t.Fatalf("seq=%d ok=%v", seq, ok)
	}
}

// 历史消息在新旧版本间仍应可读：手工构造一个不含 outcomes/extensions 区
// 的最小帧，模拟"字段只增不改"的旧形态，新版本必须读出全部已知字段。
func TestFrameDecodeOlderShapeReadable(t *testing.T) {
	var b []byte
	b = append(b, frameMagic0, frameMagic1, FrameV1, byte(FramePublish), 0, 0, 1)
	b = appendU16Str(b, "s1")
	b = appendU16Str(b, "m1")
	b = appendU16Str(b, "") // filter
	b = appendU16Str(b, "a.b")
	var l4 [4]byte
	binary.BigEndian.PutUint32(l4[:], 2)
	b = append(b, l4[:]...)
	b = append(b, 'o', 'k')
	// 旧形态没有 outcomes/extensions 区：以 checksum 直接结尾。
	var cb [4]byte
	binary.BigEndian.PutUint32(cb[:], crc32.Checksum(b, crc32.IEEETable))
	b = append(b, cb[:]...)

	res, err := Decode(b)
	if err != nil {
		t.Fatalf("legacy frame must be readable: %v reason=%s", err, res.Reason)
	}
	if res.Frame.Version != FrameV1 || res.Frame.Topic != "a.b" || string(res.Frame.Payload) != "ok" {
		t.Fatalf("legacy fields mismatch: %+v", res.Frame)
	}
}

func appendU16Str(b []byte, s string) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	b = append(b, l[:]...)
	return append(b, s...)
}

// RoundTrip：含投递结果与尝试次数的完整帧往返。
func TestFrameRoundTrip(t *testing.T) {
	f := Frame{
		Type:           FramePublishAck,
		Attempt:        3,
		SubscriptionID: "s1",
		MessageID:      "m9",
		Outcomes: []DeliveryOutcome{
			{SubscriptionID: "s1", State: DeliveryStateRejected, Reason: ReasonBacklogFullRejectNewest},
			{SubscriptionID: "s2", State: DeliveryStateDegraded, Reason: ReasonSlowConsumerDropOldest},
		},
	}
	raw, err := f.Encode()
	if err != nil {
		t.Fatal(err)
	}
	res, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecodeProcess || res.Reason != ReasonNone {
		t.Fatalf("decision=%d reason=%q", res.Decision, res.Reason)
	}
	got := res.Frame
	if got.Attempt != 3 || got.MessageID != "m9" || len(got.Outcomes) != 2 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Outcomes[0].Reason != ReasonBacklogFullRejectNewest ||
		got.Outcomes[1].State != DeliveryStateDegraded {
		t.Fatalf("outcomes mismatch: %+v", got.Outcomes)
	}
	if s := reasonString(got.Outcomes[0]); !strings.Contains(s, "backlog_full_reject_newest") {
		t.Fatalf("reason string=%s", s)
	}
}

func reasonString(o DeliveryOutcome) string { return string(o.Reason) }

func goodFrame() Frame {
	return Frame{
		Version:        FrameCurrentVersion,
		Type:           FramePublish,
		SubscriptionID: "s1",
		MessageID:      "m1",
		Topic:          "a.b",
		Payload:        []byte("hello"),
	}
}
