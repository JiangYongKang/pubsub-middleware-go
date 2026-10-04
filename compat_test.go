package pubsub

import (
	"testing"
)

// 兼容自检：自行生成一批早期格式（v1）的帧，用当前版本解码器读回，
// 内容与顺序必须原样还原——不能因为版本号不同就解不出来。
func TestLegacyBatchRoundTripSelfCheck(t *testing.T) {
	batch := []Frame{
		{Type: FrameSubscribe, SubscriptionID: "sub-a", Filter: "order.>"},
		{Type: FramePublish, MessageID: "m-1", Topic: "order.created", Payload: []byte(`{"id":1}`)},
		{Type: FrameDeliver, SubscriptionID: "sub-a", MessageID: "m-1", Topic: "order.created", Attempt: 1, Payload: []byte("v1-payload-1")},
		{Type: FrameAck, SubscriptionID: "sub-a", MessageID: "m-1"},
		{Type: FramePublish, MessageID: "m-2", Topic: "order.paid", Payload: []byte{}}, // 空 payload 也要还原
		{Type: FrameDeliver, SubscriptionID: "sub-a", MessageID: "m-2", Topic: "order.paid", Attempt: 3, Payload: []byte("redelivered")},
		{Type: FrameSubscribed, SubscriptionID: "sub-a", Filter: "order.>"},
	}

	// 公开自检入口：批量编码为 v1 再用当前解码器读回。
	if err := CheckLegacyCompat(batch, FrameV1); err != nil {
		t.Fatalf("legacy compat self-check failed: %v", err)
	}

	// 再逐帧验证：解码出的版本号确实是 v1，且顺序与内容完全一致。
	for i, f := range batch {
		raw, err := f.EncodeAs(FrameV1)
		if err != nil {
			t.Fatalf("frame %d encode: %v", i, err)
		}
		res, err := Decode(raw)
		if err != nil {
			t.Fatalf("frame %d must be readable by current decoder: %v", i, err)
		}
		if res.Decision != DecodeProcess || res.Reason != ReasonNone {
			t.Fatalf("frame %d decision=%v reason=%q, want clean process", i, res.Decision, res.Reason)
		}
		if res.Frame.Version != FrameV1 {
			t.Fatalf("frame %d version=%d, want v1", i, res.Frame.Version)
		}
		if !frameSemanticEqual(f, res.Frame) {
			t.Fatalf("frame %d mismatch: wrote %+v read %+v", i, f, res.Frame)
		}
		t.Logf("SELF-CHECK frame %d type=%d msg=%s: v1 bytes -> current decoder OK (content identical)",
			i, f.Type, f.MessageID)
	}
}

// v1 布局装不下 outcomes/extensions：编码必须明确报错，而不是静默丢数据。
func TestEncodeAsV1RejectsUnrepresentableFields(t *testing.T) {
	f := Frame{Type: FramePublishAck, MessageID: "m1",
		Outcomes: []DeliveryOutcome{{SubscriptionID: "s", State: DeliveryStateQueued}}}
	if _, err := f.EncodeAs(FrameV1); err == nil {
		t.Fatal("encoding outcomes as v1 must fail explicitly")
	}
	g := Frame{Type: FrameDeliver, MessageID: "m2"}
	g.SetExt(ExtKeySeq, []byte("7"))
	if _, err := g.EncodeAs(FrameV1); err == nil {
		t.Fatal("encoding extensions as v1 must fail explicitly")
	}
	// 超出支持范围的版本号同样明确拒绝。
	h := Frame{Type: FramePublish, MessageID: "m3"}
	if _, err := h.EncodeAs(FrameCurrentVersion + 1); err == nil {
		t.Fatal("encoding with unsupported version must fail")
	}
}

// 混合批次：v1 历史帧、带未知新字段的 v2 帧、带应用保留字段的 v2 帧、
// 以及真正损坏的帧交错出现。每一帧的判定结论必须确定且可区分：
// "安全忽略"（Process + unknown_field_ignored）与"必须拒绝"（Reject + 各自原因）不混淆。
func TestMixedBatchDecodeDecisions(t *testing.T) {
	type expectation struct {
		decision DecodeDecision
		reason   Reason
	}
	var batch [][]byte
	var wants []expectation
	add := func(raw []byte, d DecodeDecision, r Reason) {
		batch = append(batch, raw)
		wants = append(wants, expectation{d, r})
	}
	enc := func(raw []byte, err error) []byte {
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	// 1) v1 历史帧：干净处理。
	add(enc((Frame{Type: FramePublish, MessageID: "legacy-1", Topic: "a.b", Payload: []byte("old")}).EncodeAs(FrameV1)),
		DecodeProcess, ReasonNone)
	// 2) v2 帧 + 未知新字段（0x0100 段）：安全忽略，正常处理。
	f2 := Frame{Type: FramePublish, MessageID: "new-1", Topic: "a.b", Payload: []byte("new")}
	f2.SetExt("0100", []byte("future-field"))
	add(enc(f2.Encode()), DecodeProcess, ReasonUnknownFieldIgnored)
	// 3) v2 帧 + 应用保留字段（0x0001=seq）：已知字段，干净处理。
	f3 := Frame{Type: FrameDeliver, SubscriptionID: "s", MessageID: "d-1"}
	f3.PutSeq(9)
	add(enc(f3.Encode()), DecodeProcess, ReasonNone)
	// 4) 校验和损坏：必须拒绝。
	broken, err := (Frame{Type: FramePublish, MessageID: "bad-1", Topic: "a.b"}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	broken[len(broken)-1] ^= 0xFF
	add(broken, DecodeReject, ReasonChecksumMismatch)
	// 5) 版本过新：必须拒绝（与"未知字段可忽略"明确区分）。
	toNew, err := (Frame{Type: FramePublish, MessageID: "bad-2", Topic: "a.b"}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	toNew[2] = FrameCurrentVersion + 3
	add(fixChecksum(toNew), DecodeReject, ReasonUnsupportedVersion)
	// 6) 字段截断：必须拒绝。
	truncated, err := (Frame{Type: FramePublish, MessageID: "bad-3", Topic: "a.b", Payload: []byte("xyz")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	add(truncated[:len(truncated)-6], DecodeReject, ReasonChecksumMismatch) // 截断首先表现为校验失败

	ignored, rejected, processed := 0, 0, 0
	for i, raw := range batch {
		res, err := Decode(raw)
		w := wants[i]
		if res.Decision != w.decision || res.Reason != w.reason {
			t.Fatalf("frame %d: decision=%v reason=%q, want %v/%q (err=%v)",
				i, res.Decision, res.Reason, w.decision, w.reason, err)
		}
		switch {
		case w.decision == DecodeReject:
			if err == nil {
				t.Fatalf("frame %d: reject must come with an error", i)
			}
			rejected++
			t.Logf("DECISION frame %d: REJECT reason=%s (damaged/incomprehensible, distinct from ignorable)", i, res.Reason)
		case w.reason == ReasonUnknownFieldIgnored:
			ignored++
			t.Logf("DECISION frame %d: PROCESS with ignored future fields tags=%v (safe to ignore, NOT a reject)",
				i, res.IgnoredFields)
		default:
			processed++
			t.Logf("DECISION frame %d: PROCESS clean (msg=%s)", i, res.Frame.MessageID)
		}
	}
	if ignored == 0 || rejected == 0 || processed == 0 {
		t.Fatalf("batch must exercise all three outcomes: processed=%d ignored=%d rejected=%d",
			processed, ignored, rejected)
	}
}
