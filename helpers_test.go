package pubsub

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// logBus 收集代理与客户端日志，测试据此等待确定性事件；
// t.Logf 实时打印，单测日志中保留完整投递流水与判定依据。
type logBus struct {
	mu   sync.Mutex
	cond *sync.Cond
	all  []string
}

func newLogBus() *logBus {
	b := &logBus{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *logBus) logger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		line := sprintf(format, args...)
		t.Logf("%s", line)
		b.mu.Lock()
		b.all = append(b.all, line)
		b.cond.Broadcast()
		b.mu.Unlock()
	}
}

func countLines(lines []string, sub string) int {
	n := 0
	for _, l := range lines {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

func (b *logBus) count(sub string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return countLines(b.all, sub)
}

func (b *logBus) contains(sub string) bool { return b.count(sub) > 0 }

// waitFor 阻塞直到判定依据 sub 出现（带全局超时）。
func (b *logBus) waitFor(t *testing.T, sub string) {
	t.Helper()
	b.waitForN(t, sub, 1)
}

// waitForN 等待包含 sub 的日志累计出现 n 次。
func (b *logBus) waitForN(t *testing.T, sub string, n int) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		b.mu.Lock()
		for countLines(b.all, sub) < n {
			b.cond.Wait()
		}
		b.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-testTimeout():
		t.Fatalf("timeout waiting for %d logs like %q (got %d)", n, sub, b.count(sub))
	}
}

// deliverySeqsWithAttempt 解析代理流水 "DELIVER seq=N ... attempt=A"，
// 返回指定尝试次数下的 seq 序列，用于断言补投严格按原顺序。
func (b *logBus) deliverySeqsWithAttempt(attempt int) []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	var seqs []int
	for _, l := range b.all {
		i := strings.Index(l, "DELIVER seq=")
		if i < 0 {
			continue
		}
		var seq, a int
		if n, _ := fmtSscanf(l[i:], "DELIVER seq=%d", &seq); n != 1 {
			continue
		}
		j := strings.Index(l[i:], "attempt=")
		if j < 0 {
			continue
		}
		if n, _ := fmtSscanf(l[i+j:], "attempt=%d", &a); n != 1 || a != attempt {
			continue
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

type testSystem struct {
	tr     *LocalTransport
	broker *Broker
	logs   *logBus
}

func newTestSystem(t *testing.T, ctx context.Context, opts ...BrokerOption) *testSystem {
	t.Helper()
	bus := newLogBus()
	tr := NewLocalTransport()
	allOpts := append([]BrokerOption{WithBrokerLogger(bus.logger(t))}, opts...)
	broker := NewBroker(tr, allOpts...)
	serveDone := make(chan struct{})
	go func() {
		_ = broker.Serve(ctx)
		close(serveDone)
	}()
	t.Cleanup(func() {
		broker.Close()
		tr.Close()
		<-serveDone
	})
	return &testSystem{tr: tr, broker: broker, logs: bus}
}

func (s *testSystem) newClient(t *testing.T, id string, opts ...ClientOption) *Client {
	t.Helper()
	allOpts := append([]ClientOption{WithClientLogger(s.logs.logger(t))}, opts...)
	return NewClient(id, s.tr, allOpts...)
}

func sprintf(format string, args ...any) string {
	return goSprintf(format, args...)
}
