package pubsub

import (
	"fmt"
	"time"
)

func goSprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func fmtSscanf(str, format string, a ...any) (int, error) {
	return fmt.Sscanf(str, format, a...)
}

const testWaitTimeout = 3 * time.Second

func testTimeout() <-chan time.Time { return time.After(testWaitTimeout) }
