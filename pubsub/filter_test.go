package pubsub

import "testing"

func TestFilterMatch(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/+", "a/x", true},
		{"a/+", "a/x/y", false},
		{"a/#", "a/x/y", true},
		{"a/#", "a", true},
		{"+/b/+", "x/b/y", true},
		{"+/b/+", "x/c/y", false},
		{"#", "anything/here", true},
		{"a/b", "a/b/c", false},
	}
	for _, c := range cases {
		if got := NewFilter(c.pattern).Match(c.topic); got != c.want {
			t.Errorf("pattern=%q topic=%q want %v got %v", c.pattern, c.topic, c.want, got)
		}
	}
}
