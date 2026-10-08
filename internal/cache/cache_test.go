package cache

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	c := New()
	c.Set("k", json.RawMessage(`{"ok":true}`), time.Minute)
	v, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(v) != `{"ok":true}` {
		t.Errorf("value = %s", v)
	}
	if _, ok := c.Get("missing"); ok {
		t.Error("expected miss")
	}
}

func TestExpiry(t *testing.T) {
	c := New()
	now := time.Now()
	c.now = func() time.Time { return now }
	c.Set("k", json.RawMessage(`1`), 10*time.Millisecond)
	now = now.Add(20 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Error("expected expiry")
	}
}

func TestKeyCanonicalization(t *testing.T) {
	k1 := Key("tool", map[string]any{"b": 2, "a": 1, "nested": map[string]any{"z": 1, "a": 2}})
	k2 := Key("tool", map[string]any{"a": 1, "b": 2, "nested": map[string]any{"a": 2, "z": 1}})
	if k1 != k2 {
		t.Errorf("keys differ for same args: %s vs %s", k1, k2)
	}
	k3 := Key("tool", map[string]any{"a": 1, "b": 3})
	if k1 == k3 {
		t.Error("keys equal for different args")
	}
	k4 := Key("other", map[string]any{"a": 1, "b": 2})
	if k1 == k4 {
		t.Error("keys equal for different tools")
	}
}

func TestStats(t *testing.T) {
	c := New()
	c.Set("k", json.RawMessage(`1`), time.Minute)
	c.Get("k")       // hit
	c.Get("missing") // miss
	s := c.Stats()
	if s.Hits != 1 || s.Misses != 1 || s.Size != 1 {
		t.Errorf("stats = %+v", s)
	}
}
