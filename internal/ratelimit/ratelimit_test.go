package ratelimit

import (
	"testing"
	"time"
)

func TestBurstThenReject(t *testing.T) {
	l := New(1000, 3) // high refill so the test is deterministic
	for i := 0; i < 3; i++ {
		if !l.Allow("tool") {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if l.Allow("tool") {
		t.Error("4th request should be rejected")
	}
	if l.Rejected() != 1 {
		t.Errorf("rejected = %d", l.Rejected())
	}
}

func TestRefill(t *testing.T) {
	l := New(10, 1)
	now := time.Now()
	l.now = func() time.Time { return now }
	if !l.Allow("tool") {
		t.Fatal("first request should pass")
	}
	if l.Allow("tool") {
		t.Fatal("second request should be rejected")
	}
	now = now.Add(200 * time.Millisecond) // 2 tokens at 10/s
	if !l.Allow("tool") {
		t.Error("request after refill should pass")
	}
}

func TestPerKeyIsolation(t *testing.T) {
	l := New(1000, 1)
	if !l.Allow("a") || !l.Allow("b") {
		t.Fatal("different keys should have independent buckets")
	}
	if l.Allow("a") {
		t.Error("key a should be exhausted")
	}
}
