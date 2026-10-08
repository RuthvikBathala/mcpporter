package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLogRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	l.Log(Entry{
		Client:    "stdio",
		Tool:      "demo__echo",
		ArgsHash:  ArgsHash(map[string]any{"secret": "hunter2"}),
		Upstream:  "demo",
		LatencyMs: 12,
		Status:    "ok",
		Cache:     "miss",
	})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		t.Fatal("expected one log line")
	}
	line := sc.Text()
	if strings.Contains(line, "hunter2") {
		t.Error("raw secret leaked into audit log")
	}
	var e Entry
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if e.Tool != "demo__echo" || e.Status != "ok" || e.Cache != "miss" {
		t.Errorf("entry = %+v", e)
	}
	if e.Timestamp.IsZero() || time.Since(e.Timestamp) > time.Minute {
		t.Errorf("timestamp not set: %v", e.Timestamp)
	}
	if sc.Scan() {
		t.Error("expected exactly one line")
	}
}

func TestDiscardingLogger(t *testing.T) {
	l, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	// Must not panic or fail.
	l.Log(Entry{Tool: "x"})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestArgsHashStable(t *testing.T) {
	h1 := ArgsHash(map[string]any{"a": 1})
	h2 := ArgsHash(map[string]any{"a": 1})
	if h1 != h2 || len(h1) != 64 {
		t.Errorf("hash unstable: %q %q", h1, h2)
	}
}
