// Package audit implements append-only JSONL audit logging for every
// proxied tool call. Arguments are never logged raw — only a SHA-256 hash —
// so secrets passed as tool arguments can't leak into the log.
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Entry is one audited tool call.
type Entry struct {
	Timestamp time.Time `json:"ts"`
	Client    string    `json:"client"`
	Tool      string    `json:"tool"`
	ArgsHash  string    `json:"args_hash"`
	Upstream  string    `json:"upstream"`
	LatencyMs int64     `json:"latency_ms"`
	Status    string    `json:"status"` // ok | error | rate_limited
	Cache     string    `json:"cache"`  // hit | miss | bypass
}

// Logger appends entries to a JSONL file. The zero value discards entries
// (used when no audit path is configured).
type Logger struct {
	mu   sync.Mutex
	file *os.File
}

// New opens (creating if needed) the audit log at path. An empty path
// returns a discarding logger.
func New(path string) (*Logger, error) {
	if path == "" {
		return &Logger{}, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &Logger{file: f}, nil
}

// ArgsHash returns the hex SHA-256 of the canonical JSON encoding of args.
func ArgsHash(args map[string]any) string {
	raw, err := json.Marshal(args)
	if err != nil {
		return "unhashable"
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Log appends e to the audit log. It never fails the caller: write errors
// are swallowed because auditing must not break proxying.
func (l *Logger) Log(e Entry) {
	if l.file == nil {
		return
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	raw = append(raw, '\n')
	_, _ = l.file.Write(raw)
}

// Close flushes and closes the log file.
func (l *Logger) Close() error {
	if l.file == nil {
		return nil
	}
	return l.file.Close()
}
