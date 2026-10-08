// Package cache implements a small in-memory TTL cache for tool-call
// results. Keys are derived from the tool name and a canonical hash of the
// arguments, so identical calls hit the cache regardless of argument order.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// Cache is a concurrency-safe TTL cache of raw JSON results.
type Cache struct {
	mu    sync.Mutex
	items map[string]entry
	// now is overridden in tests.
	now func() time.Time

	hits   int64
	misses int64
}

type entry struct {
	value     json.RawMessage
	expiresAt time.Time
}

// New returns an empty cache.
func New() *Cache {
	return &Cache{items: map[string]entry{}, now: time.Now}
}

// Key builds a cache key from a tool name and its arguments. Arguments are
// canonicalized (sorted keys) so {"a":1,"b":2} and {"b":2,"a":1} map to the
// same key.
func Key(tool string, args map[string]any) string {
	canonical, err := canonicalJSON(args)
	if err != nil {
		// Fall back to an unhashable marker; the call simply won't be cached
		// usefully but nothing breaks.
		return tool + ":unhashable"
	}
	sum := sha256.Sum256(canonical)
	return tool + ":" + hex.EncodeToString(sum[:])
}

// canonicalJSON marshals v with sorted map keys, recursively.
func canonicalJSON(v any) ([]byte, error) {
	return json.Marshal(canonicalValue(v))
}

func canonicalValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(t))
		for _, k := range keys {
			out[k] = canonicalValue(t[k])
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = canonicalValue(e)
		}
		return out
	default:
		return v
	}
}

// Get returns the cached value for key, or false on miss/expiry.
func (c *Cache) Get(key string) (json.RawMessage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	if c.now().After(e.expiresAt) {
		delete(c.items, key)
		c.misses++
		return nil, false
	}
	c.hits++
	return e.value, true
}

// Set stores value under key for ttl.
func (c *Cache) Set(key string, value json.RawMessage, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = entry{value: value, expiresAt: c.now().Add(ttl)}
}

// Stats returns cache hit/miss counters and current size.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Hits: c.hits, Misses: c.misses, Size: len(c.items)}
}

// Stats is a snapshot of cache counters.
type Stats struct {
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`
	Size   int   `json:"size"`
}
