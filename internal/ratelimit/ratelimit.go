// Package ratelimit implements per-key token-bucket rate limiters.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a single token bucket: tokens refill at rate per second up to
// burst capacity.
type Bucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time // overridden in tests
}

func newBucket(rate float64, burst int, now func() time.Time) *Bucket {
	return &Bucket{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   now(),
		now:    now,
	}
}

// Allow reports whether one token can be consumed now.
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Limiter holds a token bucket per key (e.g. per tool name).
type Limiter struct {
	mu      sync.Mutex
	rate    float64
	burst   int
	buckets map[string]*Bucket
	now     func() time.Time

	rejected int64
}

// New returns a Limiter with the given refill rate (tokens/sec) and burst.
func New(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:    rate,
		burst:   burst,
		buckets: map[string]*Bucket{},
		now:     time.Now,
	}
}

// Allow reports whether key may proceed.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		b = newBucket(l.rate, l.burst, l.now)
		l.buckets[key] = b
	}
	l.mu.Unlock()
	if !b.Allow() {
		l.mu.Lock()
		l.rejected++
		l.mu.Unlock()
		return false
	}
	return true
}

// Rejected returns the total number of rejected requests.
func (l *Limiter) Rejected() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rejected
}
