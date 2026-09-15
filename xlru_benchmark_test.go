package xlru

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkGetHit(b *testing.B) {
	for _, ttl := range []time.Duration{-1, time.Hour} {
		b.Run(ttl.String(), func(b *testing.B) {
			c := NewXLRUCache[string, testValue](8192, Option[string, testValue]{TTL: ttl})
			_ = c.Set("hot", 1)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := c.Get("hot"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetDistributed(b *testing.B) {
	const count = 16384
	c := NewXLRUCache[int64, testValue](count*4, Option[int64, testValue]{TTL: time.Hour})
	for key := range int64(count) {
		_ = c.Set(key, testValue(key))
	}
	var key int64
	b.ReportAllocs()
	for b.Loop() {
		key = (key + 7919) & (count - 1)
		if _, err := c.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	const count = 16384
	c := NewXLRUCache[int64, testValue](count*4, Option[int64, testValue]{TTL: time.Hour})
	for key := range int64(count) {
		_ = c.Set(key, testValue(key))
	}
	var workers atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := workers.Add(1) * 101
		for pb.Next() {
			key = (key + 7919) & (count - 1)
			if _, err := c.Get(key); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSetEviction(b *testing.B) {
	c := NewXLRUCache[int64, testValue](8192, Option[int64, testValue]{TTL: time.Hour})
	// Pre-fill so the timed portion measures replacement of resident slots.
	for key := range int64(16384) {
		_ = c.Set(key, testValue(key))
	}
	key := int64(16384)
	b.ReportAllocs()
	for b.Loop() {
		_ = c.Set(key, testValue(key))
		key++
	}
}

func BenchmarkSetString(b *testing.B) {
	c := NewXLRUCache[string, testValue](8192, Option[string, testValue]{TTL: time.Hour})
	keys := make([]string, 32768)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
		_ = c.Set(keys[i], testValue(i))
	}
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		_ = c.Set(keys[i], testValue(i))
		i = (i + 1) & (len(keys) - 1)
	}
}

// Includes touching every resident to force a complete SIEVE sweep on each eviction.
func BenchmarkSieveAllVisited(b *testing.B) {
	var s shard[int64, testValue]
	s.init(256)
	for key := range int64(256) {
		s.set(uint64(key)<<32, key, testValue(key), 0)
	}
	key := int64(256)
	b.ReportAllocs()
	for b.Loop() {
		for i := 1; i < len(s.nodes); i++ {
			s.nodes[i].visited = true
		}
		s.set(uint64(key)<<32, key, testValue(key), 0)
		key++
	}
}

func BenchmarkEvictExpiredEmpty(b *testing.B) {
	c := NewXLRUCache[int64, testValue](65536, Option[int64, testValue]{})
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.EvictExpired(4096)
	}
}
