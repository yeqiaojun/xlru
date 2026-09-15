package xlru

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// Stats is a per-shard aggregate. EntriesCount includes expired residents.
type Stats struct {
	GetCalls     uint64
	SetCalls     uint64
	Misses       uint64
	EntriesCount uint64
}

type store[K LruKey, V any] struct {
	shards              []shard[K, V]
	seed                uintptr
	hasher              keyHasher[K]
	sliding             bool
	capacity            int
	saveMu              sync.Mutex
	saveShard, saveSlot int
	scanMu              sync.Mutex
	scanShard, scanSlot int
}

func newStore[K LruKey, V any](capacity int, sliding bool) *store[K, V] {
	if capacity <= 0 || uint64(capacity) > 1<<30 {
		panic("xlru: capacity must be between 1 and 1<<30")
	}
	// Small caches retain useful eviction choice; large caches spread lock contention.
	count := 1
	limit := min(256, max(1, capacity/256))
	for count < runtime.GOMAXPROCS(0)*16 && count*2 <= limit {
		count *= 2
	}
	c := &store[K, V]{shards: make([]shard[K, V], count), seed: uintptr(rand.Uint64()), hasher: selectHasher[K](), sliding: sliding, capacity: capacity, scanSlot: 1, saveSlot: 1}
	for i := range c.shards {
		size := capacity / count
		if i < capacity%count {
			size++
		}
		c.shards[i].init(size)
	}
	return c
}

func (c *store[K, V]) shardFor(key K) (*shard[K, V], uint64) {
	hash := uint64(c.hasher(key, c.seed))
	if unsafe.Sizeof(uintptr(0)) == 4 {
		hash |= uint64(c.hasher(key, c.seed^0x9e3779b9)) << 32
	}
	return &c.shards[hash&uint64(len(c.shards)-1)], hash
}

// resident allows persistence to include expired entries that are still stored.
func (c *store[K, V]) peek(key K, resident bool) (value V, ok bool) {
	s, hash := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	pos, ok := s.find(hash, key)
	if !ok {
		return value, false
	}
	n := &s.nodes[s.buckets[pos].index]
	if !resident && n.expires != 0 && time.Since(clockEpoch).Nanoseconds() >= n.expires {
		return value, false
	}
	return n.value, true
}

func (c *store[K, V]) set(key K, value V, ttl time.Duration) (V, bool) {
	s, hash := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	var expires int64
	if ttl > 0 {
		expires = time.Since(clockEpoch).Nanoseconds() + int64(ttl)
	}
	return s.set(hash, key, value, expires)
}

func (c *store[K, V]) delete(key K) (value V, ok bool) {
	s, hash := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	pos, ok := s.find(hash, key)
	if !ok {
		return value, false
	}
	return s.remove(s.buckets[pos].index), true
}

func (c *store[K, V]) stats() (stats Stats) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		stats.GetCalls += s.stats.GetCalls
		stats.SetCalls += s.stats.SetCalls
		stats.Misses += s.stats.Misses
		stats.EntriesCount += uint64(s.length)
		s.mu.Unlock()
	}
	return stats
}

func (c *store[K, V]) appendKeys(keys []K) []K {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for index := s.nodes[0].next; index != 0; index = s.nodes[index].next {
			keys = append(keys, s.nodes[index].key)
		}
		s.mu.Unlock()
	}
	return keys
}

// takeExpired scans each physical slot at most once per call, including empty slots.
func (c *store[K, V]) takeExpired(limit int) (removed []V, scanned int) {
	if limit <= 0 {
		return nil, 0
	}
	c.scanMu.Lock()
	defer c.scanMu.Unlock()
	limit = min(limit, c.capacity)
	for scanned < limit {
		s := &c.shards[c.scanShard]
		s.mu.Lock()
		count := min(limit-scanned, len(s.nodes)-c.scanSlot)
		now := time.Since(clockEpoch).Nanoseconds()
		for index := c.scanSlot; index < c.scanSlot+count; index++ {
			n := &s.nodes[index]
			if n.occupied && n.expires != 0 && now >= n.expires {
				removed = append(removed, s.remove(uint32(index)))
			}
		}
		s.mu.Unlock()
		scanned += count
		c.scanSlot += count
		if c.scanSlot == len(s.nodes) {
			c.scanSlot = 1
			c.scanShard = (c.scanShard + 1) % len(c.shards)
		}
	}
	return removed, scanned
}

// batchKeys has its own cursor so flushing does not alter expiration progress.
func (c *store[K, V]) batchKeys(limit int) []K {
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	keys := make([]K, 0, limit)
	for scanned := 0; scanned < limit; {
		s := &c.shards[c.saveShard]
		s.mu.Lock()
		count := min(limit-scanned, len(s.nodes)-c.saveSlot)
		for index := c.saveSlot; index < c.saveSlot+count; index++ {
			if n := &s.nodes[index]; n.occupied {
				keys = append(keys, n.key)
			}
		}
		s.mu.Unlock()
		scanned += count
		c.saveSlot += count
		if c.saveSlot == len(s.nodes) {
			c.saveSlot = 1
			c.saveShard = (c.saveShard + 1) % len(c.shards)
		}
	}
	return keys
}
