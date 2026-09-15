package xlru

import (
	"time"
	"unsafe"
)

// Get returns a live value, loading a miss when OnLoader is configured.
// Expiration and capacity save failures are logged but do not fail a read.
func (c *XLRUCache[K, V]) Get(key K) (V, error) {
	return c.get(key, true)
}

// get shares the same read path with the singleflight recheck. Keeping the value
// and error return here avoids a second non-inlined wrapper on every cache hit.
func (c *XLRUCache[K, V]) get(key K, load bool) (value V, err error) {
	data := c.data
	hash := uint64(data.hasher(key, data.seed))
	if unsafe.Sizeof(uintptr(0)) == 4 {
		hash |= uint64(data.hasher(key, data.seed^0x9e3779b9)) << 32
	}
	// The mask is bounded by the fixed, power-of-two shard array.
	s := (*shard[K, V])(unsafe.Add(unsafe.Pointer(unsafe.SliceData(data.shards)), uintptr(hash&uint64(len(data.shards)-1))*unsafe.Sizeof(shard[K, V]{})))
	s.mu.Lock()
	s.stats.GetCalls++
	pos, ok := s.find(hash, key)
	if !ok {
		s.stats.Misses++
		s.mu.Unlock()
		if load && c.opt.OnLoader != nil {
			return c.Load(key)
		}
		return value, ErrorCacheEntryNotFound
	}
	index := s.bucketAt(pos).index
	n := s.nodeAt(index)
	if n.expires != 0 {
		now := clockUpper.Load()
		if now >= n.expires {
			now = time.Since(clockEpoch).Nanoseconds()
		}
		if now >= n.expires {
			s.stats.Misses++
			value = s.remove(index)
			s.mu.Unlock()
			_ = c.onEvict(value, "expired")
			return c.getMiss(key, load)
		}
		if data.sliding {
			n.expires = now + int64(c.opt.TTL)
		}
	}
	if !n.visited {
		n.visited = true
	}
	value = n.value
	s.mu.Unlock()
	return value, nil
}

func (c *XLRUCache[K, V]) getMiss(key K, load bool) (value V, err error) {
	if load && c.opt.OnLoader != nil {
		return c.Load(key)
	}
	return value, ErrorCacheEntryNotFound
}
