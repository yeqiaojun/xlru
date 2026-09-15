package xlru

import (
	"sync"
	"unsafe"
)

// Index zero is the list sentinel and the empty hash bucket marker.
type node[K LruKey, V any] struct {
	key               K
	value             V
	hash              uint64
	expires           int64
	prev, next        uint32
	occupied, visited bool
}

type bucket struct {
	hash            uint64
	index, distance uint32
}

// All shard fields are protected by mu. The list runs oldest to newest.
type shard[K LruKey, V any] struct {
	mu         sync.Mutex
	nodes      []node[K, V]
	buckets    []bucket
	free, hand uint32
	length     int
	stats      Stats
}

func (s *shard[K, V]) init(capacity int) {
	s.nodes = make([]node[K, V], capacity+1)
	tableSize := 8
	for tableSize-tableSize/5 < capacity {
		tableSize *= 2
	}
	s.buckets = make([]bucket, tableSize)
	for i := 1; i <= capacity; i++ {
		s.nodes[i].next = uint32(i + 1)
	}
	s.nodes[capacity].next = 0
	s.free = 1
}

// nodeAt and bucketAt are used only with indices established by the table/list.
// The backing arrays are fixed for the lifetime of the shard; zero is a valid sentinel.
func (s *shard[K, V]) nodeAt(index uint32) *node[K, V] {
	return (*node[K, V])(unsafe.Add(unsafe.Pointer(unsafe.SliceData(s.nodes)), uintptr(index)*unsafe.Sizeof(node[K, V]{})))
}

func (s *shard[K, V]) bucketAt(index uint32) *bucket {
	return (*bucket)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(s.buckets)), uintptr(index)*unsafe.Sizeof(bucket{})))
}

// The high hash bits choose buckets; low bits have already selected the shard.
func (s *shard[K, V]) find(hash uint64, key K) (uint32, bool) {
	mask := uint32(len(s.buckets) - 1)
	pos := uint32(hash>>32) & mask
	for distance := uint32(1); ; distance++ {
		b := &s.buckets[pos]
		if b.distance < distance {
			return pos, false
		}
		if b.hash == hash && s.nodes[b.index].key == key {
			return pos, true
		}
		pos = (pos + 1) & mask
	}
}

func (s *shard[K, V]) insertBucket(hash uint64, index uint32) {
	mask := uint32(len(s.buckets) - 1)
	pos := uint32(hash>>32) & mask
	incoming := bucket{hash: hash, index: index, distance: 1}
	for {
		b := &s.buckets[pos]
		if b.distance == 0 {
			*b = incoming
			return
		}
		if b.distance < incoming.distance {
			*b, incoming = incoming, *b
		}
		incoming.distance++
		pos = (pos + 1) & mask
	}
}

func (s *shard[K, V]) remove(index uint32) V {
	n := s.nodes[index]
	pos, _ := s.find(n.hash, n.key)
	mask := uint32(len(s.buckets) - 1)
	for {
		next := (pos + 1) & mask
		b := s.buckets[next]
		if b.distance <= 1 {
			s.buckets[pos] = bucket{}
			break
		}
		b.distance--
		s.buckets[pos] = b
		pos = next
	}

	s.nodes[n.prev].next = n.next
	s.nodes[n.next].prev = n.prev
	if s.hand == index {
		s.hand = n.next
	}
	// Clear keys and values so deleted objects can be collected.
	s.nodes[index] = node[K, V]{next: s.free}
	s.free = index
	s.length--
	return n.value
}

func (s *shard[K, V]) victim() uint32 {
	for {
		if s.hand == 0 {
			s.hand = s.nodes[0].next
		}
		n := &s.nodes[s.hand]
		if !n.visited {
			return s.hand
		}
		n.visited = false
		s.hand = n.next
	}
}

func (s *shard[K, V]) set(hash uint64, key K, value V, expires int64) (old V, displaced bool) {
	s.stats.SetCalls++
	if pos, ok := s.find(hash, key); ok {
		n := &s.nodes[s.buckets[pos].index]
		old = n.value
		n.value, n.expires, n.visited = value, expires, true
		return old, true
	}
	if s.free == 0 {
		old = s.remove(s.victim())
		displaced = true
	}
	index := s.free
	s.free = s.nodes[index].next
	tail := s.nodes[0].prev
	s.nodes[index] = node[K, V]{key: key, value: value, hash: hash, expires: expires, prev: tail, occupied: true}
	s.nodes[tail].next = index
	s.nodes[0].prev = index
	s.insertBucket(hash, index)
	s.length++
	return old, displaced
}
