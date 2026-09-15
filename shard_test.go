package xlru

import (
	"math/rand/v2"
	"testing"
)

// Exercise a collision chain longer than an 8-bit probe distance, including wraparound.
func TestShardCollisionChain(t *testing.T) {
	var s shard[int64, int]
	s.init(600)
	hash := uint64(len(s.buckets)-2)<<32 | 7
	for i := range 600 {
		s.set(hash, int64(i), i, 0)
	}
	for i := range 600 {
		pos, ok := s.find(hash, int64(i))
		if !ok || s.nodes[s.buckets[pos].index].value != i {
			t.Fatalf("lost colliding key %d", i)
		}
	}
	for i := 0; i < 600; i += 2 {
		pos, _ := s.find(hash, int64(i))
		s.remove(s.buckets[pos].index)
	}
	for i := range 600 {
		_, ok := s.find(hash, int64(i))
		if ok != (i%2 == 1) {
			t.Fatalf("incorrect presence for key %d", i)
		}
	}
	for i := 600; i < 900; i++ {
		s.set(hash, int64(i), i, 0)
	}
	checkShard(t, &s)
}

func checkShard(t *testing.T, s *shard[int64, int]) {
	t.Helper()
	seen := make(map[uint32]bool)
	var previous uint32
	for index := s.nodes[0].next; index != 0; index = s.nodes[index].next {
		if seen[index] {
			t.Fatal("cycle in resident list")
		}
		seen[index] = true
		n := s.nodes[index]
		if !n.occupied || n.prev != previous {
			t.Fatal("broken resident links")
		}
		pos, ok := s.find(n.hash, n.key)
		if !ok || s.buckets[pos].index != index {
			t.Fatal("resident missing from hash table")
		}
		previous = index
	}
	if len(seen) != s.length || previous != s.nodes[0].prev {
		t.Fatal("incorrect list length or tail")
	}
	if s.hand != 0 && !seen[s.hand] {
		t.Fatal("hand points outside resident list")
	}
	for index := s.free; index != 0; index = s.nodes[index].next {
		if seen[index] || s.nodes[index].occupied {
			t.Fatal("free list overlaps residents or contains a cycle")
		}
		seen[index] = true
	}
	if len(seen) != len(s.nodes)-1 {
		t.Fatal("lost node slot")
	}
	count := 0
	for _, b := range s.buckets {
		if b.distance != 0 {
			count++
			if !s.nodes[b.index].occupied || s.nodes[b.index].hash != b.hash {
				t.Fatal("bucket points to wrong node")
			}
		}
	}
	if count != s.length {
		t.Fatal("incorrect bucket count")
	}
}

// This model uses a plain FIFO slice and a visited map, independently of the array lists.
func TestShardAgainstSieveModel(t *testing.T) {
	for _, capacity := range []int{1, 2, 7, 64} {
		var s shard[int64, int]
		s.init(capacity)
		values := make(map[int64]int)
		visited := make(map[int64]bool)
		var order []int64
		hand := 0
		rng := rand.New(rand.NewPCG(41, uint64(capacity)))
		hashOf := func(key int64) uint64 { return uint64(key%5)<<32 | uint64(key%3) }
		for step := range 10000 {
			key := int64(rng.IntN(capacity * 3))
			hash := hashOf(key)
			switch rng.IntN(4) {
			case 0, 1:
				old, replaced := values[key]
				if replaced {
					visited[key] = true
				} else {
					if len(order) == capacity {
						for visited[order[hand]] {
							visited[order[hand]] = false
							hand = (hand + 1) % len(order)
						}
						victim := order[hand]
						old, replaced = values[victim], true
						delete(values, victim)
						delete(visited, victim)
						order = append(order[:hand], order[hand+1:]...)
						if hand == len(order) {
							hand = 0
						}
					}
					order = append(order, key)
					visited[key] = false
				}
				values[key] = step
				got, displaced := s.set(hash, key, step, 0)
				if displaced != replaced || (displaced && got != old) {
					t.Fatalf("capacity=%d step=%d: displaced %d/%v, want %d/%v", capacity, step, got, displaced, old, replaced)
				}
			case 2:
				pos, ok := s.find(hash, key)
				if ok {
					s.nodes[s.buckets[pos].index].visited = true
					visited[key] = true
				}
			case 3:
				pos, ok := s.find(hash, key)
				if ok {
					s.remove(s.buckets[pos].index)
					delete(values, key)
					delete(visited, key)
					for i, k := range order {
						if k == key {
							order = append(order[:i], order[i+1:]...)
							if i < hand {
								hand--
							}
							break
						}
					}
				}
			}

			if hand >= len(order) {
				hand = 0
			}
			checkShard(t, &s)
			for k, want := range values {
				pos, ok := s.find(hashOf(k), k)
				if !ok || s.nodes[s.buckets[pos].index].value != want {
					t.Fatalf("model mismatch for key %d at step %d", k, step)
				}
			}
		}
	}
}
