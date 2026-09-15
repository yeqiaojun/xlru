package benchmarks

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phuslu/lru"
	"github.com/placeholder/xlru"
)

type value int64

func (value) NeedSave() bool { return false }

func BenchmarkGetInt64(b *testing.B) {
	keys := make([]int64, 16384)
	for i := range keys {
		keys[i] = int64(i)
	}
	benchmarkGet(b, keys)
}

func BenchmarkGetString(b *testing.B) {
	keys := make([]string, 16384)
	for i := range keys {
		keys[i] = "player:" + strconv.Itoa(i)
	}
	benchmarkGet(b, keys)
}

func benchmarkGet[K xlru.LruKey](b *testing.B, keys []K) {
	for _, workload := range []string{"Hot", "Sliding", "Distributed", "Parallel", "Miss"} {
		b.Run(workload, func(b *testing.B) {
			for _, implementation := range []string{"phuslu", "xlru"} {
				b.Run(implementation, func(b *testing.B) {
					const capacity = 65536
					var get func(K) bool
					var set func(K)
					if implementation == "phuslu" {
						c := lru.NewTTLCache[K, value](capacity, lru.WithSliding[K, value](workload == "Sliding"))
						get = func(key K) bool { _, ok := c.Get(key); return ok }
						set = func(key K) { c.Set(key, 1, time.Hour) }
					} else {
						c := xlru.NewXLRUCache[K, value](capacity, xlru.Option[K, value]{TTL: time.Hour, Sliding: workload == "Sliding"})
						get = func(key K) bool { _, err := c.Get(key); return err == nil }
						set = func(key K) { _ = c.Set(key, 1) }
					}
					if workload == "Hot" || workload == "Sliding" {
						set(keys[0])
					} else if workload != "Miss" {
						for _, key := range keys {
							set(key)
						}
					}
					b.ReportAllocs()
					if workload == "Parallel" {
						var workers atomic.Uint64
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							index := int(workers.Add(1)) * 101
							for pb.Next() {
								index = (index + 7919) & (len(keys) - 1)
								if !get(keys[index]) {
									b.Error("unexpected miss")
									return
								}
							}
						})
						return
					}
					index := 0
					wantHit := workload != "Miss"
					for b.Loop() {
						if workload == "Distributed" {
							index = (index + 7919) & (len(keys) - 1)
						}
						if get(keys[index]) != wantHit {
							b.Fatal("unexpected cache result")
						}
					}
				})
			}
		})
	}
}
