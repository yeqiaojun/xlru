package xlru

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/bluele/gcache"
	"github.com/phuslu/shardmap"
)

func legacyGet[K LruKey, V DataAccessor](cache *XLRUCache[K, V], key K) (v V, err error) {
	if v, ok := cache.Data.Get(key); ok {
		return v, nil
	}
	return cache.Load(key)
}

// TestItem 是用于基准测试的简单数据结构
type TestItem struct {
	Key      string
	Value    string
	needSave bool
}

func (t *TestItem) NeedSave() bool {
	if t == nil {
		return false
	}
	return t.needSave
}
func (t *TestItem) SetNeedSave(needs bool) {
	t.needSave = needs
}

func (t *TestItem) Save() error {
	return nil
}

// BenchmarkGetHit 测试缓存命中的情况
func BenchmarkGetHit(b *testing.B) {
	opt := Option[string, *TestItem]{
		OnLoader: func(key string) (*TestItem, error) {
			return &TestItem{key, key, false}, nil
		},
		OnBatchSaver: func(value []*TestItem) error {
			return nil
		},
	}
	cache := NewXLRUCache(1000000, opt)
	for i := 0; i < 1000000; i++ {
		key := strconv.Itoa(i)
		v, _ := cache.Get(key)
		v.SetNeedSave(true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// key := strconv.Itoa(i % 10000)
		// _, _ = cache.Get(key)
		cache.FlushToDB(nil)
	}
}

func BenchmarkGetHitLegacy(b *testing.B) {
	opt := Option[string, *TestItem]{
		OnLoader: func(key string) (*TestItem, error) {
			return &TestItem{key, key, false}, nil
		},
	}
	cache := NewXLRUCache(8192, opt)
	if err := cache.Set("hot", &TestItem{Key: "hot", Value: "hot", needSave: false}); err != nil {
		b.Fatalf("set failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := legacyGet(cache, "hot"); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGetHitCurrent(b *testing.B) {
	opt := Option[string, *TestItem]{
		OnLoader: func(key string) (*TestItem, error) {
			return &TestItem{key, key, false}, nil
		},
	}
	cache := NewXLRUCache(8192, opt)
	if err := cache.Set("hot", &TestItem{Key: "hot", Value: "hot", needSave: false}); err != nil {
		b.Fatalf("set failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get("hot"); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGetMissLegacy(b *testing.B) {
	opt := Option[int64, *TestItem]{
		OnLoader: func(key int64) (*TestItem, error) {
			return &TestItem{Key: strconv.FormatInt(key, 10), Value: "value", needSave: false}, nil
		},
	}
	cache := NewXLRUCache(1<<20, opt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := legacyGet(cache, int64(i)); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGetMissCurrent(b *testing.B) {
	opt := Option[int64, *TestItem]{
		OnLoader: func(key int64) (*TestItem, error) {
			return &TestItem{Key: strconv.FormatInt(key, 10), Value: "value", needSave: false}, nil
		},
	}
	cache := NewXLRUCache(1<<20, opt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get(int64(i)); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

// BenchmarkGetMiss 测试缓存未命中的情况
func BenchmarkGetMiss(b *testing.B) {
	opt := Option[int64, *TestItem]{
		OnLoader: func(key int64) (*TestItem, error) {
			return &TestItem{strconv.Itoa(int(key)), strconv.Itoa(int(key)), false}, nil
		},
	}
	cache := NewXLRUCache(8192, opt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//key := strconv.Itoa(i)
		_, _ = cache.Get(int64(i))
	}
}

func BenchmarkGetMiss1(b *testing.B) {
	m := make(map[int64]*TestItem)
	for i := 0; i < 1000000; i++ {
		m[int64(i)] = &TestItem{strconv.Itoa(i), strconv.Itoa(i), false}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//key := strconv.Itoa(i)
		_, _ = m[int64(i)%1000000]
	}
}

func BenchmarkGetMiss2(b *testing.B) {
	m := shardmap.New[int64, *TestItem](1000000)
	for i := 0; i < 1000000; i++ {
		m.Set(int64(i), &TestItem{strconv.Itoa(i), strconv.Itoa(i), false})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//key := strconv.Itoa(i)
		_, _ = m.Get(int64(i) % 1000000)
	}
}
func BenchmarkGetMiss3(b *testing.B) {
	m := gcache.New(1000000).LRU().Build()
	for i := 0; i < 1000000; i++ {
		m.Set(int64(i), &TestItem{strconv.Itoa(i), strconv.Itoa(i), false})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//key := strconv.Itoa(i)
		_, _ = m.Get(int64(i) % 1000000)
	}
}

func BenchmarkGetMiss4(b *testing.B) {
	opt := Option[int64, *TestItem]{

		OnBatchSaver: func(value []*TestItem) error {
			return nil
		},
	}
	cache := NewXLRUCache(1000000, opt)
	for i := 0; i < 1000000; i++ {
		cache.Set(int64(i), &TestItem{strconv.Itoa(i), strconv.Itoa(i), false})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, _ = cache.Get(int64(i) % 1000000)
		cache.BatchSave()
	}
}

// BenchmarkSaveAllMillion 测试1000000数据的SaveAll性能
func BenchmarkSaveAllMillion(b *testing.B) {
	opt := Option[int64, *TestItem]{
		OnBatchSaver: func(value []*TestItem) error {
			// 模拟保存操作
			for _, item := range value {
				item.SetNeedSave(false) // 保存后重置标记
			}
			return nil
		},
		BatchSaveCount: 1000, // 设置批处理大小
	}

	cache := NewXLRUCache(1000000, opt)

	// 预先填充1000000条数据，并标记为需要保存
	for i := int64(0); i < 1000000; i++ {
		item := &TestItem{
			Key:      strconv.FormatInt(i, 10),
			Value:    fmt.Sprintf("value_%d", i),
			needSave: true,
		}
		cache.Set(i, item)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.FlushToDB(nil) // 保存所有需要保存的数据
	}
}

// BenchmarkBatchSaveMillion 测试1000000数据的BatchSave性能
func BenchmarkBatchSaveMillion(b *testing.B) {
	opt := Option[int64, *TestItem]{
		OnBatchSaver: func(value []*TestItem) error {
			// 模拟保存操作
			for _, item := range value {
				item.SetNeedSave(false) // 保存后重置标记
			}
			return nil
		},
		BatchSaveCount: 1000, // 设置批处理大小
	}

	cache := NewXLRUCache(1000000, opt)

	// 预先填充1000000条数据，并标记为需要保存
	for i := int64(0); i < 1000000; i++ {
		item := &TestItem{
			Key:      strconv.FormatInt(i, 10),
			Value:    fmt.Sprintf("value_%d", i),
			needSave: true,
		}
		cache.Set(i, item)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		//cache.BatchSave() // 批量保存数据
		keys := make([]int64, 0, cache.Size+100)
		_ = cache.Data.AppendKeys(keys)
	}
}

// BenchmarkSet 测试Set操作，包含淘汰开销
func BenchmarkSet(b *testing.B) {
	opt := Option[string, *TestItem]{}
	cache := NewXLRUCache(8192, opt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		cache.Set(key, &TestItem{key, key, false})
	}
}

// BenchmarkGetParallel 测试并发读性能
func BenchmarkGetParallel(b *testing.B) {
	opt := Option[string, *TestItem]{
		OnLoader: func(key string) (*TestItem, error) {
			return &TestItem{key, key, false}, nil
		},
	}
	cache := NewXLRUCache(8192, opt)
	// 预先填充数据
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(i)
		_, _ = cache.Get(key)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := strconv.Itoa(i % 10000)
			_, _ = cache.Get(key)
			i++
		}
	})
}

// BenchmarkGetSetParallel 测试并发读写性能
func BenchmarkGetSetParallel(b *testing.B) {
	opt := Option[string, *TestItem]{
		OnLoader: func(key string) (*TestItem, error) {
			return &TestItem{key, key, false}, nil
		},
	}
	cache := NewXLRUCache(8192, opt)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := strconv.Itoa(i)
			// 80% 读, 20% 写
			if i%10 < 8 {
				_, _ = cache.Get(key)
			} else {
				cache.Set(key, &TestItem{key, key, false})
			}
			i++
		}
	})
}
