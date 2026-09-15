package xlru

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type testValue int

func (testValue) NeedSave() bool { return false }

func TestSieveVisitedAndPeek(t *testing.T) {
	c := NewXLRUCache[int64, testValue](3, Option[int64, testValue]{TTL: -1})
	for i := int64(1); i <= 3; i++ {
		_ = c.Set(i, testValue(i))
	}
	_, _ = c.Get(1)
	_, _ = c.Peek(2)
	_ = c.Set(4, 4)
	if _, ok := c.Peek(2); ok {
		t.Fatal("Peek must not protect the oldest unvisited entry")
	}
	if _, ok := c.Peek(1); !ok {
		t.Fatal("visited entry should survive one pass")
	}
	_ = c.Delete(3) // Remove the current hand, then reuse its slot.
	_ = c.Set(5, 5)
	_ = c.Set(6, 6)
	if _, ok := c.Peek(4); ok {
		t.Fatal("hand should continue to the next resident")
	}
}

func TestSieveAllVisited(t *testing.T) {
	c := NewXLRUCache[int64, testValue](3, Option[int64, testValue]{TTL: -1})
	for i := int64(1); i <= 3; i++ {
		_ = c.Set(i, testValue(i))
		_, _ = c.Get(i)
	}
	_ = c.Set(4, 4)
	if _, ok := c.Peek(1); ok {
		t.Fatal("full visited pass should clear bits and evict the oldest")
	}
	if c.Len() != 3 {
		t.Fatal("capacity changed")
	}
}

func TestTTLAndSliding(t *testing.T) {
	for _, sliding := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			c := NewXLRUCache[int64, testValue](2, Option[int64, testValue]{TTL: time.Second, Sliding: sliding})
			_ = c.Set(1, 7)
			time.Sleep(600 * time.Millisecond)
			if _, err := c.Get(1); err != nil {
				t.Fatal(err)
			}
			time.Sleep(400 * time.Millisecond)
			_, ok := c.Peek(1)
			if ok != sliding {
				t.Fatalf("sliding=%v: unexpected presence %v", sliding, ok)
			}
			time.Sleep(600 * time.Millisecond)
			if _, ok := c.Peek(1); ok {
				t.Fatal("Peek refreshed TTL or expiration boundary is wrong")
			}
			if c.Len() != 1 {
				t.Fatal("Peek should leave the expired resident for persistence")
			}
			if _, err := c.Get(1); !errors.Is(err, ErrorCacheEntryNotFound) {
				t.Fatal(err)
			}
			if c.Len() != 0 {
				t.Fatal("Get did not remove expired entry without loader")
			}
		})
	}
}

func TestExpirationScanBoundAndReentry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c *XLRUCache[int64, *MockDataAccessor]
		saveErr := errors.New("save failed")
		var saved int
		c = NewXLRUCache(7, Option[int64, *MockDataAccessor]{TTL: time.Second, OnEvict: func(v *MockDataAccessor) error {
			saved++
			// Both shard and scan locks must be released before callbacks.
			_, _ = c.EvictExpired(1)
			return saveErr
		}})
		_ = c.Set(1, &MockDataAccessor{needSave: true})
		_ = c.Set(2, &MockDataAccessor{needSave: false})
		time.Sleep(time.Second)
		report, err := c.EvictExpired(2)
		if report != (EvictReport{Scanned: 2, Evicted: 2, DirtyEvicted: 1}) || !errors.Is(err, saveErr) {
			t.Fatalf("report=%+v err=%v", report, err)
		}
		if c.Len() != 0 || saved != 1 {
			t.Fatal("failed save retained or retried an entry")
		}
		report, _ = c.EvictExpired(100)
		if report.Scanned != 7 {
			t.Fatalf("must scan each physical slot at most once: %+v", report)
		}
		report, _ = c.EvictExpired(0)
		if report.Scanned != 0 {
			t.Fatal("zero budget must be a no-op")
		}
	})
}

func TestExpiredSaveCannotDeleteReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c *XLRUCache[int64, *MockDataAccessor]
		replacement := &MockDataAccessor{Data: "new"}
		c = NewXLRUCache(2, Option[int64, *MockDataAccessor]{TTL: time.Second,
			OnEvict: func(*MockDataAccessor) error { return c.Set(1, replacement) },
			OnLoader: func(int64) (*MockDataAccessor, error) {
				t.Fatal("replacement must be rechecked before loading")
				return nil, nil
			},
		})
		_ = c.Set(1, &MockDataAccessor{needSave: true})
		time.Sleep(time.Second)
		got, err := c.Get(1)
		if got != replacement || err != nil {
			t.Fatalf("lost replacement: %v %v", got, err)
		}
	})
}

func TestReplaceSaveAndLoadSaveFailure(t *testing.T) {
	logger := &testLogger{}
	saveErr := errors.New("save failed")
	var saved []*MockDataAccessor
	c := NewXLRUCache(1, Option[int64, *MockDataAccessor]{Logger: logger,
		OnEvict:  func(v *MockDataAccessor) error { saved = append(saved, v); return saveErr },
		OnLoader: func(int64) (*MockDataAccessor, error) { return &MockDataAccessor{Data: "loaded"}, nil },
	})
	old := &MockDataAccessor{Data: "old", needSave: true}
	_ = c.Set(1, old)
	if err := c.Set(1, &MockDataAccessor{needSave: true}); !errors.Is(err, saveErr) {
		t.Fatal(err)
	}
	if len(saved) != 1 || saved[0] != old {
		t.Fatal("replacement did not save old value")
	}
	value, err := c.Get(2)
	if err != nil || value.Data != "loaded" || len(saved) != 2 || logger.Count() != 2 {
		t.Fatalf("load must survive logged capacity save error: %v %v", value, err)
	}
}

type namedKey string

func (namedKey) String() string { return "same" }

func TestNamedKeysDoNotShareFlights(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := NewXLRUCache(8, Option[namedKey, testValue]{OnLoader: func(key namedKey) (testValue, error) {
			calls.Add(1)
			time.Sleep(time.Second)
			return testValue(key[0]), nil
		}})
		var wg sync.WaitGroup
		for _, key := range []namedKey{"a", "b", "a", "b"} {
			wg.Go(func() {
				v, err := c.Get(key)
				if err != nil || v != testValue(key[0]) {
					t.Errorf("wrong flight result: %v %v", v, err)
				}
			})
		}
		wg.Wait()
		if calls.Load() != 2 {
			t.Fatalf("want two flights, got %d", calls.Load())
		}
	})
}

func TestConcurrentCacheOperations(t *testing.T) {
	c := NewXLRUCache[int64, testValue](1024, Option[int64, testValue]{TTL: -1})
	var wg sync.WaitGroup
	for worker := range 12 {
		wg.Go(func() {
			for i := range 3000 {
				key := int64((i*17 + worker*101) % 4096)
				switch i % 6 {
				case 0, 1:
					_ = c.Set(key, testValue(key))
				case 2:
					v, err := c.Get(key)
					if err == nil && v != testValue(key) {
						t.Errorf("wrong value %d for %d", v, key)
					}
				case 3:
					_ = c.Delete(key)
				case 4:
					_, _ = c.EvictExpired(17)
				case 5:
					_ = c.Stats()
					_, _ = c.Peek(key)
				}
			}
		})
	}
	wg.Wait()
	if c.Len() > c.Capacity() {
		t.Fatal("capacity exceeded")
	}
}

func TestLocalCacheMissReturnsError(t *testing.T) {
	c := NewLocalCache[int64, string](2, false, time.Second, nil)
	if value, err := c.Get(1); value != "" || !errors.Is(err, ErrorCacheEntryNotFound) {
		t.Fatalf("got %q %v", value, err)
	}
}

func TestExpirationScanAcrossShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewXLRUCache[int64, testValue](1025, Option[int64, testValue]{TTL: time.Second})
		for key := range int64(200) {
			_ = c.Set(key, testValue(key))
		}
		if c.Len() != 200 || c.Capacity() != 1025 {
			t.Fatal("incorrect capacity or unexpected displacement")
		}
		time.Sleep(time.Second)
		scanned, evicted := 0, 0
		for scanned < 1025 {
			report, err := c.EvictExpired(min(17, 1025-scanned))
			if err != nil || report.Scanned > 17 {
				t.Fatalf("bad scan: %+v %v", report, err)
			}
			scanned += report.Scanned
			evicted += report.Evicted
		}
		if scanned != 1025 || evicted != 200 || c.Len() != 0 {
			t.Fatalf("scan lost residents: scanned=%d evicted=%d", scanned, evicted)
		}
	})
}

func TestBatchSaveSkipsEmptyWindowsAndAdvances(t *testing.T) {
	var saved int
	c := NewXLRUCache(100, Option[int64, *MockDataAccessor]{BatchSaveCount: 1,
		OnBatchSaver: func(values []*MockDataAccessor) error {
			if len(values) > 1 {
				t.Fatal("batch size exceeded")
			}
			for _, v := range values {
				saved++
				v.needSave = false
			}
			return nil
		},
	})
	for key := range int64(90) {
		_ = c.Set(key, &MockDataAccessor{needSave: true})
	}
	for key := range int64(80) {
		_ = c.Delete(key)
	}
	for range 8 {
		c.BatchSave()
	}
	if saved != 0 {
		t.Fatal("empty window incorrectly flushed the whole cache")
	}
	c.BatchSave()
	if saved != 10 {
		t.Fatalf("cursor did not reach occupied slots: %d", saved)
	}
	for range 10 {
		c.BatchSave()
	}
	if saved != 10 {
		t.Fatal("clean residents were saved again")
	}
}

func TestInvalidConfigurationPanics(t *testing.T) {
	for _, size := range []int{-1, 0} {
		t.Run("capacity"+strconv.Itoa(size), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid capacity must fail at construction")
				}
			}()
			NewXLRUCache[int64, testValue](size, Option[int64, testValue]{})
		})
	}
	t.Run("batch", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("negative batch size must fail at construction")
			}
		}()
		NewXLRUCache[int64, testValue](1, Option[int64, testValue]{BatchSaveCount: -1})
	})
}

func TestLocalCachePeekReturnsValue(t *testing.T) {
	c := NewLocalCache[int64, string](2, false, -1, nil)
	_ = c.Set(1, "one")
	value, ok := c.Peek(1)
	if !ok || value != "one" {
		t.Fatalf("unexpected peek: %q %v", value, ok)
	}
}
