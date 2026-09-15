package xlru

import (
	"math"
	"strings"
	"testing"
)

type namedInt64 int64

func (namedInt64) String() string { return "same" }

func TestRuntimeHashNamedInt64(t *testing.T) {
	c := NewXLRUCache[namedInt64, testValue](16, Option[namedInt64, testValue]{TTL: -1})
	keys := []namedInt64{math.MinInt64, -1, 0, 1, math.MaxInt64}
	for i, key := range keys {
		_ = c.Set(key, testValue(i))
	}
	for i, key := range keys {
		value, err := c.Get(key)
		if err != nil || value != testValue(i) {
			t.Fatalf("hash mismatch for %d: %v %v", int64(key), value, err)
		}
	}

}

func TestRuntimeHashNamedString(t *testing.T) {
	c := NewXLRUCache[namedKey, testValue](16, Option[namedKey, testValue]{TTL: -1})
	keys := []namedKey{"", "a", "玩家", "a\x00b", namedKey(strings.Repeat("long key ", 256))}
	for i, key := range keys {
		_ = c.Set(key, testValue(i))
	}
	for i, key := range keys {
		value, err := c.Get(key)
		if err != nil || value != testValue(i) {
			t.Fatalf("hash mismatch for %q: %v %v", string(key), value, err)
		}
	}

}

// Keep allocation assertions separate: checkptr=2 intentionally forces converted
// pointers to the heap. Run this test with the normal compiler settings.
func TestGetAllocations(t *testing.T) {
	integers := NewXLRUCache[namedInt64, testValue](16, Option[namedInt64, testValue]{TTL: -1})
	strings := NewXLRUCache[namedKey, testValue](16, Option[namedKey, testValue]{TTL: -1})
	_ = integers.Set(1, 1)
	_ = strings.Set("key", 1)
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _ = integers.Get(1)
		_, _ = strings.Get("key")
	}); allocations != 0 {
		t.Fatalf("Get allocated: %v", allocations)
	}
}
