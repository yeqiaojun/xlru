package xlru

import (
	"errors"
	"testing"
	"time"
)

func TestSharedClockAdvances(t *testing.T) {
	before := clockUpper.Load()
	deadline := time.Now().Add(time.Second)
	for clockUpper.Load() <= before {
		if time.Now().After(deadline) {
			t.Fatal("shared TTL clock stopped advancing")
		}
		time.Sleep(time.Millisecond)
	}
}

// Exercise the actual package-wide updater outside a synctest bubble.
func TestCachedClockExpiration(t *testing.T) {
	c := NewXLRUCache[int64, testValue](1, Option[int64, testValue]{TTL: 3 * clockInterval})
	_ = c.Set(1, 1)
	if _, err := c.Get(1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * clockInterval)
	if _, err := c.Get(1); !errors.Is(err, ErrorCacheEntryNotFound) {
		t.Fatalf("expired value survived clock updates: %v", err)
	}
}
