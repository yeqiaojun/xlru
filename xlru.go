package xlru

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"
)

var ErrorCacheEntryNotFound = errors.New("cache entry not found")

type LruKey interface {
	~string | ~int64
}

// DataAccessor reports whether a value needs persistence.
type DataAccessor interface {
	NeedSave() bool // Must be safe to call concurrently; callbacks own value synchronization.
}

// Option holds configuration for the XLRUCache.
// It is generic over the key type K and the value type V.
type Option[K LruKey, V DataAccessor] struct {
	TTL            time.Duration // Zero defaults to 24 hours; negative disables expiration.
	Sliding        bool          // cache item sliding
	Logger         Logger
	OnLoader       func(key K) (V, error)
	OnEvict        func(value V) error
	OnBatchSaver   func(value []V) error // The saver owns clearing dirty state after successful persistence.
	BatchSaveCount int
}

// XLRUCache is a sharded SIEVE cache with TTL and optional persistence.
// Options are copied at construction and remain private.
type XLRUCache[K LruKey, V DataAccessor] struct {
	data  *store[K, V]
	opt   Option[K, V]
	group singleflight.Group
}

// NewXLRUCache creates a new XLRUCache with the given options.
func NewXLRUCache[K LruKey, V DataAccessor](size int, opt Option[K, V]) *XLRUCache[K, V] {
	if opt.BatchSaveCount < 0 {
		panic("xlru: BatchSaveCount must not be negative")
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	opt.TTL = cmp.Or(opt.TTL, 24*time.Hour)
	opt.BatchSaveCount = cmp.Or(opt.BatchSaveCount, 1000)
	cache := newStore[K, V](size, opt.Sliding)

	return &XLRUCache[K, V]{
		data: cache,
		opt:  opt,
	}
}

// Peek returns a live value without touching the visited bit or sliding TTL.
func (c *XLRUCache[K, V]) Peek(key K) (V, bool) {
	return c.data.peek(key, false)
}

// Load coalesces concurrent loads of the same key and rechecks the cache first.
func (c *XLRUCache[K, V]) Load(key K) (value V, err error) {
	if c.opt.OnLoader == nil {
		return value, ErrorCacheEntryNotFound
	}
	// Read the underlying value so named keys with String methods cannot collide.
	keyValue := reflect.ValueOf(key)
	var loadKey string
	if keyValue.Kind() == reflect.String {
		loadKey = keyValue.String()
	} else {
		loadKey = strconv.FormatInt(keyValue.Int(), 10)
	}
	loaded, err, _ := c.group.Do(loadKey, func() (any, error) {
		value, err := c.get(key, false)
		if err == nil {
			return value, nil
		}
		value, err = c.opt.OnLoader(key)
		if err != nil {
			return nil, err
		}
		_ = c.Set(key, value)
		return value, nil
	})
	if err != nil {
		return value, err
	}
	return loaded.(V), nil
}

// Set installs value before saving a displaced dirty value. Save errors do not roll back the write.
func (c *XLRUCache[K, V]) Set(key K, value V) error {
	if old, displaced := c.data.set(key, value, c.opt.TTL); displaced {
		return c.onEvict(old, "displaced")
	}
	return nil
}

// Delete removes a value before saving it. Save errors do not restore the entry.
func (c *XLRUCache[K, V]) Delete(key K) error {
	if value, removed := c.data.delete(key); removed {
		return c.onEvict(value, "delete")
	}
	return nil
}

func (c *XLRUCache[K, V]) onEvict(value V, action string) error {
	if c.opt.OnEvict == nil || !value.NeedSave() {
		return nil
	}

	err := c.opt.OnEvict(value)
	if err != nil {
		c.logError("xlru on evict failed", "action", action, "error", err)
		return fmt.Errorf("XLRUCache %s %w", action, err)
	}
	return nil
}

func (c *XLRUCache[K, V]) Len() int {
	return int(c.data.stats().EntriesCount)
}

func (c *XLRUCache[K, V]) Stats() Stats {
	return c.data.stats()
}

// Capacity returns the configured total capacity across all shards.
func (c *XLRUCache[K, V]) Capacity() int { return c.data.capacity }

// BatchSave advances through at most ten batches of resident slots per call.
// This bounds scanning work even when the cache contains mostly clean values.
func (c *XLRUCache[K, V]) BatchSave() {
	if c.opt.OnBatchSaver == nil {
		return
	}
	limit := c.data.capacity
	if c.opt.BatchSaveCount <= limit/10 {
		limit = c.opt.BatchSaveCount * 10
	}
	keys := c.data.batchKeys(limit)
	if len(keys) != 0 {
		c.FlushToDB(keys)
	}
}

// FlushToDB saves all values in the cache.
func (c *XLRUCache[K, V]) FlushToDB(saveKeys []K) {
	if err := c.FlushToDBWithErr(saveKeys); err != nil {
		c.logError("xlru batch save failed", "error", err)
	}
}

// FlushToDBWithErr saves all values in the cache and returns the first error (if any).
func (c *XLRUCache[K, V]) FlushToDBWithErr(saveKeys []K) error {
	if c.opt.OnBatchSaver == nil {
		c.logError("xlru batch save skipped", "reason", "OnBatchSaver is nil")
		return nil
	}

	var firstErr error
	keys := saveKeys
	if len(saveKeys) == 0 {
		keys = make([]K, 0, c.data.capacity)
		keys = c.data.appendKeys(keys)
	}

	needSave := make([]V, 0, min(c.opt.BatchSaveCount, len(keys)))
	for _, k := range keys {
		v, ok := c.data.peek(k, true)
		if !ok {
			continue
		}

		if v.NeedSave() {
			needSave = append(needSave, v)
		}

		if len(needSave) >= c.opt.BatchSaveCount {
			if err := c.opt.OnBatchSaver(needSave); err != nil && firstErr == nil {
				firstErr = err
			}
			needSave = make([]V, 0, min(c.opt.BatchSaveCount, len(keys)))
		}
	}

	if len(needSave) > 0 {
		if err := c.opt.OnBatchSaver(needSave); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (c *XLRUCache[K, V]) logError(msg string, args ...any) {
	c.opt.Logger.Error(msg, args...)
}

// EvictReport describes progress even when saving a removed value fails.
type EvictReport struct {
	Scanned      int
	Evicted      int
	DirtyEvicted int
}

// EvictExpired checks at most scanLimit physical slots, including empty slots.
// It saves removed dirty values after releasing all cache locks. Nonpositive limits are a no-op.
func (c *XLRUCache[K, V]) EvictExpired(scanLimit int) (EvictReport, error) {
	values, scanned := c.data.takeExpired(scanLimit)
	report := EvictReport{Scanned: scanned, Evicted: len(values)}
	var firstErr error
	for _, value := range values {
		if !value.NeedSave() {
			continue
		}
		report.DirtyEvicted++
		if err := c.onEvict(value, "expired_scan"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return report, firstErr
}
