package xlru

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/phuslu/lru"

	"golang.org/x/sync/singleflight"
)

var ErrorCacheEntryNotFound = fmt.Errorf("cache entry not found")

type LruKey interface {
	~string | ~int64
}

// DataAccessor is a generic interface for data access operations.
// The save method is intended to persist the data.
type DataAccessor interface {
	NeedSave() bool //本对象为nil也要返回false
}

// Option holds configuration for the XLRUCache.
// It is generic over the key type K and the value type V.
type Option[K LruKey, V DataAccessor] struct {
	TTL            time.Duration // cache item ttl, nanoseconds
	Sliding        bool          // cache item sliding
	Logger         Logger
	OnLoader       func(key K) (V, error)
	OnEvict        func(value V) error
	OnBatchSaver   func(value []V) error //批量save中，必须修改value 的 NeedSave返回值
	BatchSaveCount int
}

// XLRUCache is a generic LRU cache with TTL support.
type XLRUCache[K LruKey, V DataAccessor] struct {
	Data  *lru.TTLCache[K, V]
	Opt   Option[K, V]
	group *singleflight.Group
	Size  int
	index int32
}

// NewXLRUCache creates a new XLRUCache with the given options.
func NewXLRUCache[K LruKey, V DataAccessor](size int, opt Option[K, V]) *XLRUCache[K, V] {
	cache := lru.NewTTLCache(
		size,
		lru.WithSliding[K, V](opt.Sliding),
	)

	if opt.TTL == 0 {
		opt.TTL = time.Hour * 24
	}

	if opt.BatchSaveCount == 0 && opt.OnBatchSaver != nil {
		opt.BatchSaveCount = 1000
	}

	return &XLRUCache[K, V]{
		Data:  cache,
		Opt:   opt,
		Size:  size,
		group: &singleflight.Group{},
	}
}

// Get retrieves a value from the cache for the given key.
// If the value is not found, it loads it from the Load function and saves it to the cache.
func (c *XLRUCache[K, V]) Get(key K) (v V, err error) {
	v, state := c.Data.GetWithState(key)
	if state == lru.TTLStateHit {
		return v, nil
	}

	return c.Load(key)
}

// Peek retrieves a value from the cache for the given key without updating the LRU order.
func (c *XLRUCache[K, V]) Peek(key K) (v V, ok bool) {
	v, _, ok = c.Data.Peek(key)
	return
}

// Load loads a value for the given key from the data source. store load value to cache
func (c *XLRUCache[K, V]) Load(key K) (v V, err error) {
	if c.Opt.OnLoader == nil {
		return v, ErrorCacheEntryNotFound
	}

	nv, err, _ := c.group.Do(fmt.Sprintf("%v", key), func() (interface{}, error) {
		if v, state := c.Data.GetWithState(key); state == lru.TTLStateHit {
			return v, nil
		} else if state == lru.TTLStateExpired {
			if err := c.persistExpiredEntry(key, v); err != nil {
				return nil, err
			}
		}

		if v, state := c.Data.GetWithState(key); state == lru.TTLStateHit {
			return v, nil
		}

		v, err := c.Opt.OnLoader(key)
		if err != nil {
			return nil, err
		}
		if err := c.Set(key, v); err != nil {
			return nil, err
		}
		return v, nil
	})

	v, _ = nv.(V)
	return v, err
}

func (c *XLRUCache[K, V]) persistExpiredEntry(key K, value V) error {
	err := c.onEvict(value, "Expired Save Error")
	c.Data.Delete(key)
	return err
}

// Set sets a value in the cache for the given key.
func (c *XLRUCache[K, V]) Set(key K, value V) error {
	prev, rep := c.Data.Set(key, value, c.Opt.TTL)
	if !rep {
		return c.onEvict(prev, "Set Error")
	}

	return nil
}
func (c *XLRUCache[K, V]) Delete(key K) error {
	prev := c.Data.Delete(key)
	return c.onEvict(prev, "Remove Error")
}

func (c *XLRUCache[K, V]) onEvict(value V, action string) error {
	if !value.NeedSave() || c.Opt.OnEvict == nil {
		return nil
	}

	err := c.Opt.OnEvict(value)
	if err != nil {
		c.logError("xlru on evict failed", "action", action, "error", err)
		return fmt.Errorf("XLRUCache %s %w", action, err)
	}
	return nil
}

func (c *XLRUCache[K, V]) Len() int {
	return c.Data.Len()
}

func (c *XLRUCache[K, V]) Stats() lru.Stats {
	return c.Data.Stats()
}

func (c *XLRUCache[K, V]) BatchSave() {
	if c.Opt.OnBatchSaver == nil {
		return
	}
	keys := make([]K, 0, c.Size)
	keys = c.Data.AppendAllKeys(keys)

	// 如果keys数量较少，直接调用SaveAll
	if len(keys) <= 10*c.Opt.BatchSaveCount {
		c.FlushToDB(keys)
		return
	}

	batchCount := c.Size / (10 * c.Opt.BatchSaveCount)
	index := int(atomic.AddInt32(&c.index, 1) % int32(batchCount))

	batchSize := (len(keys) + batchCount - 1) / batchCount
	start := index * batchSize
	end := start + batchSize

	if start >= len(keys) {
		start = 0
		end = batchSize
	}

	if end > len(keys) {
		end = len(keys)
	}

	batchKeys := keys[start:end]
	c.FlushToDB(batchKeys)
}

// FlushToDB saves all values in the cache.
func (c *XLRUCache[K, V]) FlushToDB(saveKeys []K) {
	if err := c.FlushToDBWithErr(saveKeys); err != nil {
		c.logError("xlru batch save failed", "error", err)
	}
}

// FlushToDBWithErr saves all values in the cache and returns the first error (if any).
func (c *XLRUCache[K, V]) FlushToDBWithErr(saveKeys []K) error {
	if c.Opt.OnBatchSaver == nil {
		c.logError("xlru batch save skipped", "reason", "OnBatchSaver is nil")
		return nil
	}

	var firstErr error
	keys := saveKeys
	if len(saveKeys) == 0 {
		keys = make([]K, 0, c.Size)
		keys = c.Data.AppendAllKeys(keys)
	}

	needSave := make([]V, 0, c.Opt.BatchSaveCount)
	for _, k := range keys {
		v, _, ok := c.Data.Peek(k)
		if !ok {
			continue
		}

		if v.NeedSave() {
			needSave = append(needSave, v)
		}

		if len(needSave) >= c.Opt.BatchSaveCount {
			if err := c.Opt.OnBatchSaver(needSave); err != nil && firstErr == nil {
				firstErr = err
			}
			needSave = make([]V, 0, c.Opt.BatchSaveCount)
		}
	}

	if len(needSave) > 0 {
		if err := c.Opt.OnBatchSaver(needSave); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (c *XLRUCache[K, V]) logError(msg string, args ...any) {
	if c.Opt.Logger == nil {
		return
	}
	c.Opt.Logger.Error(msg, args...)
}
