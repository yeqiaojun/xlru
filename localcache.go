package xlru

import "time"

// localValue keeps ordinary values in the node itself, without allocating a wrapper per entry.
type localValue[V any] struct{ data V }

func (localValue[V]) NeedSave() bool { return false }

// LocalCache provides the same storage and expiration policy for values without persistence.
type LocalCache[K LruKey, V any] struct{ cache *XLRUCache[K, localValue[V]] }

func NewLocalCache[K LruKey, V any](size int, sliding bool, ttl time.Duration, loader func(K) (V, error)) *LocalCache[K, V] {
	opt := Option[K, localValue[V]]{TTL: ttl, Sliding: sliding}
	if loader != nil {
		opt.OnLoader = func(key K) (localValue[V], error) {
			value, err := loader(key)
			return localValue[V]{data: value}, err
		}
	}
	return &LocalCache[K, V]{cache: NewXLRUCache(size, opt)}
}

func (c *LocalCache[K, V]) Get(key K) (V, error) {
	value, err := c.cache.Get(key)
	return value.data, err
}
func (c *LocalCache[K, V]) Peek(key K) (V, bool) {
	value, ok := c.cache.Peek(key)
	return value.data, ok
}
func (c *LocalCache[K, V]) Set(key K, value V) error {
	return c.cache.Set(key, localValue[V]{data: value})
}
func (c *LocalCache[K, V]) Delete(key K) error { return c.cache.Delete(key) }
func (c *LocalCache[K, V]) Len() int           { return c.cache.Len() }
func (c *LocalCache[K, V]) Capacity() int      { return c.cache.Capacity() }
func (c *LocalCache[K, V]) Stats() Stats       { return c.cache.Stats() }
func (c *LocalCache[K, V]) EvictExpired(limit int) (EvictReport, error) {
	return c.cache.EvictExpired(limit)
}
