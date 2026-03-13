package xlru

import (
	"time"
)

type LocalCacheDataHelper[T any] struct {
	data T
}

func (d *LocalCacheDataHelper[T]) Data() T {
	return d.data
}
func (d *LocalCacheDataHelper[T]) NeedSave() bool {
	return false
}
func (d *LocalCacheDataHelper[T]) SetNeedSave(needs bool) {

}

type LocalCache[K LruKey, V any] struct {
	*XLRUCache[K, *LocalCacheDataHelper[V]]
}

func NewLocalCache[K LruKey, V any](size int, Sliding bool, TTL time.Duration, loader func(key K) (V, error)) *LocalCache[K, V] {

	xloader := func(key K) (*LocalCacheDataHelper[V], error) {
		v, err := loader(key)
		return &LocalCacheDataHelper[V]{data: v}, err
	}

	opt := Option[K, *LocalCacheDataHelper[V]]{
		TTL:      TTL,
		OnLoader: xloader,
		Sliding:  Sliding,
	}

	XLRUCache := NewXLRUCache(size, opt)
	return &LocalCache[K, V]{
		XLRUCache,
	}
}

func (c *LocalCache[K, V]) Get(key K) (V, error) {
	v, err := c.XLRUCache.Get(key)
	return v.Data(), err
}

func (c *LocalCache[K, V]) Set(key K, value V) error {
	return c.XLRUCache.Set(key, &LocalCacheDataHelper[V]{
		data: value,
	})
}

func (c *LocalCache[K, V]) Delete(key K) error {
	return c.XLRUCache.Delete(key)
}
func (c *LocalCache[K, V]) Len() int {
	return c.XLRUCache.Len()
}
