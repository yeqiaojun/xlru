package xlru

import (
	"reflect"
	"unsafe"
)

type keyHasher[K LruKey] func(K, uintptr) uintptr

// Go explicitly preserves these linkname signatures for external users.
// Passing &key directly to a noescape declaration keeps the temporary key on
// the stack; no unsafe uintptr roundtrip or copied runtime type layout is needed.
//
//go:noescape
//go:linkname stringHash runtime.strhash
func stringHash(key unsafe.Pointer, seed uintptr) uintptr

//go:noescape
//go:linkname memoryHash runtime.memhash
func memoryHash(key unsafe.Pointer, seed, size uintptr) uintptr

func selectHasher[K LruKey]() keyHasher[K] {
	if reflect.TypeFor[K]().Kind() == reflect.String {
		return func(key K, seed uintptr) uintptr {
			return stringHash(unsafe.Pointer(&key), seed)
		}
	}
	return func(key K, seed uintptr) uintptr {
		return memoryHash(unsafe.Pointer(&key), seed, 8)
	}
}
