# xlru

Go 1.27 cache with sharded SIEVE eviction, TTL, coalesced loading, and optional dirty-value persistence. The storage engine uses only the standard library; it does not depend on another LRU implementation.

## Storage

- Construction selects Go runtime's `strhash` or the eight-byte `memhash` path once for `string` or `int64` keys, including named types. Reads hash once per storage operation on 64-bit targets. The bridge lives in `hash.go`; it avoids repeated generic type-hasher lookup and copying the private `abi.MapType` layout. On 32-bit targets, two seeded hashes form the 64-bit table hash.
- Shards use independent mutexes and Robin Hood hash tables. A bucket stores the hash, probe distance, and node index. Probe distances use 32 bits rather than an 8-bit packed field.
- Nodes live in preallocated arrays. An indexed insertion-order list, a free list, a visited bit, and an eviction hand implement SIEVE. Hits set the visited bit without moving nodes. Replacing an existing key also marks it visited.
- Capacity is split across shards, with at least 256 slots per shard when multiple shards are used. The shard count is a power of two, capped at 256 and scaled with `GOMAXPROCS` at construction. Small caches use one shard. The sum of shard capacities equals `Capacity()`; a hot shard can evict while others have space.
- Capacity eviction clears visited bits until it finds an unvisited victim. One eviction can inspect the whole shard and revisit its first node. There is no global LRU ordering.
- The Get path uses narrowly scoped `unsafe.Add` for shard/node indices established by the fixed arrays and hash table. The hash bridge uses runtime-preserved `go:linkname` signatures. Selected functions accept keys by value and pass stack addresses directly to declared non-retaining runtime calls; no uintptr pointer roundtrip is used. It is validated on Go 1.27 and must be revalidated when upgrading Go.
- One package-wide goroutine refreshes a monotonic clock estimate every 10 ms. Ordinary TTL hits read an atomic value; near-deadline reads, `Peek`, writes, and expiration scans use exact monotonic time. The cached estimate looks one tick ahead to make near-deadline reads take the exact path. Scheduler stalls can still delay expiration observation until the updater runs. Sliding hits use the estimate and may extend the deadline by up to roughly one tick during normal scheduling. The clock is shared by all caches and lives for the process lifetime.

## Ordinary values

```go
cache := xlru.NewLocalCache[string, string](4096, false, time.Minute, nil)
_ = cache.Set("name", "Alice")
value, ok := cache.Peek("name")
value, err := cache.Get("name")
```

`LocalCache` offers `Get`, `Peek`, `Set`, `Delete`, `Len`, `Capacity`, `Stats`, and `EvictExpired`. Values are stored directly without a separately allocated wrapper. A miss without a loader returns `ErrorCacheEntryNotFound` and the zero value.

## Values with persistence

Use `NewXLRUCache[K, V](capacity, Option[K, V]{...})` for values implementing `NeedSave() bool`.

- `OnLoader` loads missing values. Concurrent loads of the same key share one result through `golang.org/x/sync/singleflight`. Loader errors reach callers.
- `OnEvict` attempts one save of a removed dirty value, including replacement, deletion, expiration, and capacity eviction. It is optional.
- `OnBatchSaver` saves resident dirty values in batches. It owns synchronizing values and clearing their dirty state after successful persistence. `BatchSaveCount` defaults to 1000; negative values panic at construction.
- `Logger` accepts `*slog.Logger` directly and defaults to `slog.Default()`.

All loader, saver, and logger calls run outside storage locks. `NeedSave()` must be safe to call concurrently; the cache protects its own structure, not fields inside returned values. Values must support `NeedSave`; nil interface values are not supported.

Removal happens before saving. A failed save does not restore an entry, retain retry state, or block a later load. `Set` and `Delete` return save errors after applying the mutation. `Get` and `Load` log eviction save errors and continue. Concurrent callbacks for the same value/key are possible. Load coalescing does not serialize loads with explicit `Set`/`Delete` or guarantee save-before-load ordering across goroutines; a late load may overwrite a concurrent write. Persistence ordering and recovery belong to the caller.

## TTL and maintenance

`TTL == 0` defaults to 24 hours. A negative TTL disables expiration. A positive TTL applies on insert/replacement; with `Sliding`, a successful `Get` refreshes the deadline.

- `Peek` checks TTL without refreshing the deadline or marking a visit. It leaves expired residents for later cleanup or persistence.
- `Get` atomically removes an expired entry, saves it if needed, and follows the missing-value path.
- `EvictExpired(scanLimit)` advances a physical-slot cursor and returns `EvictReport` plus the first save error. It checks at most `min(scanLimit, Capacity())` slots, including empty ones. A nonpositive limit is a no-op. The budget limits slots, not lock wait time, hash probe work, or saver duration.
- `BatchSave()` scans at most `10 * BatchSaveCount` resident slots per call using a separate cursor, then saves dirty values in batches. Errors are logged.
- `FlushToDBWithErr(keys)` saves the selected resident dirty values and returns the first batch error while continuing later batches. Nil or empty keys select all current resident keys. `FlushToDB(keys)` logs that error instead. Both require `OnBatchSaver`.

Batch/explicit flush includes expired values that are still resident. Snapshotting is per shard, not a transactional global snapshot; concurrent mutation can change which values are saved. Schedule maintenance and a final flush in the application lifecycle.

`Len` and `Stats.EntriesCount` include expired residents. Statistics are aggregated shard by shard; `GetCalls`/`Misses` count internal read probes, including load rechecks. Configuration is copied at construction. Capacity must be in `[1, 1<<30]` and is allocated eagerly.

## Changes from the former wrapper

- Strict LRU is replaced by per-shard SIEVE.
- The public third-party `Data` field and mutable `Opt`/`Size` fields are removed. Use `Capacity()` and this package's `Stats`.
- `LocalCacheDataHelper` and its no-op dirty setter are removed. `LocalCache.Peek` returns the user's value directly.
- Deadlines retain nanosecond duration resolution. Get uses a shared cached clock estimate for ordinary hits and exact monotonic checks near expiration.

## Validation

```sh
go test ./...
go test -race ./...
go test -gcflags=all=-d=checkptr=2 -skip '^TestGetAllocations$' ./...
go vet ./...
go test -run '^$' -bench . -benchmem
```

`checkptr=2` deliberately forces converted pointers to the heap, so the allocation-only test runs with normal compiler settings instead. All functional tests still run under checkptr.

Tests cover SIEVE against an independent model, collision chains beyond 255 probes, backward-shift deletion, slot reuse, TTL/sliding behavior, load coalescing, save failure/reentry, bounded scanning, and concurrent access. TTL tests use `testing/synctest` instead of real sleeps.

The Mongo driver is used only by integration tests. Those tests use `XLRU_MONGO_URI` (default `mongodb://localhost:27017`), create/drop dedicated test collections, and skip when MongoDB is unavailable. They do not start MongoDB. Run `go test -v ./...` to inspect whether they ran or skipped.

## Get performance gate

`benchmarks/` is a separate Go module. Its phuslu dependency is only a reference for benchmarks and is not part of this library's module or runtime dependencies. The reference is the same phuslu fork/version used before this rewrite (`github.com/yeqiaojun/lru v1.0.22`). The benchmark calls its `TTLCache.Get` directly, without the old xlru wrapper.

```sh
cd benchmarks
go test -run '^$' -bench . -benchmem -benchtime=1s -count=7 > results.txt
python3 check.py results.txt
```

The checker rejects any workload whose median Get time exceeds phuslu by more than 5%. Cases cover int64/string keys, single-hot-key hits, sliding TTL hits, distributed hits, parallel hits, and misses without a loader. Both implementations use identical inputs, capacity, TTL, and benchmark call adapters. Hot/Sliding cases contain one key, Distributed/Parallel contain 16,384 keys, and Miss starts empty; the latter two read workloads cover collisions and locality with many residents. Run without concurrent builds or tests; nanosecond measurements are sensitive to scheduling noise. This is a performance regression gate for these workloads, not a promise about every key distribution or production tail latency.

Large-scale Mongo stress testing remains opt-in through `RUN_XLRU_LARGE_SCALE=1`.

### Verified result (Apple M4, Go 1.27.0, darwin/arm64)

The recorded benchmark log is `benchmarks/get-results.txt`: seven samples of 600 ms per case, with the default `GOMAXPROCS=10`. All ten median comparisons pass the 5% regression limit; the largest slowdown is 2.05% (int64 miss). All measured operations allocate zero bytes.

- int64 hot hit: phuslu 6.942 ns → xlru 7.028 ns (+1.24%).
- string hot hit: phuslu 9.041 ns → xlru 8.404 ns (−7.05%).
- int64 distributed/parallel hits: 22.410/16.870 ns → 12.980/11.630 ns.
- string distributed/parallel hits: 26.880/17.140 ns → 18.620/12.490 ns.

The former 21.5 ns hot TTL path paid for `time.Since` on every read, deferred unlock, two non-inlined read wrappers, and generic per-call hasher lookup. Get now uses the shared clock estimate, explicit unlock, one read implementation shared with load rechecks, and a hasher selected at construction. Already-visited SIEVE nodes are not written again. Probe lookup remains ordinary Go code because an unsafe rewrite increased its compiler inline cost and performed worse; unsafe access is limited to established shard/node indices and the hash bridge.
