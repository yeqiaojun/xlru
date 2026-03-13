# xlru

Standalone generic LRU cache library for Go with TTL, lazy loading, dirty-write callbacks, batch flush support, and optional `slog` adapter integration.

## Highlights

- Generic cache API with `string` and `int64` keys
- TTL and sliding expiration support
- Singleflight-backed cache loading
- Optional eviction persistence and batch persistence hooks
- Minimal logger interface with `slog` adapter

## Logging

`xlru` does not depend directly on project-specific logging packages. If you use `log/slog`, wire it through:

```go
logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

cache := xlru.NewXLRUCache[string, *MyData](1024, xlru.Option[string, *MyData]{
	Logger: xlru.NewSlogAdapter(logger),
})
```

## Tests

- Run all tests: `go test ./...`
- Mongo integration tests use `XLRU_MONGO_URI`
- If `XLRU_MONGO_URI` is unset, tests default to `mongodb://localhost:27017`
- Mongo-dependent tests skip automatically when MongoDB is unavailable
