# xlru Standalone Design

**Context**

`D:\xlru` is a standalone extraction of `deps/xlru` from the game server repository. The extracted code still imports internal packages from the original monorepo, which prevents independent module builds and public publishing.

**Goals**

- Make `xlru` buildable as an independent Go module.
- Remove imports on `game/deps/xlog`, `game/deps/mongoclient`, and `game/deps/xsync`.
- Add a small logger abstraction and a `slog` adapter.
- Keep the public cache API small and compatible where practical.
- Preserve unit tests and Mongo integration tests without requiring the original project.

**Non-Goals**

- Reworking cache semantics or introducing a broader logging abstraction.
- Adding new production features unrelated to decoupling.
- Keeping monorepo-specific benchmark comparisons.

**Design**

1. Logger abstraction
   - Introduce a minimal `Logger` interface in the library with only the behavior needed by `xlru`.
   - Add a helper that logs only when a logger is configured.
   - Provide a small adapter that wraps `*slog.Logger` into the local `Logger` interface.

2. Option wiring
   - Extend `Option[K, V]` with an optional `Logger` field.
   - Replace direct `xlog.Errorf` calls with internal helper calls.
   - Keep existing cache error returns unchanged.

3. Module isolation
   - Add `go.mod` with only the dependencies required by this repository.
   - Keep the module path publish-ready; if the final GitHub path changes, only the module line should need updating.

4. Mongo integration tests
   - Replace `mongoclient` usage with direct `mongo-driver/v2` connection code.
   - Read `XLRU_MONGO_URI`, default to `mongodb://localhost:27017`, and skip tests if the server is unreachable.
   - Keep the tests as integration tests so standalone users can validate Mongo persistence behavior.

5. Benchmark cleanup
   - Remove monorepo-only benchmark cases that depend on `xsync`.
   - Retain benchmarks that exercise `xlru` and portable third-party comparisons already used in this repo.

**Testing**

- Run focused unit tests for cache and local cache behavior.
- Run the full package test suite.
- Mongo integration tests should skip automatically when MongoDB is unavailable.
- Run `go test ./...` as the completion gate.
