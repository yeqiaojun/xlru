# xlru Standalone Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `xlru` an independently buildable Go module by removing monorepo-only dependencies, adding a minimal logger abstraction with a `slog` adapter, and keeping tests portable.

**Architecture:** The library will own a tiny logger interface and optional logger wiring through `Option`. Internal monorepo logging and Mongo helpers will be replaced with local helpers and direct `mongo-driver` usage. The module file will include only the dependencies needed by the extracted package and tests.

**Tech Stack:** Go modules, generics, `github.com/phuslu/lru`, `golang.org/x/sync/singleflight`, `log/slog`, `go.mongodb.org/mongo-driver/v2`

---

### Task 1: Add module metadata and capture current failures

**Files:**
- Create: `D:/xlru/go.mod`
- Create: `D:/xlru/go.sum`

**Step 1: Write the failing test**

Use the existing package tests as the failure surface.

**Step 2: Run test to verify it fails**

Run: `go test ./...`
Expected: FAIL because `go.mod` is missing or imports refer to monorepo-only packages.

**Step 3: Write minimal implementation**

Create `go.mod` with the standalone module declaration and direct dependencies required by the package and tests.

**Step 4: Run test to verify progress**

Run: `go test ./...`
Expected: FAIL on unresolved internal imports that still need code changes.

### Task 2: Replace internal logging dependency with local logger support

**Files:**
- Create: `D:/xlru/logger.go`
- Create: `D:/xlru/logger_slog.go`
- Modify: `D:/xlru/xlru.go`

**Step 1: Write the failing test**

Add a small unit test that configures a logger and verifies cache paths can log without panicking.

**Step 2: Run test to verify it fails**

Run: `go test ./...`
Expected: FAIL because the logger abstraction is not yet present.

**Step 3: Write minimal implementation**

Add a minimal `Logger` interface, add `Option.Logger`, replace `xlog` calls with guarded helper methods, and add an adapter for `*slog.Logger`.

**Step 4: Run test to verify it passes**

Run: `go test ./...`
Expected: PASS for logger-related tests, with remaining failures limited to other internal dependencies.

### Task 3: Remove monorepo Mongo helper dependency from tests

**Files:**
- Modify: `D:/xlru/xlru_test.go`
- Modify: `D:/xlru/xlru_mongo_test.go`

**Step 1: Write the failing test**

Use existing Mongo integration tests as the failure surface after removing imports.

**Step 2: Run test to verify it fails**

Run: `go test ./...`
Expected: FAIL until direct Mongo connection helpers are added.

**Step 3: Write minimal implementation**

Add local test helpers to connect with `mongo-driver/v2`, use `XLRU_MONGO_URI` or a localhost default, and skip when Mongo is unavailable.

**Step 4: Run test to verify it passes**

Run: `go test ./...`
Expected: PASS for unit tests; Mongo tests PASS or SKIP depending on environment.

### Task 4: Remove benchmark references to monorepo-only code

**Files:**
- Modify: `D:/xlru/xlru_benchmark_test.go`

**Step 1: Write the failing test**

Use package compilation as the failure surface because benchmark files compile during `go test`.

**Step 2: Run test to verify it fails**

Run: `go test ./...`
Expected: FAIL because `game/deps/xsync` is unavailable.

**Step 3: Write minimal implementation**

Delete the benchmark cases that import `xsync` and keep the portable benchmark set.

**Step 4: Run test to verify it passes**

Run: `go test ./...`
Expected: PASS or SKIP only for external Mongo availability.

### Task 5: Verify standalone module behavior

**Files:**
- Modify: `D:/xlru/README.md`

**Step 1: Write the failing test**

No code test needed; use build and package tests as the acceptance gate.

**Step 2: Run verification**

Run: `go test ./...`
Expected: PASS or Mongo integration tests SKIP cleanly.

Run: `go test -run TestXLRUCache_WithMongo ./...`
Expected: PASS when MongoDB is available, otherwise SKIP.

**Step 3: Write minimal documentation**

Update `README.md` to show the package is a standalone module and note the optional `slog` adapter and `XLRU_MONGO_URI` for integration tests.

**Step 4: Run final verification**

Run: `go test ./...`
Expected: PASS or clean SKIP behavior only.
