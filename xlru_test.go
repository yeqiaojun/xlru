package xlru

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// MockDataAccessor is a mock implementation of the DataAccessor interface for testing.
type MockDataAccessor struct {
	ID       int
	Data     string
	needSave bool
	saveDoc  bson.M
}

func (m *MockDataAccessor) Save() error {
	m.needSave = false
	return nil
}

func (m *MockDataAccessor) NeedSave() bool {
	if m == nil {
		return false
	}
	return m.needSave
}
func (m *MockDataAccessor) SetNeedSave(needs bool) {
	m.needSave = needs
}

func (m *MockDataAccessor) SaveDoc() bson.M {
	return m.saveDoc
}

type testLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *testLogger) Error(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprint(append([]any{msg}, args...)...))
}

func (l *testLogger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func TestNewSlogAdapter_Nil(t *testing.T) {
	if NewSlogAdapter(nil) != nil {
		t.Fatal("expected nil adapter for nil slog logger")
	}
}

func TestNewSlogAdapter(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if NewSlogAdapter(logger) == nil {
		t.Fatal("expected adapter for slog logger")
	}
}

func TestXLRUCache_FlushToDB_UsesLogger(t *testing.T) {
	logger := &testLogger{}
	cache := NewXLRUCache[string, *MockDataAccessor](10, Option[string, *MockDataAccessor]{
		Logger: logger,
	})

	cache.FlushToDB(nil)

	if logger.Count() != 1 {
		t.Fatalf("expected one log entry, got %d", logger.Count())
	}
}

func newMongoCollection(t *testing.T, dbName string) (*mongo.Client, *mongo.Collection) {
	t.Helper()

	uri := os.Getenv("XLRU_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Skipf("failed to connect to MongoDB: %v", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		t.Skipf("failed to ping MongoDB: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})

	coll := client.Database(dbName).Collection("test_data")
	_ = coll.Drop(context.Background())
	t.Cleanup(func() {
		_ = coll.Drop(context.Background())
	})

	return client, coll
}

func TestNewXLRUCache(t *testing.T) {
	opt := Option[string, *MockDataAccessor]{
		TTL:     time.Minute,
		Sliding: true,
	}
	cache := NewXLRUCache(100, opt)

	if cache.Size != 100 {
		t.Errorf("expected size 100, got %d", cache.Size)
	}
	if cache.Opt.TTL != time.Minute {
		t.Errorf("expected TTL %v, got %v", time.Minute, cache.Opt.TTL)
	}
	if !cache.Opt.Sliding {
		t.Errorf("expected Sliding to be true")
	}
}

func TestNewXLRUCache_Defaults(t *testing.T) {
	cache := NewXLRUCache(10, Option[string, *MockDataAccessor]{
		OnBatchSaver: func(values []*MockDataAccessor) error { return nil },
	})

	if cache.Opt.TTL != 24*time.Hour {
		t.Fatalf("expected default TTL 24h, got %v", cache.Opt.TTL)
	}
	if cache.Opt.BatchSaveCount != 1000 {
		t.Fatalf("expected default BatchSaveCount 1000, got %d", cache.Opt.BatchSaveCount)
	}
}

func TestXLRUCache_Get_Set(t *testing.T) {
	var loaderCount int32
	opt := Option[string, *MockDataAccessor]{
		OnLoader: func(key string) (*MockDataAccessor, error) {
			atomic.AddInt32(&loaderCount, 1)
			id, _ := strconv.Atoi(key)
			return &MockDataAccessor{ID: id, Data: "loaded-" + key}, nil
		},
	}
	cache := NewXLRUCache(10, opt)

	// Test Get with loader
	v, err := cache.Get("1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if v.Data != "loaded-1" {
		t.Errorf("expected data 'loaded-1', got '%s'", v.Data)
	}
	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Errorf("expected loader to be called once, got %d", loaderCount)
	}

	// Test Get again, should not call loader
	v, err = cache.Get("1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Errorf("expected loader to be called once, got %d", loaderCount)
	}

	// Test Set
	cache.Set("2", &MockDataAccessor{ID: 2, Data: "set-2"})
	v, err = cache.Get("2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if v.Data != "set-2" {
		t.Errorf("expected data 'set-2', got '%s'", v.Data)
	}
	// Loader should not be called for key "2"
	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Errorf("expected loader to be called once, got %d", loaderCount)
	}
}

func TestXLRUCache_OnEvictCalledOnEviction(t *testing.T) {
	const cacheSize = 1032
	const maxInsertions = 100000

	var evicted []int
	opt := Option[string, *MockDataAccessor]{
		TTL: time.Minute,
		OnEvict: func(value *MockDataAccessor) error {
			if value == nil {
				t.Fatalf("OnEvict received nil value")
			}
			if !value.NeedSave() {
				t.Fatalf("OnEvict should only receive dirty values")
			}
			evicted = append(evicted, value.ID)
			return nil
		},
	}
	cache := NewXLRUCache(cacheSize, opt)

	mustSet := func(id int) bool {
		t.Helper()
		key := fmt.Sprintf("key-%d", id)
		if err := cache.Set(key, &MockDataAccessor{ID: id, Data: key, needSave: true}); err != nil {
			t.Fatalf("Set(%s) returned error: %v", key, err)
		}
		return len(evicted) > 0
	}

	for i := 0; i < maxInsertions; i++ {
		mustSet(i)
	}

	fmt.Printf("%d", cache.Len())
	if len(evicted) != maxInsertions-cache.Len() {
		t.Fatalf("expected OnEvict to fire once after up to %d insertions, got %v", maxInsertions, evicted)
	}

}

func TestXLRUCache_SingleFlight(t *testing.T) {
	var loaderCount int32
	opt := Option[string, *MockDataAccessor]{
		OnLoader: func(key string) (*MockDataAccessor, error) {
			atomic.AddInt32(&loaderCount, 1)
			time.Sleep(100 * time.Millisecond) // Simulate work
			return &MockDataAccessor{Data: "loaded-" + key}, nil
		},
	}
	cache := NewXLRUCache(10, opt)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := cache.Get("1")
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if v.Data != "loaded-1" {
				t.Errorf("expected data 'loaded-1', got '%s'", v.Data)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Errorf("expected loader to be called once, got %d", loaderCount)
	}
}

func TestXLRUCache_TTL(t *testing.T) {
	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
	}
	cache := NewXLRUCache(10, opt)

	cache.Set("1", &MockDataAccessor{Data: "test"})
	_, ok := cache.Data.Get("1")
	if !ok {
		t.Fatalf("expected key 1 to be in cache %d", cache.Len())
	}
	time.Sleep(1100 * time.Millisecond)
	_, ok = cache.Data.Get("1")
	if ok {
		t.Fatal("expected key 1 to be expired")
	}
}

func TestXLRUCache_GetExpiredDirty(t *testing.T) {
	var loaderCount int32
	var evicted []*MockDataAccessor

	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
		OnLoader: func(key string) (*MockDataAccessor, error) {
			atomic.AddInt32(&loaderCount, 1)
			return &MockDataAccessor{ID: 2, Data: "reloaded-" + key}, nil
		},
		OnEvict: func(value *MockDataAccessor) error {
			evicted = append(evicted, value)
			value.needSave = false
			return nil
		},
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "dirty-1", needSave: true}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	v, err := cache.Get("1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if v.Data != "reloaded-1" {
		t.Fatalf("expected reloaded value, got %s", v.Data)
	}
	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Fatalf("expected loader to be called once, got %d", loaderCount)
	}
	if len(evicted) != 1 {
		t.Fatalf("expected OnEvict to be called once, got %d", len(evicted))
	}
	if evicted[0].Data != "dirty-1" {
		t.Fatalf("expected evicted value dirty-1, got %s", evicted[0].Data)
	}
}

func TestXLRUCache_GetExpiredDirtyOnEvictError(t *testing.T) {
	var loaderCount int32
	evictErr := errors.New("save failed")

	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
		OnLoader: func(key string) (*MockDataAccessor, error) {
			atomic.AddInt32(&loaderCount, 1)
			return &MockDataAccessor{ID: 2, Data: "reloaded-" + key}, nil
		},
		OnEvict: func(value *MockDataAccessor) error {
			return evictErr
		},
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "dirty-1", needSave: true}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	_, err := cache.Get("1")
	if !errors.Is(err, evictErr) {
		t.Fatalf("expected evict err, got %v", err)
	}
	if atomic.LoadInt32(&loaderCount) != 0 {
		t.Fatalf("expected loader not to run, got %d", loaderCount)
	}

	if _, _, ok := cache.Data.Peek("1"); ok {
		t.Fatal("expected expired dirty entry to be removed after evict failure")
	}
}

func TestXLRUCache_GetExpiredClean(t *testing.T) {
	var loaderCount int32
	var evictCount int32

	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
		OnLoader: func(key string) (*MockDataAccessor, error) {
			atomic.AddInt32(&loaderCount, 1)
			return &MockDataAccessor{ID: 2, Data: "reloaded-" + key}, nil
		},
		OnEvict: func(value *MockDataAccessor) error {
			atomic.AddInt32(&evictCount, 1)
			return nil
		},
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "clean-1", needSave: false}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	v, err := cache.Get("1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if v.Data != "reloaded-1" {
		t.Fatalf("expected reloaded value, got %s", v.Data)
	}
	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Fatalf("expected loader to be called once, got %d", loaderCount)
	}
	if atomic.LoadInt32(&evictCount) != 0 {
		t.Fatalf("expected OnEvict not to be called, got %d", evictCount)
	}
}

var errNotFound = errors.New("not found")

func TestXLRUCache_SetEviction(t *testing.T) {
	var evictedKey string
	opt := Option[string, *MockDataAccessor]{
		OnEvict: func(v *MockDataAccessor) error {
			evictedKey = v.Data
			return nil
		},
	}
	cache := NewXLRUCache(16, opt)

	var insertedKey string
	for i := 0; i < 10000; i++ {
		insertedKey = fmt.Sprintf("key-%d", i)
		err := cache.Set(insertedKey, &MockDataAccessor{Data: insertedKey, needSave: true})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if evictedKey != "" {
			break
		}
	}

	if evictedKey == "" {
		t.Fatal("expected an eviction to happen")
	}
	if _, _, ok := cache.Data.Peek(evictedKey); ok {
		t.Fatalf("expected evicted key %s to be removed", evictedKey)
	}
	if v, _, ok := cache.Data.Peek(insertedKey); !ok || v.Data != insertedKey {
		t.Fatalf("expected inserted key %s to remain in cache", insertedKey)
	}
}

func TestXLRUCache_SetEvictionOnEvictError(t *testing.T) {
	evictErr := errors.New("save failed")
	var evictedKey string
	opt := Option[string, *MockDataAccessor]{
		OnEvict: func(v *MockDataAccessor) error {
			evictedKey = v.Data
			return evictErr
		},
	}
	cache := NewXLRUCache(16, opt)

	var insertedKey string
	var err error
	for i := 0; i < 10000; i++ {
		insertedKey = fmt.Sprintf("key-%d", i)
		err = cache.Set(insertedKey, &MockDataAccessor{Data: insertedKey, needSave: true})
		if err != nil {
			break
		}
	}

	if !errors.Is(err, evictErr) {
		t.Fatalf("expected evict err, got %v", err)
	}
	if evictedKey == "" {
		t.Fatal("expected an eviction to happen")
	}
	if _, _, ok := cache.Data.Peek(evictedKey); ok {
		t.Fatalf("expected evicted key %s to be removed", evictedKey)
	}
	if v, _, ok := cache.Data.Peek(insertedKey); !ok || v.Data != insertedKey {
		t.Fatalf("expected inserted key %s to remain in cache", insertedKey)
	}
}

func TestXLRUCache_DeleteDirty(t *testing.T) {
	var evicted []*MockDataAccessor
	opt := Option[string, *MockDataAccessor]{
		OnEvict: func(v *MockDataAccessor) error {
			evicted = append(evicted, v)
			return nil
		},
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{Data: "dirty-1", needSave: true}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := cache.Delete("1"); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(evicted) != 1 || evicted[0].Data != "dirty-1" {
		t.Fatalf("expected dirty delete to evict old value, got %+v", evicted)
	}
	if _, _, ok := cache.Data.Peek("1"); ok {
		t.Fatal("expected deleted key to be removed")
	}
}

func TestXLRUCache_DeleteDirtyOnEvictError(t *testing.T) {
	evictErr := errors.New("save failed")
	opt := Option[string, *MockDataAccessor]{
		OnEvict: func(v *MockDataAccessor) error {
			return evictErr
		},
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{Data: "dirty-1", needSave: true}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err := cache.Delete("1")
	if !errors.Is(err, evictErr) {
		t.Fatalf("expected evict err, got %v", err)
	}
	if _, _, ok := cache.Data.Peek("1"); ok {
		t.Fatal("expected deleted key to be removed even after evict failure")
	}
}

func TestXLRUCache_SaveAll(t *testing.T) {
	var savedValues []*MockDataAccessor
	opt := Option[string, *MockDataAccessor]{
		OnBatchSaver: func(values []*MockDataAccessor) error {
			savedValues = append(savedValues, values...)
			return nil
		},
		BatchSaveCount: 2,
	}
	// Leave enough per-shard headroom so random shard placement cannot evict these test keys.
	cache := NewXLRUCache(4096, opt)

	for i := 0; i < 5; i++ {
		cache.Set(fmt.Sprintf("%d", i), &MockDataAccessor{ID: i, needSave: true})
	}
	cache.FlushToDB(nil)

	if len(savedValues) != 5 {
		t.Errorf("expected 5 saved values, got %d", len(savedValues))
	}
}

func TestXLRUCache_SaveAllBatching(t *testing.T) {
	var batches []int
	opt := Option[string, *MockDataAccessor]{
		OnLoader: func(key string) (*MockDataAccessor, error) {
			return &MockDataAccessor{Data: "value-" + key}, nil
		},
		OnBatchSaver: func(values []*MockDataAccessor) error {
			batches = append(batches, len(values))
			return nil
		},
		BatchSaveCount: 3,
	}
	cache := NewXLRUCache(4096, opt)

	for i := 0; i < 7; i++ {
		id := strconv.Itoa(i)
		item, err := cache.Get(id)
		if err != nil {
			t.Fatalf("expected get(%s) to succeed, got %v", id, err)
		}
		item.Data = id
		item.needSave = true
	}

	cache.FlushToDB(nil)
	sort.Ints(batches)

	if got, want := len(batches), 3; got != want {
		t.Fatalf("expected 3 saver calls, got %d (%v)", got, batches)
	}
	expected := []int{1, 3, 3}
	for i := range expected {
		if batches[i] != expected[i] {
			t.Fatalf("expected batch sizes %v, got %v", expected, batches)
		}
	}
}

func TestXLRUCache_SaveAllIncludesExpiredDirty(t *testing.T) {
	var savedValues []*MockDataAccessor
	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
		OnBatchSaver: func(values []*MockDataAccessor) error {
			savedValues = append(savedValues, values...)
			return nil
		},
		BatchSaveCount: 2,
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "dirty-1", needSave: true}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(2100 * time.Millisecond)
	cache.FlushToDB(nil)

	if len(savedValues) != 1 {
		t.Fatalf("expected 1 saved value, got %d", len(savedValues))
	}
	if savedValues[0].Data != "dirty-1" {
		t.Fatalf("expected expired dirty value to be saved, got %s", savedValues[0].Data)
	}
}

func TestXLRUCache_BatchSaveIncludesExpiredDirty(t *testing.T) {
	var savedValues []*MockDataAccessor
	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
		OnBatchSaver: func(values []*MockDataAccessor) error {
			savedValues = append(savedValues, values...)
			return nil
		},
		BatchSaveCount: 1,
	}
	cache := NewXLRUCache(10, opt)

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "dirty-1", needSave: true}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(2100 * time.Millisecond)
	cache.BatchSave()

	if len(savedValues) != 1 {
		t.Fatalf("expected 1 saved value, got %d", len(savedValues))
	}
	if savedValues[0].Data != "dirty-1" {
		t.Fatalf("expected expired dirty value to be batch saved, got %s", savedValues[0].Data)
	}
}

func TestXLRUCache_BatchSaveLargeIncludesExpiredDirty(t *testing.T) {
	var savedValues []*MockDataAccessor
	opt := Option[string, *MockDataAccessor]{
		TTL: 1000 * time.Millisecond,
		OnBatchSaver: func(values []*MockDataAccessor) error {
			savedValues = append(savedValues, values...)
			return nil
		},
		BatchSaveCount: 1,
	}
	cache := NewXLRUCache(50, opt)

	for i := 0; i < 11; i++ {
		key := fmt.Sprintf("%d", i)
		if err := cache.Set(key, &MockDataAccessor{ID: i, Data: key, needSave: true}); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	time.Sleep(2100 * time.Millisecond)
	cache.BatchSave()

	if len(savedValues) == 0 {
		t.Fatal("expected large-path BatchSave to flush at least one expired dirty value")
	}
}

func TestXLRUCache_FlushToDBLargeScaleUnique(t *testing.T) {
	const count = 20000
	const cacheSize = count * 8

	savedCounts := make(map[int]int, count)
	opt := Option[string, *MockDataAccessor]{
		OnBatchSaver: func(values []*MockDataAccessor) error {
			for _, value := range values {
				savedCounts[value.ID]++
				value.needSave = false
			}
			return nil
		},
		BatchSaveCount: 256,
	}
	cache := NewXLRUCache(cacheSize, opt)

	for i := 0; i < count; i++ {
		key := strconv.Itoa(i)
		if err := cache.Set(key, &MockDataAccessor{ID: i, Data: key, needSave: true}); err != nil {
			t.Fatalf("expected set(%s) to succeed, got %v", key, err)
		}
	}

	cache.FlushToDB(nil)

	if got := len(savedCounts); got != count {
		t.Fatalf("expected %d unique saved ids, got %d", count, got)
	}
	for id, saved := range savedCounts {
		if saved != 1 {
			t.Fatalf("expected id %d to be saved once, got %d", id, saved)
		}
	}
}

func TestXLRUCache_LoadWithoutLoader(t *testing.T) {
	cache := NewXLRUCache[string, *MockDataAccessor](10, Option[string, *MockDataAccessor]{})

	_, err := cache.Load("1")
	if !errors.Is(err, ErrorCacheEntryNotFound) {
		t.Fatalf("expected ErrorCacheEntryNotFound, got %v", err)
	}
}

func TestXLRUCache_LoadLoaderError(t *testing.T) {
	loaderErr := errors.New("loader failed")
	cache := NewXLRUCache[string, *MockDataAccessor](10, Option[string, *MockDataAccessor]{
		OnLoader: func(key string) (*MockDataAccessor, error) {
			return nil, loaderErr
		},
	})

	_, err := cache.Load("1")
	if !errors.Is(err, loaderErr) {
		t.Fatalf("expected loader error, got %v", err)
	}
}

func TestXLRUCache_Peek(t *testing.T) {
	cache := NewXLRUCache[string, *MockDataAccessor](10, Option[string, *MockDataAccessor]{})

	if _, ok := cache.Peek("missing"); ok {
		t.Fatal("expected missing key peek to miss")
	}

	want := &MockDataAccessor{ID: 1, Data: "peek"}
	if err := cache.Set("1", want); err != nil {
		t.Fatalf("expected set to succeed, got %v", err)
	}

	got, ok := cache.Peek("1")
	if !ok {
		t.Fatal("expected peek to hit")
	}
	if got != want {
		t.Fatalf("expected peek to return same pointer")
	}
}

func TestXLRUCache_FlushToDBWithNilBatchSaver(t *testing.T) {
	cache := NewXLRUCache[string, *MockDataAccessor](10, Option[string, *MockDataAccessor]{})
	if err := cache.FlushToDBWithErr(nil); err != nil {
		t.Fatalf("expected nil error when batch saver is unset, got %v", err)
	}
}

func TestXLRUCache_FlushToDBLogsBatchSaverError(t *testing.T) {
	batchErr := errors.New("batch failed")
	var calls int
	cache := NewXLRUCache[string, *MockDataAccessor](10, Option[string, *MockDataAccessor]{
		OnBatchSaver: func(values []*MockDataAccessor) error {
			calls++
			return batchErr
		},
		BatchSaveCount: 1,
	})

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "dirty", needSave: true}); err != nil {
		t.Fatalf("expected set to succeed, got %v", err)
	}

	cache.FlushToDB(nil)
	if calls != 1 {
		t.Fatalf("expected batch saver to be called once, got %d", calls)
	}
}

func TestXLRUCache_Stats(t *testing.T) {
	cache := NewXLRUCache[string, *MockDataAccessor](16, Option[string, *MockDataAccessor]{})

	if err := cache.Set("1", &MockDataAccessor{ID: 1, Data: "value"}); err != nil {
		t.Fatalf("expected set to succeed, got %v", err)
	}
	if _, err := cache.Get("1"); err != nil {
		t.Fatalf("expected get hit to succeed, got %v", err)
	}
	if _, err := cache.Get("missing"); !errors.Is(err, ErrorCacheEntryNotFound) {
		t.Fatalf("expected missing get to return ErrorCacheEntryNotFound, got %v", err)
	}

	stats := cache.Stats()
	if stats.SetCalls == 0 {
		t.Fatal("expected SetCalls to be recorded")
	}
	if stats.GetCalls < 2 {
		t.Fatalf("expected GetCalls to include hit and miss, got %d", stats.GetCalls)
	}
	if stats.Misses == 0 {
		t.Fatal("expected miss to be recorded")
	}
}

func TestLocalCacheRoundTrip(t *testing.T) {
	var loaderCount int32
	cache := NewLocalCache[string, string](10, false, time.Minute, func(key string) (string, error) {
		atomic.AddInt32(&loaderCount, 1)
		return "loaded-" + key, nil
	})

	v, err := cache.Get("a")
	if err != nil || v != "loaded-a" {
		t.Fatalf("expected loader-backed get, got value=%v err=%v", v, err)
	}
	if cache.Len() != 1 {
		t.Fatalf("expected len 1 after initial load, got %d", cache.Len())
	}

	if err := cache.Set("a", "manual-a"); err != nil {
		t.Fatalf("expected set to succeed, got %v", err)
	}
	v, err = cache.Get("a")
	if err != nil || v != "manual-a" {
		t.Fatalf("expected manual value after set, got value=%v err=%v", v, err)
	}

	if err := cache.Delete("a"); err != nil {
		t.Fatalf("expected delete to succeed, got %v", err)
	}
	if cache.Len() != 0 {
		t.Fatalf("expected len 0 after delete, got %d", cache.Len())
	}

	v, err = cache.Get("a")
	if err != nil || v != "loaded-a" {
		t.Fatalf("expected loader-backed value after delete, got value=%v err=%v", v, err)
	}
	if atomic.LoadInt32(&loaderCount) != 2 {
		t.Fatalf("expected loader to run twice, got %d", loaderCount)
	}
}

func TestDataAccessorHelper(t *testing.T) {
	helper := &LocalCacheDataHelper[string]{data: "test"}
	if helper.NeedSave() {
		t.Error("expected NeedSave to be false")
	}
	helper.SetNeedSave(true)
	if helper.NeedSave() {
		t.Error("expected SetNeedSave to remain a no-op")
	}
	if helper.Data() != "test" {
		t.Errorf("expected helper data to remain unchanged")
	}
}

// MongoTestData is a test data structure for MongoDB integration tests.
type MongoTestData struct {
	ID       string `bson:"_id"`
	Data     string `bson:"data"`
	needSave bool   `bson:"-"`
}

func (m *MongoTestData) Save() error {
	// This is a single save, can be implemented if needed, but we focus on batch save.
	m.needSave = false
	return nil
}

func (m *MongoTestData) SetNeedSave(needs bool) {
	m.needSave = needs
}
func (m *MongoTestData) NeedSave() bool {
	if m == nil {
		return false
	}
	return m.needSave
}

func (m *MongoTestData) SaveDoc() bson.M {
	if !m.needSave {
		return nil
	}
	return bson.M{"$set": bson.M{"data": m.Data}}
}

func TestXLRUCache_WithMongo(t *testing.T) {
	_, coll := newMongoCollection(t, "test_xlru")

	var loaderCount int32
	var saverCount int32

	opt := Option[string, *MongoTestData]{
		OnLoader: func(key string) (*MongoTestData, error) {
			atomic.AddInt32(&loaderCount, 1)
			data := &MongoTestData{ID: key}
			err := coll.FindOne(context.Background(), bson.M{"_id": key}).Decode(data)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					// Create a new one if not found
					data.Data = "loaded-" + key
					data.needSave = true // mark as new, needs saving
					return data, nil
				}
				return nil, err
			}
			return data, nil
		},
		OnBatchSaver: func(values []*MongoTestData) error {
			atomic.AddInt32(&saverCount, int32(len(values)))
			models := make([]mongo.WriteModel, 0, len(values))
			for _, v := range values {
				if v.NeedSave() {
					model := mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": v.ID}).
						SetUpdate(v.SaveDoc()).
						SetUpsert(true)
					models = append(models, model)
				}
			}
			if len(models) == 0 {
				return nil
			}
			_, err := coll.BulkWrite(context.Background(), models)
			return err
		},
		BatchSaveCount: 2,
	}
	cache := NewXLRUCache(10, opt)

	// 1. Get a non-existent item, trigger loader
	item1, err := cache.Get("1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if item1.Data != "loaded-1" {
		t.Errorf("expected data 'loaded-1', got '%s'", item1.Data)
	}
	if atomic.LoadInt32(&loaderCount) != 1 {
		t.Errorf("expected loader to be called once, got %d", loaderCount)
	}

	// 2. Modify the item
	item1.Data = "modified-1"
	item1.needSave = true

	// 3. Get another item, which will also be marked as needSave
	item2, err := cache.Get("2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	item2.needSave = true

	// 4. Send all dirty items
	cache.FlushToDB(nil)

	// 5. Verify data in MongoDB
	var result MongoTestData
	err = coll.FindOne(context.Background(), bson.M{"_id": "1"}).Decode(&result)
	if err != nil {
		t.Fatalf("failed to find item 1 in mongo: %v", err)
	}
	if result.Data != "modified-1" {
		t.Errorf("expected data 'modified-1' in mongo, got '%s'", result.Data)
	}

	err = coll.FindOne(context.Background(), bson.M{"_id": "2"}).Decode(&result)
	if err != nil {
		t.Fatalf("failed to find item 2 in mongo: %v", err)
	}
	if result.Data != "loaded-2" {
		t.Errorf("expected data 'loaded-2' in mongo, got '%s'", result.Data)
	}

	if atomic.LoadInt32(&saverCount) != 2 {
		t.Errorf("expected saver to be called with 2 items, got %d", atomic.LoadInt32(&saverCount))
	}
}
