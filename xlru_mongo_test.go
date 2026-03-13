package xlru

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoTestItem 测试数据结构
type MongoTestItem struct {
	ID    string `bson:"_id"`
	Value string `bson:"value"`
	Extra string `bson:"extra"`

	// saveMu 用于保护 needSave 标志，防止并发读写问题
	saveMu   sync.Mutex `bson:"-"`
	needSave bool       `bson:"-"`
}

// SetNeedSave 安全地设置 needSave 标志
func (m *MongoTestItem) SetNeedSave(needs bool) {
	m.saveMu.Lock()
	m.needSave = needs
	m.saveMu.Unlock()
}

func (m *MongoTestItem) NeedSave() bool {
	if m == nil {
		return false
	}
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	return m.needSave
}

func (m *MongoTestItem) SaveDoc() bson.M {
	// SaveDoc 应该在 NeedSave() 检查之后调用，因此这里不需要再次加锁
	return bson.M{"$set": bson.M{"value": m.Value, "extra": m.Extra}}
}

// TestXLRUCache_WithMongoLocal 测试使用本地MongoDB的XLRUCache
func TestXLRUCache_WithMongoLocal(t *testing.T) {
	_, coll := newMongoCollection(t, "test_xlru_local")

	// 创建XLRUCache，大小为50
	var loaderCount int32
	var saverCount int32
	var mu sync.Mutex
	accessed := make(map[string]bool)

	opt := Option[string, *MongoTestItem]{
		OnLoader: func(key string) (*MongoTestItem, error) {
			mu.Lock()
			loaderCount++
			mu.Unlock()

			data := &MongoTestItem{ID: key}
			err := coll.FindOne(context.Background(), bson.M{"_id": key}).Decode(data)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					// 如果未找到，则创建新数据
					data.Value = "value-" + key
					data.Extra = ""
					// 立即插入到数据库中，以便后续查询
					_, err := coll.InsertOne(context.Background(), data)
					if err != nil {
						return nil, err
					}
					return data, nil
				}
				return nil, err
			}
			return data, nil
		},
		OnBatchSaver: func(values []*MongoTestItem) error {
			mu.Lock()
			saverCount += int32(len(values))
			mu.Unlock()

			models := make([]mongo.WriteModel, 0, len(values))
			updatedItems := make([]*MongoTestItem, 0, len(values))

			for _, v := range values {
				if v.NeedSave() {
					model := mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": v.ID}).
						SetUpdate(v.SaveDoc()).
						SetUpsert(true)
					models = append(models, model)
					updatedItems = append(updatedItems, v)
				}
			}

			if len(models) == 0 {
				return nil
			}

			_, err := coll.BulkWrite(context.Background(), models)
			if err != nil {
				return err
			}

			// 保存成功后重置标记
			for _, v := range updatedItems {
				v.SetNeedSave(false)
			}

			return nil
		},
		OnEvict: func(v *MongoTestItem) error {
			if !v.NeedSave() {
				return nil
			}
			model := mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": v.ID}).
				SetUpdate(v.SaveDoc()).
				SetUpsert(true)
			_, err := coll.BulkWrite(context.Background(), []mongo.WriteModel{model})
			if err != nil {
				t.Logf("WARN: failed to save evicted item %s: %v", v.ID, err)
				return err
			}
			v.SetNeedSave(false)
			return nil
		},
		BatchSaveCount: 10, // 设置较小的批次大小以测试批处理功能
	}

	// 创建大小为50的缓存
	cache := NewXLRUCache(50, opt)

	// 1. 初始化100个对象，ID从0到99
	t.Log("初始化100个对象...")
	for i := 0; i < 100; i++ {
		id := strconv.Itoa(i)
		item, err := cache.Get(id)
		if err != nil {
			t.Fatalf("获取ID %s 失败: %v", id, err)
		}
		// 验证初始值
		expectedValue := "value-" + id
		if item.Value != expectedValue {
			t.Errorf("ID %s 的值应该是 %s，但得到 %s", id, expectedValue, item.Value)
		}
		item.Extra = "extra-" + id
		item.SetNeedSave(true)
	}

	t.Logf("初始数据加载完成，加载器调用次数: %d", loaderCount)

	// 重置计数器
	mu.Lock()
	loaderCount = 0
	saverCount = 0
	mu.Unlock()

	// 2. 随机访问0到99的对象，修改value和needSave
	t.Log("随机访问并修改对象...")
	rand.Seed(time.Now().UnixNano())
	accessed = make(map[string]bool)

	// 进行200次随机访问
	for i := 0; i < 200; i++ {
		id := strconv.Itoa(i % 100) // 随机选择0-99之间的ID
		item, err := cache.Get(id)
		if err != nil {
			t.Fatalf("获取ID %s 失败: %v", id, err)
		}

		// 修改value为与ID相同的值
		item.Value = id
		// 标记为需要保存
		item.SetNeedSave(true)

		mu.Lock()
		accessed[id] = true
		mu.Unlock()
	}

	mu.Lock()
	loaderCountVal := loaderCount
	accessedCount := len(accessed)
	mu.Unlock()
	lxen := cache.Len()
	t.Logf("随机访问完成，访问了 %d 个不同的对象，加载器调用次数: %d len%d", accessedCount, loaderCountVal, lxen)

	// 3. 使用SaveAll保存所有修改
	t.Log("保存所有修改...")
	cache.FlushToDB(nil)

	mu.Lock()
	saverCountVal := saverCount
	mu.Unlock()
	t.Logf("保存完成，保存器调用次数: %d", saverCountVal)

	// 4. 从数据库加载数据进行对比
	t.Log("从数据库加载数据进行对比...")
	findOptions := options.Find()
	cursor, err := coll.Find(context.Background(), bson.M{}, findOptions)
	if err != nil {
		t.Fatalf("查询数据库失败: %v", err)
	}
	defer cursor.Close(context.Background())

	var results []*MongoTestItem
	if err = cursor.All(context.Background(), &results); err != nil {
		t.Fatalf("解析结果失败: %v", err)
	}

	// 验证结果
	if len(results) != 100 {
		t.Errorf("数据库中应该有100条记录，但找到 %d 条", len(results))
	}

	// 检查每条记录的值是否正确
	verifiedCount := 0
	for _, result := range results {
		// 如果该记录被访问过，值应该等于ID
		// 如果未被访问，值应该是"value-" + ID
		mu.Lock()
		accessedVal := accessed[result.ID]
		mu.Unlock()

		if accessedVal {
			// 被访问过，值应该等于ID
			if result.Value != result.ID {
				t.Errorf("ID %s 的值应该是 %s，但得到 %s", result.ID, result.ID, result.Value)
			} else {
				verifiedCount++
			}
		} else {
			// 未被访问，值应该是"value-" + ID
			expectedValue := "value-" + result.ID
			if result.Value != expectedValue {
				t.Errorf("ID %s 的值应该是 %s，但得到 %s", result.ID, expectedValue, result.Value)
			} else {
				verifiedCount++
			}
		}
	}

	if verifiedCount != 100 {
		t.Errorf("验证通过的数据条数应该是100，但只有 %d 条", verifiedCount)
	} else {
		t.Log("测试完成，所有数据验证通过")
	}

	fmt.Printf("缓存统计: %+v\n", cache.Stats())
}

// TestXLRUCache_SequentialUpdate 测试顺序更新和SaveAll
func TestXLRUCache_SequentialUpdate(t *testing.T) {
	_, coll := newMongoCollection(t, "test_xlru_sequential")

	var saverCount int32
	var mu sync.Mutex

	opt := Option[string, *MongoTestItem]{
		OnLoader: func(key string) (*MongoTestItem, error) {
			data := &MongoTestItem{ID: key}
			err := coll.FindOne(context.Background(), bson.M{"_id": key}).Decode(data)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					data.Value = "value-" + key
					data.Extra = "extra-" + key
					_, err := coll.InsertOne(context.Background(), data)
					if err != nil {
						return nil, err
					}
					return data, nil
				}
				return nil, err
			}
			return data, nil
		},
		OnBatchSaver: func(values []*MongoTestItem) error {
			mu.Lock()
			saverCount += int32(len(values))
			mu.Unlock()

			models := make([]mongo.WriteModel, 0, len(values))
			updatedItems := make([]*MongoTestItem, 0, len(values))

			for _, v := range values {
				if v.NeedSave() {
					model := mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": v.ID}).
						SetUpdate(v.SaveDoc()).
						SetUpsert(true)
					models = append(models, model)
					updatedItems = append(updatedItems, v)
				}
			}

			if len(models) == 0 {
				return nil
			}

			_, err := coll.BulkWrite(context.Background(), models)
			if err != nil {
				return err
			}

			// 保存成功后重置标记
			for _, v := range updatedItems {
				v.SetNeedSave(false)
			}

			return nil
		},
		OnEvict: func(v *MongoTestItem) error {
			if !v.NeedSave() {
				return nil
			}
			model := mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": v.ID}).
				SetUpdate(v.SaveDoc()).
				SetUpsert(true)
			_, err := coll.BulkWrite(context.Background(), []mongo.WriteModel{model})
			if err != nil {
				t.Logf("WARN: failed to save evicted item %s: %v", v.ID, err)
				return err
			}
			v.SetNeedSave(false)
			return nil
		},
	}

	cache := NewXLRUCache(50, opt)

	// 1. 初始化100个对象
	t.Log("初始化100个对象...")
	for i := 0; i < 100; i++ {
		id := strconv.Itoa(i)
		_, err := cache.Get(id)
		if err != nil {
			t.Fatalf("获取ID %s 失败: %v", id, err)
		}
	}

	// 2. 按顺序访问所有100个id，并把id复制给value
	t.Log("按顺序修改100个对象...")
	for i := 0; i < 100; i++ {
		id := strconv.Itoa(i)
		item, err := cache.Get(id)
		if err != nil {
			t.Fatalf("顺序获取ID %s 失败: %v", id, err)
		}
		item.Value = id
		item.SetNeedSave(true)
	}

	// 3. saveall保存所有值
	t.Log("保存所有修改...")
	cache.FlushToDB(nil)

	// 4. 从数据库拉起，检查值是否符合预期
	t.Log("从数据库加载数据进行对比...")
	cursor, err := coll.Find(context.Background(), bson.M{})
	if err != nil {
		t.Fatalf("查询数据库失败: %v", err)
	}
	defer cursor.Close(context.Background())

	var results []*MongoTestItem
	if err = cursor.All(context.Background(), &results); err != nil {
		t.Fatalf("解析结果失败: %v", err)
	}

	if len(results) != 100 {
		t.Fatalf("数据库中应该有100条记录，但找到 %d 条", len(results))
	}

	for _, item := range results {
		if item.Value != item.ID {
			t.Errorf("ID %s 的值应该是 %s，但得到 %s", item.ID, item.ID, item.Value)
		}
	}
	t.Log("顺序更新测试完成，所有数据验证通过")
}

// TestXLRUCache_SaveAll_Batching 测试SaveAll的分批保存功能
func TestXLRUCache_SaveAll_Batching(t *testing.T) {
	if testing.CoverMode() != "" {
		t.Skip("skip mongo batching integration test in coverage mode; batching semantics are covered by unit tests")
	}
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	_, coll := newMongoCollection(t, "test_xlru_batch_save")

	const batchSize = 3
	const dirtyItems = 7
	batchInfos := make(chan int, 5) // 用于记录每批保存的数量

	opt := Option[string, *MongoTestItem]{
		OnLoader: func(key string) (*MongoTestItem, error) {
			data := &MongoTestItem{ID: key, Value: "value-" + key}
			_, err := coll.InsertOne(context.Background(), data)
			if err != nil && !mongo.IsDuplicateKeyError(err) {
				return nil, err
			}
			return data, nil
		},
		OnBatchSaver: func(values []*MongoTestItem) error {
			// 实际的保存逻辑
			models := make([]mongo.WriteModel, 0, len(values))
			for _, v := range values {
				models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": v.ID}).SetUpdate(v.SaveDoc()).SetUpsert(true))
			}
			if len(models) > 0 {
				coll.BulkWrite(context.Background(), models)
			}
			// 发送信号，记录该批次的大小
			batchInfos <- len(values)
			return nil
		},
		BatchSaveCount: batchSize,
	}

	// Leave enough per-shard headroom so exact batch-count assertions are deterministic.
	cache := NewXLRUCache(4096, opt)

	// 1. 初始化并修改`dirtyItems`个对象
	t.Logf("修改 %d 个对象...", dirtyItems)
	for i := 0; i < dirtyItems; i++ {
		id := strconv.Itoa(i)
		item, err := cache.Get(id)
		if err != nil {
			t.Fatalf("获取ID %s 失败: %v", id, err)
		}
		item.Value = id
		item.SetNeedSave(true)
	}

	// 2. 调用SaveAll来触发批量保存
	cache.FlushToDB(nil)
	close(batchInfos) // 关闭channel，以便我们可以遍历它

	// 3. 验证OnBatchSaver的调用情况
	var receivedBatches []int
	for size := range batchInfos {
		receivedBatches = append(receivedBatches, size)
	}
	sort.Ints(receivedBatches) // 排序以确保断言稳定

	expectedBatches := []int{1, 3, 3} // 7个对象, batchsize=3 -> 3, 3, 1
	if len(receivedBatches) != len(expectedBatches) {
		t.Fatalf("期望OnBatchSaver被调用 %d 次, 但实际是 %d 次. 收到的批次: %v", len(expectedBatches), len(receivedBatches), receivedBatches)
	}
	for i := range expectedBatches {
		if receivedBatches[i] != expectedBatches[i] {
			t.Errorf("期望的批次大小为 %v, 但得到 %v", expectedBatches, receivedBatches)
			break
		}
	}
	t.Logf("批量保存行为符合预期，批次: %v", receivedBatches)
}

// TestXLRUCache_BatchSave_LargeScale 测试SaveAll(nil)在大数据量下的可靠性
func TestXLRUCache_BatchSave_LargeScale(t *testing.T) {
	if testing.Short() || testing.CoverMode() != "" {
		t.Skip("Skipping large scale test in short mode or with coverage")
	}
	if os.Getenv("RUN_XLRU_LARGE_SCALE") != "1" {
		t.Skip("Skipping large scale Mongo stress test; set RUN_XLRU_LARGE_SCALE=1 to enable")
	}
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	_, coll := newMongoCollection(t, "test_xlru_batchsave_large")

	const itemCount = 1000000
	const cacheSize = itemCount * 6 / 5
	const batchSaveCount = 10000
	var totalSavedCount int64

	// 用于记录每次批保存的统计信息
	type batchStat struct {
		count    int
		duration time.Duration
	}
	statsChan := make(chan batchStat, 200) // 增加channel容量

	opt := Option[string, *MongoTestItem]{
		OnLoader: func(key string) (*MongoTestItem, error) {
			// 在这个测试中，所有数据都应该预先加载，不应该调用Loader
			return nil, fmt.Errorf("loader should not be called in this test")
		},
		OnBatchSaver: func(values []*MongoTestItem) error {
			startTime := time.Now()
			models := make([]mongo.WriteModel, 0, len(values))
			for _, v := range values {
				v.SetNeedSave(false)
				models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": v.ID}).SetUpdate(v.SaveDoc()).SetUpsert(true))
			}
			duration1 := time.Since(startTime)
			if len(models) > 0 {
				opts := options.BulkWrite().SetOrdered(false)
				_, err := coll.BulkWrite(context.Background(), models, opts)
				if err != nil {
					t.Logf("OnBatchSaver error: %v", err)
					return err
				}
			}
			duration := time.Since(startTime)
			atomic.AddInt64(&totalSavedCount, int64(len(values)))
			t.Logf("OnBatchSaver: %d, %v %v", len(values), duration, duration1)
			statsChan <- batchStat{count: len(values), duration: duration}
			return nil
		},
		OnEvict: func(value *MongoTestItem) error {
			opt := options.UpdateOne().SetUpsert(true)
			coll.UpdateOne(context.Background(), bson.M{"_id": value.ID}, value.SaveDoc(), opt)
			//t.Log("OnEvict" + value.ID)
			atomic.AddInt64(&totalSavedCount, 1)
			return nil
		},
		BatchSaveCount: batchSaveCount,
	}

	cache := NewXLRUCache(cacheSize, opt)

	// 1. 预加载10万个元素到缓存中
	t.Logf("正在加载 %d 个元素到缓存中...", itemCount)
	for i := 0; i < itemCount; i++ {
		id := strconv.Itoa(i)
		item := &MongoTestItem{ID: id, Value: "initial"}
		cache.Set(id, item)
		item.SetNeedSave(true) // 全部标记为需要保存
	}
	t.Log("加载完成")

	// 2. 直接调用 SaveAll(nil) 来保存所有数据
	t.Log("调用 SaveAll(nil) 保存所有脏数据...")
	if err := cache.FlushToDBWithErr(nil); err != nil {
		t.Fatalf("批量保存失败: %v", err)
	}
	close(statsChan)

	// 3. 收集并打印统计信息
	t.Log("---")
	var batchLog []string
	for stat := range statsChan {
		logStr := fmt.Sprintf("批次保存: %d 个元素, 耗时: %v", stat.count, stat.duration)
		batchLog = append(batchLog, logStr)
		t.Log(logStr)
	}
	t.Logf("总计通过 OnBatchSaver 保存的元素数量: %d", totalSavedCount)
	t.Log("---")

	// 4. 验证数据库
	if totalSavedCount != itemCount {
		t.Errorf("期望保存 %d 个元素，但实际只保存了 %d 个", itemCount, totalSavedCount)
	}
	time.Sleep(time.Second * 2)

	t.Log("正在验证数据库中的计数值...")
	count, err := coll.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		t.Fatalf("无法从数据库中统计文档数量: %v", err)
	}
	if count != itemCount {
		t.Errorf("数据库中的文档总数应该是 %d, 但得到 %d", itemCount, count)
	}
	t.Log("数据库文档总数验证通过")
}
