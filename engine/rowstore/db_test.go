// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
package rowstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"engine/errors"
	"engine/model"
	"engine/rowstore/cache"
	"engine/rowstore/comparer"
	"engine/rowstore/filesystem"
	"engine/rowstore/opt"
	ts "model/pkg/timestamp"
	"util/assert"
	"util/log"
)

func TestDBOpen(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_eopen_")
	if err != nil {
		t.Fatal(err)
	}

	opt := &opt.Options{}
	// opt.ErrorIfMissing = true

	// db, applied, err := OpenFile("/tmp/mytest", opt)
	db, _, err := OpenFile(dir, opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	if err != nil {
		t.Error(err)
	}
	_ = db
}

func TestPutBigKeyValue(t *testing.T) {
	db := &DB{}
	err := db.Put(make([]byte, maxKeySize+1), nil, 0, ts.Timestamp{1, 3}, 1)
	if err != errKeyTooLarge {
		t.Fatalf("expected error: %v, actual: %v", errKeyTooLarge, err)
	}
	err = db.Put(nil, make([]byte, maxValueSize+1), 0, ts.Timestamp{1, 1}, 1)
	if err != errValueTooLarge {
		t.Fatalf("expected error: %v, actual: %v", errValueTooLarge, err)
	}
}

func TestDBReOpen(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_reopen_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	if err := db.Close(); err != nil {
		t.Errorf("close error:%v", err)
	}

	var apply uint64
	db, apply, err = OpenFile(dir, opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, apply, uint64(1000))

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := db.Get(key, ts.MaxTimestamp)
		// t.Log(key, value)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}
}

func TestGetPut(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_getput_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.RemoveAll(dir)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}

	// 刷入磁盘再get
	if err := db.flushAllMemdbUnlocked(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}

	t.Logf("frozen mem: %v", db.frozenMemLen())
}

func TestSnapshot(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_snapshot_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.RemoveAll(dir)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	snap, err := db.GetSnapshot()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := snap.Get(key)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}

	snap2, err := db.GetSnapshot()
	if err != nil {
		t.Error(err)
	}

	if db.snapList.Len() != 1 {
		t.Errorf("unexpectd snapshot size: %d\n", db.snapList.Len())
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := snap2.Get(key)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}
	stat, err := db.GetStats()
	if err != nil {
		t.Error(err)
	}
	t.Logf("%v", stat)

	snape3 := db.acquireSnapshot(ts.Timestamp{WallTime: 789})
	snape4 := db.acquireSnapshot(ts.Timestamp{WallTime: 10000})
	if db.minSnapshotTS().WallTime != 789 {
		t.Errorf("unexpceted min snapshoted timestamp: %v", db.minSnapshotTS())
	}
	if db.snapList.Len() != 3 {
		t.Errorf("unexpectd snapshot size: %d\n", db.snapList.Len())
	}
	db.releaseSnapshot(snape3)
	db.releaseSnapshot(snape4)

	snap.Release()
	if db.snapList.Len() != 1 {
		t.Errorf("unexpectd snapshot size: %d\n", db.snapList.Len())
	}

	snap2.Release()
	if db.snapList.Len() != 0 {
		t.Errorf("unexpectd snapshot size: %d\n", db.snapList.Len())
	}

	stat, err = db.GetStats()
	if err != nil {
		t.Error(err)
	}
	t.Logf("%v", stat)
}

func TestFindMiddleKey(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_midkey_")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("path: %v", dir)

	db, _, err := OpenFile(dir, opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	time.Sleep(time.Second)

	midKey, err := db.FindMiddleKey()
	if err != nil {
		t.Error(err)
	}

	t.Logf("middle key: %s", string(midKey))
}

func TestKnapsackSolver(t *testing.T) {
	capacity := 10
	weights := []int{4, 3, 5, 2, 5}
	values := []float64{9, 6, 1, 4, 1}
	err := knapsackSolverTest(capacity, weights, values, 19, []bool{true, true, false, true, false})
	if err != nil {
		t.Error(err)
	}

	capacity = 9
	weights = []int{4, 3, 4, 2}
	values = []float64{20, 6, 20, 4}
	err = knapsackSolverTest(capacity, weights, values, 40, []bool{true, false, true, false})
	if err != nil {
		t.Error(err)
	}

	capacity = 5
	weights = []int{2, 2, 1, 3}
	values = []float64{4, 5, 2, 8}
	err = knapsackSolverTest(capacity, weights, values, 13, []bool{false, true, false, true})
	if err != nil {
		t.Error(err)
	}

	capacity = 10
	weights = []int{8}
	values = []float64{20}
	err = knapsackSolverTest(capacity, weights, values, 20, []bool{true})
	if err != nil {
		t.Error(err)
	}
}

func knapsackSolverTest(capacity int, weights []int, values []float64, expectS float64, expostC []bool) error {
	n := len(weights)
	items := make([]knapsackItem, n)
	for i := 0; i < n; i++ {
		items[i].weight = weights[i]
		items[i].value = values[i]
	}
	s := newKnapsackSolver(capacity, items)
	for s.advance() {
	}
	_, solution := s.getSolution()
	if solution != expectS {
		return fmt.Errorf("unexpected solution:%v, expected:%v", solution, expectS)
	}
	if !reflect.DeepEqual(expostC, s.getChosen()) {
		return fmt.Errorf("unexpected chosens:%v, expected:%v", s.getChosen(), expectS)
	}
	return nil
}

func TestSplit(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
		BlockCache:  cache.NewCache(cache.NewLRU(1024 * 1024 * 10)),
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_split_")
	if err != nil {
		t.Fatal(err)
	}

	// log.InitFileLog(dir, "engine", "TRACE")

	t.Logf("path: %v", dir)

	db, _, err := OpenStore(1, filepath.Join(dir, "db"), []byte("key0"), []byte("key1"), opt)
	if err != nil {
		t.Fatal(err)
	}
	// defer os.RemoveAll(dir)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	midKey, err := db.FindMiddleKey()
	if err != nil {
		t.Error(err)
	}
	t.Logf("split midKey: %v, %v", string(midKey), midKey)

	ldb, rdb, err := db.Split(midKey, filepath.Join(dir, "left"), filepath.Join(dir, "right"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("src kvsize: %v, left: %v, right:%v", db.Size(), ldb.Size(), rdb.Size())

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("Get error from %v: %v", "db", err)
		}
		if string(value) != fmt.Sprintf("value%06d", i) {
			t.Fatalf("unexpected value:%s, expected:%s", value, fmt.Sprintf("value%06d", i))
		}
	}
	db.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		var value []byte
		var err error
		var from string
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("Get error from %v: %v, key=%s", from, err, string(key))
		}
		if string(value) != fmt.Sprintf("value%06d", i) {
			t.Fatalf("unexpected value:%s, expected:%s", value, fmt.Sprintf("value%06d", i))
		}
	}

	// 测试关闭后重新打开
	ldb.Close()
	rdb.Close()

	ldb, _, err = OpenStore(2, filepath.Join(dir, "left"), nil, nil, opt)
	if err != nil {
		t.Fatal(err)
	}
	rdb, _, err = OpenStore(3, filepath.Join(dir, "right"), nil, nil, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb.Close()
	defer rdb.Close()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		var value []byte
		var err error
		var from string
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("Get error from %v: %v, key=%s", from, err, string(key))
		}
		if string(value) != fmt.Sprintf("value%06d", i) {
			t.Fatalf("unexpected value:%s, expected:%s", value, fmt.Sprintf("value%06d", i))
		}
	}
}

var randomBaseBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomBytes(rnd *rand.Rand, n int) []byte {
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		ret[i] = byte(randomBaseBytes[rnd.Intn(len(randomBaseBytes))])
	}
	return ret
}

func TestSplitRandom(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer:          1024 * 1024,
		CompactionBudgetSize: 1024 * 1024 * 4,
		TableFileSize:        1024 * 1024,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_split_random_")
	if err != nil {
		t.Fatal(err)
	}

	log.InitFileLog(dir, "engine", "TRACE")

	fmt.Println("path:", dir)
	t.Logf("path: %v", dir)

	db, _, err := OpenStore(1, filepath.Join(dir, "db"), []byte{'\x00'}, []byte{'\xFF'}, opt)
	if err != nil {
		t.Fatal(err)
	}
	// defer os.RemoveAll(dir)

	// 作为对比数据同时存goleveldb里一份
	comparedb, err := leveldb.OpenFile(filepath.Join(dir, "leveldb"), nil)

	// var seed int64 = 1499162645815691318
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("rand seed: %v", seed)
	for i := 0; i < 10000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.flushAllMemdbUnlocked(); err != nil {
		t.Fatal(err)
	}

	var compCnt int
	for {
		score, err := db.GetCompactionScore()
		if err != nil {
			t.Fatal(err)
		}
		if score <= 0.0 {
			t.Logf("compaction run %d times", compCnt)
			break
		}

		task, err := db.NewCompactionTask()
		if err != nil {
			t.Fatal(err)
		}
		compCnt++
		var stat model.CompactionStat
		for {
			finish, err := task.Next(&stat)
			if err != nil {
				t.Fatal(err)
			}
			if finish {
				break
			}
		}
	}

	stat, err := db.GetStats()
	t.Logf("stat: %v", stat)
	data, err := json.Marshal(stat)
	t.Logf("json stat: %v", string(data))

	// 比较数据是否有丢失
	iter := comparedb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		value, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get error from db: %v", err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("check not pass when get key: %v", string(key))
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}
	iter.Release()

	midKey, err := db.FindMiddleKey()
	if err != nil {
		t.Error(err)
	}
	t.Logf("middle key: %v %v", midKey, string(midKey))

	ldb, rdb, err := db.Split(midKey, filepath.Join(dir, "left"), filepath.Join(dir, "right"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("src kvsize: %v, left: %v, right:%v", db.Size(), ldb.Size(), rdb.Size())
	t.Logf("left shared: %v, right shared: %v", ldb.SharedFilesNum(), rdb.SharedFilesNum())

	// 分裂后比较 数据是否有丢失
	// 写入2000条
	for i := 0; i < 2000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		var err error
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(key, midKey) < 0 {
			err = ldb.Put(key, value, 0, ts.Timestamp{WallTime: int64(20000 + i)}, uint64(20000+i))
		} else {
			err = rdb.Put(key, value, 0, ts.Timestamp{WallTime: int64(20000 + i)}, uint64(20000+i))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	iter = comparedb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		var value []byte
		var err error
		from := ""
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("get error from %v: %v, key:%v", from, err, string(key))
		} else {
			if !bytes.Equal(value, expectedValue) {
				// t.Fatalf("check not pass when get key: %v, from %v", string(key), from)
				t.Fatalf("check not pass when get key: %v, from %v, value: %v, expected:%v", string(key), from, value, expectedValue)
			}
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}
	iter.Release()

	// 关闭后重新检查
	ldb.Close()
	rdb.Close()
	ldb, _, err = OpenStore(2, filepath.Join(dir, "left"), []byte{'\x00'}, midKey, opt)
	if err != nil {
		t.Fatal(err)
	}
	rdb, _, err = OpenStore(3, filepath.Join(dir, "right"), midKey, []byte{'\xff'}, opt)
	if err != nil {
		t.Fatal(err)
	}

	// 写入1000条
	for i := 0; i < 1000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		var err error
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(key, midKey) < 0 {
			err = ldb.Put(key, value, 0, ts.Timestamp{WallTime: int64(40000 + i)}, uint64(40000+i))
		} else {
			err = rdb.Put(key, value, 0, ts.Timestamp{WallTime: int64(40000 + i)}, uint64(40000+i))
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	iter = comparedb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		var value []byte
		var err error
		from := ""
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("get error from %v: %v, key:%v", from, err, string(key))
		} else {
			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("check not pass when get key: %v, from %v, value: %v, expected:%v", string(key), from, value, expectedValue)
			}
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}

	// 测试再次分裂
	t.Logf("try to split again")
	ldb.Close()
	rdb.Close()
	db.Close()
	db, _, err = OpenStore(1, filepath.Join(dir, "db"), []byte{'\x00'}, []byte{'\xFF'}, opt)
	if err != nil {
		t.Fatal(err)
	}
	ldb, rdb, err = db.Split(midKey, filepath.Join(dir, "left"), filepath.Join(dir, "right"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	// 再写入1000条
	for i := 0; i < 1000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		var err error
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(key, midKey) < 0 {
			err = ldb.Put(key, value, 0, ts.Timestamp{WallTime: int64(60000 + i)}, uint64(60000+i))
		} else {
			err = rdb.Put(key, value, 0, ts.Timestamp{WallTime: int64(60000 + i)}, uint64(60000+i))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	iter = comparedb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		var value []byte
		var err error
		from := ""
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("get error from %v: %v, key:%v", from, err, string(key))
		} else {
			if !bytes.Equal(value, expectedValue) {
				// t.Fatalf("check not pass when get key: %v, from %v", string(key), from)
				t.Fatalf("check not pass when get key: %v, from %v, value: %v, expected:%v", string(key), from, value, expectedValue)
			}
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}
	iter.Release()
}

func TestSplitEmpty(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer:          1024 * 1024,
		CompactionBudgetSize: 1024 * 1024 * 4,
		TableFileSize:        1024 * 1024,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_split_empty_")
	if err != nil {
		t.Fatal(err)
	}

	log.InitFileLog(dir, "engine", "TRACE")

	fmt.Println("path:", dir)
	t.Logf("path: %v", dir)

	db, _, err := OpenStore(1, filepath.Join(dir, "db"), []byte{'a'}, []byte{'z'}, opt)
	if err != nil {
		t.Fatal(err)
	}
	// defer os.RemoveAll(dir)

	// 作为对比数据同时存goleveldb里一份
	comparedb, err := leveldb.OpenFile(filepath.Join(dir, "leveldb"), nil)

	// var seed int64 = 1499162645815691318
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("rand seed: %v", seed)

	midKey, err := db.FindMiddleKey()
	if err != nil {
		t.Error(err)
	}
	t.Logf("middle key: %v %v", midKey, string(midKey))
	db.vs.timestamp = ts.Timestamp{WallTime: 1}

	ldb, rdb, err := db.Split(midKey, filepath.Join(dir, "left"), filepath.Join(dir, "right"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("src kvsize: %v, left: %v, right:%v", db.Size(), ldb.Size(), rdb.Size())
	t.Logf("left shared: %v, right shared: %v", ldb.SharedFilesNum(), rdb.SharedFilesNum())

	// 分裂后比较 数据是否有丢失
	// 写入2000条
	for i := 0; i < 2000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		var err error
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(key, midKey) < 0 {
			err = ldb.Put(key, value, 0, ts.Timestamp{WallTime: int64(20000 + i)}, uint64(20000+i))
		} else {
			err = rdb.Put(key, value, 0, ts.Timestamp{WallTime: int64(20000 + i)}, uint64(20000+i))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	iter := comparedb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		var value []byte
		var err error
		from := ""
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("get error from %v: %v, key:%v", from, err, string(key))
		} else {
			if !bytes.Equal(value, expectedValue) {
				// t.Fatalf("check not pass when get key: %v, from %v", string(key), from)
				t.Fatalf("check not pass when get key: %v, from %v, value: %v, expected:%v", string(key), from, value, expectedValue)
			}
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}
	iter.Release()

	// 关闭后重新检查
	ldb.Close()
	rdb.Close()
	ldb, _, err = OpenStore(2, filepath.Join(dir, "left"), []byte{'\x00'}, midKey, opt)
	if err != nil {
		t.Fatal(err)
	}
	rdb, _, err = OpenStore(3, filepath.Join(dir, "right"), midKey, []byte{'\xff'}, opt)
	if err != nil {
		t.Fatal(err)
	}

	// 写入1000条
	for i := 0; i < 1000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		var err error
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(key, midKey) < 0 {
			err = ldb.Put(key, value, 0, ts.Timestamp{WallTime: int64(40000 + i)}, uint64(40000+i))
		} else {
			err = rdb.Put(key, value, 0, ts.Timestamp{WallTime: int64(40000 + i)}, uint64(40000+i))
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	iter = comparedb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		var value []byte
		var err error
		from := ""
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("get error from %v: %v, key:%v", from, err, string(key))
		} else {
			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("check not pass when get key: %v, from %v, value: %v, expected:%v", string(key), from, value, expectedValue)
			}
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}

	// 测试再次分裂
	t.Logf("try to split again")
	ldb.Close()
	rdb.Close()
	db.Close()
	db, _, err = OpenStore(1, filepath.Join(dir, "db"), []byte{'\x00'}, []byte{'\xFF'}, opt)
	if err != nil {
		t.Fatal(err)
	}
	ldb, rdb, err = db.Split(midKey, filepath.Join(dir, "left"), filepath.Join(dir, "right"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	// 再写入1000条
	for i := 0; i < 1000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		var err error
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(key, midKey) < 0 {
			err = ldb.Put(key, value, 0, ts.Timestamp{WallTime: int64(60000 + i)}, uint64(60000+i))
		} else {
			err = rdb.Put(key, value, 0, ts.Timestamp{WallTime: int64(60000 + i)}, uint64(60000+i))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	iter = comparedb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		var value []byte
		var err error
		from := ""
		if bytes.Compare(key, midKey) < 0 {
			value, err = ldb.Get(key, ts.MaxTimestamp)
			from = "left"
		} else {
			value, err = rdb.Get(key, ts.MaxTimestamp)
			from = "right"
		}
		if err != nil {
			t.Fatalf("get error from %v: %v, key:%v", from, err, string(key))
		} else {
			if !bytes.Equal(value, expectedValue) {
				// t.Fatalf("check not pass when get key: %v, from %v", string(key), from)
				t.Fatalf("check not pass when get key: %v, from %v, value: %v, expected:%v", string(key), from, value, expectedValue)
			}
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}
	iter.Release()
}

func TestBigBatch(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_big_batch_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.RemoveAll(dir)

	wb := db.NewWriteBatch()
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		wb.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1))
	}
	if err := wb.Commit(); err != nil {
		t.Fatal(err)
	}

	wb = db.NewWriteBatch()
	for i := 500; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		wb.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1))
	}
	if err := wb.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}

	// 刷入磁盘再get
	if err := db.flushAllMemdbUnlocked(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}
}

func TestCompactionTable(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer:          1024 * 1024,
		CompactionBudgetSize: 1024 * 1024 * 4,
		TableFileSize:        1024 * 1024,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_compact_tbl_")
	if err != nil {
		t.Fatal(err)
	}

	log.InitFileLog(dir, "engine", "TRACE")

	fmt.Println("path:", dir)
	t.Logf("path: %v", dir)

	db, _, err := OpenStore(13, filepath.Join(dir, "db"), []byte{'\x00'}, []byte{'\xFF'}, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 作为对比数据同时存goleveldb里一份
	comparedb, err := leveldb.OpenFile(filepath.Join(dir, "leveldb"), nil)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10000; i++ {
		key := randomBytes(rnd, 16)
		value := randomBytes(rnd, 1024)
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
		if err := comparedb.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.flushAllMemdbUnlocked(); err != nil {
		t.Fatal(err)
	}

	var compCnt int
	for {
		score, err := db.GetCompactionScore()
		if err != nil {
			t.Fatal(err)
		}
		if score <= 0.0 {
			t.Logf("compaction run %d times", compCnt)
			break
		}

		task, err := db.NewCompactionTask()
		if err != nil {
			t.Fatal(err)
		}
		compCnt++
		var stat model.CompactionStat
		for {
			finish, err := task.Next(&stat)
			if err != nil {
				t.Fatal(err)
			}
			if finish {
				break
			}
		}
	}

	stat, err := db.GetStats()
	t.Logf("stat: %v", stat)
	data, err := json.Marshal(stat)
	t.Logf("json stat: %v", string(data))

	// 跟存到leveldb中的数据对比， 比较数据是否有丢失
	iter := comparedb.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		value, err := db.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get error from db: %v", err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("check not pass when get key: %v", string(key))
		}
	}
	if iter.Error() != nil {
		t.Fatal(iter.Error())
	}
	iter.Release()

	// 检查是否同一个userkey是否只存在一个版本
	snap, err := db.GetSnapshot()
	if err != nil {
		t.Fatalf("get snapshot failed: %v", err)
	}
	miter := snap.NewMVCCIterator(nil, nil)
	var prevUkey []byte
	for miter.Next() {
		if miter.IsDelete() {
			t.Fatalf("unexpected delete key type")
		}
		if bytes.Equal(miter.Key(), prevUkey) {
			t.Fatalf("exist same ukey: %v", prevUkey)
		}
		prevUkey = append(prevUkey[:0], miter.Key()...)
	}
	if miter.Error() != nil {
		t.Fatal(miter.Error())
	}
	miter.Release()
}

func TestCompactionPolicy(t *testing.T) {
	logger := newDBLogger(0)
	icmp := iComparer{
		ucmp: comparer.DefaultComparer,
	}
	compolicy := newWidthCompactPolicy(100, &icmp, logger)
	tfiles := []*tFile{
		&tFile{
			size: 30 * 1024 * 1024,
			imin: makeInternalKey(nil, []byte("aaa"), ts.MinTimestamp, keyTypeVal),
			imax: makeInternalKey(nil, []byte("bbb"), ts.MinTimestamp, keyTypeVal),
		},
		&tFile{
			size: 30 * 1024 * 1024,
			imin: makeInternalKey(nil, []byte("aaa"), ts.MinTimestamp, keyTypeVal),
			imax: makeInternalKey(nil, []byte("bbb"), ts.MinTimestamp, keyTypeVal),
		},
		&tFile{
			size: 30 * 1024 * 1024,
			imin: makeInternalKey(nil, []byte("ccc"), ts.MinTimestamp, keyTypeVal),
			imax: makeInternalKey(nil, []byte("ddd"), ts.MinTimestamp, keyTypeVal),
		}}

	selected, score := compolicy.pick(tfiles)
	fmt.Println(selected, score)
	t.Log(selected)
}

func TestVersionedIterator(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_versioned_iterator_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// defer os.RemoveAll(dir)

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	snap, err := db.GetSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	for i := 500; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	iter := snap.NewMVCCIterator(nil, nil)
	defer iter.Release()
	var index int
	for iter.Next() {
		if index >= 500 {
			t.Fatalf("should not see timestamp > 500")
		}
		key := []byte(fmt.Sprintf("key%06d", index))
		value := []byte(fmt.Sprintf("value%06d", index))
		if !bytes.Equal(key, iter.Key()) {
			t.Fatalf("unexpected key: %v, expected: %v, at index %d", string(iter.Key()), string(key), index)
		}
		if !bytes.Equal(value, iter.Value()) {
			t.Fatalf("unexpected value: %v, expected: %v, at index %d", string(iter.Value()), string(value), index)
		}
		index++
	}
	if err := iter.Error(); err != nil {
		t.Error(err)
	}
	if index != 500 {
		t.Fatalf("index should be 500")
	}

	// test with slice
	iter = snap.NewMVCCIterator([]byte("key000022"), []byte("key000100"))
	defer iter.Release()
	index = 22
	for iter.Next() {
		if index >= 100 {
			t.Fatalf("should not see timestamp > 500")
		}
		key := []byte(fmt.Sprintf("key%06d", index))
		value := []byte(fmt.Sprintf("value%06d", index))
		if !bytes.Equal(key, iter.Key()) {
			t.Fatalf("unexpected key: %v, expected: %v, at index %d", string(iter.Key()), string(key), index)
		}
		if !bytes.Equal(value, iter.Value()) {
			t.Fatalf("unexpected value: %v, expected: %v, at index %d", string(iter.Value()), string(value), index)
		}
		index++
	}
	if err := iter.Error(); err != nil {
		t.Error(err)
	}

	if index != 100 {
		t.Fatalf("index should be 100, actual: %v", index)
	}
}

func TestCompactBackoff(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_backoff_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, nil)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	assert.NilError(t, err)

	tfiles := []*tFile{
		&tFile{
			fd:     filesystem.FileDesc{Num: 1},
			imin:   makeInternalKey(nil, []byte("a"), ts.MaxTimestamp, keyTypeVal),
			imax:   makeInternalKey(nil, []byte("b"), ts.MaxTimestamp, keyTypeVal),
			shared: true,
		},
		&tFile{
			fd:     filesystem.FileDesc{Num: 2},
			imin:   makeInternalKey(nil, []byte("a"), ts.MaxTimestamp, keyTypeVal),
			imax:   makeInternalKey(nil, []byte("b"), ts.MaxTimestamp, keyTypeVal),
			shared: true,
		},
		&tFile{
			fd:     filesystem.FileDesc{Num: 3},
			imin:   makeInternalKey(nil, []byte("a"), ts.MaxTimestamp, keyTypeVal),
			imax:   makeInternalKey(nil, []byte("b"), ts.MaxTimestamp, keyTypeVal),
			shared: true,
		},
		&tFile{
			fd:     filesystem.FileDesc{Num: 4},
			imin:   makeInternalKey(nil, []byte("a"), ts.MaxTimestamp, keyTypeVal),
			imax:   makeInternalKey(nil, []byte("b"), ts.MaxTimestamp, keyTypeVal),
			shared: false,
		},
	}
	err = db.s.commit(&versionEdit{addedTables: tfiles, timestamp: ts.Timestamp{1, 1}})
	assert.NilError(t, err)

	assert.Equal(t, db.SharedFilesNum(), 3)

	score, err := db.GetCompactionScore()
	assert.NilError(t, err)
	assert.Equal(t, score, sharedScore)

	for i := uint64(0); i < maxSplitCompactBackoff; i++ {
		db.PrepareForSplit()
		score, err = db.GetCompactionScore()
		assert.NilError(t, err)
		if i+1 <= maxSplitCompactBackoff {
			assert.Equal(t, score, sharedScore*float64(i+2))
		} else {
			assert.Equal(t, score, sharedScore*float64(maxSplitCompactBackoff+1))
		}
	}
}

func TestTransaction(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_transaction_")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("path: %v", dir)

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// defer os.RemoveAll(dir)

	tr, err := db.openTransaction()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := tr.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	fds, err := db.s.fs.List(filesystem.TypeTemp)
	assert.NilError(t, err)
	assert.Equal(t, len(fds), len(tr.tables))
	t.Logf("table: %s", tFiles(tr.tables).DebugString())
	err = tr.Commit()
	assert.NilError(t, err)
	assert.Equal(t, tr.db.vs.raftIndex, uint64(500))
	assert.Equal(t, tr.db.vs.timestamp.WallTime, int64(500))

	fds2, err := db.s.fs.List(filesystem.TypeTable)
	assert.NilError(t, err)
	if len(fds2) < len(fds) {
		t.Fatalf("%d less than %d", len(fds2), len(fds))
	}
}

func TestInternalValueParse(t *testing.T) {
	var dst, uvalue, ivalue []byte
	var err error
	var expireAt int64
	value := []byte{1, 2, 3}

	dst = make([]byte, 1)
	makeInternalValueSuffix(dst, 0)
	assert.DeepEqual(t, dst, []byte{0})
	uvalue, expireAt, err = parseInternalValue(dst)
	assert.NilError(t, err)
	assert.Equal(t, expireAt, int64(0))
	assert.Equal(t, len(uvalue), 0)

	ivalue = append(value, dst...)
	uvalue, expireAt, err = parseInternalValue(ivalue)
	assert.NilError(t, err)
	assert.Equal(t, expireAt, int64(0))
	assert.DeepEqual(t, uvalue, value)

	dst = make([]byte, 9)
	makeInternalValueSuffix(dst, time.Now().Add(time.Second).UnixNano())
	assert.Equal(t, dst[8], byte(1))
	num := binary.BigEndian.Uint64(dst)
	uvalue, expireAt, err = parseInternalValue(dst)
	assert.NilError(t, err)
	assert.Equal(t, len(uvalue), 0)
	assert.Equal(t, expireAt, int64(num))
	t.Log(time.Duration(expireAt - time.Now().UnixNano()))

	ivalue = append(value, dst...)
	uvalue, expireAt, err = parseInternalValue(ivalue)
	assert.NilError(t, err)
	assert.Equal(t, expireAt, int64(num))
	assert.DeepEqual(t, uvalue, value)
}

func TestDBTTL(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_ttl_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	key1 := []byte("key001")
	value1 := []byte("value001")
	if err := db.Put(key1, value1, time.Now().Add(time.Millisecond*10).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}
	key2 := []byte("key002")
	value2 := []byte("value002")
	if err := db.Put(key2, value2, time.Now().Add(time.Millisecond*20).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}
	key3 := []byte("key003")
	value3 := []byte("value003")
	if err := db.Put(key3, value3, time.Now().Add(time.Millisecond*30).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}
	key4 := []byte("key004")
	value4 := []byte("value004")
	if err := db.Put(key4, value4, time.Now().Add(time.Second*30).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}

	var value []byte

	value, err = db.Get(key1, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value1)
	value, err = db.Get(key2, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value2)
	value, err = db.Get(key3, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value3)

	// key1过期
	time.Sleep(time.Millisecond * 10)
	value, err = db.Get(key1, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key2, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value2)
	value, err = db.Get(key3, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value3)

	// key2过期
	time.Sleep(time.Millisecond * 10)
	value, err = db.Get(key1, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key2, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key3, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value3)

	// key3过期
	time.Sleep(time.Millisecond * 10)
	value, err = db.Get(key1, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key2, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key3, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)

	db.Close()
	db, _, err = OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	value, err = db.Get(key1, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key2, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key3, ts.MaxTimestamp)
	assert.Equal(t, err, errors.ErrNotFound)
	value, err = db.Get(key4, ts.MaxTimestamp)
	assert.NilError(t, err)
	assert.DeepEqual(t, value, value4)
}
func TestDBTTLIterator(t *testing.T) {
	opt := &opt.Options{
		WriteBuffer: 1024 * 3,
	}

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_ttl_iter_")
	if err != nil {
		t.Fatal(err)
	}

	db, _, err := OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	key1 := []byte("key001")
	value1 := []byte("value001")
	if err := db.Put(key1, value1, time.Now().Add(time.Millisecond*10).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}
	key2 := []byte("key002")
	value2 := []byte("value002")
	if err := db.Put(key2, value2, time.Now().Add(time.Millisecond*20).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}
	key3 := []byte("key003")
	value3 := []byte("value003")
	if err := db.Put(key3, value3, time.Now().Add(time.Millisecond*300).UnixNano(), ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}
	key4 := []byte("key004")
	value4 := []byte("value004")
	if err := db.Put(key4, value4, 0, ts.Timestamp{WallTime: 2}, 2); err != nil {
		t.Error(err)
	}

	var iter model.Iterator
	var count int

	time.Sleep(time.Millisecond * 10)
	iter = db.NewIterator(nil, nil, ts.MaxTimestamp)
	count = 0
	for iter.Next() {
		count++
		switch count {
		case 1:
			assert.DeepEqual(t, iter.Key(), key2)
			assert.DeepEqual(t, iter.Value(), value2)
		case 2:
			assert.DeepEqual(t, iter.Key(), key3)
			assert.DeepEqual(t, iter.Value(), value3)
		case 3:
			assert.DeepEqual(t, iter.Key(), key4)
			assert.DeepEqual(t, iter.Value(), value4)
		default:
			t.Fatalf("unexpected key: %v", iter.Key())
		}
	}
	assert.NilError(t, iter.Error())
	iter.Release()

	time.Sleep(time.Millisecond * 10)
	iter = db.NewIterator(nil, nil, ts.MaxTimestamp)
	count = 0
	for iter.Next() {
		count++
		switch count {
		case 1:
			assert.DeepEqual(t, iter.Key(), key3)
			assert.DeepEqual(t, iter.Value(), value3)
		case 2:
			assert.DeepEqual(t, iter.Key(), key4)
			assert.DeepEqual(t, iter.Value(), value4)
		default:
			t.Fatalf("unexpected key: %v", iter.Key())
		}
	}
	assert.NilError(t, iter.Error())
	iter.Release()

	db.Close()
	db, _, err = OpenFile(dir, opt)
	if err != nil {
		t.Fatal(err)
	}

	iter = db.NewIterator(nil, nil, ts.MaxTimestamp)
	count = 0
	for iter.Next() {
		count++
		switch count {
		case 1:
			assert.DeepEqual(t, iter.Key(), key3)
			assert.DeepEqual(t, iter.Value(), value3)
		case 2:
			assert.DeepEqual(t, iter.Key(), key4)
			assert.DeepEqual(t, iter.Value(), value4)
		default:
			t.Fatalf("unexpected key: %v", iter.Key())
		}
	}
	assert.NilError(t, iter.Error())
	iter.Release()

	time.Sleep(time.Millisecond * 300)
	iter = db.NewIterator(nil, nil, ts.MaxTimestamp)
	count = 0
	for iter.Next() {
		count++
		switch count {
		case 1:
			assert.DeepEqual(t, iter.Key(), key4)
			assert.DeepEqual(t, iter.Value(), value4)
		default:
			t.Fatalf("unexpected key: %v", iter.Key())
		}
	}
	assert.NilError(t, iter.Error())
	iter.Release()
}
