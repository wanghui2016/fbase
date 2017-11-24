package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"engine/rowstore/cache"
	"engine/rowstore/opt"
	ts "model/pkg/timestamp"
)

var dbPath string = "/tmp/test_fbase_store"
var rangeId uint64 = 45

func TestCreateStore(t *testing.T) {
	os.RemoveAll(dbPath)
	startKey := []byte("a")
	endKey := []byte("z^")
	db, applyId, err := NewRowStore(rangeId, fmt.Sprintf("%s/%d", dbPath, rangeId), startKey, endKey, nil)
	if err != nil {
		t.Error(err.Error())
	}
	fmt.Printf("==========applyId:%d", applyId)
	raftIndex := uint64(0)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i)); err != nil {
			fmt.Print(err.Error())
		}
		raftIndex = uint64(i)
	}
	key := fmt.Sprintf("key%06d", 0)
	v, _ := db.Get([]byte(key), ts.MaxTimestamp)
	fmt.Printf("=========get %s", string(v))

	db.Close()

	db, applyId2, _ := NewRowStore(rangeId, fmt.Sprintf("%s/%d", dbPath, rangeId), startKey, endKey, &opt.Options{BlockCache: cache.NewCache(cache.NewLRU(32 * 1024 * 1024))})

	if raftIndex != applyId2 {
		t.Error(fmt.Sprintf("not eq :applyId2:%d,raftIndex:%d", applyId2, raftIndex))
	}

	v2, _ := db.Get([]byte(key), ts.MaxTimestamp)

	if bytes.Compare(v, v2) != 0 {
		fmt.Errorf("get key %s;value: %s != %s", key, v, v2)
	} else {
		fmt.Printf("get key %s return %s", key, string(v2))
	}
	db.Close()
}

func TestStoreSplit(t *testing.T) {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	os.RemoveAll(dbPath)
	startKey := []byte("a")
	endKey := []byte("z^")
	opts := &opt.Options{WriteBuffer: 1024 * 128, BlockCache: cache.NewCache(cache.NewLRU(100 * 1024 * 1024))}
	db, applyId, err := NewRowStore(rangeId, fmt.Sprintf("%s/%d", dbPath, rangeId), startKey, endKey, opts)
	if err != nil {
		t.Error(err.Error())
	}
	if applyId != 0 {
		t.Error("applyId is not zero %d", applyId)
	}
	v := make([]byte, 1024)

	for i := 0; i < 5000; i++ {
		for i := 0; i < 1024; i++ {
			v = append(v, letterBytes[rand.Intn(len(letterBytes))])
		}
		key := []byte(fmt.Sprintf("key%06d", i))
		value := v
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i)); err != nil {
			fmt.Print(err.Error())
		}
	}

	midKey, err := db.FindMiddleKey()
	keyType := midKey[len(midKey)-1]
	timestamp := binary.LittleEndian.Uint64(midKey[len(midKey)-9 : len(midKey)-1])
	userKey := string(midKey[:len(midKey)-9])
	fmt.Printf("midKey:%s,timestamp:%d,keyType:%d \n", userKey, timestamp, keyType)
	leftRId := rangeId + 10000
	rightRid := leftRId + 1

	left, right, err := db.Split(midKey, fmt.Sprintf("%s/%d", dbPath, leftRId), fmt.Sprintf("%s/%d", dbPath, rightRid), uint64(leftRId), uint64(rightRid))
	if db.Applied() != left.Applied() || db.Applied() != right.Applied() {
		t.Error("applied:db %d,left %d,right %d", db.Applied(), left.Applied(), right.Applied())
	}

	it := left.NewIterator(startKey, endKey, ts.MaxTimestamp)
	for it.Next() {
		key := it.Key()
		if bytes.Compare(midKey, key[:len(key)-9]) < 0 {
			t.Error("key:%s out of range[%s-%s] ", key[:len(key)-9], startKey, userKey)
		}
	}
	it.Release()

	it = right.NewIterator(startKey, endKey, ts.MaxTimestamp)
	for it.Next() {
		key := it.Key()
		if bytes.Compare(midKey, key[:len(key)-9]) > 0 {
			t.Error("key:%s out of range[%s-%s] ", key[:len(key)-9], userKey, endKey)
		}
	}
	it.Release()
}

func TestStoreBasic(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_store_")
	if err != nil {
		t.Fatal(err)
	}

	store, _, err := NewRowStore(0, dir, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := store.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i)); err != nil {
			t.Error(err)
		}
	}

	time.Sleep(time.Second)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		actualValue, err := store.Get(key, ts.MaxTimestamp)
		if err != nil {
			t.Fatalf("get %v error: %v", string(key), err)
		}
		if !bytes.Equal(value, actualValue) {
			t.Fatalf("unexpected value. expected=%v, actual=%v", value, actualValue)
		}
	}
}
