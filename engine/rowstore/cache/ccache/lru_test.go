package ccache

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	gocache "github.com/syndtr/goleveldb/leveldb/cache"
)

func TestCLRUCache(t *testing.T) {
	cache := NewCLRUCache(1024)
	key := "aaa"
	data := []byte{1, 2, 3, 4, 5}
	cache.Put(key, data)

	h := cache.Get(key)
	actualData := cache.Value(h)
	if !bytes.Equal(actualData, data) {
		t.Errorf("unexpected data: %v, expected: %v", actualData, data)
	}
}

func randomBytes(r *rand.Rand, dst []byte) {
	for i := 0; i < len(dst); i++ {
		dst[i] = ' ' + byte(r.Intn(95))
	}
}

const keyLength = 32
const valueLength = 256
const cacheCapacity = 1024

func BenchmarkCLRU(b *testing.B) {
	cache := NewCLRUCache(cacheCapacity)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%d/%d", 1, i))
		value := make([]byte, valueLength)
		randomBytes(rnd, value)
		cache.Put(string(key), value)

		h := cache.Get(string(key))
		actualValue := cache.Value(h)
		if !bytes.Equal(actualValue, value) {
			b.Errorf("not equal. expected: %v, actual: %v", value, actualValue)
		}
		cache.Release(h)
	}
}

func BenchmarkGoLRU(b *testing.B) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	cache := gocache.NewCache(gocache.NewLRU(cacheCapacity))
	for i := 0; i < b.N; i++ {
		value := make([]byte, valueLength)
		randomBytes(rnd, value)
		handle := cache.Get(1, uint64(i), func() (int, gocache.Value) {
			return valueLength, value
		})
		actualValue := handle.Value().([]byte)
		if !bytes.Equal(actualValue, value) {
			b.Errorf("not equal. expected: %v, actual: %v", value, actualValue)
		}
		handle.Release()
	}
}

func BenchmarkBigBlock(b *testing.B) {
	cache := NewCLRUCache(128 * 1024) // 128k
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	value := make([]byte, 128*1024)
	randomBytes(rnd, value)
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%d/%d", 1, i))
		cache.Put(string(key), value)

		h := cache.Get(string(key))
		actualValue := cache.Value(h)
		if !bytes.Equal(actualValue, value) {
			b.Errorf("not equal. expected: %v, actual: %v", value, actualValue)
		}
		cache.Release(h)
	}
}
