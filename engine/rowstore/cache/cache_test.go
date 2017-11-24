package cache

import (
	"math/rand"
	"testing"
	"time"

	"util/assert"
)

func TestMurmurHash(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	bucketNum := 32
	buckets := make([]uint64, 32)
	n := 10000000
	for i := 0; i < n; i++ {
		hash := murmur32(rnd.Uint64(), rnd.Uint64(), rnd.Uint64(), 0xf00) % uint32(bucketNum)
		buckets[hash]++
	}
	for i := 0; i < bucketNum; i++ {
		t.Logf("[%d]: %f", i, float64(buckets[i])/float64(n))
	}
}

type Data struct {
	a, b, c uint64
}

func TestCache(t *testing.T) {
	cache := NewCache(NewLRU(1024))
	var datas []Data
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			for k := 1; k <= 3; k++ {
				datas = append(datas, Data{uint64(i), uint64(j), uint64(k)})
			}
		}
	}
	// set
	for _, d := range datas {
		h := cache.Get(d.a, d.b, d.c, func() (size int64, value Value) {
			return 1, d
		})
		h.Release()
	}
	assert.Equal(t, cache.Nodes(), 27)
	assert.Equal(t, cache.Size(), int64(27))
	// get
	for _, d := range datas {
		handle := cache.Get(d.a, d.b, d.c, nil)
		assert.NotNil(t, handle)
		assert.DeepEqual(t, handle.Value().(Data), d)
		handle.Release()
	}

	handle := cache.Get(9, 9, 9, func() (int64, Value) {
		return 1024 - 27 + 2, 1
	})
	handle.Release()

	assert.Equal(t, cache.Nodes(), 26)
	assert.Equal(t, cache.Size(), int64(1024))
	handle = cache.Get(1, 1, 1, nil)
	assert.Nil(t, handle)
	handle = cache.Get(1, 1, 2, nil)
	assert.Nil(t, handle)
}
