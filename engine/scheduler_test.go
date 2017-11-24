package engine

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"engine/model"
	"engine/rowstore/cache"
	"engine/rowstore/opt"
	"engine/scheduler"
	ts "model/pkg/timestamp"
	"util/log"
)

var randomBaseBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomBytes(rnd *rand.Rand, n int) []byte {
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		ret[i] = byte(randomBaseBytes[rnd.Intn(len(randomBaseBytes))])
	}
	return ret
}

func TestSchedulerOne(t *testing.T) {

	// c := &scheduler.Config{
	// 	WriteRateLimit: 1024 * 1024 * 5,
	// 	Concurrency:    4,
	// }
	// _ = c
	// scheduler.StartScheduler(c)

	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_engine_scheduler_")
	if err != nil {
		t.Fatal(err)
	}
	// defer os.RemoveAll(dir)
	t.Logf("path: %v", dir)
	fmt.Println("path: ", dir)
	log.InitFileLog(dir, "scheduler", "DEBUG")

	testN := 10
	dbs := make(map[uint64]model.Store)
	var wg sync.WaitGroup
	wg.Add(testN)
	// 并发十个db
	opt := &opt.Options{
		WriteBuffer:          1024 * 1024 * 2,
		CompactionBudgetSize: 1024 * 1024 * 6,
		TableFileSize:        1024 * 1024 * 2,
		BlockCache:           cache.NewCache(cache.NewLRU(100 * 1024 * 1024)),
	}
	for i := 1; i <= testN; i++ {
		s, _, err := NewRowStore(uint64(i), filepath.Join(dir, fmt.Sprintf("%d", i)), nil, nil, opt)
		if err != nil {
			t.Fatal(err)
		}
		dbs[uint64(i)] = s
	}

	for _, db := range dbs {
		go func(db model.Store) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < 1024*2*5; i++ {
				key := randomBytes(rnd, 16)
				value := randomBytes(rnd, 1024)
				if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i+1)); err != nil {
					t.Fatal(err)
				}
			}
		}(db)
	}
	wg.Wait()

	c := &scheduler.Config{
		WriteRateLimit: 1024 * 1024 * 200,
		Concurrency:    4,
	}
	scheduler.StartScheduler(c)
	for i, db := range dbs {
		scheduler.RegisterDB(i, db)
	}

	for {
		over := true
		for _, db := range dbs {
			score, _ := db.GetCompactionScore()
			fmt.Println("score: ", score)
			if score > 0.0 {
				over = false
			}
		}

		if !over {
			time.Sleep(time.Millisecond * 500)
		} else {
			break
		}
	}

	t.Logf("compact finish..............")

	time.Sleep(time.Second * 10)

	scheduler.StopScheduler()
	time.Sleep(time.Second)
}
