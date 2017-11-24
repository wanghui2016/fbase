package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	le "github.com/syndtr/goleveldb/leveldb/errors"

	levelopt "github.com/syndtr/goleveldb/leveldb/opt"
	"engine"
	ee "engine/errors"
	"engine/model"
	"engine/rowstore/cache"
	"engine/rowstore/opt"
	ts "model/pkg/timestamp"
	"util"
	"util/log"
)

var c = flag.Int("c", 1, "concurrency")
var n = flag.Int64("n", 100000, "requests")
var l = flag.Int("l", 1024, "value length")
var e = flag.String("e", "fbase", "engine type: leveldb or fbase")
var d = flag.String("d", "", "db directory")
var t = flag.Int("t", 0, "test mode, 0: put; 1: get")

type DB interface {
	Put(k, v []byte) error
	Get(key []byte) error
}

type leveldbImpl struct {
	db *leveldb.DB
}

func (l leveldbImpl) Put(key, value []byte) error {
	return l.db.Put(key, value, nil)
}

func (l leveldbImpl) Get(key []byte) (err error) {
	_, err = l.db.Get(key, nil)
	return
}

type fbaseImpl struct {
	db model.Store
}

func (f fbaseImpl) Put(key, value []byte) error {
	return f.db.Put(key, value, ts.MinTimestamp, 0)
}

func (f fbaseImpl) Get(key []byte) (err error) {
	_, err = f.db.Get(key, ts.MaxTimestamp)
	return
}

func openDB(dir string, dbType string) DB {
	switch dbType {
	case "leveldb":
		var opt levelopt.Options
		opt.WriteBuffer = 32 * 1024 * 1024
		db, err := leveldb.OpenFile(dir, &opt)
		if err != nil {
			panic(err)
		}
		return leveldbImpl{db: db}
	case "fbase":
		db, _, err := engine.NewRowStore(0, dir, nil, nil, &opt.Options{BlockCache: cache.NewCache(cache.NewLRU(100 * 1024 * 1024))})
		if err != nil {
			panic(err)
		}
		return fbaseImpl{db: db}
	default:
		panic("unsupported db type")
	}
}

type result struct {
	duration time.Duration
	err      error
}

func main() {
	flag.Parse()

	if *d == "" {
		var err error
		*d, err = ioutil.TempDir(os.TempDir(), "db_bench_")
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("db path:", *d)

	log.InitFileLog("./logs", "bench", "debug")

	db := openDB(*d, *e)

	var wg sync.WaitGroup
	results := make([]result, *n)
	count := *n
	start := time.Now()
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go bench(db, &wg, &count, results)
	}
	wg.Wait()

	report(results, time.Since(start))
}

func bench(db DB, wg *sync.WaitGroup, count *int64, results []result) {
	defer wg.Done()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	key := make([]byte, 32)
	value := make([]byte, *l)
	for {
		i := atomic.AddInt64(count, -1)
		if i >= 0 {
			util.RandomBytes(r, key)
			util.RandomBytes(r, value)
			start := time.Now()
			if *t == 0 {
				results[int(*n-1-i)].err = db.Put(key, value)
			} else {
				err := db.Get(key)
				if err != le.ErrNotFound && err != ee.ErrNotFound {
					results[int(*n-1-i)].err = db.Get(key)
				}
			}
			results[int(*n-1-i)].duration = time.Since(start)
		} else {
			return
		}
	}
}

func report(results []result, totalSpend time.Duration) {
	errStat := make(map[string]int)
	nErr := 0
	var total, min, max time.Duration
	min = time.Second * 1024
	h := newHistogram()
	for _, r := range results {
		if r.err != nil {
			errStat[r.err.Error()]++
			nErr++
			continue
		}

		if r.duration < min {
			min = r.duration
		}
		if r.duration > max {
			max = r.duration
		}
		total += r.duration

		h.Add(float64(r.duration))
	}
	fmt.Printf("Parameters:\n")
	fmt.Printf("  engine:\t%s\n", *e)
	if *t == 0 {
		fmt.Printf("  type:\t%s\n", "put")
	} else {
		fmt.Printf("  type:\t%s\n", "get")
	}
	fmt.Printf("  path:\t%s\n", *d)
	fmt.Printf("  concurrency:\t%d\n", *c)
	fmt.Printf("  requests:\t%d\n", *n)
	fmt.Printf("  value length:\t%d\n", *l)
	fmt.Println()

	ops := int(float64(*n) / totalSpend.Seconds())

	fmt.Printf("Summary:\n")
	fmt.Printf("  Ops:\t%d\n", ops)
	fmt.Printf("  Success:\t%d\n", *n-int64(nErr))
	fmt.Printf("  Failed:\t%d\n", nErr)
	fmt.Printf("  Total:\t%v\n", total)
	fmt.Printf("  Slowest:\t%v\n", max)
	fmt.Printf("  Fastest:\t%v\n", min)
	fmt.Printf("  Average:\t%v\n", total/(time.Duration)(*n-int64(nErr)))
	fmt.Println()

	if nErr > 0 {
		fmt.Printf("Errors:\n")
		fmt.Printf("  Total:\t%v\n", nErr)
		for k, v := range errStat {
			fmt.Printf("  %s:\t%d\n", k, v)
		}
		fmt.Println()
	}

	fmt.Printf("Histogram:\n")
	fmt.Println(h.ToString())
}
