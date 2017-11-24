package server

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"engine"
	"engine/model"
	"engine/rowstore/opt"
	"model/pkg/raft_cmdpb"
	"raftgroup"
	"util"
	"engine/rowstore/cache"
	"model/pkg/timestamp"
)

type raftSnapTestIter struct {
	ctx   context.Context
	r     *rand.Rand
	ldb   *leveldb.DB // Next返回的kv存到leveldb中，最后做个比较
	index uint64
	total uint64
}

func newRaftSnapTestIter(ctx context.Context, total uint64, path string) *raftSnapTestIter {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(err)
	}

	return &raftSnapTestIter{
		ctx:   ctx,
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
		ldb:   ldb,
		total: total,
	}
}

func (iter *raftSnapTestIter) Next() ([]byte, error) {
	select {
	case <-iter.ctx.Done():
		return nil, io.EOF
	default:
	}

	iter.index++
	if iter.index > iter.total {
		return nil, io.EOF
	}

	p := new(raft_cmdpb.MVCCKVPair)
	p.ApplyIndex = iter.index
	p.Timestamp = timestamp.MaxTimestamp
	p.Key = make([]byte, 16)
	util.RandomBytes(iter.r, p.Key)
	p.Value = make([]byte, 256)
	util.RandomBytes(iter.r, p.Value)
	err := iter.ldb.Put(p.Key, p.Value, nil)
	if err != nil {
		return nil, err
	}

	data, err := p.Marshal()
	return data, err
}

func (iter *raftSnapTestIter) Compare(s model.Store) error {
	t := iter.ldb.NewIterator(nil, nil)
	defer t.Release()
	for t.Next() {
		value, err := s.Get(t.Key(), timestamp.MaxTimestamp)
		if err != nil {
			return err
		}
		if !bytes.Equal(value, t.Value()) {
			return fmt.Errorf("compare failed. key: %v, expected value: %v, actual: %v", t.Key(), t.Value(), value)
		}
	}
	return t.Error()
}

func TestRowStoreSnapshot(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_dataserver_rowstore_snap_")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("path: %v", dir)

	opt := &opt.Options{WriteBuffer: 1024 * 128,BlockCache:cache.NewCache(cache.NewLRU(100*1024*1024)), }
	s, _, err := engine.NewRowStore(1, filepath.Join(dir, "db"), nil, nil, opt)
	if err != nil {
		t.Fatal(err)
	}

	rs := NewRowStore(1, s, nil, nil, nil,nil)
	ctx := context.Background()
	iter := newRaftSnapTestIter(context.Background(), 10000, filepath.Join(dir, "ldb"))
	err = rs.ApplySnapshot(ctx, raftgroup.NewSnapshotKVIterator(iter))
	if err != nil {
		t.Fatal(err)
	}
	err = iter.Compare(s)
	if err != nil {
		t.Fatal(err)
	}

	// test stop
	s, _, err = engine.NewRowStore(1, filepath.Join(dir, "db2"), nil, nil, opt)
	rs = NewRowStore(1, s, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	iter = newRaftSnapTestIter(context.Background(), math.MaxUint64, filepath.Join(dir, "ldb2"))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err = rs.ApplySnapshot(ctx, raftgroup.NewSnapshotKVIterator(iter))
		wg.Done()
		if err != errRangeShutDown {
			t.Fatalf("unexpected error: %v", err)
		}
	}()
	for {
		if iter.index > 20000 {
			select {
			case <-ctx.Done():
			default:
				cancel()
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
}
