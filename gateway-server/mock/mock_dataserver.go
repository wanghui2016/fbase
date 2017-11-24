package mock

import (
	"fmt"
	"io/ioutil"
	"time"

	"os"

	ds "data-server/server"
	"engine"
	"engine/rowstore/cache"
	"engine/rowstore/opt"
	"model/pkg/kvrpcpb"
	ts "model/pkg/timestamp"
	"util"
	"util/log"
)

type mockClock struct{}

func (c mockClock) Now() ts.Timestamp {
	return ts.Timestamp{WallTime: time.Now().UnixNano()}
}

// DataServerCli : 模拟单节点dataserver(一个本地store)
type DataServerCli struct {
	rowstores map[uint64]*ds.RowStore
	clock     mockClock
	ts        uint64
	ri        uint64
}

// NewDataServerCli dir指定db目录，dir == "" 则使用随机的临时目录
func NewDataServerCli(dir string, ranges []*util.Range) *DataServerCli {
	var err error
	rowstores := make(map[uint64]*ds.RowStore)
	if len(ranges) == 0 {
		if dir == "" {
			dir, err = ioutil.TempDir(os.TempDir(), "fbase_mock_data_server_")
			if err != nil {
				log.Panic("make temp dir failed(%v)", err)
			}
		}
		s, _, err := engine.NewRowStore(1, dir, []byte{'\x00'}, []byte{'\xFF'}, &opt.Options{BlockCache: cache.NewCache(cache.NewLRU(100 * 1024 * 1024))})
		// s, _, err := engine.NewRowStore(1, dir, nil, nil, nil)
		if err != nil {
			log.Panic("create row store failed(%v)", err)
		}
		rowstores[uint64(1)] = ds.NewRowStore(1, s, []byte{'\x00'}, []byte{'\xFF'}, mockClock{}, nil)
	} else {
		for i, r := range ranges {
			dir, err = ioutil.TempDir(os.TempDir(), fmt.Sprintf("fbase_mock_data_server_%d", i+1))
			if err != nil {
				log.Panic("make temp dir failed(%v)", err)
			}
			var startKey, endKey []byte
			if len(r.Start) == 0 {
				startKey = RowPrefix
			} else {
				startKey = EncodeKey(r.Start)
			}
			if len(r.Limit) == 0 {
				rng := util.BytesPrefix(RowPrefix)
				endKey = rng.Limit
			} else {
				endKey = EncodeKey(r.Limit)
			}
			s, _, err := engine.NewRowStore(1, dir, startKey, endKey, &opt.Options{BlockCache: cache.NewCache(cache.NewLRU(100 * 1024 * 1024))})
			// s, _, err := engine.NewRowStore(1, dir, nil, nil, nil)
			if err != nil {
				log.Panic("create row store failed(%v)", err)
			}
			rowstores[uint64(i+1)] = ds.NewRowStore(uint64(i+1), s, startKey, endKey, mockClock{}, nil)
		}
	}

	return &DataServerCli{
		rowstores: rowstores,
	}
}

// Close should release all data.
func (c *DataServerCli) Close() error {
	for _, s := range c.rowstores {
		s.Close()
	}
	return nil
}

func (c *DataServerCli) RawPut(addr string, req *kvrpcpb.DsKvRawPutRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawPutResponse, error) {
	rangeId := req.GetHeader().GetRangeId()
	rowstore, ok := c.rowstores[rangeId]
	if !ok {
		return nil, fmt.Errorf("DataserverCli::SendKVReq: unknown range(%v)", rangeId)
	}
	c.ri++
	resp, err := rowstore.RawPut(req.GetReq(), c.ri)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.DsKvRawPutResponse{Resp: resp}, nil
}

func (c *DataServerCli) RawGet(addr string, req *kvrpcpb.DsKvRawGetRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawGetResponse, error) {
	rangeId := req.GetHeader().GetRangeId()
	rowstore, ok := c.rowstores[rangeId]
	if !ok {
		return nil, fmt.Errorf("DataserverCli::SendKVReq: unknown range(%v)", rangeId)
	}
	resp, err := rowstore.RawGet(req.GetReq())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.DsKvRawGetResponse{Resp: resp}, nil
}
func (c *DataServerCli) RawDelete(addr string, req *kvrpcpb.DsKvRawDeleteRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawDeleteResponse, error) {
	rangeId := req.GetHeader().GetRangeId()
	rowstore, ok := c.rowstores[rangeId]
	if !ok {
		return nil, fmt.Errorf("DataserverCli::SendKVReq: unknown range(%v)", rangeId)
	}
	c.ri++
	resp, err := rowstore.RawDelete(req.GetReq(), c.ri)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.DsKvRawDeleteResponse{Resp: resp}, nil
}
func (c *DataServerCli) Insert(addr string, req *kvrpcpb.DsKvInsertRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvInsertResponse, error) {
	rangeId := req.GetHeader().GetRangeId()
	rowstore, ok := c.rowstores[rangeId]
	if !ok {
		return nil, fmt.Errorf("DataserverCli::SendKVReq: unknown range(%v)", rangeId)
	}
	c.ri++
	resp, err := rowstore.Insert(req.GetReq(), c.ri)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.DsKvInsertResponse{Resp: resp}, nil
}
func (c *DataServerCli) Select(addr string, req *kvrpcpb.DsKvSelectRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvSelectResponse, error) {
	rangeId := req.GetHeader().GetRangeId()
	rowstore, ok := c.rowstores[rangeId]
	if !ok {
		return nil, fmt.Errorf("DataserverCli::SendKVReq: unknown range(%v)", rangeId)
	}
	resp, err := rowstore.Select(req.GetReq())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.DsKvSelectResponse{Resp: resp}, nil
}
func (c *DataServerCli) Delete(addr string, req *kvrpcpb.DsKvDeleteRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvDeleteResponse, error) {
	rangeId := req.GetHeader().GetRangeId()
	rowstore, ok := c.rowstores[rangeId]
	if !ok {
		return nil, fmt.Errorf("DataserverCli::SendKVReq: unknown range(%v)", rangeId)
	}
	c.ri++
	resp, err := rowstore.Delete(req.GetReq(), c.ri)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.DsKvDeleteResponse{Resp: resp}, nil
}

var (
	RowPrefix   = []byte("0")
	IndexPrefix = []byte("1")
)

func DecodeKey(key []byte) []byte {
	return key[1:]
}

// key in store is | flag(1 byte)| real key |
// flag: 0  nomal row.
// flag: 1  column index.
func EncodeKey(key []byte) []byte {
	buff := make([]byte, 0, len(key)+1)
	buff = append(buff, RowPrefix...)
	if len(key) > 0 {
		buff = append(buff, key...)
	}
	return buff
}
