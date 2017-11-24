package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"

	"golang.org/x/net/context"
	"util/errcode"
	sErr "engine/errors"
	"engine/model"
	"model/pkg/kvrpcpb"
	"model/pkg/raft_cmdpb"
	ts "model/pkg/timestamp"
	"raftgroup"
	"util"
	"util/log"
	"util/metrics"
)

var DefaultMaxKvCountLimit uint64 = 10000

// Clock clock interface
type Clock interface {
	Now() ts.Timestamp
}

// RowStore row store
type RowStore struct {
	id       uint64
	store    model.Store
	startKey []byte
	endKey   []byte
	clock    Clock
	metric   *metrics.MetricMeter

	status storeStatus
}

// NewRowStore create new row store
func NewRowStore(id uint64, store model.Store, startKey, endKey []byte, clock Clock, metric *metrics.MetricMeter) *RowStore {
	return &RowStore{
		id:       id,
		store:    store,
		startKey: startKey,
		endKey:   endKey,
		clock:    clock,
		metric:   metric,
	}
}

// Status get store status
func (s *RowStore) Status() string {
	return s.status.String()
}

// RawGet get raw kv, not row
func (s *RowStore) RawGet(req *kvrpcpb.KvRawGetRequest) (*kvrpcpb.KvRawGetResponse, error) {
	resp := new(kvrpcpb.KvRawGetResponse)
	start := time.Now()
	value, err := s.store.Get(req.GetKey(), s.clock.Now())
	s.addMetric("store_get", start)
	if err != nil {
		if err == sErr.ErrNotFound {
			resp.Code = SUCCESS
			return resp, nil
		}
		return nil, err
	}
	resp.Code = SUCCESS
	resp.Value = value
	return resp, nil
}

// RawPut raw put
func (s *RowStore) RawPut(req *kvrpcpb.KvRawPutRequest, raftIndex uint64) (*kvrpcpb.KvRawPutResponse, error) {
	resp := new(kvrpcpb.KvRawPutResponse)
	start := time.Now()
	err := s.store.Put(req.GetKey(), req.GetValue(), 0, s.clock.Now(), raftIndex)
	s.addMetric("store_put", start)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

// RawDelete raw delete
func (s *RowStore) RawDelete(req *kvrpcpb.KvRawDeleteRequest, raftIndex uint64) (*kvrpcpb.KvRawDeleteResponse, error) {
	resp := new(kvrpcpb.KvRawDeleteResponse)
	start := time.Now()
	err := s.store.Delete(req.GetKey(), s.clock.Now(), raftIndex)
	s.addMetric("store_delete", start)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

// Insert insert rows
func (s *RowStore) Insert(req *kvrpcpb.KvInsertRequest, raftIndex uint64) (*kvrpcpb.KvInsertResponse, error) {
	resp := new(kvrpcpb.KvInsertResponse)
	rows := req.GetRows()

	var affected uint64
	batch := s.store.NewWriteBatch()
	start := time.Now()
	for _, row := range rows {
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[range %d] insert key %v", s.id, row.Key)
		}
		// 添加主键前缀
		key := EncodeKey(row.Key)
		// 判断是否在该分片的区域
		if !s.checkKeyInRange(key) {
			log.Warn("key[%v] not in range[%v, %v]!!!", key, s.startKey, s.endKey)
			continue
		}
		// 检查主键不存在
		if req.CheckDuplicate {
			start := time.Now()
			_, err := s.store.Get(key, req.GetTimestamp())
			s.addMetric("store_get", start)
			if err != nil {
				if err != sErr.ErrNotFound {
					log.Error("[range %d]check insert key(%v) duplicate error: %v", s.id, row.Key, err)
					return nil, fmt.Errorf("[range %d] check insert key(%d) duplicate failed(%v)", s.id, row.Key, err)
				}
			} else {
				if log.GetFileLogger().IsEnableDebug() {
					log.Debug("[range %d]found insert duplicate key(%v)", s.id, row.Key)
				}
				resp.Code = 1062
				resp.DuplicateKey = row.GetKey()
				return resp, nil
			}
		}
		// 插入到存储层

		//if err := s.store.Put(key, row.Value, time.Duration(row.TTL), req.GetTimestamp(), raftIndex); err != nil {
		//	log.Error("[range %d]put kv to store failed: %v", s.id, row.Key, err)
		//	return nil, fmt.Errorf("[range %d] put kv to store failed(%v)", s.id, err)
		//}
		batch.Put(key, row.Value, row.ExpireAt, req.GetTimestamp(), raftIndex)
		affected++
		continue
	}
	if affected > 0 {
		if err := batch.Commit(); err != nil {
			log.Error("[range %d]put kv to store failed: %v", s.id, err)
			return nil, fmt.Errorf("[range %d] put kv to store failed(%v)", s.id, err)
		}
		s.addMetric("store_put", start)
	}

	resp.Code = SUCCESS
	resp.AffectedKeys = affected
	return resp, nil
}

// BatchInsert batch insert
func (s *RowStore) BatchInsert(req *kvrpcpb.KvBatchInsertRequest, raftIndex uint64) (*kvrpcpb.KvBatchInsertResponse, error) {
	resp := &kvrpcpb.KvBatchInsertResponse{
		Resps: make([]*kvrpcpb.KvInsertResponse, 0, len(req.Reqs)),
	}

	batch := s.store.NewWriteBatch()
	start := time.Now()
	count := 0
	for _, insertReq := range req.Reqs {
		insertResp := new(kvrpcpb.KvInsertResponse)
		for _, row := range insertReq.Rows {
			if log.GetFileLogger().IsEnableDebug() {
				log.Debug("[range %d] batch insert key %v", s.id, row.Key)
			}
			key := EncodeKey(row.Key)
			if !s.checkKeyInRange(key) {
				log.Warn("key[%v] not in range[%v, %v]!!!", key, s.startKey, s.endKey)
				continue
			}
			if insertReq.CheckDuplicate {
				return nil, fmt.Errorf("pk duplicate check not allowed in batch insert")
			}
			batch.Put(key, row.Value, row.ExpireAt, insertReq.GetTimestamp(), raftIndex)
			insertResp.AffectedKeys++
			count++
		}
		resp.Resps = append(resp.Resps, insertResp)
	}

	if err := batch.Commit(); err != nil {
		log.Error("[range %d] batch commit failed(%v), count:%d", s.id, err, count)
		return nil, err
	}

	s.addMetric("store_put", start)
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("[range %d] batch insert %d rows, take: %v", s.id, count, time.Since(start))
	}

	return resp, nil
}

// Update update row
func (s *RowStore) Update(req *kvrpcpb.KvUpdateRequest) (*kvrpcpb.KvUpdateResponse, error) {
	return nil, errors.New("not implement")
}

// Replace replace row
func (s *RowStore) Replace(req *kvrpcpb.KvReplaceRequest) (*kvrpcpb.KvReplaceResponse, error) {
	return nil, errors.New("not implement")
}

// Delete delete one row
func (s *RowStore) Delete(req *kvrpcpb.KvDeleteRequest, raftIndex uint64) (*kvrpcpb.KvDeleteResponse, error) {
	resp := new(kvrpcpb.KvDeleteResponse)

	fetcher := newDeleteRowFetcher(s, req)
	defer fetcher.Close()
	batch := s.store.NewWriteBatch()
	start := time.Now()
	for fetcher.Next() {
		log.Debug("[range %d] delete key %v", s.id, fetcher.Key())
		batch.Delete(fetcher.Key(), req.GetTimestamp(), raftIndex)
		//start := time.Now()
		//err := s.store.Delete(fetcher.Key(), req.GetTimestamp(), raftIndex)
		//s.addMetric("store_delete", start)
		//if err != nil {
		//	log.Debug("[ragnge %d] delete failed(%v), key: %v", s.id, err, fetcher.Key())
		//	return nil, err
		//}
		//log.Debug("[range %d] delete key %v success", s.id, fetcher.Key())
		resp.AffectedKeys++
	}
	if fetcher.Error() != nil {
		return nil, fetcher.Error()
	}
	if resp.AffectedKeys > 0 {
		err := batch.Commit()
		s.addMetric("store_delete", start)
		if err != nil {
			log.Debug("[ragnge %d] delete failed(%v)", s.id, err)
			return nil, err
		}
	}

	resp.Code = errcode.SUCCESS
	return resp, nil
}

// GetSnapshot get snapshot
func (s *RowStore) GetSnapshot() (model.Snapshot, error) {
	return s.store.GetSnapshot()
}

// ApplySnapshot apply snapshot
func (s *RowStore) ApplySnapshot(ctx context.Context, iter *raftgroup.SnapshotKVIterator) error {
	s.status.startSnapshot()
	defer func() {
		s.status.endSnapshot()
	}()

	var (
		tr    model.Transaction
		err   error
		count int
	)

	tr, err = s.store.OpenTransaction()
	if err != nil {
		return err
	}

	start := time.Now()
	for {
		shouldStop := false
		select {
		case <-ctx.Done():
			shouldStop = true
		default:
		}
		if shouldStop {
			err = errRangeShutDown
			break
		}

		count++
		if count%16 == 0 {
			runtime.Gosched()
		}

		var kvpair *raft_cmdpb.MVCCKVPair
		kvpair, err = iter.Next()
		if err != nil {
			break
		}

		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[range %d] apply snapshot. key: %v, is_delete: %v", s.id, kvpair.Key, kvpair.IsDelete)
		}

		if kvpair.IsDelete {
			err = tr.Delete(kvpair.Key, kvpair.Timestamp, kvpair.ApplyIndex)
			if err != nil {
				break
			}
			s.status.addSnapDelCout()
			s.status.addSnapApplySize(int64(len(kvpair.Key)))
		} else {
			now := time.Now().UnixNano()
			if kvpair.ExpireAt > 0 && now > kvpair.ExpireAt {
				// expired, skip
				continue
			}
			err = tr.Put(kvpair.Key, kvpair.Value, kvpair.ExpireAt, kvpair.Timestamp, kvpair.ApplyIndex)
			if err != nil {
				break
			}
			s.status.addSnapPutCount()
			s.status.addSnapApplySize(int64(len(kvpair.Key) + len(kvpair.Value)))
		}
	}

	if err == io.EOF {
		err = tr.Commit()
	}
	if err != nil {
		tr.Discard()
		log.Error("[range %d] apply snapshot failed[%v]", s.id, err)
		return err
	}
	log.Info("[range %d] receive snapshot finished. stat: %s, spend: %v", s.id, s.status.String(), time.Since(start))
	return nil
}

// GetSplitKey get split key
func (s *RowStore) GetSplitKey() ([]byte, error) {
	return s.store.FindMiddleKey()
}

// Split 分裂
func (s *RowStore) Split(midKey []byte, leftPath, rigthPath string, leftDBId, rigthDBId uint64) (model.Store, model.Store, error) {
	start := time.Now()
	l, r, err := s.store.Split(midKey, leftPath, rigthPath, leftDBId, rigthDBId)
	log.Info("[range %d] splited. spend: %v, result: %v", s.id, time.Since(start), err)
	return l, r, err
}

// Applied 获取底层存储应用位置
func (s *RowStore) Applied() uint64 {
	return s.store.Applied()
}

// Size 获取底层store磁盘占用
func (s *RowStore) Size() uint64 {
	return s.store.DiskUsage()
}

// Close 关闭
func (s *RowStore) Close() error {
	log.Info("[range %d] close store", s.id)
	return s.store.Close()
}

func (s *RowStore) PrepareSplit() bool {
	s.store.PrepareForSplit()
	return s.store.SharedFilesNum() == 0
}

func (s *RowStore) addMetric(method string, start time.Time) {
	if s.metric != nil {
		s.metric.AddApiWithDelay(method, true, time.Since(start))
	}
}

// 添加RowPrefix前缀，保证区间落到[s.startKey, s.endKey)中
func (s *RowStore) adjustRange(startKey, endKey []byte) ([]byte, []byte) {
	var _startKey, _endKey []byte
	if len(startKey) == 0 {
		_startKey = RowPrefix
	} else {
		_startKey = EncodeKey(startKey)
	}
	if len(endKey) == 0 {
		rng := util.BytesPrefix(RowPrefix)
		_endKey = rng.Limit
	} else {
		_endKey = EncodeKey(endKey)
	}
	if s.startKey != nil && bytes.Compare(s.startKey, _startKey) > 0 {
		_startKey = s.startKey
	}
	if s.endKey != nil && bytes.Compare(s.endKey, _endKey) < 0 {
		_endKey = s.endKey
	}
	return _startKey, _endKey
}

func (s *RowStore) checkKeyInRange(key []byte) bool {
	if bytes.Compare(key, s.startKey) >= 0 && bytes.Compare(key, s.endKey) < 0 {
		return true
	}
	return false
}
