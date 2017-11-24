package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	sErr "engine/errors"
	"engine/model"
	"model/pkg/kvrpcpb"
	"model/pkg/raft_cmdpb"
	ts "model/pkg/timestamp"
	"raft"
	raftproto "raft/proto"
	"raftgroup"
	"util/log"
)

const (
	SUCCESS            int32 = 0
	ER_NOT_LEADER            = 1
	ER_SERVER_BUSY           = 2
	ER_SERVER_STOP           = 3
	ER_READ_ONLY             = 4
	ER_ENTITY_NOT_EXIT       = 5
	ER_UNKNOWN               = 6

	// SQL ERROR CODE from 1000
)

var DefaultMaxSubmitTimeout      time.Duration = time.Second * 60

type Iterator interface {
	// return false if over or error
	Next() bool

	Key() []byte
	Value() []byte

	Error() error

	// Release iterator使用完需要释放
	Release()
}

type Store interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Scan(startKey, limitKey []byte) Iterator
	NewBatch() Batch
	Close() error
	LocalStore() model.Store
}

type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)

	Commit() error
}

type SaveStore struct {
	store      model.Store
	raft       *raftgroup.RaftGroup
	raftServer *raft.RaftServer
	localRead  bool
	quit       chan struct{}
	wg         sync.WaitGroup
}

func NewSaveStore(raft *raftgroup.RaftGroup, store model.Store) *SaveStore {
	s := &SaveStore{store: store, raft: raft, localRead: true, quit: make(chan struct{})}
	s.wg.Add(1)
	go s.raftLogCleanup()
	return s
}

func (s *SaveStore) raftLogCleanup() {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					switch x := r.(type) {
					case string:
						fmt.Printf("Error: %s.\n", x)
					case error:
						fmt.Printf("Error: %s.\n", x.Error())
					default:
						fmt.Printf("Unknown panic error.%v", r)
					}
				}
			}()
			select {
			case <-s.quit:
				return
			case <-ticker.C:
				applyId := s.store.Applied()
				if applyId < DefaultRaftLogCount {
					return
				}
				s.raftServer.Truncate(1, applyId-DefaultRaftLogCount)
			}
		}()
	}
}

func (s *SaveStore) Put(key, value []byte) error {
	req := &raft_cmdpb.RaftCmdRequest{
		Type: raft_cmdpb.MessageType_Data,
		Request: &raft_cmdpb.Request{
			CmdType: raft_cmdpb.CmdType_RawPut,
			KvRawPutReq: &kvrpcpb.KvRawPutRequest{
				Key:   key,
				Value: value,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultMaxSubmitTimeout)
	defer cancel()
	_, err := s.raft.SubmitCommand(ctx, req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}

func (s *SaveStore) Delete(key []byte) error {
	req := &raft_cmdpb.RaftCmdRequest{
		Type: raft_cmdpb.MessageType_Data,
		Request: &raft_cmdpb.Request{
			CmdType: raft_cmdpb.CmdType_RawDelete,
			KvRawDeleteReq: &kvrpcpb.KvRawDeleteRequest{
				Key: key,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultMaxSubmitTimeout)
	defer cancel()
	_, err := s.raft.SubmitCommand(ctx, req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}

func (s *SaveStore) Get(key []byte) ([]byte, error) {
	if s.localRead {
		return s.store.Get(key, ts.MaxTimestamp)
	}
	req := &raft_cmdpb.RaftCmdRequest{
		Type: raft_cmdpb.MessageType_Data,
		Request: &raft_cmdpb.Request{
			CmdType: raft_cmdpb.CmdType_RawGet,
			KvRawGetReq: &kvrpcpb.KvRawGetRequest{
				Key: key,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultMaxSubmitTimeout)
	defer cancel()
	resp, err := s.raft.SubmitCommand(ctx, req)
	if err != nil {
		return nil, err
	}
	value := resp.GetResponse().GetKvRawGetResp().GetValue()
	return value, nil
}

func (s *SaveStore) Scan(startKey, limitKey []byte) Iterator {
	return s.store.NewIterator(startKey, limitKey, ts.MaxTimestamp)
}

func (s *SaveStore) NewBatch() Batch {
	return NewSaveBatch(s.raft)
}

func (s *SaveStore) Close() error {
	s.raft.Release()
	close(s.quit)
	s.wg.Wait()
	return s.store.Close()
}

func (s *SaveStore) LocalStore() model.Store {
	return s.store
}

func (s *SaveStore) raftKvRawGet(req *kvrpcpb.KvRawGetRequest, raftIndex uint64) (*kvrpcpb.KvRawGetResponse, error) {
	resp := new(kvrpcpb.KvRawGetResponse)
	//log.Info("raft put")
	// TODO write in one batch
	value, err := s.store.Get(req.GetKey(), ts.MaxTimestamp)
	if err != nil {
		if err == sErr.ErrNotFound {
			resp.Code = SUCCESS
			resp.Value = nil
			return resp, nil
		}
		return nil, err
	}
	resp.Code = SUCCESS
	resp.Value = value
	return resp, nil
}

func (s *SaveStore) raftKvRawPut(req *kvrpcpb.KvRawPutRequest, raftIndex uint64) (*kvrpcpb.KvRawPutResponse, error) {
	resp := new(kvrpcpb.KvRawPutResponse)
	//log.Info("raft put")
	// TODO write in one batch
	err := s.store.Put(req.GetKey(), req.GetValue(), 0, ts.Timestamp{WallTime: int64(raftIndex)}, raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (s *SaveStore) raftKvRawDelete(req *kvrpcpb.KvRawDeleteRequest, raftIndex uint64) (*kvrpcpb.KvRawDeleteResponse, error) {
	resp := new(kvrpcpb.KvRawDeleteResponse)
	err := s.store.Delete(req.GetKey(), ts.Timestamp{WallTime: int64(raftIndex)}, raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (s *SaveStore) raftKvRawExecute(req *kvrpcpb.KvRawExecuteRequest, raftIndex uint64) (*kvrpcpb.KvRawExecuteResponse, error) {
	resp := new(kvrpcpb.KvRawExecuteResponse)
	batch := s.store.NewWriteBatch()
	for _, e := range req.GetExecs() {
		switch e.Do {
		case kvrpcpb.ExecuteType_RawPut:
			batch.Put(e.KvPair.Key, e.KvPair.Value, 0, ts.Timestamp{WallTime: int64(raftIndex)}, raftIndex)
		case kvrpcpb.ExecuteType_RawDelete:
			batch.Delete(e.KvPair.Key, ts.Timestamp{WallTime: int64(raftIndex)}, raftIndex)
		}
	}
	err := batch.Commit()
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (s *SaveStore) HandleRaftKv(req *raft_cmdpb.Request, raftIndex uint64) (resp *raft_cmdpb.Response, err error) {
	resp = new(raft_cmdpb.Response)
	resp.CmdType = req.GetCmdType()

	// TODO check split status
	switch req.GetCmdType() {
	case raft_cmdpb.CmdType_RawGet:
		_resp, err := s.raftKvRawGet(req.GetKvRawGetReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvRawGetResp = _resp
	case raft_cmdpb.CmdType_RawPut:
		_resp, err := s.raftKvRawPut(req.GetKvRawPutReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvRawPutResp = _resp
	case raft_cmdpb.CmdType_RawDelete:
		_resp, err := s.raftKvRawDelete(req.GetKvRawDeleteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvRawDeleteResp = _resp
	case raft_cmdpb.CmdType_RawExecute:
		_resp, err := s.raftKvRawExecute(req.GetKvRawExecuteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvRawExecuteResp = _resp
	}
	return resp, nil
}

func (s *SaveStore) GetSnapshot() (model.Snapshot, error) {
	return s.store.GetSnapshot()
}

func (s *SaveStore) ApplySnapshot(iter *raftgroup.SnapshotKVIterator) error {
	var err error
	var pair *raft_cmdpb.MVCCKVPair
	for {
		pair, err = iter.Next()
		if err != nil {
			log.Error("apply snapshot error [%v]", err)
			break
		}
		s.store.Put(pair.Key, pair.Value, 0, pair.Timestamp, pair.ApplyIndex)
	}
	return err
}

func (s *SaveStore) HandleCmd(req *raft_cmdpb.RaftCmdRequest, raftIndex uint64) (resp *raft_cmdpb.RaftCmdResponse, err error) {
	resp = new(raft_cmdpb.RaftCmdResponse)
	resp.Type = req.GetType()
	switch req.GetType() {
	/*case raft_cmdpb.MessageType_Kv:*/
	/*case raft_cmdpb.MessageType_Admin:*/
	case raft_cmdpb.MessageType_Data:
		kvResp, err := s.HandleRaftKv(req.GetRequest(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.Response = kvResp
	default:
		resp, err = nil, ErrUnknownCommandType
	}
	return
}

func (s *SaveStore) HandlePeerChange(confChange *raftproto.ConfChange) (res interface{}, err error) {
	switch confChange.Type {
	case raftproto.ConfAddNode:

		res, err = nil, nil
	case raftproto.ConfRemoveNode:

		res, err = nil, nil
	case raftproto.ConfUpdateNode:
		log.Debug("update range peer")
		res, err = nil, nil
	default:
		res, err = nil, ErrUnknownCommandType
	}

	return
}

////TODO
func (s *SaveStore) HandleGetSnapshot() (model.Snapshot, error) {
	log.Debug("get snapshot")
	return s.GetSnapshot()
}

func (s *SaveStore) HandleApplySnapshot(peers []raftproto.Peer, iter *raftgroup.SnapshotKVIterator) error {
	log.Debug("apply snapshot")
	return s.ApplySnapshot(iter)
}

type SaveBatch struct {
	raft  *raftgroup.RaftGroup
	lock  sync.RWMutex
	batch []*kvrpcpb.KvPairRawExecute
}

func NewSaveBatch(raft *raftgroup.RaftGroup) Batch {
	return &SaveBatch{raft: raft, batch: nil}
}

func (b *SaveBatch) Put(key, value []byte) {
	_key := make([]byte, len(key))
	_value := make([]byte, len(value))
	copy(_key, key)
	copy(_value, value)
	exec := &kvrpcpb.KvPairRawExecute{
		Do:     kvrpcpb.ExecuteType_RawPut,
		KvPair: &kvrpcpb.KvPair{Key: _key, Value: _value},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Delete(key []byte) {
	_key := make([]byte, len(key))
	copy(_key, key)
	exec := &kvrpcpb.KvPairRawExecute{
		Do:     kvrpcpb.ExecuteType_RawDelete,
		KvPair: &kvrpcpb.KvPair{Key: _key},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Commit() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	batch := b.batch
	b.batch = nil
	// 空提交
	if len(batch) == 0 {
		return nil
	}
	req := &raft_cmdpb.RaftCmdRequest{
		Type: raft_cmdpb.MessageType_Data,
		Request: &raft_cmdpb.Request{
			CmdType: raft_cmdpb.CmdType_RawExecute,
			KvRawExecuteReq: &kvrpcpb.KvRawExecuteRequest{
				Execs: batch,
			},
		},
	}
	_, err := b.raft.SubmitCommand(context.Background(), req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}
