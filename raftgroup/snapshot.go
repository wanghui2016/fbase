package raftgroup

import (
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
	"engine/model"
	"model/pkg/raft_cmdpb"
	raftproto "raft/proto"
)

var (
	errCorruptData = errors.New("corrupt data")
)

type RaftSnapshot struct {
	snap       model.Snapshot
	applyIndex uint64
	iter       model.MVCCIterator
}

func NewRaftSnapshot(snap model.Snapshot, applyIndex uint64, beginKey, endKey []byte) *RaftSnapshot {
	s := &RaftSnapshot{
		snap:       snap,
		applyIndex: applyIndex,
		iter:       snap.NewMVCCIterator(beginKey, endKey),
	}

	return s
}

func (s *RaftSnapshot) Next() ([]byte, error) {
	err := s.iter.Error()
	if err != nil {
		return nil, err
	}

	hasNext := s.iter.Next()
	if !hasNext {
		return nil, io.EOF
	}
	kvPair := raft_cmdpb.MVCCKVPair{
		Key:        s.iter.Key(),
		IsDelete:   s.iter.IsDelete(),
		Value:      s.iter.Value(),
		Timestamp:  s.iter.Timestamp(),
		ApplyIndex: s.ApplyIndex(),
		ExpireAt:   s.iter.ExpireAt(),
	}
	return kvPair.Marshal()
	//sizeBuf := make([]byte, 4)
	//key := s.iter.Key()
	//value := s.iter.Value()
	//buf := make([]byte, 0, len(key)+len(value)+2*4)
	//
	//// write key
	//
	//binary.BigEndian.PutUint32(sizeBuf, uint32(len(key)))
	//buf = append(buf, sizeBuf...)
	//buf = append(buf, key...)
	//
	//// write value
	//
	//binary.BigEndian.PutUint32(sizeBuf, uint32(len(value)))
	//buf = append(buf, sizeBuf...)
	//buf = append(buf, value...)
	//
	//return buf, nil
}

func (s *RaftSnapshot) ApplyIndex() uint64 {
	return s.applyIndex
}

func (s *RaftSnapshot) Close() {
	s.iter.Release()
	s.snap.Release()
}

type SnapshotKVIterator struct {
	rawIter raftproto.SnapIterator
}

func NewSnapshotKVIterator(rawIter raftproto.SnapIterator) *SnapshotKVIterator {
	return &SnapshotKVIterator{
		rawIter: rawIter,
	}
}

func (i *SnapshotKVIterator) Next() (kvPair *raft_cmdpb.MVCCKVPair, err error) {
	var data []byte
	data, err = i.rawIter.Next()
	if err != nil {
		return
	}
	kvPair = &raft_cmdpb.MVCCKVPair{}
	err = proto.Unmarshal(data, kvPair)
	if err != nil {
		return nil, err
	}
	return kvPair, nil
	//if len(data) < 8 {
	//	return nil, nil, errCorruptData
	//}
	//
	//klen := binary.BigEndian.Uint32(data)
	//if len(data) < 8+int(klen) {
	//	return nil, nil, errCorruptData
	//}
	//key := data[4 : 4+klen]
	//
	//vlen := binary.BigEndian.Uint32(data[4+klen:])
	//if len(data) < 8+int(klen)+int(vlen) {
	//	return nil, nil, errCorruptData
	//}
	//value := data[4+klen+4 : 4+klen+4+vlen]

	// return kvPair.Key, kvPair.Value, kvPair.Timestamp, kvPair.ApplyIndex, nil
}
