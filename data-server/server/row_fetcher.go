package server

import (
	"time"

	sErr "engine/errors"
	"engine/model"
	"model/pkg/kvrpcpb"
	"model/pkg/timestamp"
	"util/log"
)

type rowFetcher struct {
	rs *RowStore
	rd *RowDecoder
	ts timestamp.Timestamp

	singleKey []byte
	count     int
	iter      model.Iterator

	matched bool
	key     []byte
	fields  map[uint64]FieldValue
	err     error
}

func newSelectRowFetcher(rs *RowStore, req *kvrpcpb.KvSelectRequest) *rowFetcher {
	fetcher := &rowFetcher{
		rs: rs,
		rd: newRowDecoder(req.FieldList, req.WhereFilters, req.GroupBys),
		ts: req.GetTimestamp(),
	}
	fetcher.init(req.Key, req.Scope)
	return fetcher
}

func newDeleteRowFetcher(rs *RowStore, req *kvrpcpb.KvDeleteRequest) *rowFetcher {
	fetcher := &rowFetcher{
		rs: rs,
		rd: newRowDecoder(nil, req.WhereFilters, nil),
		ts: req.GetTimestamp(),
	}
	fetcher.init(req.Key, req.Scope)
	return fetcher
}

func (f *rowFetcher) init(key []byte, scope *kvrpcpb.Scope) {
	// 单行
	if len(key) != 0 {
		f.singleKey = EncodeKey(key)
		return
	}
	// 多行，某个范围
	var startPrefix, limitPrefix []byte
	if scope != nil {
		startPrefix = scope.GetStart()
		limitPrefix = scope.GetLimit()
	}
	startKey, endKey := f.rs.adjustRange(startPrefix, limitPrefix)
	f.iter = f.rs.store.NewIterator(startKey, endKey, f.ts)
}

func (f *rowFetcher) nextSingle() bool {
	if f.count > 0 {
		return false
	}
	// single get
	start := time.Now()
	value, err := f.rs.store.Get(f.singleKey, f.ts)
	f.rs.addMetric("store_get", start)
	if err != nil {
		if err != sErr.ErrNotFound {
			log.Error("[range %d]get from store failed(%v), key: %v", f.rs.id, err.Error(), f.singleKey)
			f.err = err
		}
		return false
	}
	// Decode
	f.fields, f.matched, f.err = f.rd.DecodeAndFilter(value)
	if f.err != nil {
		return false
	}
	if !f.matched {
		log.Debug("[range %d]skip row. key: %v", f.rs.id, f.singleKey)
		return false
	}
	f.key = f.singleKey
	f.count++
	return true
}

func (f *rowFetcher) Close() {
	if f.iter != nil {
		f.iter.Release()
	}
}

func (f *rowFetcher) Next() bool {
	if f.err != nil {
		return false
	}

	if len(f.singleKey) > 0 {
		return f.nextSingle()
	}

	for f.iter.Next() {
		f.key = f.iter.Key()
		f.fields, f.matched, f.err = f.rd.DecodeAndFilter(f.iter.Value())
		if f.err != nil {
			return false
		}
		if !f.matched {
			if log.GetFileLogger().IsEnableDebug() {
				log.Debug("[range %d]skip row. key: %v", f.rs.id, f.key)
			}
			continue
		}
		f.count++
		return true
	}
	f.err = f.iter.Error()
	return false
}

func (f *rowFetcher) Key() []byte {
	return f.key
}

func (f *rowFetcher) Fields() map[uint64]FieldValue {
	return f.fields
}

func (f *rowFetcher) Error() error {
	return f.err
}
