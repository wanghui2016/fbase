// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"sync"

	"engine/model"
	"engine/rowstore/memdb"
	ts "model/pkg/timestamp"
	"util/bufalloc"
)

var (
	batchPool = &sync.Pool{
		New: func() interface{} {
			return &Batch{}
		},
	}
)

func getBatch() *Batch {
	batch := batchPool.Get().(*Batch)
	batch.Reset()
	return batch
}

func putBatch(batch *Batch) {
	if batch.buffer != nil {
		bufalloc.FreeBuffer(batch.buffer)
		batch.buffer = nil
	}
	batch.db = nil
	batchPool.Put(batch)
}

// Batch batch
type Batch struct {
	db *DB

	buffer bufalloc.Buffer
	index  []int // 3个整型一组, 0: pos, 1: ikey length, 2: ivalue length

	raftIndex uint64
	timestamp ts.Timestamp
}

func (b *Batch) appendRec(kt keyType, key, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) {
	ikeyLen := KeyInternalSuffixLen + len(key)
	iValueLen := 0

	// make buffer
	n := ikeyLen
	if kt == keyTypeVal {
		iValueLen = len(value) + internalValueSuffixLen(expireAt)
		n += iValueLen
	}
	if b.buffer == nil {
		b.buffer = bufalloc.AllocBuffer(n)
	}
	pos := b.buffer.Len()
	data := b.buffer.Alloc(n)

	// append ikey
	makeInternalKey(data, key, timestamp, kt)
	// append value
	if kt == keyTypeVal {
		copy(data[ikeyLen:], value)
		makeInternalValueSuffix(data[ikeyLen+len(value):], expireAt)
	}

	b.index = append(b.index, pos, ikeyLen, iValueLen)
	b.timestamp = timestamp
	b.raftIndex = raftIndex
}

// Put batch put
func (b *Batch) Put(key, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) {
	b.appendRec(keyTypeVal, key, value, expireAt, timestamp, raftIndex)
}

// Delete batch delete
func (b *Batch) Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64) {
	b.appendRec(keyTypeDel, key, nil, 0, timestamp, raftIndex)
}

// Count entries count
func (b *Batch) Count() int {
	return len(b.index) / 3
}

// Size kv size
func (b *Batch) Size() int {
	return b.buffer.Len()
}

// Reset reset
func (b *Batch) Reset() {
	b.index = b.index[:0]
}

func (b *Batch) kv(i int) ([]byte, []byte) {
	pos := b.index[i*3]
	iklen := b.index[i*3+1]
	vlen := b.index[i*3+2]
	return b.buffer.Bytes()[pos : pos+iklen], b.buffer.Bytes()[pos+iklen : pos+iklen+vlen]
}

func (b *Batch) k(i int) []byte {
	pos := b.index[i*3]
	iklen := b.index[i*3+1]
	return b.buffer.Bytes()[pos : pos+iklen]
}

func (b *Batch) v(i int) []byte {
	pos := b.index[i*3]
	iklen := b.index[i*3+1]
	vlen := b.index[i*3+2]
	return b.buffer.Bytes()[pos+iklen : pos+iklen+vlen]
}

func (b *Batch) putMem(mdb memdb.DB) error {
	var key, value []byte
	for i := 0; i < b.Count(); i++ {
		key, value = b.kv(i)
		if err := mdb.Put(key, value); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) putTrans(tr *transaction) error {
	var key, value []byte
	for i := 0; i < b.Count(); i++ {
		key, value = b.kv(i)
		if err := tr.putInternal(key, value); err != nil {
			return err
		}
	}
	return nil
}

// Commit commit batch
func (b *Batch) Commit() (err error) {
	err = b.db.Write(b)
	putBatch(b)
	return
}

// NewWriteBatch new batch
func (db *DB) NewWriteBatch() model.WriteBatch {
	b := getBatch()
	b.db = db
	return b
}
