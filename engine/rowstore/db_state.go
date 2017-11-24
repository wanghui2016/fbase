// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"sync"
	"sync/atomic"

	"engine/errors"
	"engine/rowstore/memdb"
	ts "model/pkg/timestamp"
	"util"
)

type volatileState struct {
	timestamp ts.Timestamp
	raftIndex uint64
	sync.RWMutex
}

func (vs *volatileState) get() (timestamp ts.Timestamp, raftIndex uint64) {
	vs.RLock()
	timestamp = vs.timestamp
	raftIndex = vs.raftIndex
	vs.RUnlock()
	return
}

func (vs *volatileState) set(timestamp ts.Timestamp, raftIndex uint64) {
	vs.Lock()
	vs.timestamp = timestamp
	vs.raftIndex = raftIndex
	vs.Unlock()
}

// Timestamp return db's current timestamp
func (db *DB) Timestamp() ts.Timestamp {
	tm, _ := db.vs.get()
	return tm
}

// Applied return current applied(已持久化的) raft index
func (db *DB) Applied() uint64 {
	return db.s.Applied()
}

type memDB struct {
	db *DB
	memdb.DB
	ref             int32
	frozenTimestamp ts.Timestamp
	fronzeRaftIndex uint64
}

func (m *memDB) getref() int32 {
	return atomic.LoadInt32(&m.ref)
}

func (m *memDB) incref() {
	atomic.AddInt32(&m.ref, 1)
}

func (m *memDB) decref() {
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 {
		// Only put back memdb with std capacity.
		if m.Capacity() == m.db.s.o.GetWriteBuffer() {
			memdb.PutDB(m.DB)
		}
		m.db = nil
		m.DB = nil
	} else if ref < 0 {
		panic("negative memdb ref")
	}
}

func (db *DB) getMemdb(capcity int) *memDB {
	return &memDB{
		DB: memdb.GetDB(db.s.icmp, util.MaxInt(db.s.o.GetWriteBuffer(), capcity)),
		db: db,
	}
}

// Get all memdbs.
func (db *DB) getMems() (mems []*memDB) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
		mems = append(mems, db.mem)
	} else if !db.isClosed() {
		panic("nil effective mem")
	}

	// 最新的在前面
	for i := len(db.frozenMems) - 1; i >= 0; i-- {
		f := db.frozenMems[i]
		f.incref()
		mems = append(mems, f)
	}
	return
}

// Get effective memdb.
func (db *DB) getEffectiveMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}

func (db *DB) frozenMemLen() (size int) {
	db.memMu.RLock()
	size = len(db.frozenMems)
	db.memMu.RUnlock()
	return
}

// Check whether we has frozen memdb.
func (db *DB) hasFrozenMem() (has bool) {
	db.memMu.RLock()
	has = len(db.frozenMems) != 0
	db.memMu.RUnlock()
	return
}

// Get frozen memdb.
func (db *DB) getFrozenMemFront() (mem *memDB) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if len(db.frozenMems) > 0 {
		mem = db.frozenMems[0]
		mem.incref()
	}
	return
}

// Remove the oldest frozen mem
func (db *DB) popFrozenMem() {
	db.memMu.Lock()
	size := len(db.frozenMems)
	if size > 0 {
		mem := db.frozenMems[0]
		mem.decref()
		copy(db.frozenMems, db.frozenMems[1:])
		db.frozenMems[size-1] = nil
		db.frozenMems = db.frozenMems[:size-1]
	}
	db.memMu.Unlock()
}

// Used by DB.Close()
func (db *DB) clearMems() {
	db.memMu.Lock()
	if db.mem != nil {
		db.mem.decref()
		db.mem = nil
	}
	if db.frozenMems != nil {
		for _, mem := range db.frozenMems {
			if mem != nil {
				mem.decref()
			}
		}
		db.frozenMems = nil
	}
	db.memMu.Unlock()
}

// Create new memdb and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
func (db *DB) newMem(n int) (mem *memDB, err error) {
	db.memMu.Lock()
	defer db.memMu.Unlock()

	// +1 for flushAllMemtables
	if len(db.frozenMems) >= db.s.o.GetWriteBufferPauseTrigger()+1 {
		panic("too much frozen memtables")
	}

	db.mem.frozenTimestamp, db.mem.fronzeRaftIndex = db.vs.get()
	db.frozenMems = append(db.frozenMems, db.mem)
	mem = db.getMemdb(n)
	mem.incref() // for self
	mem.incref() // for caller
	db.mem = mem
	return
}

// return true if not already closed
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.closed, 0, 1)
}

func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

func (db *DB) ok() error {
	if db.isClosed() {
		return errors.ErrClosed
	}
	return nil
}
