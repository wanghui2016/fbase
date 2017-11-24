// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"
	"time"

	"engine/errors"
	ts "model/pkg/timestamp"
)

const (
	maxKeySize   = 1024 * 1024 * 4 // 4M
	maxValueSize = 1024 * 1024 * 4 // 4M
)

var (
	errKeyTooLarge   = fmt.Errorf("key is too large(max:%d)", maxKeySize)
	errValueTooLarge = fmt.Errorf("value is too large(max:%d)", maxValueSize)
)

func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	// Create new memdb and journal.
	mem, err = db.newMem(n)
	if err != nil {
		return
	}

	// Schedule memdb compaction.
	if wait {
		err = db.triggerMemFlushWait()
	} else {
		db.triggerMemFlush()
	}
	return
}

func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	slowdownTrigger := db.s.o.GetWriteBufferSlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteBufferPauseTrigger()
	flush := func() (retry bool) {
		mdb = db.getEffectiveMem()
		if mdb == nil {
			err = errors.ErrClosed
			return false
		}
		defer func() {
			if retry {
				mdb.decref()
				mdb = nil
			}
		}()

		mdbFree = mdb.Free()
		frozenMemLen := db.frozenMemLen()
		switch {
		case frozenMemLen >= slowdownTrigger && !delayed:
			delayed = true
			time.Sleep(time.Millisecond)
		case mdbFree >= n: // 空间足够
			return false
		case frozenMemLen >= pauseTrigger: // 需要停止
			delayed = true
			err = db.triggerMemFlushWait()
			if err != nil {
				return false
			}
		default:
			// Allow memdb to grow if it has no entry.
			if mdb.Len() == 0 {
				mdbFree = n
			} else {
				mdb.decref()
				mdb, err = db.rotateMem(n, false)
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	for flush() {
	}
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logger.Warn("write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

func (db *DB) writeLocked(batch *Batch) error {
	if err := checkTSValid(batch.timestamp); err != nil {
		<-db.writeLockC
		return err
	}

	mdb, mdbFree, err := db.flush(batch.Size())
	if err != nil {
		// 释放写锁
		<-db.writeLockC
		return err
	}
	defer mdb.decref()

	if err := batch.putMem(mdb.DB); err != nil {
		panic(err)
	}

	// update timestamp and raft index
	db.vs.set(batch.timestamp, batch.raftIndex)

	if batch.Size() >= mdbFree {
		db.rotateMem(0, false)
	}

	// 释放写锁
	<-db.writeLockC
	return nil
}

func (db *DB) putRec(kt keyType, key, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) (err error) {
	if len(key) > maxKeySize {
		return errKeyTooLarge
	}
	if len(value) > maxValueSize {
		return errValueTooLarge
	}

	if err := db.ok(); err != nil {
		return err
	}

	select {
	case db.writeLockC <- struct{}{}:
	case <-db.closeC:
		return errors.ErrClosed
	case <-db.splitC:
		return errors.ErrSplit
	}

	batch := getBatch()
	batch.appendRec(kt, key, value, expireAt, timestamp, raftIndex)
	err = db.writeLocked(batch)
	putBatch(batch)
	return
}

func checkTSValid(timestamp ts.Timestamp) error {
	if !timestamp.Valid() {
		return errors.ErrInvalidTimestamp
	}
	return nil
}

func (db *DB) Put(key, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) error {
	if err := checkTSValid(timestamp); err != nil {
		return err
	}
	db.stats.addPutCount()
	return db.putRec(keyTypeVal, key, value, expireAt, timestamp, raftIndex)
}

func (db *DB) Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64) error {
	if err := checkTSValid(timestamp); err != nil {
		return err
	}
	return db.putRec(keyTypeDel, key, nil, 0, timestamp, raftIndex)
}

func (db *DB) Write(batch *Batch) error {
	if err := db.ok(); err != nil {
		return err
	}
	if batch == nil || batch.Count() == 0 {
		return nil
	}

	// 较大的batch，先写入table文件，再把tables一起提交
	// 不然直接写入db的memtable，有可能外部会看见batch的一半（中间状态）
	if batch.Size() > db.s.o.GetWriteBuffer() {
		tr, err := db.openTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch); err != nil {
			return err
		}
		if err := tr.Commit(); err != nil {
			tr.Discard()
			return err
		}
	}

	select {
	case db.writeLockC <- struct{}{}:
	case <-db.closeC:
		return errors.ErrClosed
	case <-db.splitC:
		return errors.ErrSplit
	}

	return db.writeLocked(batch)
}

// Destroy 删除DB(目录)
func (db *DB) Destroy() error {
	if db.s == nil || db.s.fs == nil {
		return errors.ErrClosed
	}
	return db.s.fs.Destroy()
}
