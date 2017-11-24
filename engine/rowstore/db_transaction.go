// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"sync"

	"engine/errors"
	"engine/model"
	"engine/rowstore/filesystem"
	ts "model/pkg/timestamp"
	"util"
	"util/bufalloc"
)

var errTransactionClosed = errors.New("fbase/engine: transaction already closed")

type transaction struct {
	db        *DB
	mu        sync.RWMutex
	mem       *memDB
	tables    []*tFile
	ikScratch []byte
	ivBuffer  bufalloc.Buffer

	timestamp ts.Timestamp
	raftIndex uint64

	closed bool
}

// OpenTransaction open transaction
func (db *DB) OpenTransaction() (model.Transaction, error) {
	tr, err := db.openTransaction()
	return tr, err
}

func (db *DB) openTransaction() (*transaction, error) {
	// check db ok
	if err := db.ok(); err != nil {
		return nil, err
	}

	// 获取写锁
	select {
	case db.writeLockC <- struct{}{}:
	case <-db.closeC:
		return nil, errors.ErrClosed
	case <-db.splitC:
		return nil, errors.ErrSplit
	}

	if db.tr != nil {
		return nil, errors.New("fbase/engine: transaction has already been opened")
	}

	// 持久化之前的memtable
	if err := db.flushAllMemdbUnlocked(); err != nil {
		return nil, err
	}

	tr := &transaction{
		db:  db,
		mem: db.getMemdb(0),
	}
	tr.mem.incref()
	db.tr = tr

	return tr, nil
}

func (tr *transaction) Get(key []byte, timestamp ts.Timestamp) (value []byte, err error) {
	// TODO:
	return nil, nil
}

func (tr *transaction) NewIterator(startKey, endKey []byte, timestamp ts.Timestamp) model.Iterator {
	// TODO:
	return nil
}

func (tr *transaction) Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64) (err error) {
	tr.mu.Lock()
	if tr.closed {
		err = errTransactionClosed
	} else {
		err = tr.put(keyTypeDel, key, nil, 0, timestamp, raftIndex)
	}
	tr.mu.Unlock()
	return
}

func (tr *transaction) Put(key, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) (err error) {
	tr.mu.Lock()
	if tr.closed {
		err = errTransactionClosed
	} else {
		err = tr.put(keyTypeVal, key, value, expireAt, timestamp, raftIndex)
	}
	tr.mu.Unlock()
	return
}

func (tr *transaction) Write(b *Batch) error {
	tr.timestamp = b.timestamp
	tr.raftIndex = b.raftIndex
	return b.putTrans(tr)
}

func (tr *transaction) rename() error {
	for i := 0; i < len(tr.tables); i++ {
		t := tr.tables[i]
		newfd := filesystem.FileDesc{Type: filesystem.TypeTable, Num: t.fd.Num}
		if err := tr.db.s.fs.Rename(t.fd, newfd); err != nil {
			tr.db.logger.Error("transaction rename failed(%v), %s->%s", err, t.fd.String(), newfd.String())
			return err
		}
		tr.tables[i].fd = newfd
	}
	return nil
}

func (tr *transaction) Commit() (err error) {
	if err := tr.db.ok(); err != nil {
		return err
	}

	tr.mu.Lock()
	defer tr.mu.Unlock()

	if tr.closed {
		return errTransactionClosed
	}
	if err := tr.flush(); err != nil {
		return err
	}
	if tr.db.logger.IsEnableDebug() {
		tr.db.logger.Info("transaction commit. T:%d, A:%d, tables: %s", tr.timestamp, tr.raftIndex, tFiles(tr.tables).DebugString())
	} else {
		tr.db.logger.Info("transaction commit. N@%d, T@%d, A@%d", len(tr.tables), tr.timestamp, tr.raftIndex)
	}

	if len(tr.tables) > 0 {
		if err := checkTSValid(tr.timestamp); err != nil {
			return err
		}
		if err := tr.rename(); err != nil {
			return err
		}
		var edit versionEdit
		edit.timestamp = tr.timestamp
		edit.raftIndex = tr.raftIndex
		edit.addedTables = tr.tables
		if err := tr.db.s.commit(&edit); err != nil {
			return err
		}
		// commit成功，更新db的时间戳和raftindex
		tr.db.vs.set(tr.timestamp, tr.raftIndex)
	}

	tr.setDoneUnlocked()

	return nil
}

func (tr *transaction) Discard() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.closed {
		return
	}

	// 删除table文件
	for _, t := range tr.tables {
		tr.db.logger.Info("transaction discard table N@%d", t.fd.Num)
		err := tr.db.s.fs.Remove(t.fd)
		if err != nil {
			tr.db.logger.Warn("transaction discard table N@%d failed: %v", t.fd.Num, err)
		} else {
			tr.db.s.reuseFileNum(t.fd.Num)
		}
	}

	tr.setDoneUnlocked()
}

func (tr *transaction) setDoneUnlocked() {
	tr.closed = true
	tr.db.tr = nil
	if tr.mem != nil {
		tr.mem.decref()
	}
	if tr.ivBuffer != nil {
		bufalloc.FreeBuffer(tr.ivBuffer)
		tr.ivBuffer = nil
	}
	<-tr.db.writeLockC
}

func (tr *transaction) flush() error {
	if tr.mem.Len() == 0 {
		return nil
	}

	iter := tr.mem.NewIterator(nil)
	t, n, err := tr.db.s.tops.createFrom(iter, true)
	iter.Release()
	if err != nil {
		return err
	}
	if tr.mem.getref() == 1 {
		tr.mem.Reset()
	} else {
		tr.mem.decref()
		tr.mem = tr.db.getMemdb(0)
		tr.mem.incref()
	}
	tr.tables = append(tr.tables, t)

	tr.db.logger.Info("transaction flushed. Num:%d, Entries:%d, Size:%s, TS:%d, Key:[%s, %s]",
		t.fd.Num, n, util.ShorteNBytes(int(t.size)), tr.timestamp, t.imin, t.imax)
	return nil
}

func (tr *transaction) put(kt keyType, key, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) error {
	if err := checkTSValid(timestamp); err != nil {
		return err
	}
	tr.timestamp = timestamp
	tr.raftIndex = raftIndex
	tr.ikScratch = makeInternalKey(tr.ikScratch, key, timestamp, kt)
	if kt == keyTypeVal {
		if tr.ivBuffer == nil {
			tr.ivBuffer = bufalloc.AllocBuffer(len(value))
		} else {
			tr.ivBuffer.Truncate(0)
		}
		buf := tr.ivBuffer.Alloc(len(value) + internalValueSuffixLen(expireAt))
		copy(buf, value)
		makeInternalValueSuffix(buf[len(value):], expireAt)
		return tr.putInternal(tr.ikScratch, buf)
	}
	return tr.putInternal(tr.ikScratch, nil)
}

func (tr *transaction) putInternal(ikey []byte, value []byte) error {
	if tr.mem.Free() < len(ikey)+len(value) {
		if err := tr.flush(); err != nil {
			return err
		}
	}
	return tr.mem.Put(ikey, value)
}
