// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/google/btree"
	"engine/errors"
	"engine/model"
	"engine/rowstore/filesystem"
	"engine/rowstore/iterator"
	"engine/rowstore/memdb"
	"engine/rowstore/opt"
	"engine/scheduler"
	ts "model/pkg/timestamp"
	"util"
)

// DB the db
type DB struct {
	id                 uint64
	startUKey, endUKey []byte // 存储的key的范围

	logger *dbLogger

	s *session

	vs volatileState

	// memdb
	memMu      sync.RWMutex
	mem        *memDB
	frozenMems []*memDB // 冻结的memtable队列，头在前面

	// snapshot
	snapMu   sync.Mutex
	snapList *btree.BTree

	// Stats
	stats dbStats

	// write
	writeLockC  chan struct{}
	writeDelay  time.Duration
	writeDelayN int
	tr          *transaction

	// split
	splitC              chan struct{}
	splitOnce           sync.Once
	splitCompactBackoff uint64 // compact分裂补偿（优先compact将要分裂的db）

	// memtable flush
	mflushCmdC    chan struct{} // 用于通知flush memdb
	mflushResultC chan error

	// close
	closeW sync.WaitGroup // 等待compaction完成
	closeC chan struct{}
	closed uint32
	closer io.Closer // 关闭filesystem
}

func openDB(s *session, startKey, endKey []byte, dbID uint64) (db *DB, err error) {
	start := time.Now()
	db = &DB{
		id:            dbID,
		logger:        s.logger,
		startUKey:     startKey,
		endUKey:       endKey,
		s:             s,
		snapList:      btree.New(4),
		frozenMems:    make([]*memDB, 0, s.o.GetWriteBufferPauseTrigger()),
		writeLockC:    make(chan struct{}, 1),
		mflushCmdC:    make(chan struct{}, s.o.GetWriteBufferPauseTrigger()),
		mflushResultC: make(chan error),
		splitC:        make(chan struct{}),
		closeC:        make(chan struct{}),
	}

	if db.logger.IsEnableDebug() {
		db.logger.Debug("db opening")
	}

	// new memdb
	db.vs.set(s.timestamp, s.applied)
	mdb := memdb.GetDB(db.s.icmp, s.o.GetWriteBuffer())
	db.mem = &memDB{db: db, DB: mdb, ref: 1}

	// background goroutine
	db.closeW.Add(1)
	go db.mFlushBackgroud()

	db.logger.Info("open done. take %v", time.Since(start))
	runtime.SetFinalizer(db, (*DB).Close)

	return
}

func openSession(fs filesystem.FileSystem, o *opt.Options, dbID uint64) (s *session, applied uint64, err error) {
	s, err = newSession(fs, o, dbID)
	if err != nil {
		return
	}
	applied, err = s.recover()
	if err != nil {
		if !os.IsNotExist(err) || s.o.GetErrorIfMissing() {
			return
		}
		err = s.create()
		if err != nil {
			return
		}
	} else if s.o.GetErrorIfExist() {
		err = os.ErrExist
		return
	}
	return
}

func OpenFile(path string, o *opt.Options) (db *DB, applied uint64, err error) {
	return OpenStore(0, path, nil, nil, o)
}

func OpenStore(dbID uint64, path string, startKey, endKey []byte, o *opt.Options) (db *DB, applied uint64, err error) {
	// open filesystem
	fs, err := filesystem.OpenFile(path, o.GetReadOnly())
	if err != nil {
		err = fmt.Errorf("open filesystem error: %v", err)
		return
	}

	// open session
	var s *session
	s, applied, err = openSession(fs, o, dbID)
	if err != nil {
		err = fmt.Errorf("open session error: %v", err)
		fs.Close()
		return
	}

	// open db
	db, err = openDB(s, startKey, endKey, dbID)
	if err != nil {
		err = fmt.Errorf("open db error: %v", err)
		fs.Close()
		return
	}
	db.closer = fs

	scheduler.RegisterDB(dbID, db)

	return
}

func memGet(mdb memdb.DB, ikey internalKey, icmp *iComparer) (ok bool, mv []byte, err error) {
	mk, mv, err := mdb.Find(ikey)
	if err == nil {
		ukey, _, kt, kerr := parseInternalKey(mk)
		if kerr != nil {
			panic(kerr)
		}
		// find
		if icmp.uCompare(ukey, ikey.ukey()) == 0 {
			if kt == keyTypeDel {
				return true, nil, errors.ErrNotFound
			}
			return true, mv, nil
		}
	} else if err != errors.ErrNotFound {
		return true, nil, err
	}
	return
}

func (db *DB) get(key []byte, timestamp ts.Timestamp) (value []byte, err error) {
	ikey := makeInternalKey(nil, key, timestamp, keyTypeSeek)

	var ok bool
	var mv []byte
	var ivalue []byte
	var expiredAt int64
	mdbs := db.getMems()
	for _, m := range mdbs {
		if m == nil {
			continue
		}
		if !ok {
			if ok, mv, err = memGet(m.DB, ikey, db.s.icmp); ok {
				ivalue = append([]byte{}, mv...)
			}
		}
		m.decref()
	}
	// 在memtable找到了，返回
	if ok {
		if err == nil {
			value, expiredAt, err = parseInternalValue(ivalue)
			if err == nil && isExpired(expiredAt) {
				err = errors.ErrNotFound
			}
		}
		return
	}

	// from table file
	v := db.s.version()
	ivalue, err = v.get(ikey, false)
	if err == nil {
		value, expiredAt, err = parseInternalValue(ivalue)
		if err == nil && isExpired(expiredAt) {
			err = errors.ErrNotFound
		}
	}
	v.release()
	return
}

// Get get key's value
func (db *DB) Get(key []byte, timestamp ts.Timestamp) (value []byte, err error) {
	err = db.ok()
	if err != nil {
		return
	}

	db.stats.addGetCount()

	se := db.acquireSnapshot(timestamp)
	defer db.releaseSnapshot(se)
	return db.get(key, timestamp)
}

// Close close db
func (db *DB) Close() error {
	if !db.setClosed() {
		return errors.ErrClosed
	}

	start := time.Now()
	db.logger.Info("db closing")

	// unregister
	scheduler.UnRegisterDB(db.id)

	// Acquire writer lock.
	db.writeLockC <- struct{}{}

	// flush memtable
	if err := db.flushAllMemdbUnlocked(); err != nil {
		db.logger.Error("flush memtable failed: %v", err)
	}

	runtime.SetFinalizer(db, nil)
	close(db.closeC)

	// Wait for all gorotines to exit.
	db.closeW.Wait()

	// Close session.
	db.s.close()
	db.logger.Info("close done. take %v", time.Since(start))
	db.s.release()

	var err error
	if db.closer != nil {
		if err1 := db.closer.Close(); err == nil {
			err = err1
		}
		db.closer = nil
	}

	// Clear memdbs.
	db.clearMems()

	return err
}

// NewIterator new iterator
func (db *DB) NewIterator(beginKey, endKey []byte, timestamp ts.Timestamp) model.Iterator {
	if err := db.ok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}

	se := db.acquireSnapshot(timestamp)
	defer db.releaseSnapshot(se)

	return db.newIterator(nil, nil, se.timestamp, &util.Range{Start: beginKey, Limit: endKey})
}

// DiskUsage 返回fdb文件大小总和
func (db *DB) DiskUsage() uint64 {
	var size uint64
	ver := db.s.version()
	defer ver.release()
	for _, f := range ver.tfiles {
		size += uint64(f.size)
	}
	return size
}

// Size 返回db的大小，包含memtable和fdb文件，单位字节
func (db *DB) Size() uint64 {
	var size uint64

	mems := db.getMems()
	ver := db.s.version()
	defer ver.release()
	for _, m := range mems {
		size += uint64(m.Size())
		m.decref()
	}
	for _, f := range ver.tfiles {
		size += f.kvsize
	}
	return size
}

func (db *DB) Status() (s string) {
	ver := db.s.version()
	s = ver.String()
	ver.release()
	return
}
