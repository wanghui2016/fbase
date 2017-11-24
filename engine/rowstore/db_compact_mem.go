// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"time"

	"engine/errors"
	"engine/rowstore/filesystem"
	"util"
)

var (
	errCompactionTransactExiting = errors.New("fbase/engine: compaction transact exiting")
)

func (db *DB) mFlushBackgroud() {
	defer func() {
		db.closeW.Done()
	}()

	for {
		select {
		case <-db.mflushCmdC:
			err := db.flushMemtable()
			select {
			case db.mflushResultC <- err:
			default:
			}
		case <-db.closeC:
			return
		}
	}
}

// 通知compaction不等待结果
func (db *DB) triggerMemFlush() {
	select {
	case db.mflushCmdC <- struct{}{}:
	default:
	}
}

// 触发compaction并等待结果
func (db *DB) triggerMemFlushWait() (err error) {
	// 发送通知
	select {
	case db.mflushCmdC <- struct{}{}:
	case <-db.closeC:
		return errors.ErrClosed
	default:
	}
	// 等待compaction结果
	select {
	case err = <-db.mflushResultC:
	case <-db.closeC:
		return errors.ErrClosed
	}
	return err
}

func (db *DB) flushMemtable() error {
	mdb := db.getFrozenMemFront()
	// 没有冻结的memdb
	if mdb == nil {
		return nil
	}

	defer mdb.decref()

	// 跳过空的memdb
	if mdb.Len() == 0 {
		db.logger.Debug("memtable flushed. skipping zero length memtable")
		db.popFrozenMem()
		db.logger.Debug("memtable flushed. current frozen: %v", db.frozenMemLen())
		return nil
	}

	start := time.Now()

	var nt *tFile
	nt, err := db.s.flushMemdb(mdb.DB)
	if err != nil {
		if nt != nil {
			db.logger.Error("memtable flushed revert @%d, error: %v", nt.fd.Num, err)
			if err := db.s.fs.Remove(nt.fd); err != nil {
				return err
			}
		}
		return err
	}
	flushEnd := time.Now()

	db.stats.addFlush()

	edit := &versionEdit{
		timestamp:   mdb.frozenTimestamp,
		raftIndex:   mdb.fronzeRaftIndex,
		addedTables: []*tFile{nt},
	}

	err = db.s.commit(edit)
	if err != nil {
		db.logger.Error("memtable flush commit error(%v), ", err)
		return err
	}
	commitEnd := time.Now()

	db.logger.Info("memdb@flush ML:%d MS:%s FN:%d FS:%s K:[%s-%s] T:f=%v,c=%v",
		mdb.Len(), util.ShorteNBytes(mdb.Size()), nt.fd.Num, util.ShorteNBytes(int(nt.size)),
		nt.imin.String(), nt.imax.String(), flushEnd.Sub(start), commitEnd.Sub(flushEnd))

	// session commit之后删除memdb
	db.popFrozenMem()

	return nil
}

func (db *DB) compactionExitTransact() {
	panic(errCompactionTransactExiting)
}

// 事务保护：重试和回滚
func (db *DB) compactionTransact(name string, run func() error, revert func() error) {
	defer func() {
		if x := recover(); x != nil {
			// 回滚
			if x == errCompactionTransactExiting {
				if revert != nil {
					if err := revert(); err != nil {
						db.logger.Error("%s revert error %q", name, err)
					}
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)

	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
	)

	for n := 0; ; n++ {
		// 检查DB是否已经关闭
		if db.isClosed() {
			db.logger.Info("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logger.Warn("%s retrying N·%d", name, n)
		}

		// 执行
		err := run()
		if err != nil {
			db.logger.Error("%s error %q", name, err)
		}

		if filesystem.IsCorrupted(err) {
			db.logger.Error("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		backoffT.Reset(backoff)
		if backoff < backoffMax {
			backoff *= backoffMul
			if backoff > backoffMax {
				backoff = backoffMax
			}
		}
		select {
		case <-db.closeC:
			db.logger.Info("%s exiting", name)
			db.compactionExitTransact()
		case <-backoffT.C:
		}
	}
}
