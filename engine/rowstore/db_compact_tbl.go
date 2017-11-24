// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"errors"
	"sync/atomic"
	"time"

	"engine/model"
	"engine/rowstore/iterator"
	ts "model/pkg/timestamp"
)

// GetCompactionScore 获取compaction优先级分数
func (db *DB) GetCompactionScore() (float64, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}

	v := db.s.version()
	defer v.release()

	scratch := v.getCompactScratch()

	// 准备分裂的优先compact共享文件
	if scratch.shared > 0 {
		backoff := atomic.LoadUint64(&db.splitCompactBackoff)
		if backoff > maxSplitCompactBackoff {
			backoff = maxSplitCompactBackoff
		}
		return scratch.score * float64(backoff+1), nil
	}

	return scratch.score, nil
}

// NewCompactionTask 创建compaction任务
func (db *DB) NewCompactionTask() (model.CompactionTask, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	v := db.s.version()
	scratch := v.getCompactScratch()
	if !scratch.needCompact() {
		v.release()
		return nil, nil
	}

	db.logger.Info("new compaction task. score: %v, shared: %v, table files: %d",
		scratch.score, scratch.shared, len(scratch.selected))

	task := newTableCompactTask(db, v)

	return task, nil
}

type tableCompactTask struct {
	db        *DB
	s         *session
	hasShared bool
	input     []*tFile
	tableSize int
	minTS     ts.Timestamp
	isCleared bool

	exclude     []*tFile
	iter        iterator.Iterator
	output      []*tFile
	tw          *tWriter
	hasLastUKey bool
	lastUKey    []byte
	lastTS      ts.Timestamp

	start      time.Time
	kvCount    int
	discardCnt int
}

func newTableCompactTask(db *DB, v *version) *tableCompactTask {
	scrach := v.getCompactScratch()

	t := &tableCompactTask{
		db:        db,
		s:         db.s,
		hasShared: scrach.shared > 0,
		input:     scrach.selected,
		tableSize: db.s.o.GetTableFileSize(),
		minTS:     db.minSnapshotTS(),
	}

	t.setupInput(v)

	return t
}

func (t *tableCompactTask) GetDBId() uint64 {
	return t.db.id
}

func (t *tableCompactTask) setupInput(v *version) {
	// 计算没被选择的文件
	selected := make(map[*tFile]struct{})
	for _, f := range t.input {
		if _, ok := selected[f]; ok {
			panic("duplicated table file in compaction input tfiles")
		} else {
			selected[f] = struct{}{}
		}
	}
	for _, f := range v.tfiles {
		if _, ok := selected[f]; !ok {
			t.exclude = append(t.exclude, f)
		}
	}
	if len(t.input)+len(t.exclude) != len(v.tfiles) {
		panic("DB::doCompaction(): includes + excludes != v.tfiles")
	}

	if t.db.logger.IsEnableDebug() {
		t.db.logger.Debug("compact base version: %v", tFiles(v.tfiles).DebugString())
		t.db.logger.Debug("compact input: %v", tFiles(t.input).DebugString())
		t.db.logger.Debug("compact exclude: %v", tFiles(t.exclude).DebugString())
	}

	// read iterator
	var its []iterator.Iterator
	for _, f := range t.input {
		its = append(its, t.s.tops.newIterator(f, nil))
	}
	t.iter = iterator.NewMergedIterator(its, t.s.icmp, true)
	t.iter.SetReleaser(&versionReleaser{v: v})

	t.start = time.Now()
	return
}

// Next next step
func (t *tableCompactTask) Next(stat *model.CompactionStat) (finish bool, err error) {
	// WARN: 请确保步骤中的每一步返回的错误都是写入返回列表中err变量
	defer func() {
		if err != nil {
			t.Clear(true)
		} else if finish {
			t.Clear(false)
		}
	}()

	select {
	case <-t.db.closeC:
		err = errors.New("db is closing")
		return
	default:
	}

	// 结束了
	if !t.iter.Next() {
		if err = t.iter.Error(); err != nil {
			return
		}

		if err = t.finish(); err != nil {
			return
		}

		finish = true
		return
	}

	stat.BytesRead = int64(len(t.iter.Key()) + len(t.iter.Value()))

	ikey := t.iter.Key()
	var (
		ukey      []byte
		timestmap ts.Timestamp
		kt        keyType
		isExpired bool
	)
	ukey, timestmap, kt, err = parseInternalKey(ikey)
	if err != nil {
		return
	}
	if kt == keyTypeVal {
		isExpired, err = isInternalValueExpired(t.iter.Value())
		if err != nil {
			return
		}
	}

	if !t.hasLastUKey || t.s.icmp.uCompare(t.lastUKey, ukey) != 0 {
		// 遇到不一样的ukey时才可以flush，以保证输出的文件不重叠
		if t.tw != nil && t.needFlush() {
			if err = t.flush(); err != nil {
				return
			}
		}

		t.hasLastUKey = true
		t.lastUKey = append(t.lastUKey[:0], ukey...)
		t.lastTS = keyMaxTimestamp
	}
	switch {
	// 不在[db.startKey, db.endKey]里
	case t.hasShared && !t.ukeyInRange(ukey):
		t.lastTS = timestmap
		t.discardCnt++
		return
	// 前面（ukey一样)已经出现了小于等于minTimestamp的ikey，则以后的都可以删除，不论是del还是value
	case t.lastTS.LessOrEqual(t.minTS):
		t.lastTS = timestmap
		t.discardCnt++
		return
	// keytype是delete，且exclude里的表文件中没有这个key，则可以compact
	case kt == keyTypeDel && timestmap.LessOrEqual(t.minTS) && t.ukeyNotOverlapExclude(ukey):
		t.lastTS = timestmap
		t.discardCnt++
		return
	// key已过期，且exclude里的表文件中没有这个key，则可以compact
	case kt == keyTypeVal && isExpired && t.ukeyNotOverlapExclude(ukey):
		t.lastTS = timestmap
		t.discardCnt++
		return
	default:
		t.lastTS = timestmap
	}

	if err = t.appendKV(ikey, t.iter.Value()); err != nil {
		return
	}

	t.kvCount++
	stat.BytesWritten = int64(len(ikey) + len(t.iter.Value()))

	return
}

func (t *tableCompactTask) finish() error {
	// flush last
	if t.tw != nil && !t.tw.empty() {
		if err := t.flush(); err != nil {
			return err
		}
	}

	t.db.logger.Info("compaction finish. take:%v, write:%d, discard:%d", time.Since(t.start), t.kvCount, t.discardCnt)
	t.db.stats.addCompaction()

	if len(t.output) == 0 {
		if t.kvCount > 0 {
			panic("DB::doCompaction() compact output is empty")
		} else {
			t.db.logger.Warn("compaction output is empty, discard: %v", t.discardCnt)
		}
	}

	edit := &versionEdit{
		addedTables: t.output,
		deleted:     t.input,
	}
	return t.db.s.commit(edit)
}

// 非线程安全的，只能在同一个goroutine中调用(Next方法也必须在一个goroutine里)
func (t *tableCompactTask) Clear(discard bool) {
	if !t.isCleared {
		t.iter.Release()

		if discard {
			for _, f := range t.output {
				t.db.logger.Info("compact discard table N@%d", f.fd.Num)
				err := t.db.s.fs.Remove(f.fd)
				if err != nil {
					t.db.logger.Warn("compact discard table N@%d failed: %v", f.fd.Num, err)
				} else {
					t.db.s.reuseFileNum(f.fd.Num)
				}
			}
		}

		t.isCleared = true
	}
}

func (t *tableCompactTask) appendKV(key, value []byte) error {
	// 新建tableWriter
	if t.tw == nil {
		// 检查db是否关闭
		select {
		case <-t.db.closeC:
			return errors.New("db is closed")
		default:
		}

		var err error
		t.tw, err = t.s.tops.create(false)
		if err != nil {
			return err
		}
	}
	return t.tw.append(key, value)
}

func (t *tableCompactTask) needFlush() bool {
	return t.tw.tw.BytesLen() >= t.tableSize
}

func (t *tableCompactTask) flush() error {
	f, err := t.tw.finish()
	if err != nil {
		return err
	}
	t.output = append(t.output, f)
	t.tw = nil
	return nil
}

func (t *tableCompactTask) ukeyInRange(ukey []byte) bool {
	geThanStart := t.db.startUKey == nil || t.s.icmp.uCompare(ukey, t.db.startUKey) >= 0
	lessThanEnd := t.db.endUKey == nil || t.s.icmp.uCompare(ukey, t.db.endUKey) < 0
	return geThanStart && lessThanEnd
}

func (t *tableCompactTask) ukeyNotOverlapExclude(ukey []byte) bool {
	// TODO: 区间树
	for _, f := range t.exclude {
		if f.overlaps(t.s.icmp, ukey, ukey) {
			return false
		}
	}
	return true
}
