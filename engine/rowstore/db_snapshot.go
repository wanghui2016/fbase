// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"runtime"
	"sync"

	"github.com/google/btree"
	"engine/errors"
	"engine/model"
	"engine/rowstore/iterator"
	ts "model/pkg/timestamp"
	"util"
)

type snapshotElement struct {
	timestamp ts.Timestamp
	ref       int
}

var snapshotElementKey snapshotElement

func (s *snapshotElement) Less(than btree.Item) bool {
	return s.timestamp.Less(than.(*snapshotElement).timestamp)
}

func (db *DB) acquireSnapshot(timestamp ts.Timestamp) *snapshotElement {
	db.snapMu.Lock()
	defer db.snapMu.Unlock()

	snapshotElementKey.timestamp = timestamp
	if e := db.snapList.Get(&snapshotElementKey); e != nil {
		se := e.(*snapshotElement)
		se.ref++
		return se
	} else {
		se := &snapshotElement{timestamp: timestamp, ref: 1}
		db.snapList.ReplaceOrInsert(se)
		return se
	}
}

func (db *DB) releaseSnapshot(se *snapshotElement) {
	db.snapMu.Lock()
	defer db.snapMu.Unlock()

	se.ref--

	if se.ref == 0 {
		db.snapList.Delete(se)
	} else if se.ref < 0 {
		panic("fbase/engine: Snapshot: negative element reference")
	}
}

func (db *DB) minSnapshotTS() ts.Timestamp {
	db.snapMu.Lock()
	defer db.snapMu.Unlock()

	if db.snapList.Len() > 0 {
		if e := db.snapList.Min(); e != nil {
			return e.(*snapshotElement).timestamp
		}
	}

	timestmap, _ := db.vs.get()
	return timestmap
}

// Snapshot the snapshot
type Snapshot struct {
	db         *DB
	elem       *snapshotElement
	mu         sync.RWMutex // 保护释放和读取的并发冲突
	applyIndex uint64
	released   bool
}

func (db *DB) GetSnapshot() (model.Snapshot, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	tm, ri := db.vs.get()

	snap := &Snapshot{
		db:         db,
		elem:       db.acquireSnapshot(tm),
		applyIndex: ri,
	}

	db.stats.addAliveSnapshot()
	runtime.SetFinalizer(snap, (*Snapshot).Release)

	return snap, nil
}

func (s *Snapshot) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.released {
		runtime.SetFinalizer(s, nil)

		s.released = true
		s.db.stats.subAliveSnapshot()
		s.db.releaseSnapshot(s.elem)
		s.db = nil
		s.elem = nil
	}
}

func (s *Snapshot) ApplyIndex() uint64 {
	return s.applyIndex
}

func (s *Snapshot) NewIterator(startKey, endKey []byte) model.Iterator {
	if err := s.db.ok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.released {
		return iterator.NewEmptyIterator(errors.ErrSnapshotReleased)
	}

	return s.db.newIterator(nil, nil, s.elem.timestamp, &util.Range{Start: startKey, Limit: endKey})
}

// NewVersionedIterator version iterator
func (s *Snapshot) NewMVCCIterator(startKey, endKey []byte) model.MVCCIterator {
	islice := &util.Range{}
	if startKey != nil {
		islice.Start = makeInternalKey(nil, startKey, keyMaxTimestamp, keyTypeSeek)
	}
	if endKey != nil {
		islice.Limit = makeInternalKey(nil, endKey, keyMaxTimestamp, keyTypeSeek)
	}

	mems := s.db.getMems()
	v := s.db.s.version()
	tableIts := v.getIterators(islice)

	n := len(tableIts) + len(mems)
	its := make([]iterator.Iterator, 0, n)

	// add memtable iterators
	for _, m := range mems {
		mi := m.NewIterator(islice)
		mi.SetReleaser(&memdbReleaser{m: m})
		its = append(its, mi)
	}

	// add table iterators
	its = append(its, tableIts...)

	mi := iterator.NewMergedIterator(its, s.db.s.icmp, true)
	mi.SetReleaser(&versionReleaser{v: v})

	return &versionIterator{
		snapTimestamp: s.elem.timestamp,
		mi:            mi,
	}
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if err := s.db.ok(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.released {
		return nil, errors.ErrSnapshotReleased
	}

	return s.db.get(key, s.elem.timestamp)
}

type versionIterator struct {
	mi iterator.Iterator

	snapTimestamp ts.Timestamp

	curUKey      []byte
	curValue     []byte
	curTimestamp ts.Timestamp
	curKType     keyType
	curExpireAt  int64
	err          error
}

func (vi *versionIterator) Next() bool {
	for {
		if !vi.mi.Next() {
			return false
		}

		ukey, tm, kt, kerr := parseInternalKey(vi.mi.Key())
		if kerr != nil {
			vi.err = kerr
			return false
		}

		if tm.LessOrEqual(vi.snapTimestamp) {
			vi.curUKey = append(vi.curUKey[:0], ukey...)
			vi.curTimestamp = tm
			vi.curKType = kt

			if kt == keyTypeVal {
				uvalue, expireAt, err := parseInternalValue(vi.mi.Value())
				if err != nil {
					vi.err = err
					return false
				}
				if isExpired(expireAt) {
					// expired as delte
					vi.curKType = keyTypeDel
					vi.curValue = nil
				} else {
					vi.curValue = uvalue
					vi.curExpireAt = expireAt
				}
			}

			return true
		}
	}
}

func (vi *versionIterator) Key() []byte {
	return vi.curUKey
}

func (vi *versionIterator) Timestamp() ts.Timestamp {
	return vi.curTimestamp
}

func (vi *versionIterator) ExpireAt() int64 {
	return vi.curExpireAt
}

func (vi *versionIterator) IsDelete() bool {
	return vi.curKType == keyTypeDel
}

func (vi *versionIterator) Value() []byte {
	if vi.curKType == keyTypeDel {
		return nil
	}
	return vi.curValue
}

func (vi *versionIterator) Error() error {
	if vi.err != nil {
		return vi.err
	}
	return vi.mi.Error()
}

func (vi *versionIterator) Release() {
	vi.mi.Release()
}
