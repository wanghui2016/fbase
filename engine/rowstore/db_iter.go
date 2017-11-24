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

	"engine/errors"
	"engine/rowstore/iterator"
	ts "model/pkg/timestamp"
	"util"
)

var (
	errInvalidInternalKey = errors.New("fbase/engine: Iterator: invalid internal key")
)

type memdbReleaser struct {
	once sync.Once
	m    *memDB
}

func (mr *memdbReleaser) Release() {
	mr.once.Do(func() {
		mr.m.decref()
	})
}

func (db *DB) newRawIterator(auxm *memDB, auxt tFiles, slice *util.Range) iterator.Iterator {
	mems := db.getMems()
	v := db.s.version()
	tableIts := v.getIterators(slice)

	n := len(tableIts) + len(auxt) + len(mems) + 1
	its := make([]iterator.Iterator, 0, n)
	if auxm != nil {
		ami := auxm.NewIterator(slice)
		ami.SetReleaser(&memdbReleaser{m: auxm})
		its = append(its, ami)
	}
	for _, t := range auxt {
		its = append(its, v.s.tops.newIterator(t, slice))
	}
	// mem iterators
	for _, m := range mems {
		mi := m.NewIterator(slice)
		mi.SetReleaser(&memdbReleaser{m: m})
		its = append(its, mi)
	}

	// add table iterators
	its = append(its, tableIts...)

	mi := iterator.NewMergedIterator(its, db.s.icmp, true)
	mi.SetReleaser(&versionReleaser{v: v})
	return mi
}

func (db *DB) newIterator(auxm *memDB, auxt tFiles, timestamp ts.Timestamp, slice *util.Range) *dbIter {
	var islice *util.Range
	if slice != nil {
		islice = &util.Range{}
		if slice.Start != nil {
			islice.Start = makeInternalKey(nil, slice.Start, keyMaxTimestamp, keyTypeSeek)
		}
		if slice.Limit != nil {
			islice.Limit = makeInternalKey(nil, slice.Limit, keyMaxTimestamp, keyTypeSeek)
		}
	}
	rawIter := db.newRawIterator(auxm, auxt, islice)
	iter := &dbIter{
		db:        db,
		icmp:      db.s.icmp,
		iter:      rawIter,
		timestamp: timestamp,
		strict:    true,
		key:       make([]byte, 0),
		value:     make([]byte, 0),
	}
	db.stats.addAliveIterator()
	runtime.SetFinalizer(iter, (*dbIter).Release)
	return iter
}

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

// dbIter represent an interator states over a database session.
type dbIter struct {
	db        *DB
	icmp      *iComparer
	iter      iterator.Iterator
	timestamp ts.Timestamp
	strict    bool

	dir      dir
	key      []byte
	value    []byte
	err      error
	releaser util.Releaser
}

func (i *dbIter) setErr(err error) {
	i.err = err
	i.key = nil
	i.value = nil
}

func (i *dbIter) iterErr() {
	if err := i.iter.Error(); err != nil {
		i.setErr(err)
	}
}

func (i *dbIter) Valid() bool {
	return i.err == nil && i.dir > dirEOI
}

func (i *dbIter) First() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = errors.ErrIterReleased
		return false
	}

	if i.iter.First() {
		i.dir = dirSOI
		return i.next()
	}
	i.dir = dirEOI
	i.iterErr()
	return false
}

func (i *dbIter) Last() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = errors.ErrIterReleased
		return false
	}

	if i.iter.Last() {
		return i.prev()
	}
	i.dir = dirSOI
	i.iterErr()
	return false
}

func (i *dbIter) Seek(key []byte) bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = errors.ErrIterReleased
		return false
	}

	ikey := makeInternalKey(nil, key, i.timestamp, keyTypeSeek)
	if i.iter.Seek(ikey) {
		i.dir = dirSOI
		return i.next()
	}
	i.dir = dirEOI
	i.iterErr()
	return false
}

func (i *dbIter) next() bool {
	for {
		if ukey, timestamp, kt, kerr := parseInternalKey(i.iter.Key()); kerr == nil {
			// if seq <= i.seq {
			if timestamp.LessOrEqual(i.timestamp) {
				switch kt {
				case keyTypeDel:
					// Skip deleted key.
					i.key = append(i.key[:0], ukey...)
					i.dir = dirForward
				case keyTypeVal:
					value, expiredAt, err := parseInternalValue(i.iter.Value())
					if err != nil {
						i.setErr(err)
						return false
					}
					if isExpired(expiredAt) {
						// Skip expired key.
						i.key = append(i.key[:0], ukey...)
						i.dir = dirForward
					} else {
						if i.dir == dirSOI || i.icmp.uCompare(ukey, i.key) > 0 {
							i.key = append(i.key[:0], ukey...)
							i.value = append(i.value[:0], value...)
							i.dir = dirForward
							return true
						}
					}
				}
			}
		} else if i.strict {
			i.setErr(kerr)
			break
		}
		if !i.iter.Next() {
			i.dir = dirEOI
			i.iterErr()
			break
		}
	}
	return false
}

func (i *dbIter) Next() bool {
	if i.dir == dirEOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = errors.ErrIterReleased
		return false
	}

	if !i.iter.Next() || (i.dir == dirBackward && !i.iter.Next()) {
		i.dir = dirEOI
		i.iterErr()
		return false
	}
	return i.next()
}

func (i *dbIter) prev() bool {
	i.dir = dirBackward
	del := true
	if i.iter.Valid() {
		for {
			if ukey, timestamp, kt, kerr := parseInternalKey(i.iter.Key()); kerr == nil {
				if timestamp.LessOrEqual(i.timestamp) {
					if !del && i.icmp.uCompare(ukey, i.key) < 0 {
						return true
					}
					del = (kt == keyTypeDel)
					if !del {
						uvalue, expireAt, err := parseInternalValue(i.iter.Value())
						if err != nil {
							i.setErr(err)
							return false
						}
						if isExpired(expireAt) {
							del = true
						} else {
							i.key = append(i.key[:0], ukey...)
							i.value = append(i.value[:0], uvalue...)
						}
					}
				}
			} else if i.strict {
				i.setErr(kerr)
				return false
			}
			if !i.iter.Prev() {
				break
			}
		}
	}
	if del {
		i.dir = dirSOI
		i.iterErr()
		return false
	}
	return true
}

func (i *dbIter) Prev() bool {
	if i.dir == dirSOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = errors.ErrIterReleased
		return false
	}

	switch i.dir {
	case dirEOI:
		return i.Last()
	case dirForward:
		for i.iter.Prev() {
			if ukey, _, _, kerr := parseInternalKey(i.iter.Key()); kerr == nil {
				if i.icmp.uCompare(ukey, i.key) < 0 {
					goto cont
				}
			} else if i.strict {
				i.setErr(kerr)
				return false
			}
		}
		i.dir = dirSOI
		i.iterErr()
		return false
	}

cont:
	return i.prev()
}

func (i *dbIter) Key() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.key
}

func (i *dbIter) Value() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.value
}

func (i *dbIter) Release() {
	if i.dir != dirReleased {
		// Clear the finalizer.
		runtime.SetFinalizer(i, nil)

		if i.releaser != nil {
			i.releaser.Release()
			i.releaser = nil
		}

		i.dir = dirReleased
		i.key = nil
		i.value = nil
		i.iter.Release()
		i.iter = nil
		i.db.stats.subAliveIterator()
		i.db = nil
	}
}

func (i *dbIter) SetReleaser(releaser util.Releaser) {
	if i.dir == dirReleased {
		panic(util.ErrReleased)
	}
	if i.releaser != nil && releaser != nil {
		panic(util.ErrHasReleaser)
	}
	i.releaser = releaser
}

func (i *dbIter) Error() error {
	return i.err
}
