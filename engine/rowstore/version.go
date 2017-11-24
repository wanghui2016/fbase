// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"engine/errors"
	"engine/rowstore/filesystem"
	"engine/rowstore/iterator"
	ts "model/pkg/timestamp"
	"util"
)

type version struct {
	s      *session
	tfiles []*tFile // sst table files

	ref      int
	closing  bool
	released bool

	// 缓存该version对应的compact状态数据（分数，选中的文件）
	tcompScratch unsafe.Pointer
}

func newVersion(s *session, tfileMetas []TableFileMeta) *version {
	ver := &version{s: s}
	for _, m := range tfileMetas {
		tfile := &tFile{
			fd:     filesystem.FileDesc{Type: filesystem.TypeTable, Num: m.Num},
			size:   m.FileSize,
			kvsize: m.KVSize,
			imin:   m.StartIKey,
			imax:   m.EndIKey,
			shared: m.Shared,
		}
		ver.tfiles = append(ver.tfiles, tfile)
	}

	return ver
}

func (v *version) get(ikey internalKey, novalue bool) (value []byte, err error) {
	if v.closing {
		return nil, errors.ErrClosed
	}
	ukey := ikey.ukey()

	// TODO: 区间树
	tfiles := v.tfiles

	var (
		zfound     bool
		ztimestamp ts.Timestamp
		zkt        keyType
		zval       []byte
	)

	for i := len(tfiles) - 1; i >= 0; i-- {
		t := tfiles[i]

		if !t.overlaps(v.s.icmp, ukey, ukey) {
			continue
		}

		var (
			fikey, fval []byte
		)

		if novalue {
			fikey, err = v.s.tops.findKey(t, ikey)
		} else {
			fikey, fval, err = v.s.tops.find(t, ikey)
		}

		// 当前文件内没找到
		if err == errors.ErrNotFound {
			continue
		} else if err != nil {
			return
		}

		fukey, ftimestamp, fkt, fkerr := parseInternalKey(fikey)
		if fkerr != nil {
			err = fkerr
			return
		}
		// 选最大的
		if v.s.icmp.uCompare(ukey, fukey) == 0 {
			if ztimestamp.Less(ftimestamp) {
				zfound = true
				ztimestamp = ftimestamp
				zkt = fkt
				zval = fval
			}
		}
	}

	if zfound {
		switch zkt {
		case keyTypeVal:
			return zval, nil
		case keyTypeDel:
			return nil, errors.ErrNotFound
		default:
			panic("fbase/engine: invalid internalkey type")
		}
	}

	return nil, errors.ErrNotFound
}

func (v *version) getIterators(islice *util.Range) (its []iterator.Iterator) {
	var umin []byte
	var umax []byte
	if islice != nil {
		if islice.Start != nil {
			umin = internalKey(islice.Start).ukey()
		}
		if islice.Limit != nil {
			umax = internalKey(islice.Limit).ukey()
		}
	}

	for i := len(v.tfiles) - 1; i >= 0; i-- {
		t := v.tfiles[i]
		if t.overlaps(v.s.icmp, umin, umax) {
			its = append(its, v.s.tops.newIterator(v.tfiles[i], islice))
		}
	}
	return
}

func (v *version) getCompactScratch() *compactScratch {
	p := atomic.LoadPointer(&v.tcompScratch)
	if p != nil {
		return (*compactScratch)(p)
	}
	scratch := newCompactScratch(v.s.tcompPolicy, v.tfiles)
	atomic.StorePointer(&v.tcompScratch, unsafe.Pointer(scratch))
	return scratch
}

func (v *version) getSharedCount() (count int) {
	for _, t := range v.tfiles {
		if t.shared {
			count++
		}
	}
	return
}

func (v *version) String() string {
	return fmt.Sprintf("tfiles size:%d", len(v.tfiles))
}
func (v *version) DebugString() string {
	return tFiles(v.tfiles).DebugString()
}

// incref unlock
func (v *version) incref() {
	if v.released {
		panic("version already released")
	}

	v.ref++
	// 第一次incref，给version的每个文件也增加引用
	if v.ref == 1 {
		for _, f := range v.tfiles {
			v.s.addFileRef(f.fd, 1)
		}
	}
}

// unref unlock
func (v *version) releaseUnLock() {
	v.ref--

	if v.ref > 0 {
		return
	} else if v.ref == 0 {
		for _, f := range v.tfiles {
			// 文件引用计数为0,删除该文件
			if v.s.addFileRef(f.fd, -1) == 0 {
				v.s.tops.remove(f)
			}
		}
		v.released = true
	} else {
		panic("version@release: negative ref")
	}
}

// unref lock
func (v *version) release() {
	v.s.vmu.Lock()
	v.releaseUnLock()
	v.s.vmu.Unlock()
}

type versionReleaser struct {
	v    *version
	once bool
}

func (vr *versionReleaser) Release() {
	v := vr.v
	v.s.vmu.Lock()
	if !vr.once {
		v.releaseUnLock()
		vr.once = true
	}
	v.s.vmu.Unlock()
}
