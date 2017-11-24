// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"sort"

	"engine/rowstore/comparer"
	"engine/rowstore/filter"
	"util/bufalloc"
)

type block struct {
	bh             blockHandle
	buffer         bufalloc.Buffer
	data           []byte
	restartsLen    int
	restartsOffset int
}

func (b *block) seek(cmp comparer.Comparer, rstart, rlimit int, key []byte) (index, offset int, err error) {
	index = sort.Search(b.restartsLen-rstart-(b.restartsLen-rlimit), func(i int) bool {
		offset := int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*(rstart+i):]))
		offset++                                    // shared always zero, since this is a restart point
		v1, n1 := binary.Uvarint(b.data[offset:])   // key length
		_, n2 := binary.Uvarint(b.data[offset+n1:]) // value length
		m := offset + n1 + n2
		return cmp.Compare(b.data[m:m+int(v1)], key) > 0
	}) + rstart - 1
	if index < rstart {
		// The smallest key is greater-than key sought.
		index = rstart
	}
	offset = int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*index:]))
	return
}

func (b *block) restartIndex(rstart, rlimit, offset int) int {
	return sort.Search(b.restartsLen-rstart-(b.restartsLen-rlimit), func(i int) bool {
		return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*(rstart+i):])) > offset
	}) + rstart - 1
}

func (b *block) restartOffset(index int) int {
	return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*index:]))
}

func (b *block) entry(offset int) (key, value []byte, nShared, n int, err error) {
	if offset >= b.restartsOffset {
		if offset != b.restartsOffset {
			err = &ErrCorrupted{Reason: "entries offset not aligned"}
		}
		return
	}
	v0, n0 := binary.Uvarint(b.data[offset:])       // Shared prefix length
	v1, n1 := binary.Uvarint(b.data[offset+n0:])    // Key length
	v2, n2 := binary.Uvarint(b.data[offset+n0+n1:]) // Value length
	m := n0 + n1 + n2
	n = m + int(v1) + int(v2)
	if n0 <= 0 || n1 <= 0 || n2 <= 0 || offset+n > b.restartsOffset {
		err = &ErrCorrupted{Reason: "entries corrupted"}
		return
	}
	key = b.data[offset+m : offset+m+int(v1)]
	value = b.data[offset+m+int(v1) : offset+n]
	nShared = int(v0)
	return
}

func (b *block) Release() {
	bufalloc.FreeBuffer(b.buffer)
	b.buffer = nil
	b.data = nil
}

type filterBlock struct {
	buffer     bufalloc.Buffer
	data       []byte
	oOffset    int
	baseLg     uint
	filtersNum int
}

func (b *filterBlock) contains(filter filter.Filter, offset uint64, key []byte) bool {
	i := int(offset >> b.baseLg)
	if i < b.filtersNum {
		o := b.data[b.oOffset+i*4:]
		n := int(binary.LittleEndian.Uint32(o))
		m := int(binary.LittleEndian.Uint32(o[4:]))
		if n < m && m <= b.oOffset {
			return filter.Contains(b.data[n:m], key)
		} else if n == m {
			return false
		}
	}
	return true
}

func (b *filterBlock) Release() {
	bufalloc.FreeBuffer(b.buffer)
	b.buffer = nil
	b.data = nil
}
