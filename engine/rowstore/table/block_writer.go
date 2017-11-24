// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"

	"util"
)

func sharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type blockWriter struct {
	restartInterval int
	buf             util.Buffer
	nEntries        int
	prevKey         []byte
	restarts        []uint32
	scratch         []byte
}

func (w *blockWriter) append(key, value []byte) {
	nShared := 0
	if w.nEntries%w.restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(w.buf.Len()))
	} else {
		nShared = sharedPrefixLen(w.prevKey, key)
	}
	n := binary.PutUvarint(w.scratch[0:], uint64(nShared))
	n += binary.PutUvarint(w.scratch[n:], uint64(len(key)-nShared))
	n += binary.PutUvarint(w.scratch[n:], uint64(len(value)))
	w.buf.Write(w.scratch[:n])
	w.buf.Write(key[nShared:])
	w.buf.Write(value)
	w.prevKey = append(w.prevKey[:0], key...)
	w.nEntries++
}

func (w *blockWriter) finish() {
	// Write restarts entry.
	if w.nEntries == 0 {
		// Must have at least one restart entry.
		w.restarts = append(w.restarts, 0)
	}
	w.restarts = append(w.restarts, uint32(len(w.restarts)))
	for _, x := range w.restarts {
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
}

func (w *blockWriter) reset() {
	w.buf.Reset()
	w.nEntries = 0
	w.restarts = w.restarts[:0]
}

func (w *blockWriter) bytesLen() int {
	restartsLen := len(w.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	return w.buf.Len() + 4*restartsLen + 4
}
