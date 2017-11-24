// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"encoding/binary"
	"fmt"

	"math"

	"engine/rowstore/filesystem"
	ts "model/pkg/timestamp"
)

// ErrInternalKeyCorrupted records internal key corruption.
type ErrInternalKeyCorrupted struct {
	Ikey   []byte
	Reason string
}

func (e *ErrInternalKeyCorrupted) Error() string {
	return fmt.Sprintf("fbase/engine: internal key %q corrupted: %s", e.Ikey, e.Reason)
}

func newErrInternalKeyCorrupted(ikey []byte, reason string) error {
	return filesystem.NewErrCorrupted(filesystem.FileDesc{}, &ErrInternalKeyCorrupted{append([]byte{}, ikey...), reason})
}

type keyType uint

func (kt keyType) String() string {
	switch kt {
	case keyTypeDel:
		return "d"
	case keyTypeVal:
		return "v"
	}
	return fmt.Sprintf("<invalid:%#x>", uint(kt))
}

// KeyInternalSuffixLen  timestamp长度 + 一字节类型长度
const KeyInternalSuffixLen = 8 + 4 + 1

const (
	keyTypeDel = keyType(0)
	keyTypeVal = keyType(1)

	keyTypeSeek = keyTypeVal
)

var keyMaxTimestamp = ts.Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}

// Maximum number encoded in bytes.
var keyMaxNumBytes = make([]byte, KeyInternalSuffixLen)

func init() {
	binary.LittleEndian.PutUint64(keyMaxNumBytes, uint64(ts.MaxTimestamp.WallTime))
	binary.LittleEndian.PutUint32(keyMaxNumBytes[8:], uint32(ts.MaxTimestamp.Logical))
	keyMaxNumBytes[12] = byte(keyTypeSeek)
}

type internalKey []byte

func makeInternalKey(dst, ukey []byte, timestamp ts.Timestamp, kt keyType) internalKey {
	if kt > keyTypeVal {
		panic("fbase/engine: invalid type")
	}

	dst = ensureBuffer(dst, len(ukey)+KeyInternalSuffixLen)
	copy(dst, ukey)
	binary.LittleEndian.PutUint64(dst[len(ukey):], uint64(timestamp.WallTime))
	binary.LittleEndian.PutUint32(dst[len(ukey)+8:], uint32(timestamp.Logical))
	dst[len(ukey)+12] = byte(kt)
	return internalKey(dst)
}

func parseInternalKey(ik []byte) (ukey []byte, timestmap ts.Timestamp, kt keyType, err error) {
	if len(ik) < KeyInternalSuffixLen {
		return nil, ts.Timestamp{}, 0, newErrInternalKeyCorrupted(ik, "invalid length")
	}
	timestmap.WallTime = int64(binary.LittleEndian.Uint64(ik[len(ik)-KeyInternalSuffixLen:]))
	timestmap.Logical = int32(binary.LittleEndian.Uint32(ik[len(ik)-KeyInternalSuffixLen+8:]))
	kt = keyType(ik[len(ik)-1])
	if kt > keyTypeVal {
		return nil, ts.Timestamp{}, 0, newErrInternalKeyCorrupted(ik, "invalid type")
	}
	ukey = ik[:len(ik)-KeyInternalSuffixLen]
	return
}

func validInternalKey(ik []byte) bool {
	_, _, _, err := parseInternalKey(ik)
	return err == nil
}

func (ik internalKey) assert() {
	if ik == nil {
		panic("fbase/engine: nil internalKey")
	}
	if len(ik) < KeyInternalSuffixLen {
		panic(fmt.Sprintf("fbase/engine: internal key %q, len=%d: invalid length", []byte(ik), len(ik)))
	}
}

func (ik internalKey) ukey() []byte {
	ik.assert()
	return ik[:len(ik)-KeyInternalSuffixLen]
}

func (ik internalKey) timestmap() (timestamp ts.Timestamp) {
	ik.assert()
	timestamp.WallTime = int64(binary.LittleEndian.Uint64(ik[len(ik)-KeyInternalSuffixLen:]))
	timestamp.Logical = int32(binary.LittleEndian.Uint32(ik[len(ik)-KeyInternalSuffixLen+8:]))
	return
}

func (ik internalKey) String() string {
	if ik == nil {
		return "<nil>"
	}

	if ukey, seq, kt, err := parseInternalKey(ik); err == nil {
		return fmt.Sprintf("%s,%s%d", shorten(string(ukey)), kt, seq)
	}
	return fmt.Sprintf("<invalid:%#x>", []byte(ik))
}

func shorten(str string) string {
	if len(str) <= 16 {
		return str
	}
	return str[:8] + ".." + str[len(str)-8:]
}

func ensureBuffer(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}
