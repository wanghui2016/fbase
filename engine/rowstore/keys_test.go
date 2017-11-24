// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"bytes"
	"encoding/binary"
	"testing"

	ts "model/pkg/timestamp"
)

func TestKey(t *testing.T) {
	// check keyMaxNumBytes
	if len(keyMaxNumBytes) != KeyInternalSuffixLen ||
		binary.LittleEndian.Uint64(keyMaxNumBytes) != uint64(ts.MaxTimestamp.WallTime) ||
		binary.LittleEndian.Uint32(keyMaxNumBytes[8:]) != uint32(ts.MaxTimestamp.Logical) ||
		keyMaxNumBytes[len(keyMaxNumBytes)-1] != byte(keyTypeSeek) {
		t.Errorf("incorrent keyMaxNumBytes: %v", keyMaxNumBytes)
	}

	testKeyMakeParse(t, []byte("my keyssss"), ts.Timestamp{WallTime: 9999, Logical: 9999}, keyTypeDel)
	testKeyMakeParse(t, []byte("my keyssss"), ts.Timestamp{WallTime: 9999, Logical: 1}, keyTypeDel)
	testKeyMakeParse(t, []byte("my keyssss"), ts.MinTimestamp, keyTypeDel)
	testKeyMakeParse(t, []byte("my keyssss"), ts.MinTimestamp, keyTypeVal)
	testKeyMakeParse(t, []byte("my keyssss"), ts.MaxTimestamp, keyTypeDel)
	testKeyMakeParse(t, []byte("my keyssss"), ts.MaxTimestamp, keyTypeVal)
}

func testKeyMakeParse(t *testing.T, ukey []byte, timestamp ts.Timestamp, kt keyType) {
	ikey := makeInternalKey(nil, ukey, timestamp, kt)

	t.Logf("internalKey result: %v", []byte(ikey))

	if len(ikey) != len(ukey)+KeyInternalSuffixLen {
		t.Error("incorrect internal key length")
	}

	actual_ukey, actual_timestamp, actual_kt, err := parseInternalKey(ikey)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(ukey, actual_ukey) {
		t.Errorf("unexpected ukey: %v, expected: %v\n", actual_ukey, ukey)
	}
	if actual_timestamp != timestamp {
		t.Errorf("unexpected timestamp: %v, expected: %v\n", actual_timestamp, timestamp)
	}
	if actual_kt != kt {
		t.Errorf("unexpected key type: %v, expected: %v\n", actual_kt, kt)
	}
}
