// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package memdb

import (
	"bytes"
	"testing"

	"engine/errors"
	"engine/rowstore/comparer"
)

func TestBasicOperate(t *testing.T) {
	db := GetDB(comparer.DefaultComparer, 1024)
	defer PutDB(db)

	key := []byte("mykey")
	value := []byte("myvalue")
	err := db.Put(key, value)
	if err != nil {
		t.Error(err)
	}

	actualKey, actualValue, err := db.Find(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(actualKey, key) {
		t.Errorf("unexpected key. expected=%v, actual=%v", key, actualValue)
	}
	if !bytes.Equal(value, actualValue) {
		t.Errorf("unexpected value. expected=%v, actual=%v", value, actualValue)
	}

	actualValue, err = db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(value, actualValue) {
		t.Errorf("unexpected value. expected=%v, actual=%v", value, actualValue)
	}

	err = db.Delete(key)
	if err != nil {
		t.Error(err)
	}

	_, err = db.Get(key)
	if err != errors.ErrNotFound {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMinMax(t *testing.T) {
	db := GetDB(comparer.DefaultComparer, 1024)
	defer PutDB(db)

	if db.Max() != nil {
		t.Errorf("max should be nil")
	}
	if db.Min() != nil {
		t.Errorf("min should be nil")
	}

	value := []byte{'v'}
	db.Put([]byte("aaa"), value)
	db.Put([]byte("bbb"), value)
	db.Put([]byte("ccc"), value)
	db.Put([]byte("ddd"), value)
	db.Put([]byte("eee"), value)

	if !bytes.Equal(db.Min(), []byte("aaa")) {
		t.Errorf("min should be: %v, actual: %v", "aaa", string(db.Min()))
	}

	if !bytes.Equal(db.Max(), []byte("eee")) {
		t.Errorf("max should be: %v, actual: %v", "eee", string(db.Max()))
	}
}

func TestIterator(t *testing.T) {
	//TODO:
}
