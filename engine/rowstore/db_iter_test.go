// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"engine/rowstore/opt"
	ts "model/pkg/timestamp"
)

func TestDBIterator(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "test_fbase_db_iter")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, _, err := OpenFile(dir, &opt.Options{WriteBuffer: 1024})
	if err != nil {
		t.Fatal(err)
	}

	keyFmt := "key%06d"
	valueFmt := "value%06d"

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf(keyFmt, i))
		value := []byte(fmt.Sprintf(valueFmt, i))
		if err := db.Put(key, value, 0, ts.Timestamp{WallTime: int64(i + 1)}, uint64(i)); err != nil {
			t.Error(err)
		}
	}

	testFunc := func(istart, ilimit int) {
		iter := db.NewIterator([]byte(fmt.Sprintf(keyFmt, istart)), []byte(fmt.Sprintf(keyFmt, ilimit)), ts.MaxTimestamp)
		defer iter.Release()

		end := ilimit
		if end > 1000 {
			end = 1000
		}

		for i := istart; i < end; i++ {
			if !iter.Next() {
				t.Fatalf("unexpected stop on %d, error: %v", i, iter.Error())
			}

			key := []byte(fmt.Sprintf(keyFmt, i))
			value := []byte(fmt.Sprintf(valueFmt, i))
			if !bytes.Equal(key, iter.Key()) {
				t.Errorf("unexpected key: %s, expected: %v", string(iter.Key()), string(key))
			}
			if !bytes.Equal(value, iter.Value()) {
				t.Errorf("unexpected value: %v, expected: %v", string(iter.Value()), string(value))
			}
		}

		if end == 1000 && iter.Next() {
			t.Errorf("should eof")
		}
	}

	testFunc(0, 1000)
	testFunc(0, 3000)
	testFunc(1, 200)
	testFunc(3, 500)
}
