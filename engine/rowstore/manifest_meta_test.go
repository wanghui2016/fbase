// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	ts "model/pkg/timestamp"
)

func compareManifestMeta(m1, m2 *ManifestMeta) error {
	if m1.Applied != m2.Applied {
		return fmt.Errorf("inconsistent applied: %d != %d", m1.Applied, m2.Applied)
	}
	if m1.Timestamp != m2.Timestamp {
		return fmt.Errorf("inconsistent timestamp: %d != %d", m1.Timestamp, m2.Timestamp)
	}
	if m1.NextFileNum != m2.NextFileNum {
		return fmt.Errorf("inconsistent next-file-num: %d != %d", m1.NextFileNum, m2.NextFileNum)
	}
	if m1.Version != m2.Version {
		return fmt.Errorf("inconsistent next-file-num: %d != %d", m1.NextFileNum, m2.NextFileNum)
	}

	if len(m1.TableFiles) != len(m2.TableFiles) {
		return fmt.Errorf("inconsistent table file size: %d != %d", len(m1.TableFiles), len(m2.TableFiles))
	}

	for i := 0; i < len(m1.TableFiles); i++ {
		t1 := m1.TableFiles[i]
		t2 := m2.TableFiles[i]
		if t1.Num != t2.Num {
			return fmt.Errorf("inconsisent tfile num at %d: %d != %d", i, t1.Num, t2.Num)
		}
		if t1.FileSize != t2.FileSize {
			return fmt.Errorf("inconsisent tfile size at %d: %d != %d", i, t1.FileSize, t2.FileSize)
		}
		if t1.KVSize != t2.KVSize {
			return fmt.Errorf("inconsisent tfile kvsize at %d: %d != %d", i, t1.KVSize, t2.KVSize)
		}
		if t1.Shared != t2.Shared {
			return fmt.Errorf("inconsisent tfile shared at %v: %v != %v", i, t1.Shared, t2.Shared)
		}
		if !bytes.Equal(t1.StartIKey, t2.StartIKey) {
			return fmt.Errorf("inconsisent tfile start-ikey at %v: %v != %v", i, t1.StartIKey, t2.StartIKey)
		}
		if !bytes.Equal(t1.EndIKey, t2.EndIKey) {
			return fmt.Errorf("inconsisent tfile end-ikey at %v: %v != %v", i, t1.EndIKey, t2.EndIKey)
		}
	}

	return nil
}

func TestManifestMeta(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	genKey := func() []byte {
		len := rnd.Int()%20 + 10
		buf := make([]byte, len)
		for i := 0; i < len; i++ {
			buf[i] = byte(rand.Int() % 256)
		}
		return buf
	}

	// test json marshal umarshal
	meta := newMainfestMeta()
	meta.Applied = 123
	meta.Timestamp = ts.Timestamp{WallTime: 45, Logical: 6}
	meta.NextFileNum = 789
	for i := 1; i < 5; i++ {
		t := TableFileMeta{
			Num:       int64(i),
			FileSize:  int64(rnd.Int()),
			KVSize:    uint64(rnd.Int()),
			Shared:    i%2 == 0,
			StartIKey: genKey(),
			EndIKey:   genKey(),
		}
		meta.TableFiles = append(meta.TableFiles, t)
	}
	jsonData, err := json.Marshal(meta)
	if err != nil {
		t.Error(err)
	}
	t.Logf("meta: %s", string(jsonData))
	t.Logf("meta: %v", jsonData)
	meta2 := &ManifestMeta{}
	err = json.Unmarshal(jsonData, meta2)
	if err != nil {
		t.Error(err)
	}
	err = compareManifestMeta(meta, meta2)
	if err != nil {
		t.Error(err)
	}

	// test store load
	f, err := ioutil.TempFile("", "fbase_test_meta_")
	name := f.Name()
	if err != nil {
		t.Fatal(err)
	}
	err = StoreManifestMeta(meta, f)
	if err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()

	f, err = os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	meta3, err := LoadManifestMeta(f)
	if err != nil {
		t.Fatal(err)
	}
	err = compareManifestMeta(meta, meta3)
	if err != nil {
		t.Error(err)
	}
}
