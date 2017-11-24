// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"math/rand"
	"testing"
	"time"

	"model/pkg/timestamp"
	"util"
	"util/assert"
)

type batchTestItem struct {
	typ   keyType
	key   []byte
	value []byte
}

func randBatchTestItems(r *rand.Rand, n int) (items []batchTestItem) {
	for i := 0; i < n; i++ {
		if r.Int()%2 == 0 {
			item := batchTestItem{
				typ: keyTypeDel,
				key: make([]byte, 32),
			}
			util.RandomBytes(r, item.key)
			items = append(items, item)
		} else {
			item := batchTestItem{
				typ:   keyTypeVal,
				key:   make([]byte, 32),
				value: make([]byte, 256),
			}
			util.RandomBytes(r, item.key)
			util.RandomBytes(r, item.value)
			items = append(items, item)
		}
	}
	return
}

func TestBatch(t *testing.T) {
	var err error
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ts := timestamp.Timestamp{WallTime: 999}

	for j := 0; j < 100; j++ {
		batch := getBatch()
		items := randBatchTestItems(r, 100)
		size := 0
		for _, item := range items {
			batch.appendRec(item.typ, item.key, item.value, 0, ts, 1)
			size += len(item.key) + KeyInternalSuffixLen
			if item.typ == keyTypeVal {
				size += len(item.value) + internalValueSuffixLen(0)
			}
			assert.Equal(t, batch.Size(), size)
		}
		assert.Equal(t, batch.Count(), len(items))
		assert.Equal(t, batch.Size(), size)
		var actualKey, actualValue []byte
		for i := 0; i < len(items); i++ {
			expectedKey := []byte(makeInternalKey(nil, items[i].key, ts, items[i].typ))
			expectedValue := items[i].value

			actualKey = batch.k(i)
			assert.DeepEqual(t, actualKey, expectedKey)
			actualKey, _ := batch.kv(i)
			assert.DeepEqual(t, actualKey, expectedKey)

			if items[i].typ == keyTypeVal {
				_, ivalue := batch.kv(i)
				actualValue, _, err = parseInternalValue(ivalue)
				assert.NilError(t, err)
				assert.DeepEqual(t, actualValue, expectedValue)
				ivalue = batch.v(i)
				actualValue, _, err = parseInternalValue(ivalue)
				assert.NilError(t, err)
				assert.DeepEqual(t, actualValue, expectedValue)
			}
		}
		putBatch(batch)
	}
}
