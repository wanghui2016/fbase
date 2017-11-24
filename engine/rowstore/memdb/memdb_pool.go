// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package memdb

import (
	"sync"

	"engine/rowstore/comparer"
)

var (
	dbPool = &sync.Pool{
		New: func() interface{} {
			return newSkiplistDB()
		},
	}
)

func PutDB(db DB) {
	dbPool.Put(db)
}

// Creates a new initialized in-memory key/value DB.
// The capacity is the initial key/value buffer capacity. The capacity is advisory, not enforced.
func GetDB(cmp comparer.BasicComparer, capacity int) DB {
	db := dbPool.Get().(*skiplistDB)
	if db.cmp == nil {
		db.cmp = cmp
		db.kvData = make([]byte, 0, capacity)
	} else if db.Capacity() < capacity {
		// return old memdb
		PutDB(db)
		// create new memdb
		db = newSkiplistDB()
		db.cmp = cmp
		db.kvData = make([]byte, 0, capacity)
	} else {
		db.Reset()
	}

	return db
}
