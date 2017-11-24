// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package memdb

import (
	"engine/rowstore/iterator"
	"util"
)

// DB is an in-memory key/value database.
type DB interface {
	// Put sets the value for the given key. It overwrites any previous value for that key; a DB is not a multi-map.
	// It is safe to modify the contents of the arguments after Put returns.
	Put(key []byte, value []byte) error
	// Delete deletes the value for the given key. It returns ErrNotFound if the DB does not contain the key.
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte) error
	// Contains returns true if the given key are in the DB.
	// It is safe to modify the contents of the arguments after Contains returns.
	Contains(key []byte) bool
	// Get gets the value for the given key. It returns error.ErrNotFound if the DB does not contain the key.
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key []byte) (value []byte, err error)
	// Min return min key
	Min() []byte
	// Max return the max key
	Max() []byte

	// Find finds key/value pair whose key is greater than or equal to the
	// given key. It returns ErrNotFound if the table doesn't contain such pair.
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Find returns.
	Find(key []byte) (rkey, value []byte, err error)
	// Capacity returns keys/values buffer capacity.
	Capacity() int
	// Size returns sum of keys and values length. Note that deleted
	// key/value will not be accounted for, but it will still consume
	// the buffer, since the buffer is append only.
	Size() int
	// Len returns the number of entries in the DB.
	Len() int
	// Free returns keys/values free buffer before need to grow.
	Free() int
	// Reset resets the DB to initial empty state. Allows reuse the buffer.
	Reset()
	// NewIterator returns an iterator of the DB.
	// The returned iterator is not safe for concurrent use, but it is safe to use
	// multiple iterators concurrently, with each in a dedicated goroutine.
	// It is also safe to use an iterator concurrently with modifying its
	// underlying DB. However, the resultant key/value pairs are not guaranteed
	// to be a consistent snapshot of the DB at a particular point in time.
	//
	// Slice allows slicing the iterator to only contains keys in the given
	// range. A nil Range.Start is treated as a key before all keys in the
	// DB. And a nil Range.Limit is treated as a key after all keys in
	// the DB.
	//
	// The iterator must be released after use, by calling Release method.
	// Also read Iterator documentation of the store/iterator package.
	NewIterator(slice *util.Range) iterator.Iterator
}
