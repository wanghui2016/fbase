// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package filter

type Buffer interface {
	Alloc(n int) []byte

	Write(p []byte) (n int, err error)

	WriteByte(c byte) error
}

type Filter interface {
	Name() string

	NewGenerator() FilterGenerator

	Contains(filter, key []byte) bool
}

type FilterGenerator interface {
	Add(key []byte)

	Generate(b Buffer)
}
