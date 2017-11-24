// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package comparer

// BasicComparer  the basic comparer interface
type BasicComparer interface {
	Compare(a, b []byte) int
}

// Comparer the comparer
type Comparer interface {
	BasicComparer

	Name() string

	// Separator 查找a,b中间的字节数组（可以把a,b隔开）
	Separator(dst, a, b []byte) []byte

	// Successor 获取位于b后面的字节数组
	Successor(dst, b []byte) []byte
}
