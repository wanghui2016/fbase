// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
)

const (
	// 一个字节压缩类型 + 四字节checksum
	blockTrailerLen = 5

	// footer固定长度48字节
	// 40字节：metaIndexBlockHandle + indexBlockHandle + padding
	// 1字节： version
	// 7字节： magic
	footerLen = 48

	// 表格式版本号
	versionV1 = '\x01'

	// 表尾魔数
	// echo jd.com/fbase/engine | sha1sum | cut -c1-14
	magic = "\xf9\xbf\x3e\x0a\xd3\xc5\xcc"

	// block的压缩类型
	blockTypeNoCompression     = 0
	blockTypeSnappyCompression = 1

	// 每2K数据一段filter数据
	filterBaseLg = 11
	filterBase   = 1 << filterBaseLg
)

type blockHandle struct {
	offset uint64
	length uint64 // 不包括block trailer
}

func decodeBlockHandle(src []byte) (blockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return blockHandle{}, 0
	}
	return blockHandle{offset, length}, n + m
}

func encodeBlockHandle(dst []byte, b blockHandle) int {
	n := binary.PutUvarint(dst, b.offset)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}
