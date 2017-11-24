// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package opt

import (
	"engine/rowstore/cache"
	"engine/rowstore/comparer"
	"engine/rowstore/filter"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

var (
	DefaultCompressionType            = SnappyCompression
	DefaultBlockRestartInterval       = 16
	DefaultBlockSize                  = 128 * KiB
	DefaultBlockCacheCapacity         = 32 * MiB
	DefaultOpenFilesCacheCapacity     = 64
	DefaultTableFileSize              = 16 * MiB
	DefaultWriteBuffer                = 16 * MiB
	DefaultWriteBufferPauseTrigger    = 4
	DefaultWriteBufferSlowdownTrigger = 2
	DefaultFilter                     = filter.NewBloomFilter(10)

	DefaultCompactionBudgetSize = 64 * MiB

	DefaultChecksumType = 0
)

type Compression uint

func (c Compression) String() string {
	switch c {
	case DefaultCompression:
		return "default"
	case NoCompression:
		return "none"
	case SnappyCompression:
		return "snappy"
	}
	return "invalid"
}

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	nCompression
)

// Options db options
type Options struct {
	// 只读
	ReadOnly bool

	ErrorIfExist bool

	ErrorIfMissing bool

	// BlockSize sst file block size
	BlockSize int

	BlockRestartInterval int

	// Compare 比较器，默认是byteComparer
	Comparer comparer.Comparer

	// 压缩类型 默认snappy
	Compression Compression

	// Filter 过滤器 默认是布隆过滤器
	Filter filter.Filter

	// OpenFilesCacheCapacity  缓存的打开文件描述符个数
	OpenFilesCacheCapacity int

	// BlockCache 块缓存
	BlockCache *cache.Cache

	// WriteBuffer  memdb 大小
	WriteBuffer int

	// WriteBufferPauseTrigger  冻结的memdb的个数达到这个值开始delay write操作
	WriteBufferPauseTrigger int

	// WriteBufferSlowdownTrigger   冻结的memdb的个数达到这个值开始停止 write操作
	WriteBufferSlowdownTrigger int

	// 外部的Compact约束
	ExternalCompactLimiter CompactTimestampLimiter

	// 表文件大小 (最大值)
	TableFileSize int

	// Compaction预算 （每次compaction的输入最多涉及多大）
	CompactionBudgetSize int
	// no check:0; check every times:1 ;
	ChecksumType int
}

func (o *Options) GetReadOnly() bool {
	if o == nil {
		return false
	}
	return o.ReadOnly
}

func (o *Options) GetErrorIfExist() bool {
	if o == nil {
		return false
	}
	return o.ErrorIfExist
}

func (o *Options) GetErrorIfMissing() bool {
	if o == nil {
		return false
	}
	return o.ErrorIfMissing
}

func (o *Options) GetBlockSize() int {
	if o == nil || o.BlockSize <= 0 {
		return DefaultBlockSize
	}
	return o.BlockSize
}

func (o *Options) GetComparer() comparer.Comparer {
	if o == nil || o.Comparer == nil {
		return comparer.DefaultComparer
	}
	return o.Comparer
}

func (o *Options) GetCompression() Compression {
	if o == nil || o.Compression <= DefaultCompression || o.Compression >= nCompression {
		return DefaultCompressionType
	}
	return o.Compression
}

func (o *Options) GetFilter() filter.Filter {
	if o == nil || o.Filter == nil {
		return DefaultFilter
	}
	return o.Filter
}

func (o *Options) GetWriteBuffer() int {
	if o == nil || o.WriteBuffer <= 0 {
		return DefaultWriteBuffer
	}
	return o.WriteBuffer
}

func (o *Options) GetWriteBufferPauseTrigger() int {
	if o == nil || o.WriteBufferPauseTrigger <= 0 {
		return DefaultWriteBufferPauseTrigger
	}
	return o.WriteBufferPauseTrigger

}

func (o *Options) GetWriteBufferSlowdownTrigger() int {
	if o == nil || o.WriteBufferSlowdownTrigger <= 0 {
		return DefaultWriteBufferSlowdownTrigger
	}
	return o.WriteBufferSlowdownTrigger
}

func (o *Options) GetOpenFilesCacheCapacity() int {
	if o == nil || o.OpenFilesCacheCapacity == 0 {
		return DefaultOpenFilesCacheCapacity
	} else if o.OpenFilesCacheCapacity < 0 {
		return 0
	}
	return o.OpenFilesCacheCapacity
}

func (o *Options) GetBlockCache() *cache.Cache {
	if o == nil {
		return nil
	}
	return o.BlockCache
}

// GetBlockRestartInterval
func (o *Options) GetBlockRestartInterval() int {
	if o == nil || o.BlockRestartInterval <= 0 {
		return DefaultBlockRestartInterval
	}
	return o.BlockRestartInterval
}

// GetExternalCompactAtMost 0表示没有约束
func (o *Options) GetExternalCompactAtMost() uint64 {
	if o == nil || o.ExternalCompactLimiter == nil {
		return 0
	}
	return o.ExternalCompactLimiter.AtMost()
}

// GetTableFileSize 表文件大小
func (o *Options) GetTableFileSize() int {
	if o == nil || o.TableFileSize <= 0 {
		return DefaultTableFileSize
	}
	return o.TableFileSize
}

// GetCompactionBudgetSize compaction input预算
func (o *Options) GetCompactionBudgetSize() int {
	if o == nil || o.CompactionBudgetSize <= 0 {
		return DefaultCompactionBudgetSize
	}
	return o.CompactionBudgetSize
}

func (o *Options) GetChecksumType() int {
	if o == nil || o.ChecksumType <= 0 {
		return DefaultChecksumType
	}
	return o.ChecksumType
}
