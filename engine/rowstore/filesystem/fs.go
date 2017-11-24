// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package filesystem

import (
	"fmt"
	"io"
	"sort"
)

// FileType represent a file type.
type FileType int

// File types.
const (
	TypeManifest FileType = 1 << iota
	TypeManifestTemp
	TypeManifestBak
	TypeTable
	TypeTemp

	TypeAll = TypeManifest | TypeManifestTemp | TypeTable | TypeTemp | TypeManifestBak
)

func (t FileType) String() string {
	switch t {
	case TypeManifest:
		return "manifest"
	case TypeManifestTemp:
		return "mtemp"
	case TypeManifestBak:
		return "manifest.bak"
	case TypeTable:
		return "fdb"
	case TypeTemp:
		return "temp"
	}
	return fmt.Sprintf("<unknown:%d>", t)
}

type Syncer interface {
	Sync() error
}

type Reader interface {
	io.ReadSeeker
	io.ReaderAt
	io.Closer
}

type Writer interface {
	io.WriteCloser
	Syncer
}

type UnLocker interface {
	Unlock()
}

// FileDesc 文件描述符
type FileDesc struct {
	Type FileType // 文件类型，sst、mainfest、temp等
	Num  int64    // 序号
}

func (fd FileDesc) String() string {
	switch fd.Type {
	case TypeManifest:
		return "MANIFEST"
	case TypeManifestTemp:
		return "MANIFEST.TMP"
	case TypeManifestBak:
		return "MANIFEST.BAK"
	case TypeTable:
		return fmt.Sprintf("%06d.fdb", fd.Num)
	case TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		return fmt.Sprintf("%#x-%d", fd.Type, fd.Num)
	}
}

func (fd FileDesc) Zero() bool {
	return fd == (FileDesc{})
}

func FileDescOk(fd FileDesc) bool {
	switch fd.Type {
	case TypeManifest:
	case TypeManifestTemp:
	case TypeManifestBak:
	case TypeTable:
	case TypeTemp:
	default:
		return false
	}
	return fd.Num >= 0
}

// sort filedescs
type fdSorter []FileDesc

func (s fdSorter) Len() int {
	return len(s)
}

func (s fdSorter) Less(i, j int) bool {
	return s[i].Num < s[j].Num
}

func (s fdSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func sortFds(fds []FileDesc) {
	sort.Sort(fdSorter(fds))
}

// FileSystem file system
type FileSystem interface {
	// 文件锁 lock(dir/log)
	Lock() (UnLocker, error)

	// GetMeta 获取manifest文件描述符
	GetMeta() (FileDesc, error)

	// List 列举目录下某类型的文件
	List(ft FileType) ([]FileDesc, error)

	// Open 打开某个文件
	Open(fd FileDesc) (Reader, error)

	// Create 创建文件，如果存在则截断
	Create(fd FileDesc) (Writer, error)

	Remove(fd FileDesc) error

	Rename(oldfd, newfd FileDesc) error

	// MakeLink 把当前目录下的某个文件链接过去(distDir下) isHard为false表示软链接，为true硬链接
	MakeLink(srcfd, distfd FileDesc, distDir string, isHard bool) error

	Close() error

	// Destroy 销毁，删除整个目录
	Destroy() error
}
