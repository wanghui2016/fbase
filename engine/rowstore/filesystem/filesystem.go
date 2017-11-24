// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package filesystem

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"util/log"
)

var (
	errFileOpen = errors.New("fbase/filesystem: file still open")
	errReadOnly = errors.New("fbase/filesystem: storage is read-only")
)

type fileLock interface {
	release() error
}

type filesystemLock struct {
	fs *filesystem
}

func (lock *filesystemLock) Unlock() {
	if lock.fs != nil {
		lock.fs.mu.Lock()
		defer lock.fs.mu.Unlock()
		if lock.fs.slock == lock {
			lock.fs.slock = nil
		}
	}
}

type filesystem struct {
	path     string
	readOnly bool

	mu    sync.Mutex
	flock fileLock
	slock *filesystemLock
	// Opened file counter; if open < 0 means closed.
	open int
}

// OpenFile 初始化目录：判断目录是否存在，不存在创建；Lock文件锁；
// 打开日志文件
// The storage must be closed after use, by calling Close method.
func OpenFile(path string, readOnly bool) (FileSystem, error) {
	if fi, err := os.Stat(path); err == nil {
		if !fi.IsDir() {
			return nil, fmt.Errorf("fbase/storage: open %s: not a directory", path)
		}
	} else if os.IsNotExist(err) && !readOnly {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	flock, err := newFileLock(filepath.Join(path, "LOCK"), readOnly)
	if err != nil {
		return nil, fmt.Errorf("new lock file:%v", err)
	}

	defer func() {
		if err != nil {
			flock.release()
		}
	}()

	fs := &filesystem{
		path:     path,
		readOnly: readOnly,
		flock:    flock,
	}
	runtime.SetFinalizer(fs, (*filesystem).Close)
	return fs, nil
}

func (fs *filesystem) Lock() (UnLocker, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	if fs.readOnly {
		return &filesystemLock{}, nil
	}
	if fs.slock != nil {
		return nil, ErrLocked
	}
	fs.slock = &filesystemLock{fs: fs}
	return fs.slock, nil
}

func itoa(buf []byte, i int, wid int) []byte {
	u := uint(i)
	if u == 0 && wid <= 1 {
		return append(buf, '0')
	}

	// Assemble decimal in reverse order.
	var b [32]byte
	bp := len(b)
	for ; u > 0 || wid > 0; u /= 10 {
		bp--
		wid--
		b[bp] = byte(u%10) + '0'
	}
	return append(buf, b[bp:]...)
}

func (fs *filesystem) GetMeta() (fd FileDesc, err error) {
	var fds []FileDesc
	fds, err = fs.List(TypeManifest)
	if err != nil {
		return
	}
	if len(fds) == 0 {
		err = os.ErrNotExist
		return
	}
	fd = fds[0]
	return
}

func (fs *filesystem) List(ft FileType) (fds []FileDesc, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	dir, err := os.Open(fs.path)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(0)
	// Close the dir first before checking for Readdirnames error.
	if cerr := dir.Close(); cerr != nil {
		log.Warn("close dir: %v", cerr)
	}
	if err == nil {
		for _, name := range names {
			if fd, ok := fsParseName(name); ok && fd.Type&ft != 0 {
				fds = append(fds, fd)
			}
		}
	}
	return
}

func (fs *filesystem) Open(fd FileDesc) (Reader, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	of, err := os.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	fs.open++
	return &fileWrap{File: of, fs: fs, fd: fd}, nil
}

func (fs *filesystem) Create(fd FileDesc) (Writer, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}
	if fs.readOnly {
		return nil, errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	of, err := os.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	fs.open++
	return &fileWrap{File: of, fs: fs, fd: fd}, nil
}

func (fs *filesystem) Remove(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	return os.Remove(filepath.Join(fs.path, fsGenName(fd)))
}

func (fs *filesystem) Rename(oldfd, newfd FileDesc) error {
	if !FileDescOk(oldfd) || !FileDescOk(newfd) {
		return ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}

	err := rename(filepath.Join(fs.path, fsGenName(oldfd)), filepath.Join(fs.path, fsGenName(newfd)))
	if err != nil {
		return err
	}

	if newfd.Type == TypeManifest {
		if err := syncDir(fs.path); err != nil {
			log.Error("syncDir: %v", err)
			return err
		}
	}
	return nil
}
func (fs *filesystem) MakeLink(srcfd, distfd FileDesc, distDir string, isHard bool) error {
	src := filepath.Join(fs.path, fsGenName(srcfd))
	dst := filepath.Join(distDir, fsGenName(distfd))
	if isHard {
		return os.Link(src, dst)
	}
	return os.Symlink(src, dst)
}

func (fs *filesystem) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	// Clear the finalizer.
	runtime.SetFinalizer(fs, nil)

	if fs.open > 0 {
		log.Warn("close: warning, %d files still open", fs.open)
	}
	fs.open = -1
	return fs.flock.release()
}

func (fs *filesystem) Destroy() error {
	return os.RemoveAll(fs.path)
}

type fileWrap struct {
	*os.File
	fs     *filesystem
	fd     FileDesc
	closed bool
}

func (fw *fileWrap) Sync() error {
	return fw.File.Sync()
}

func (fw *fileWrap) Close() error {
	fw.fs.mu.Lock()
	defer fw.fs.mu.Unlock()
	if fw.closed {
		return ErrClosed
	}
	fw.closed = true
	fw.fs.open--
	err := fw.File.Close()
	if err != nil {
		log.Error("close %s: %v", fw.fd, err)
	}
	return err
}

func fsGenName(fd FileDesc) string {
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
		panic("invalid file type")
	}
}

func fsParseName(name string) (fd FileDesc, ok bool) {
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &fd.Num, &tail)
	if err == nil {
		switch tail {
		case "fdb":
			fd.Type = TypeTable
		case "tmp":
			fd.Type = TypeTemp
		default:
			return
		}
		return fd, true
	}

	if name == "MANIFEST" {
		fd.Type = TypeManifest
		return fd, true
	}

	if name == "MAINFEST.TMP" {
		fd.Type = TypeManifestTemp
		return fd, true
	}

	if name == "MAINFEST.BAK" {
		fd.Type = TypeManifestBak
		return fd, true
	}

	return
}

func fsParseNamePtr(name string, fd *FileDesc) bool {
	_fd, ok := fsParseName(name)
	if fd != nil {
		*fd = _fd
	}
	return ok
}
