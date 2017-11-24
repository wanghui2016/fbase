// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"engine/rowstore/filesystem"
	"engine/rowstore/memdb"
	"engine/rowstore/opt"
	ts "model/pkg/timestamp"
)

type session struct {
	id uint64 // db id

	stNextFileNum int64        // current unused file number
	timestamp     ts.Timestamp // last mem compacted timestamp; need external synchronization
	applied       uint64       // raft log apply index
	commitMu      sync.Mutex

	fs         filesystem.FileSystem
	fsUnLocker filesystem.UnLocker // 用于session关闭后解锁文件锁
	o          *opt.Options
	icmp       *iComparer
	tops       *tOps
	fileRefs   map[int64]int

	curVersion *version
	vmu        sync.Mutex

	tcompPolicy compactPolicy

	logger *dbLogger
}

func newSession(fs filesystem.FileSystem, o *opt.Options, dbID uint64) (s *session, err error) {
	unlocker, err := lockFilesystem(fs)
	if err != nil {
		return nil, err
	}

	s = &session{
		id:         dbID,
		fs:         fs,
		fsUnLocker: unlocker,
		fileRefs:   make(map[int64]int),
		logger:     newDBLogger(dbID),
	}

	s.rewriteOptions(o)
	s.tops = newTableOps(s)
	s.tcompPolicy = newWidthCompactPolicy(s.o.GetCompactionBudgetSize()/opt.MiB, s.icmp, s.logger)

	return
}

func (s *session) swarmSplit(dbID uint64, fs filesystem.FileSystem) (news *session, err error) {
	unlocker, err := lockFilesystem(fs)
	if err != nil {
		return nil, err
	}

	news = &session{
		id:         dbID,
		fs:         fs,
		fsUnLocker: unlocker,
		fileRefs:   make(map[int64]int),
		icmp:       s.icmp,
		logger:     newDBLogger(dbID),
	}
	// split直接沿用父db的opt
	dupOpt := *(s.o)
	news.o = &dupOpt
	news.tops = newTableOps(news)
	news.tcompPolicy = newWidthCompactPolicy(s.o.GetCompactionBudgetSize()/opt.MiB, s.icmp, s.logger)

	return
}

func lockFilesystem(fs filesystem.FileSystem) (filesystem.UnLocker, error) {
	if fs == nil {
		return nil, os.ErrInvalid
	}
	fsUnLocker, err := fs.Lock()
	if err != nil {
		return nil, err
	}
	return fsUnLocker, nil
}

func (s *session) rewriteOptions(o *opt.Options) {
	dupOpt := &opt.Options{}
	if o != nil {
		*dupOpt = *o
	}

	// 重新设置filter，实现从internalKey到user key之间的转换
	if o.GetFilter() != nil {
		dupOpt.Filter = iFilter{o.GetFilter()}
	}

	// 设置icmp, 可以同时比较internalKey和user key
	s.icmp = &iComparer{o.GetComparer()}
	dupOpt.Comparer = s.icmp

	s.o = dupOpt
}

func (s *session) close() {
	// 必须先关闭tops，不然一setVersion，之前的version引用计数为0,表文件会被删除
	s.tops.close()

	s.setVersion(&version{s: s, closing: true})
}

func (s *session) release() {
	s.fsUnLocker.Unlock()
}

func (s *session) create() error {
	ver := newVersion(s, nil)
	s.setVersion(ver)
	return nil
}

func (s *session) createSplit(filenum int64) error {
	s.stNextFileNum = filenum
	ver := newVersion(s, nil)
	s.setVersion(ver)
	return nil
}

//TODO: 清理临时文件
func (s *session) recover() (applied uint64, err error) {
	defer func() {
		if os.IsNotExist(err) {
			if fds, _ := s.fs.List(filesystem.TypeTable); len(fds) > 0 {
				err = &filesystem.ErrCorrupted{Fd: filesystem.FileDesc{Type: filesystem.TypeManifest}, Err: &filesystem.ErrMissingFiles{}}
			}
		}
	}()

	var fd filesystem.FileDesc
	fd, err = s.fs.GetMeta()
	if err != nil {
		return
	}

	var reader filesystem.Reader
	reader, err = s.fs.Open(fd)
	if err != nil {
		return
	}

	meta, err := LoadManifestMeta(reader)
	if err != nil {
		return 0, fmt.Errorf("DB::session::recover(): load manifest failed(%v)", err)
	}

	s.timestamp = meta.Timestamp
	s.applied = meta.Applied
	s.stNextFileNum = meta.NextFileNum
	s.setVersion(newVersion(s, meta.TableFiles))

	s.logger.Info("recovered version:%d, applied:%v, nextFileNum:%v, timestamp:%v, tablefiles:%d",
		meta.Version, s.applied, s.stNextFileNum, s.timestamp, len(meta.TableFiles))

	return s.applied, nil
}

// get table version
func (s *session) version() (ver *version) {
	s.vmu.Lock()
	ver = s.curVersion
	ver.incref()
	s.vmu.Unlock()
	return
}

// set table version
func (s *session) setVersion(v *version) {
	if s.logger.IsEnableDebug() {
		s.logger.Debug("new version: %s, A:%d, T:%d", v.DebugString(), s.applied, s.timestamp)
	} else {
		s.logger.Info("new version: N:%d, A:%d, T:%d", len(v.tfiles), s.applied, s.timestamp)
	}
	s.vmu.Lock()
	defer s.vmu.Unlock()

	v.incref()
	if s.curVersion != nil {
		s.curVersion.releaseUnLock()
	}
	s.curVersion = v
}

func (s *session) flushMemdb(mdb memdb.DB) (*tFile, error) {
	iter := mdb.NewIterator(nil)
	defer iter.Release()
	// 把mdb的kv写入table t 中
	t, _, err := s.tops.createFrom(iter, false)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *session) Applied() uint64 {
	return atomic.LoadUint64(&s.applied)
}

// 分配一个文件序号
func (s *session) allocFileNum() int64 {
	return atomic.AddInt64(&s.stNextFileNum, 1) - 1
}

func (s *session) setNextFileNum(num int64) {
	atomic.StoreInt64(&s.stNextFileNum, num)
}

func (s *session) reuseFileNum(num int64) {
	for {
		old, x := s.stNextFileNum, num
		if old != x+1 {
			x = old
		}
		if atomic.CompareAndSwapInt64(&s.stNextFileNum, old, x) {
			break
		}
	}
}

type versionEdit struct {
	timestamp ts.Timestamp
	raftIndex uint64

	addedTables []*tFile
	deleted     []*tFile
}

func (s *session) commit(edit *versionEdit) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if s.logger.IsEnableDebug() {
		s.logger.Debug("session commit. T:%d, A:%d, added: %s, deleted: %s", edit.timestamp,
			edit.raftIndex, tFiles(edit.addedTables).DebugString(), tFiles(edit.deleted).DebugString())
	} else {
		s.logger.Info("session commit. T:%d, A:%d, added: %d, deleted: %d", edit.timestamp,
			edit.raftIndex, len(edit.addedTables), len(edit.deleted))
	}
	deletedSet := make(map[int64]struct{})
	for _, t := range edit.deleted {
		deletedSet[t.fd.Num] = struct{}{}
	}

	var toAdds []*tFile
	oldVer := s.version()
	toAdds = append(toAdds, oldVer.tfiles...)
	toAdds = append(toAdds, edit.addedTables...)
	oldVer.release()

	var newTFiles []*tFile
	addedSet := make(map[int64]struct{}) // 去重
	for _, t := range toAdds {
		// 需要删除
		if _, ok := deletedSet[t.fd.Num]; ok {
			continue
		}

		if _, ok := addedSet[t.fd.Num]; ok {
			continue
		}
		newTFiles = append(newTFiles, t)
		addedSet[t.fd.Num] = struct{}{}
	}

	meta := newMainfestMeta()
	// add timestamp
	if edit.timestamp.Valid() {
		meta.Timestamp = edit.timestamp
	} else {
		meta.Timestamp = s.timestamp
	}
	// add applied id
	if edit.raftIndex > 0 {
		meta.Applied = edit.raftIndex
	} else {
		meta.Applied = s.Applied()
	}
	// add next file num
	meta.NextFileNum = atomic.LoadInt64(&s.stNextFileNum)
	// add table file
	for _, t := range newTFiles {
		meta.AddTableFile(t)
	}

	start := time.Now()
	// write mainfest
	if err := s.writeManifest(meta); err != nil {
		return err
	}
	//TODO: remove
	s.logger.Info("write mainifest take %v", time.Since(start))

	// apply to session
	if edit.timestamp.Valid() {
		s.timestamp = edit.timestamp
	}
	if edit.raftIndex > 0 {
		atomic.StoreUint64(&s.applied, edit.raftIndex)
	}
	s.setVersion(newVersion(s, meta.TableFiles))

	return nil
}

func (s *session) writeManifest(meta *ManifestMeta) error {
	// check timestamp
	if err := checkTSValid(meta.Timestamp); err != nil {
		return fmt.Errorf("DB::session::writeManifest(): invalid timestamp(%d)", meta.Timestamp)
	}

	fd := filesystem.FileDesc{Type: filesystem.TypeManifestTemp, Num: 0}
	fw, err := s.fs.Create(fd)
	if err != nil {
		return err
	}

	if err := StoreManifestMeta(meta, fw); err != nil {
		return fmt.Errorf("DB::session::writeManifest(): store manifest meta failed: %v", err)
	}

	// sync
	err = fw.Sync()
	if err != nil {
		return err
	}

	err = s.fs.Rename(fd, filesystem.FileDesc{Type: filesystem.TypeManifest, Num: 0})
	if err != nil {
		return err
	}

	// save backup
	bfd := filesystem.FileDesc{Type: filesystem.TypeManifestBak, Num: 0}
	bfw, err := s.fs.Create(bfd)
	if err != nil {
		return err
	}
	if err := StoreManifestMeta(meta, bfw); err != nil {
		return fmt.Errorf("DB::session::writeManifest(): store backup manifest meta failed: %v", err)
	}
	return bfw.Sync()
}

func (s *session) addFileRef(fd filesystem.FileDesc, ref int) int {
	ref += s.fileRefs[fd.Num]
	if ref > 0 {
		s.fileRefs[fd.Num] = ref
	} else if ref == 0 {
		delete(s.fileRefs, fd.Num)
	} else {
		panic(fmt.Sprintf("negative file ref: %v", fd))
	}
	return ref
}
