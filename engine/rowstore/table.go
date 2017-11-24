// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"

	"engine/errors"
	"engine/rowstore/cache"
	"engine/rowstore/filesystem"
	"engine/rowstore/iterator"
	"engine/rowstore/table"
	"util"
)

type tFile struct {
	fd         filesystem.FileDesc
	size       int64 // 文件实际大小
	imin, imax internalKey
	kvsize     uint64 // internalkey + value size
	shared     bool   // 分裂使用, 是否与分裂后的另一db共享
}

func (t *tFile) clone() *tFile {
	return &tFile{
		fd:     t.fd,
		size:   t.size,
		imin:   t.imin,
		imax:   t.imax,
		kvsize: t.kvsize,
		shared: t.shared,
	}
}

// return true if ukey is after largest key of this table
func (t *tFile) after(icmp *iComparer, ukey []byte) bool {
	return ukey != nil && icmp.uCompare(ukey, t.imax.ukey()) > 0
}

// return true if ukey is before smallest key of this table
func (t *tFile) before(icmp *iComparer, ukey []byte) bool {
	return ukey != nil && icmp.uCompare(ukey, t.imin.ukey()) < 0
}

func (t *tFile) overlaps(icmp *iComparer, umin, umax []byte) bool {
	return !(t.after(icmp, umin) || t.before(icmp, umax))
}

/*func (t *tFile) String() string {
	shared := ""
	if t.shared {
		shared = ", S"
	}
	return fmt.Sprintf("{N:%d, K:[%s-%s], %d, %d%s}", t.fd.Num,
		shorten(string(t.imin.ukey())), shorten(string(t.imax.ukey())), t.size, t.kvsize, shared)
}*/

func (t *tFile) DebugString() string {
	shared := ""
	if t.shared {
		shared = ", S"
	}

	return fmt.Sprintf("{N:%d, K:[%s-%s], %d, %d%s}", t.fd.Num,
		shorten(string(t.imin.ukey())), shorten(string(t.imax.ukey())), t.size, t.kvsize, shared)
}

type tFiles []*tFile

/*func (ts tFiles) String() string {
	if len(ts) == 0 {
		return "[empty]"
	}
	s := fmt.Sprintf("\n==================== table files start (%d)======================\n", len(ts))
	for i := 0; i < len(ts); i++ {
		s += "| " + ts[i].String() + "\n"
	}
	s += "==================== table files end ============================\n"
	return s
}*/

func (ts tFiles) DebugString() string {
	if len(ts) == 0 {
		return "[empty]"
	}
	s := fmt.Sprintf("\n==================== table files start (%d)======================\n", len(ts))
	for i := 0; i < len(ts); i++ {
		s += "| " + ts[i].DebugString() + "\n"
	}
	s += "==================== table files end ============================\n"
	return s
}

// Table operations
type tOps struct {
	s      *session
	fcache *cache.Cache // 缓存table.Reader
	bcache *cache.Cache
}

func newTableOps(s *session) *tOps {
	return &tOps{
		s:      s,
		fcache: cache.NewCache(cache.NewLRU(int64(s.o.GetOpenFilesCacheCapacity()))),
		bcache: s.o.GetBlockCache(),
	}
}

func (t *tOps) close() {
	t.fcache.Close()
}

func (t *tOps) open(f *tFile) (ch *cache.Handle, err error) {
	ch = t.fcache.Get(0, 0, uint64(f.fd.Num), func() (size int64, value cache.Value) {
		var r filesystem.Reader
		r, err := t.s.fs.Open(f.fd)
		if err != nil {
			return 0, nil
		}

		var bcache *cache.NamespaceGetter
		if t.bcache != nil {
			bcache = &cache.NamespaceGetter{
				Cache: t.bcache,
				RNG:   t.s.id,
				NS:    uint64(f.fd.Num),
			}
		}

		var tr *table.Reader
		tr, err = table.NewReader(r, f.size, f.fd, bcache, t.s.o)
		if err != nil {
			r.Close()
			return 0, nil
		}

		return 1, tr
	})

	if ch == nil && err == nil {
		err = errors.ErrClosed
	}
	return
}

func (t *tOps) find(f *tFile, key []byte) (rkey, rvalue []byte, err error) {
	ch, err := t.open(f)
	if err != nil {
		return nil, nil, err
	}
	defer ch.Release()
	return ch.Value().(*table.Reader).Find(key, true)
}

func (t *tOps) findKey(f *tFile, key []byte) (rkey []byte, err error) {
	ch, err := t.open(f)
	if err != nil {
		return nil, err
	}
	defer ch.Release()
	return ch.Value().(*table.Reader).FindKey(key, true)
}

func (t *tOps) create(isTemp bool) (*tWriter, error) {
	typ := filesystem.TypeTable
	if isTemp {
		typ = filesystem.TypeTemp
	}
	fd := filesystem.FileDesc{Type: typ, Num: t.s.allocFileNum()}
	fw, err := t.s.fs.Create(fd)
	if err != nil {
		return nil, err
	}

	return &tWriter{
		t:  t,
		fd: fd,
		w:  fw,
		tw: table.NewWriter(fw, t.s.o),
	}, nil
}

// 根据迭代器创建表文件
func (t *tOps) createFrom(iter iterator.Iterator, isTemp bool) (f *tFile, n int, err error) {
	var w *tWriter
	w, err = t.create(isTemp)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			w.drop()
		}
	}()

	for iter.Next() {
		err = w.append(iter.Key(), iter.Value())
		if err != nil {
			return
		}
	}
	err = iter.Error()
	if err != nil {
		return
	}

	n = w.tw.EntriesLen()
	f, err = w.finish()
	return
}

func (t *tOps) newIterator(f *tFile, slice *util.Range) iterator.Iterator {
	ch, err := t.open(f)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	iter := ch.Value().(*table.Reader).NewIterator(slice)
	iter.SetReleaser(ch)
	return iter
}

func (t *tOps) remove(f *tFile) {
	t.fcache.Delete(0, 0, uint64(f.fd.Num), func() {
		if err := t.s.fs.Remove(f.fd); err != nil {
			t.s.logger.Error("table file remove error: num=%d, err=%v", f.fd.Num, err)
		} else {
			t.s.logger.Info("table file removed: num=%d", f.fd.Num)
		}
	})
}

// Creates new tFile.
func newTableFile(fd filesystem.FileDesc, size int64, imin, imax internalKey, kvsize uint64) *tFile {
	f := &tFile{
		fd:     fd,
		size:   size,
		imin:   imin,
		imax:   imax,
		kvsize: kvsize,
	}

	return f
}

// tWriter wraps the table writer. It keep track of file descriptor
// and added key range.
type tWriter struct {
	t *tOps

	fd filesystem.FileDesc
	w  filesystem.Writer
	tw *table.Writer

	kvsize      uint64
	first, last []byte
}

// Append key/value pair to the table.
func (w *tWriter) append(key, value []byte) error {
	w.kvsize = w.kvsize + uint64(len(key)) + uint64(len(value))

	if w.first == nil {
		w.first = append([]byte{}, key...)
	}
	w.last = append(w.last[:0], key...)
	return w.tw.Append(key, value)
}

// Returns true if the table is empty.
func (w *tWriter) empty() bool {
	return w.first == nil
}

// Closes the filesystem.Writer.
func (w *tWriter) close() {
	if w.w != nil {
		w.w.Close()
		w.w = nil
	}
}

// Finalizes the table and returns table file.
func (w *tWriter) finish() (f *tFile, err error) {
	defer w.close()
	err = w.tw.Close()
	if err != nil {
		return
	}
	err = w.w.Sync()
	if err != nil {
		return
	}
	f = newTableFile(w.fd, int64(w.tw.BytesLen()), internalKey(w.first), internalKey(w.last), w.kvsize)
	return
}

// Drops the table.
func (w *tWriter) drop() {
	w.close()
	w.t.s.fs.Remove(w.fd)
	w.t.s.reuseFileNum(w.fd.Num)
	w.tw = nil
	w.first = nil
	w.last = nil
}
