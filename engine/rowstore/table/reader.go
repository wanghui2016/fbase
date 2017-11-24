// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/golang/snappy"

	"engine/errors"
	"engine/rowstore/cache"
	"engine/rowstore/comparer"
	"engine/rowstore/filesystem"
	"engine/rowstore/filter"
	"engine/rowstore/iterator"
	"engine/rowstore/opt"
	"util"
	"util/bufalloc"
)

type ErrCorrupted struct {
	Pos    int64
	Size   int64
	Kind   string
	Reason string
}

func (e *ErrCorrupted) Error() string {
	return fmt.Sprintf("fbase/engine/table: corruption on %s (pos=%d): %s", e.Kind, e.Pos, e.Reason)
}

// Reader is a table reader.
type Reader struct {
	mu     sync.RWMutex
	fd     filesystem.FileDesc
	reader io.ReaderAt
	err    error
	cache  *cache.NamespaceGetter
	// Options
	o              *opt.Options
	cmp            comparer.Comparer
	filter         filter.Filter
	verifyChecksum bool
	version        int

	dataEnd                   int64
	metaBH, indexBH, filterBH blockHandle
	indexBlock                *block
	filterBlock               *filterBlock
}

func (r *Reader) blockKind(bh blockHandle) string {
	switch bh.offset {
	case r.metaBH.offset:
		return "meta-block"
	case r.indexBH.offset:
		return "index-block"
	case r.filterBH.offset:
		if r.filterBH.length > 0 {
			return "filter-block"
		}
	}
	return "data-block"
}

func (r *Reader) newErrCorrupted(pos, size int64, kind, reason string) error {
	return &filesystem.ErrCorrupted{Fd: r.fd, Err: &ErrCorrupted{Pos: pos, Size: size, Kind: kind, Reason: reason}}
}

func (r *Reader) newErrCorruptedBH(bh blockHandle, reason string) error {
	return r.newErrCorrupted(int64(bh.offset), int64(bh.length), r.blockKind(bh), reason)
}

func (r *Reader) fixErrCorruptedBH(bh blockHandle, err error) error {
	if cerr, ok := err.(*ErrCorrupted); ok {
		cerr.Pos = int64(bh.offset)
		cerr.Size = int64(bh.length)
		cerr.Kind = r.blockKind(bh)
		return &filesystem.ErrCorrupted{Fd: r.fd, Err: cerr}
	}
	return err
}

func (r *Reader) readRawBlock(bh blockHandle) (bufalloc.Buffer, error) {
	size := int(bh.length + blockTrailerLen)
	buf := bufalloc.AllocBuffer(size)
	data := buf.Alloc(size)
	if _, err := r.reader.ReadAt(data, int64(bh.offset)); err != nil && err != io.EOF {
		return nil, err
	}

	// 验证block的checksum
	n := bh.length + 1
	if r.o.GetChecksumType() == 1 {
		checksum0 := binary.LittleEndian.Uint32(data[n:])
		checksum1 := util.NewCRC(data[:n]).Value()
		if checksum0 != checksum1 {
			bufalloc.FreeBuffer(buf)
			return nil, r.newErrCorruptedBH(bh, fmt.Sprintf("checksum mismatch, want=%#x got=%#x", checksum0, checksum1))
		}
	}
	switch data[bh.length] {
	case blockTypeNoCompression:
		buf.Truncate(int(bh.length))
	case blockTypeSnappyCompression:
		decLen, err := snappy.DecodedLen(data[:bh.length])
		if err != nil {
			return nil, r.newErrCorruptedBH(bh, err.Error())
		}
		decBuffer := bufalloc.AllocBuffer(decLen)
		_, err = snappy.Decode(decBuffer.Alloc(decLen), data[:bh.length])
		bufalloc.FreeBuffer(buf)
		if err != nil {
			bufalloc.FreeBuffer(decBuffer)
			return nil, r.newErrCorruptedBH(bh, err.Error())
		}
		buf = decBuffer
	default:
		bufalloc.FreeBuffer(buf)
		return nil, r.newErrCorruptedBH(bh, fmt.Sprintf("unknown compression type %#x", data[bh.length]))
	}
	return buf, nil
}

func (r *Reader) readBlock(bh blockHandle) (*block, error) {
	buf, err := r.readRawBlock(bh)
	if err != nil {
		return nil, err
	}
	data := buf.Bytes()
	restartsLen := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	b := &block{
		bh:             bh,
		buffer:         buf,
		data:           data,
		restartsLen:    restartsLen,
		restartsOffset: len(data) - (restartsLen+1)*4,
	}
	return b, nil
}

func (r *Reader) readBlockCached(bh blockHandle, fillCache bool) (*block, util.Releaser, error) {
	if r.cache != nil {
		var (
			err error
			ch  *cache.Handle
		)
		if fillCache {
			ch = r.cache.Get(bh.offset, func() (size int64, value cache.Value) {
				var b *block
				b, err = r.readBlock(bh)
				if err != nil {
					return 0, nil
				}
				return int64(cap(b.data)), b
			})
		} else {
			ch = r.cache.Get(bh.offset, nil)
		}
		if ch != nil {
			b, ok := ch.Value().(*block)
			if !ok {
				ch.Release()
				return nil, nil, errors.New("leveldb/table: inconsistent block type")
			}
			return b, ch, err
		} else if err != nil {
			return nil, nil, err
		}
	}

	b, err := r.readBlock(bh)
	return b, b, err
}

func (r *Reader) readFilterBlock(bh blockHandle) (*filterBlock, error) {
	buf, err := r.readRawBlock(bh)
	data := buf.Bytes()
	if err != nil {
		return nil, err
	}
	n := len(data)
	if n < 5 {
		return nil, r.newErrCorruptedBH(bh, "too short")
	}
	m := n - 5
	oOffset := int(binary.LittleEndian.Uint32(data[m:]))
	if oOffset > m {
		return nil, r.newErrCorruptedBH(bh, "invalid data-offsets offset")
	}
	b := &filterBlock{
		buffer:     buf,
		data:       data,
		oOffset:    oOffset,
		baseLg:     uint(data[n-1]),
		filtersNum: (m - oOffset) / 4,
	}
	return b, nil
}

func (r *Reader) getIndexBlock(fillCache bool) (b *block, rel util.Releaser, err error) {
	if r.indexBlock == nil {
		return r.readBlockCached(r.indexBH, true)
	}
	return r.indexBlock, util.NoopReleaser{}, nil
}

func (r *Reader) readFilterBlockCached(bh blockHandle, fillCache bool) (*filterBlock, util.Releaser, error) {
	if r.cache != nil {
		var (
			err error
			ch  *cache.Handle
		)
		if fillCache {
			ch = r.cache.Get(bh.offset, func() (size int64, value cache.Value) {
				var b *filterBlock
				b, err = r.readFilterBlock(bh)
				if err != nil {
					return 0, nil
				}
				return int64(cap(b.data)), b
			})
		} else {
			ch = r.cache.Get(bh.offset, nil)
		}
		if ch != nil {
			b, ok := ch.Value().(*filterBlock)
			if !ok {
				ch.Release()
				return nil, nil, errors.New("leveldb/table: inconsistent block type")
			}
			return b, ch, err
		} else if err != nil {
			return nil, nil, err
		}
	}

	b, err := r.readFilterBlock(bh)
	return b, b, err
}

func (r *Reader) getFilterBlock(fillCache bool) (*filterBlock, util.Releaser, error) {
	if r.filterBlock == nil {
		return r.readFilterBlockCached(r.filterBH, true)
	}
	return r.filterBlock, util.NoopReleaser{}, nil
}

func (r *Reader) newBlockIter(b *block, bReleaser util.Releaser, slice *util.Range, inclLimit bool) *blockIter {
	bi := &blockIter{
		tr:            r,
		block:         b,
		blockReleaser: bReleaser,
		// Valid key should never be nil.
		key:             make([]byte, 0),
		dir:             dirSOI,
		riStart:         0,
		riLimit:         b.restartsLen,
		offsetStart:     0,
		offsetRealStart: 0,
		offsetLimit:     b.restartsOffset,
	}
	if slice != nil {
		if slice.Start != nil {
			if bi.Seek(slice.Start) {
				bi.riStart = b.restartIndex(bi.restartIndex, b.restartsLen, bi.prevOffset)
				bi.offsetStart = b.restartOffset(bi.riStart)
				bi.offsetRealStart = bi.prevOffset
			} else {
				bi.riStart = b.restartsLen
				bi.offsetStart = b.restartsOffset
				bi.offsetRealStart = b.restartsOffset
			}
		}
		if slice.Limit != nil {
			if bi.Seek(slice.Limit) && (!inclLimit || bi.Next()) {
				bi.offsetLimit = bi.prevOffset
				bi.riLimit = bi.restartIndex + 1
			}
		}
		bi.reset()
		if bi.offsetStart > bi.offsetLimit {
			bi.sErr(errors.New("fbase/engine/table: invalid slice range"))
		}
	}
	return bi
}

func (r *Reader) getDataIter(dataBH blockHandle, slice *util.Range) iterator.Iterator {
	b, rel, err := r.readBlockCached(dataBH, true)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	return r.newBlockIter(b, rel, slice, false)
}

func (r *Reader) getDataIterErr(dataBH blockHandle, slice *util.Range) iterator.Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		return iterator.NewEmptyIterator(r.err)
	}

	return r.getDataIter(dataBH, slice)
}

func (r *Reader) NewIterator(slice *util.Range) iterator.Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		return iterator.NewEmptyIterator(r.err)
	}

	indexBlock, rel, err := r.getIndexBlock(false)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	index := &indexIter{
		blockIter: r.newBlockIter(indexBlock, rel, slice, true),
		tr:        r,
		slice:     slice,
		fillCache: false,
	}
	return iterator.NewIndexedIterator(index, true)
}

func (r *Reader) find(key []byte, filtered bool, noValue bool) (rkey, value []byte, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	indexBlock, rel, err := r.getIndexBlock(true)
	if err != nil {
		return
	}
	defer rel.Release()

	index := r.newBlockIter(indexBlock, nil, nil, true)
	defer index.Release()

	if !index.Seek(key) {
		if err = index.Error(); err == nil {
			err = errors.ErrNotFound
		}
		return
	}

	dataBH, n := decodeBlockHandle(index.Value())
	if n == 0 {
		r.err = r.newErrCorruptedBH(r.indexBH, "bad data block handle")
		return nil, nil, r.err
	}

	// The filter should only used for exact match.
	if filtered && r.filter != nil {
		filterBlock, frel, ferr := r.getFilterBlock(true)
		if ferr == nil {
			if !filterBlock.contains(r.filter, dataBH.offset, key) {
				frel.Release()
				return nil, nil, errors.ErrNotFound
			}
			frel.Release()
		} else if !filesystem.IsCorrupted(ferr) {
			return nil, nil, ferr
		}
	}

	data := r.getDataIter(dataBH, nil)
	if !data.Seek(key) {
		data.Release()
		if err = data.Error(); err != nil {
			return
		}

		// The nearest greater-than key is the first key of the next block.
		if !index.Next() {
			if err = index.Error(); err == nil {
				err = errors.ErrNotFound
			}
			return
		}

		dataBH, n = decodeBlockHandle(index.Value())
		if n == 0 {
			r.err = r.newErrCorruptedBH(r.indexBH, "bad data block handle")
			return nil, nil, r.err
		}

		data = r.getDataIter(dataBH, nil)
		if !data.Next() {
			data.Release()
			if err = data.Error(); err == nil {
				err = errors.ErrNotFound
			}
			return
		}
	}

	// Key doesn't use block buffer, no need to copy the buffer.
	rkey = data.Key()
	if !noValue {
		// Value does use block buffer, and since the buffer will be
		// recycled, it need to be copied.
		value = append([]byte{}, data.Value()...)
	}
	data.Release()
	return
}

// Find 查找大于等于指定key的KV Pair，表里不存在返回ErrNotFound
//      filtered 为true会先应用表内的过滤器
func (r *Reader) Find(key []byte, filtered bool) (rkey, value []byte, err error) {
	return r.find(key, filtered, false)
}

// FindKey 查找大于等于指定key的Key，表里不存在返回ErrNotFound
//      filtered 为true会先应用表内的过滤器
func (r *Reader) FindKey(key []byte, filtered bool) (rkey []byte, err error) {
	rkey, _, err = r.find(key, filtered, true)
	return
}

func (r *Reader) Get(key []byte) (value []byte, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	rkey, value, err := r.find(key, false, false)
	if err == nil && r.cmp.Compare(rkey, key) != 0 {
		value = nil
		err = errors.ErrNotFound
	}
	return
}

func (r *Reader) OffsetOf(key []byte) (offset int64, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	indexBlock, rel, err := r.readBlockCached(r.indexBH, true)
	if err != nil {
		return
	}
	defer rel.Release()

	index := r.newBlockIter(indexBlock, nil, nil, true)
	defer index.Release()
	if index.Seek(key) {
		dataBH, n := decodeBlockHandle(index.Value())
		if n == 0 {
			r.err = r.newErrCorruptedBH(r.indexBH, "bad data block handle")
			return
		}
		offset = int64(dataBH.offset)
		return
	}
	err = index.Error()
	if err == nil {
		offset = r.dataEnd
	}
	return
}

// Release implements util.Releaser.
// It also close the file if it is an io.Closer.
func (r *Reader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if closer, ok := r.reader.(io.Closer); ok {
		closer.Close()
	}
	if r.indexBlock != nil {
		r.indexBlock.Release()
		r.indexBlock = nil
	}
	if r.filterBlock != nil {
		r.filterBlock.Release()
		r.filterBlock = nil
	}
	r.reader = nil
	r.cache = nil
	r.err = errors.New("reader released")
}

// NewReader new table reader
func NewReader(f io.ReaderAt, size int64, fd filesystem.FileDesc, cache *cache.NamespaceGetter, o *opt.Options) (*Reader, error) {
	if f == nil {
		return nil, errors.New("fbase/engine/table: nil file")
	}

	r := &Reader{
		fd:             fd,
		reader:         f,
		cache:          cache,
		o:              o,
		cmp:            o.GetComparer(),
		verifyChecksum: true,
	}

	if size < footerLen {
		r.err = r.newErrCorrupted(0, size, "table", "too small")
		return r, nil
	}

	footerPos := size - footerLen
	var footer [footerLen]byte
	if _, err := r.reader.ReadAt(footer[:], footerPos); err != nil && err != io.EOF {
		return nil, err
	}
	r.version = int(footer[footerLen-len(magic)-1])
	if string(footer[footerLen-len(magic):footerLen]) != magic {
		r.err = r.newErrCorrupted(footerPos, footerLen, "table-footer", "bad magic number")
		return r, nil
	}

	var n int
	// Decode the metaindex block handle.
	r.metaBH, n = decodeBlockHandle(footer[:])
	if n == 0 {
		r.err = r.newErrCorrupted(footerPos, footerLen, "table-footer", "bad metaindex block handle")
		return r, nil
	}

	// Decode the index block handle.
	r.indexBH, n = decodeBlockHandle(footer[n:])
	if n == 0 {
		r.err = r.newErrCorrupted(footerPos, footerLen, "table-footer", "bad index block handle")
		return r, nil
	}

	// Read metaindex block.
	metaBlock, err := r.readBlock(r.metaBH)
	if err != nil {
		if filesystem.IsCorrupted(err) {
			r.err = err
			return r, nil
		}
		return nil, err
	}

	// Set data end.
	r.dataEnd = int64(r.metaBH.offset)

	// Read metaindex.
	metaIter := r.newBlockIter(metaBlock, nil, nil, true)
	for metaIter.Next() {
		key := string(metaIter.Key())
		if !strings.HasPrefix(key, "filter.") {
			continue
		}
		fn := key[7:]
		if f0 := o.GetFilter(); f0 != nil && f0.Name() == fn {
			r.filter = f0
		}
		if r.filter != nil {
			filterBH, n := decodeBlockHandle(metaIter.Value())
			if n == 0 {
				continue
			}
			r.filterBH = filterBH
			// Update data end.
			r.dataEnd = int64(filterBH.offset)
			break
		}
	}
	metaIter.Release()
	metaBlock.Release()

	// 没有block cache的情况下，缓存indexBlock和filterBlock
	if r.cache == nil {
		r.indexBlock, err = r.readBlock(r.indexBH)
		if err != nil {
			if filesystem.IsCorrupted(err) {
				r.err = err
				return r, nil
			}
			return nil, err
		}
		if r.filter != nil {
			r.filterBlock, err = r.readFilterBlock(r.filterBH)
			if err != nil {
				if !filesystem.IsCorrupted(err) {
					return nil, err
				}

				r.filter = nil
			}
		}
	}
	return r, nil
}
