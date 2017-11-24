// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"encoding/binary"
	"os"
	"sort"
	"sync/atomic"

	"engine/errors"
	"engine/model"
	"engine/rowstore/filesystem"
	"engine/scheduler"
)

const (
	maxSplitCompactBackoff uint64 = 100
)

// FindMiddleKey 返回db中间key，近似
func (db *DB) FindMiddleKey() ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, errors.ErrClosed
	}

	// return db.findMiddleKeyByIterate()
	return db.findMiddleKeyByWidth()
}

func (db *DB) findMiddleKeyByIterate() ([]byte, error) {
	totalSize := db.Size()
	var n uint64
	var midKey []byte

	iter := db.newRawIterator(nil, nil, nil)
	defer iter.Release()
	for iter.Next() {
		n += uint64(len(iter.Key()))
		n += uint64(len(iter.Value()))
		if n >= totalSize/2 {
			ukey, _, _, err := parseInternalKey(iter.Key())
			midKey = append(midKey, ukey...) // copy
			if err != nil {
				return nil, err
			}
			break
		}
	}

	return midKey, iter.Error()
}

// kvset is a memtable or table file
type kvSet struct {
	StartKey []byte // 起始 user key
	EndKey   []byte // 终止 user key
	Size     int    // 大小
}

type endpointType int

const (
	epTypeStart = iota
	epTypeEnd
)

// endpoint is begin or end of kvset
type endpoint struct {
	key []byte
	t   endpointType
	s   *kvSet
}

type endponitSorter struct {
	icmp      *iComparer
	endpoints []endpoint
}

func (es endponitSorter) Len() int {
	return len(es.endpoints)
}

func (es endponitSorter) Less(i, j int) bool {
	c := es.icmp.uCompare(es.endpoints[i].key, es.endpoints[j].key)
	if c == 0 { // 相等，起始端点在前
		return es.endpoints[i].t == epTypeStart
	}
	return c < 0
}

func (es endponitSorter) Swap(i, j int) {
	es.endpoints[i], es.endpoints[j] = es.endpoints[j], es.endpoints[i]
}

// 计算区间[prev, next]的长度
// 计算方法：对每个包含该区间的kvset，计算区间占kvset区间的比例，再乘以kvset的大小作为权重系数， 最后再球和
func calWidth(icmp *iComparer, prev, next []byte, active map[*kvSet]struct{}) (width float64) {
	for s := range active {
		min := s.StartKey
		max := s.EndKey
		checkRangeInside(icmp, prev, next, min, max)

		commonLen := commonPrefixLen(min, max)
		prevInt := keyTailToInt(prev, commonLen)
		nextInt := keyTailToInt(next, commonLen)
		minInt := keyTailToInt(min, commonLen)
		maxInt := keyTailToInt(max, commonLen)

		if minInt != maxInt {
			width += (float64(nextInt-prevInt) / float64(maxInt-minInt)) * float64(s.Size)
		}
	}
	return
}

func checkRangeInside(icmp *iComparer, imin, imax, omin, omax []byte) {
	if icmp.uCompare(imin, omin) < 0 || icmp.uCompare(imax, omax) > 0 {
		panic("DB::checkRangeInside: check failed")
	}
}

func keyTailToInt(key []byte, start int) uint64 {
	b := make([]byte, 8)
	for i := start; i < len(key) && i-start < 8; i++ {
		b[i-start] = key[i]
	}
	return binary.BigEndian.Uint64(b)
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if n > len(b) {
		n = len(b)
	}
	commonLen := 0
	for i := 0; i < n; i++ {
		if a[i] == b[i] {
			commonLen++
		} else {
			break
		}
	}
	return commonLen
}

func (db *DB) findMiddleKeyByWidth() ([]byte, error) {
	sets := db.getKvSets()
	if len(sets) == 0 {
		startKey := db.startUKey
		endKey := db.endUKey
		if len(startKey) == 0 {
			startKey = []byte{'\x00'}
		}
		if len(endKey) == 0 {
			endKey = []byte{'\xff'}
		}
		return calHalfWidthKey(startKey, endKey, 0, 1, 0.5), nil
	}

	// 对所有端点排序，相邻的端点组成区间
	endpoints := make([]endpoint, 0, len(sets)*2)
	for i := 0; i < len(sets); i++ {
		endpoints = append(endpoints, endpoint{key: sets[i].StartKey, s: &sets[i], t: epTypeStart})
		endpoints = append(endpoints, endpoint{key: sets[i].EndKey, s: &sets[i], t: epTypeEnd})
	}
	sort.Sort(endponitSorter{icmp: db.s.icmp, endpoints: endpoints})

	active := make(map[*kvSet]struct{})          // 跟当前区间有交叉的kvset
	widths := make([]float64, 0, len(endpoints)) // 每个端点结束时的width
	var prev endpoint
	var totalWidth float64
	for _, ep := range endpoints {
		totalWidth += calWidth(db.s.icmp, prev.key, ep.key, active)
		widths = append(widths, totalWidth)
		prev = ep
		switch ep.t {
		case epTypeStart:
			active[ep.s] = struct{}{}
		case epTypeEnd:
			delete(active, ep.s)
		default:
			panic("DB::findMiddleKeyByWidth(): unknown endpoint type")
		}
	}

	// 找到中间宽度所在的区间
	half := totalWidth / 2
	for i, w := range widths {
		if w >= half {
			if i == 0 {
				panic("DB::findMiddleKeyByWidth(): first width is half")
			}
			//TODO: 检查key先后顺序， width上一个更小
			return calHalfWidthKey(endpoints[i-1].key, endpoints[i].key, widths[i-1], w, half), nil
		}
	}

	return nil, errors.New("find falied")
}

// 按比例在startkey的基础上增长
func calHalfWidthKey(startKey, endKey []byte, startWidth, endWidth, halfWidth float64) []byte {
	commonLen := commonPrefixLen(startKey, endKey)
	istart := keyTailToInt(startKey, commonLen)
	iend := keyTailToInt(endKey, commonLen)
	ihalf := float64(istart) + (halfWidth-startWidth)*float64(iend-istart)/(endWidth-startWidth)
	key := make([]byte, commonLen+8)
	for i := 0; i < commonLen; i++ {
		key[i] = startKey[i]
	}
	binary.BigEndian.PutUint64(key[commonLen:], uint64(ihalf))
	return key
}

// 获取当前的memtable和table files
func (db *DB) getKvSets() []kvSet {
	var sets []kvSet
	// 添加memtable
	mdbs := db.getMems()
	for _, m := range mdbs {
		if m.Len() == 0 {
			m.decref()
			continue
		}
		s := kvSet{
			StartKey: internalKey(m.Min()).ukey(),
			EndKey:   internalKey(m.Max()).ukey(),
			Size:     m.Size(),
		}
		if s.Size > 0 && s.StartKey != nil && s.EndKey != nil {
			sets = append(sets, s)
		}
		m.decref()
	}
	// 添加 table files
	v := db.s.version()
	for _, t := range v.tfiles {
		sets = append(sets, kvSet{
			StartKey: t.imin.ukey(),
			EndKey:   t.imax.ukey(),
			Size:     int(t.kvsize),
		})
	}
	v.release()
	return sets
}

func (db *DB) flushAllMemdbUnlocked() error {
	// 冻结当前memtable
	if db.mem != nil && db.mem.Len() > 0 {
		mdb, err := db.rotateMem(0, true)
		if err != nil {
			return err
		}
		mdb.decref()
	}

	for db.frozenMemLen() > 0 {
		if err := db.triggerMemFlushWait(); err != nil {
			return err
		}
	}

	// 检查memtable为空
	n := 0
	memdbs := db.getMems()
	for _, m := range memdbs {
		n += m.Len()
		m.decref()
	}
	if n > 0 {
		return errors.New("db: flush memtable failed. check empty not pass")
	}
	return nil
}

func (db *DB) checkSplitable() error {
	if db.startUKey == nil || db.endUKey == nil {
		return errors.New("split operation forbidden: start key or endkey isn't specified")
	}

	v := db.s.version()
	defer v.release()

	// 有共享的table文件禁止分裂 因为统计的大小不准确（有不属于db [startKey, endKey]的kv）
	for _, t := range v.tfiles {
		if t.shared {
			return errors.New("split operation forbidden: has shard table file")
		}
	}

	return nil
}

// Split 分裂
func (db *DB) Split(splitKey []byte, leftDir, rightDir string, leftID, rightID uint64) (left, right model.Store, err error) {
	if err := db.checkSplitable(); err != nil {
		return nil, nil, err
	}

	// 获取写锁
	select {
	case db.writeLockC <- struct{}{}:
	case <-db.closeC:
		return nil, nil, errors.ErrClosed
	}
	// 避免重复分裂
	select {
	case <-db.splitC:
		return nil, nil, errors.New("duplicate split operation")
	default:
	}

	// 停写
	db.splitOnce.Do(func() {
		close(db.splitC)
	})

	// stop scheduler
	scheduler.UnRegisterDB(db.id)

	// 释放写锁
	defer func() {
		<-db.writeLockC
	}()

	// flush memdbs
	if err := db.flushAllMemdbUnlocked(); err != nil {
		return nil, nil, err
	}

	v := db.s.version()
	defer v.release()

	// 左边
	leftdb, err := db.swarmSplitDB(leftDir, leftID, db.startUKey, splitKey, v)
	if err != nil {
		return nil, nil, err
	}

	// 右边
	rightdb, err := db.swarmSplitDB(rightDir, rightID, splitKey, db.endUKey, v)
	if err != nil {
		return nil, nil, err
	}

	return leftdb, rightdb, nil
}

// 把当老db的表文件硬链接到目录dstDir下
// startUkey, endUkey是分裂后的新db的key范围
func (db *DB) linkOutTableFiles(tfiles []*tFile, startUkey, endUkey []byte, dstDir string) (results []*tFile, nextFileNum int64, err error) {
	nextFileNum = 1
	icmp := db.s.icmp
	nshared := 0
	for _, t := range tfiles {
		if icmp.uCompare(t.imax.ukey(), startUkey) < 0 || icmp.uCompare(t.imin.ukey(), endUkey) >= 0 { // 不属于[startUkey, endUkey)的范围内
			continue
		}
		fullyContain := icmp.uCompare(t.imin, startUkey) >= 0 && icmp.uCompare(t.imax.ukey(), endUkey) < 0 // 完全属于

		newfd := filesystem.FileDesc{Type: filesystem.TypeTable, Num: nextFileNum}
		nextFileNum++
		if err = db.s.fs.MakeLink(t.fd, newfd, dstDir, true); err != nil {
			return
		}
		db.logger.Info("link table file: [%s]->[%s/%s]", t.fd.String(), dstDir, newfd.String())

		newt := t.clone()
		newt.fd = newfd
		if !fullyContain { // 文件不完全属于，与分裂后的另一个实例共享
			newt.shared = true
			nshared++
		}

		results = append(results, newt)
	}
	db.logger.Info("split shared table files: %d", nshared)
	return
}

func (db *DB) splitSession(s *session, tfiles []*tFile, nextFileNum int64) error {
	// create new session
	if err := s.createSplit(nextFileNum); err != nil {
		return err
	}
	if len(tfiles) > 0 {
		edit := &versionEdit{
			addedTables: tfiles,
			raftIndex:   1, // 1代表分裂操作
			timestamp:   db.Timestamp(),
		}
		s.logger.Info("commit split table files: %d, ts: %d", len(tfiles), edit.timestamp)
		// 提交表文件到session
		return s.commit(edit)
	}
	return nil
}

func (db *DB) swarmSplitDB(dir string, dbID uint64, startUkey, endUkey []byte, ver *version) (newdb *DB, err error) {
	// 初始化目录
	fs, err := filesystem.OpenFile(dir, false)
	if err != nil {
		return nil, err
	}

	s, err := db.s.swarmSplit(dbID, fs)
	if err != nil {
		fs.Close()
		return nil, err
	}
	// 尝试恢复
	applied, err := s.recover()
	if err == nil { // 恢复了，
		db.logger.Warn("ignore split request. maybe already split(child: %d), recovered applied: %d", dbID, applied)
	} else {
		if !os.IsNotExist(err) {
			db.logger.Warn("recover child db(%d) error: %v", dbID, err)
		}
		// 链接表文件
		tfiles, nextFileNum, err := db.linkOutTableFiles(ver.tfiles, startUkey, endUkey, dir)
		if err != nil {
			fs.Close()
			return nil, err
		}
		err = db.splitSession(s, tfiles, nextFileNum)
		if err != nil {
			fs.Close()
			return nil, err
		}
	}

	newdb, err = openDB(s, startUkey, endUkey, dbID)
	if err != nil {
		fs.Close()
		return nil, err
	}
	// set closer
	newdb.closer = fs

	// register to scheduler
	scheduler.RegisterDB(dbID, newdb)

	return newdb, nil
}

// SharedFilesNum get shared files num
func (db *DB) SharedFilesNum() (count int) {
	if err := db.ok(); err != nil {
		return 0
	}

	v := db.s.version()
	count = v.getSharedCount()
	v.release()

	return
}

// PrepareForSplit 通知store准备分裂
func (db *DB) PrepareForSplit() {
	atomic.AddUint64(&db.splitCompactBackoff, 1)
}
