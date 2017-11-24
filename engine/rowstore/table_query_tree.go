// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import "sort"

// 静态区间树，加速查找跟user_key重叠的table文件
type tableQueryTree struct {
	icmp  *iComparer
	root  *tNode
	debug bool
}

func buildQueryTree(icmp *iComparer, tfiles []*tFile) *tableQueryTree {
	t := &tableQueryTree{
		icmp: icmp,
	}
	if len(tfiles) > 0 {
		t.root = t.build(tfiles)
	}
	return t
}

func buildDebugQueryTree(icmp *iComparer, tfiles []*tFile) *tableQueryTree {
	t := buildQueryTree(icmp, tfiles)
	t.debug = true
	return t
}

func (t *tableQueryTree) build(tfiles []*tFile) *tNode {
	mid, left, right, overlap := t.partition(tfiles)
	var lc, rc *tNode
	if len(left) > 0 {
		lc = t.build(left)
	}
	if len(right) > 0 {
		rc = t.build(right)
	}
	return newQTNode(t, mid, lc, rc, overlap)
}

type endpointUkeySorter struct {
	icmp  *iComparer
	ukeys [][]byte
}

func (es endpointUkeySorter) Len() int      { return len(es.ukeys) }
func (es endpointUkeySorter) Swap(i, j int) { es.ukeys[i], es.ukeys[j] = es.ukeys[j], es.ukeys[i] }
func (es endpointUkeySorter) Less(i, j int) bool {
	return es.icmp.uCompare(es.ukeys[i], es.ukeys[j]) < 0
}

func (t *tableQueryTree) partition(tfiles []*tFile) (mid []byte, left, right, overlap []*tFile) {
	endpoints := make([][]byte, 0, len(tfiles)*2)
	for _, f := range tfiles {
		endpoints = append(endpoints, f.imin.ukey())
		endpoints = append(endpoints, f.imax.ukey())
	}
	sort.Sort(endpointUkeySorter{icmp: t.icmp, ukeys: endpoints})

	if t.debug {
		for i := 1; i < len(endpoints); i++ {
			lh := endpoints[i-1]
			rh := endpoints[i]
			if t.icmp.uCompare(lh, rh) > 0 {
				panic("tableQueryTree::partition(): endpoints out of order")
			}
		}
	}

	mid = endpoints[len(endpoints)/2]

	for _, f := range tfiles {
		if t.icmp.uCompare(f.imin.ukey(), mid) > 0 { // 完全在右边
			right = append(right, f)
		} else if t.icmp.uCompare(f.imax.ukey(), mid) < 0 { // 完全在左边
			left = append(left, f)
		} else {
			overlap = append(overlap, f)
		}
	}

	return
}

func (t *tableQueryTree) query(ukey []byte) []*tFile {
	if t == nil || t.root == nil {
		return nil
	}
	return t.root.query(ukey)
}

func (t *tableQueryTree) queryR(beginUkey []byte, endUkey []byte) []*tFile {
	if t == nil || t.root == nil {
		return nil
	}
	return t.root.queryR(beginUkey, endUkey)
}

type tNode struct {
	icmp *iComparer
	mid  []byte // 区间端点中值
	lc   *tNode // 完全在mid左边的
	rc   *tNode // 完全在mid右边的

	overlapAsecLeft  []*tFile // 跟mid有重叠的，按左边界(min_ukey)升序排序
	overlapDescRight []*tFile // 跟mid有重叠的，按右边界(max_ukey)降序排列
}

func newQTNode(t *tableQueryTree, mid []byte, left, right *tNode, overlap []*tFile) *tNode {
	n := &tNode{
		icmp: t.icmp,
		mid:  mid,
		lc:   left,
		rc:   right,
	}

	if len(overlap) > 0 {
		n.overlapAsecLeft = append(n.overlapAsecLeft, overlap...)
		sort.Sort(leftAsecSorter{icmp: t.icmp, tfiles: n.overlapAsecLeft})

		n.overlapDescRight = append(n.overlapDescRight, overlap...)
		sort.Sort(rigthDescSorter{icmp: t.icmp, tfiles: n.overlapDescRight})

		if t.debug {
			if len(n.overlapAsecLeft) != len(n.overlapDescRight) {
				panic("newQTNode: length of overlapAsecLeft and overlapDescRight should be the same")
			}
			for i := 1; i < len(n.overlapAsecLeft); i++ {
				lh := n.overlapAsecLeft[i-1].imin.ukey()
				rh := n.overlapAsecLeft[i].imin.ukey()
				if n.icmp.uCompare(lh, rh) > 0 {
					panic("newQTNode: overlapAsecLeft out of order")
				}
			}
			for i := 1; i < len(n.overlapDescRight); i++ {
				lh := n.overlapDescRight[i-1].imax.ukey()
				rh := n.overlapDescRight[i].imax.ukey()
				if n.icmp.uCompare(lh, rh) < 0 {
					panic("newQTNode: overlapDescRight out of order")
				}
			}
		}
	}

	return n
}

func (tn *tNode) queryLeftOverlaps(ukey []byte) (result []*tFile) {
	for _, t := range tn.overlapAsecLeft {
		if tn.icmp.uCompare(ukey, t.imin.ukey()) < 0 {
			break
		} else {
			result = append(result, t)
		}
	}
	return
}

func (tn *tNode) queryRightOverlaps(ukey []byte) (result []*tFile) {
	for _, t := range tn.overlapDescRight {
		if tn.icmp.uCompare(ukey, t.imax.ukey()) > 0 {
			break
		} else {
			result = append(result, t)
		}
	}
	return
}

func (tn *tNode) query(ukey []byte) (result []*tFile) {
	c := tn.icmp.uCompare(ukey, tn.mid)
	if c < 0 { // left
		if tn.lc != nil {
			result = append(result, tn.lc.query(ukey)...)
		}
		result = append(result, tn.queryLeftOverlaps(ukey)...)
	} else if c > 0 { // right
		if tn.rc != nil {
			result = append(result, tn.rc.query(ukey)...)
		}
		result = append(result, tn.queryRightOverlaps(ukey)...)
	} else {
		result = append(result, tn.overlapAsecLeft...)
	}
	return
}

func (tn *tNode) queryR(startUKey, endUkey []byte) (results []*tFile) {
	if tn.icmp.uCompare(endUkey, tn.mid) < 0 { // 区间完全在中点右边
		if tn.lc != nil {
			results = append(results, tn.lc.queryR(startUKey, endUkey)...)
		}
		results = append(results, tn.queryLeftOverlaps(endUkey)...)
	} else if tn.icmp.uCompare(startUKey, tn.mid) > 0 { // 区间完全在中点左边
		if tn.rc != nil {
			results = append(results, tn.rc.queryR(startUKey, endUkey)...)
		}
		results = append(results, tn.queryRightOverlaps(startUKey)...)
	} else { // 区间跟中点有重叠
		results = append(results, tn.overlapAsecLeft...)
		if tn.lc != nil {
			results = append(results, tn.lc.queryR(startUKey, endUkey)...)
		}
		if tn.rc != nil {
			results = append(results, tn.rc.queryR(startUKey, endUkey)...)
		}
	}

	return
}

type leftAsecSorter struct {
	icmp   *iComparer
	tfiles []*tFile
}

func (ls leftAsecSorter) Len() int      { return len(ls.tfiles) }
func (ls leftAsecSorter) Swap(i, j int) { ls.tfiles[i], ls.tfiles[j] = ls.tfiles[j], ls.tfiles[i] }
func (ls leftAsecSorter) Less(i, j int) bool {
	return ls.icmp.uCompare(ls.tfiles[i].imin.ukey(), ls.tfiles[j].imin.ukey()) < 0
}

type rigthDescSorter struct {
	icmp   *iComparer
	tfiles []*tFile
}

func (rs rigthDescSorter) Len() int      { return len(rs.tfiles) }
func (rs rigthDescSorter) Swap(i, j int) { rs.tfiles[i], rs.tfiles[j] = rs.tfiles[j], rs.tfiles[i] }
func (rs rigthDescSorter) Less(i, j int) bool {
	return rs.icmp.uCompare(rs.tfiles[i].imax.ukey(), rs.tfiles[j].imax.ukey()) > 0
}
