// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"time"

	"engine/rowstore/comparer"
	"util"
)

var qTestIComparer = &iComparer{ucmp: comparer.DefaultComparer}

func TestBasic(t *testing.T) {
}

func queryNaive(icmp *iComparer, tfiles []*tFile, ukey []byte) (result []*tFile) {
	for _, f := range tfiles {
		if icmp.uCompare(f.imin.ukey(), ukey) > 0 || icmp.uCompare(f.imax.ukey(), ukey) < 0 {
			continue
		}
		result = append(result, f)
	}
	return
}

func queryRNaive(icmp *iComparer, tfiles []*tFile, startUkey, endUKey []byte) (result []*tFile) {
	for _, f := range tfiles {
		if icmp.uCompare(f.imax.ukey(), startUkey) < 0 || icmp.uCompare(f.imin.ukey(), endUKey) > 0 {
			continue
		}
		result = append(result, f)
	}
	return
}

func genTableFiles(n int) []*tFile {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	key1 := make([]byte, 10)
	util.RandomBytes(r, key1)
	key2 := make([]byte, 10)
	util.RandomBytes(r, key2)
	return nil
}

type resultSorter []*tFile

func (rs resultSorter) Len() int {
	return len(rs)
}

func (rs resultSorter) Less(i, j int) bool {
	c := qTestIComparer.uCompare(rs[i].imin.ukey(), rs[j].imin.ukey())
	if c != 0 {
		return c < 0
	}
	return qTestIComparer.uCompare(rs[i].imax.ukey(), rs[j].imax.ukey()) < 0
}

func (rs resultSorter) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func compareResults(a []*tFile, b []*tFile) error {
	sort.Sort(resultSorter(a))
	sort.Sort(resultSorter(b))
	if len(a) != len(b) {
		return fmt.Errorf("length mismatch: %d != %d\n res1=%v\n res2=%v", len(a), len(b), a, b)
	}
	for i := 0; i < len(a); i++ {
		if qTestIComparer.Compare(a[i].imin, b[i].imin) != 0 {
			return fmt.Errorf("left mismatch: %v != %v, at index %d\n res1=%v\n res2=%v", a[i].imin, b[i].imin, i, a, b)
		}
		if qTestIComparer.Compare(a[i].imax, b[i].imax) != 0 {
			return fmt.Errorf("right mismatch: %v != %v, at index %d\n res1=%v\n res2=%v", a[i].imax, b[i].imax, i, a, b)
		}
	}
	return nil
}
