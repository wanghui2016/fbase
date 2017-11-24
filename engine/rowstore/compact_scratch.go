// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

const sharedScore = 1000.0

// 每次db的version有更新时计算compact的分数、输入文件，缓存最后一次结果
// 以便可以快速返回分数和创建compact任务
type compactScratch struct {
	shared   int
	selected []*tFile
	score    float64
}

func newCompactScratch(pickPolicy compactPolicy, tfiles []*tFile) *compactScratch {
	s := new(compactScratch)

	if len(tfiles) == 0 {
		return s
	}

	// 计算共享文件个数
	for _, t := range tfiles {
		if t.shared {
			if len(s.selected) == 0 {
				s.selected = []*tFile{t}
				s.score = sharedScore
			}
			s.shared++
		}
	}
	if s.shared > 0 {
		if len(s.selected) == 0 {
			panic("DB::newCompactScratch(): selected should not be zero")
		}
		return s
	}

	// balanced
	if len(tfiles) == 1 {
		return s
	}

	selected, score := pickPolicy.pick(tfiles)
	s.selected = selected
	s.score = score
	return s
}

func (s *compactScratch) needCompact() bool {
	return len(s.selected) > 0 && s.score > 0
}
