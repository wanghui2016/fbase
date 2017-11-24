// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package scheduler

import "sync"

type runningQueue struct {
	sync.Mutex
	m map[uint64]struct{}
}

func newRunningQueue() *runningQueue {
	return &runningQueue{
		m: make(map[uint64]struct{}),
	}
}

func (q *runningQueue) exist(id uint64) bool {
	q.Lock()
	_, ok := q.m[id]
	q.Unlock()
	return ok
}

func (q *runningQueue) add(id uint64) {
	q.Lock()
	q.m[id] = struct{}{}
	q.Unlock()
}

func (q *runningQueue) remove(id uint64) {
	q.Lock()
	delete(q.m, id)
	q.Unlock()
}
