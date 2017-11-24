// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package scheduler

import (
	"runtime"
	"time"

	"github.com/juju/ratelimit"
	"golang.org/x/net/context"
	"engine/model"
	"util/log"
)

type taskRequest struct {
	respCh chan model.CompactionTask
}

type worker struct {
	wid    int
	s      *compactScheduler
	ctx    context.Context
	bucket *ratelimit.Bucket
}

func newWorker(s *compactScheduler, id int) *worker {
	w := &worker{
		wid: id,
		s:   s,
		ctx: s.ctx,
		// 每10ms，填充百分之一的token
		bucket: ratelimit.NewBucketWithQuantum(time.Millisecond*10, s.c.GetWriteRateLimit()/100, s.c.GetWriteRateLimit()),
	}

	go w.runLoop()

	return w
}

func (w *worker) shouldStop() bool {
	select {
	case <-w.ctx.Done():
		log.Info("[scheduler:%d] worker exiting...", w.wid)
		return true
	default:
	}
	return false
}

func (w *worker) takeTask() model.CompactionTask {
	req := &taskRequest{
		respCh: make(chan model.CompactionTask),
	}
	w.s.requestTask(req)
	resp := <-req.respCh
	return resp
}

func (w *worker) runLoop() {
	log.Info("[scheduler:%d] start work loop.", w.wid)

	defer func() {
		if x := recover(); x != nil {
			log.Error("[scheduler:%d] panic: %v", x)
		}
	}()

	for {
		if w.shouldStop() {
			return
		}

		task := w.takeTask()
		if task == nil {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		// do comapact
		w.doCompact(task)
	}
}

func (w *worker) doCompact(task model.CompactionTask) {
	defer func() {
		w.s.runnings.remove(task.GetDBId())
	}()

	log.Info("[scheduler:%d] start compact db(%d).", w.wid, task.GetDBId())

	var totalWriten int64
	var stat model.CompactionStat
	start := time.Now()
	var count uint64
	for {
		if w.shouldStop() {
			return
		}
		finish, err := task.Next(&stat)
		if err != nil {
			log.Error("[scheduler:%d] compact db(%d) error(%v)", w.wid, task.GetDBId(), err)
			return
		}
		if finish {
			log.Info("[scheduler:%d] compact finish for db(%d), total writen: %d, spend: %v", w.wid, task.GetDBId(), totalWriten, time.Since(start))
			return
		}
		if stat.BytesWritten > 0 {
			totalWriten += stat.BytesWritten
			w.bucket.Wait(stat.BytesWritten)
		}
		count++
		if count%15 == 0 {
			runtime.Gosched()
		}
	}
}
