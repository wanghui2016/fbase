// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package scheduler

import (
	"sync"

	"golang.org/x/net/context"
	"engine/model"
	"util/log"
)

var gCompactScheduler *compactScheduler

// StartScheduler 开始调度
func StartScheduler(c *Config) {
	if gCompactScheduler == nil {
		gCompactScheduler = newScheduler(c)
	}
}

// StopScheduler 停止调度
func StopScheduler() {
	if gCompactScheduler != nil {
		gCompactScheduler.ShutDouwn()
	}
}

// RegisterDB 加入新db到调度中
func RegisterDB(id uint64, db model.Store) {
	if gCompactScheduler != nil {
		gCompactScheduler.register(id, db)
	} else {
		log.Warn("register db(%d) failed. scheduler is not running", id)
	}
}

// UnRegisterDB 从调度中移除db
func UnRegisterDB(id uint64) {
	if gCompactScheduler != nil {
		gCompactScheduler.unRegister(id)
	}
}

type compactScheduler struct {
	c *Config

	dbs      map[uint64]model.Store // 所有注册的db
	mu       sync.RWMutex
	runnings *runningQueue
	requetCh chan *taskRequest // 向worker分发任务

	ctx      context.Context
	exitFunc context.CancelFunc
}

func newScheduler(c *Config) *compactScheduler {
	s := &compactScheduler{
		c:        c,
		dbs:      make(map[uint64]model.Store),
		runnings: newRunningQueue(),
		requetCh: make(chan *taskRequest, c.GetConcurrency()),
	}
	s.ctx, s.exitFunc = context.WithCancel(context.Background())

	log.Info("[scheduler] start, concurrency: %d, write ratelimit: %d", c.GetConcurrency(), c.GetWriteRateLimit())

	go s.run()

	for i := 0; i < c.GetConcurrency(); i++ {
		wid := i
		newWorker(s, wid)
	}
	return s
}

func (s *compactScheduler) requestTask(req *taskRequest) {
	select {
	case <-s.ctx.Done():
		return
	case s.requetCh <- req:
	}
}

func (s *compactScheduler) run() {
	for {
		// check should stop
		select {
		case <-s.ctx.Done():
			return
		case req := <-s.requetCh:
			task, err := s.newTask()
			if err != nil {
				log.Error("[scheduler] select store to compact failed(%v)", err)
				req.respCh <- nil
			} else {
				if task != nil {
					s.runnings.add(task.GetDBId())
				}
				req.respCh <- task
			}
		}
	}
}

func (s *compactScheduler) register(id uint64, db model.Store) {
	s.mu.Lock()
	s.dbs[id] = db
	s.mu.Unlock()
	log.Info("[scheduler] register db[%d]", id)
}

func (s *compactScheduler) unRegister(id uint64) {
	s.mu.Lock()
	delete(s.dbs, id)
	s.mu.Unlock()
	log.Info("[scheduler] unregister db[%d]", id)
}

func (s *compactScheduler) newTask() (model.CompactionTask, error) {
	var currentDBs map[uint64]model.Store
	// copy to currentDBs
	s.mu.RLock()
	if len(s.dbs) > 0 {
		currentDBs = make(map[uint64]model.Store)
		for id, db := range s.dbs {
			currentDBs[id] = db
		}
	}
	s.mu.RUnlock()

	if len(currentDBs) == 0 {
		return nil, nil
	}

	var maxScore float64
	var maxDB model.Store
	for id, db := range currentDBs {
		// 跳过正在做compaction的
		if s.runnings.exist(id) {
			continue
		}
		score, err := db.GetCompactionScore()
		if err != nil {
			return nil, err
		}
		if score > maxScore {
			maxScore = score
			maxDB = db
		}
	}

	if maxDB != nil {
		return maxDB.NewCompactionTask()
	}

	return nil, nil
}

func (s *compactScheduler) ShutDouwn() {
	select {
	case <-s.ctx.Done():
		return
	default:
		s.exitFunc()
	}
}
