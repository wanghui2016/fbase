// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"sync/atomic"

	"engine/model"
)

// GetStats get db stats
func (db *DB) GetStats() (*model.Stats, error) {
	err := db.ok()
	if err != nil {
		return nil, err
	}

	stat := &model.Stats{}
	stat.Flushes = atomic.LoadInt64(&db.stats.Flushes)
	stat.Compactions = atomic.LoadInt64(&db.stats.Compactions)
	stat.GetCount = atomic.LoadInt64(&db.stats.GetCount)
	stat.PutCount = atomic.LoadInt64(&db.stats.PutCount)
	stat.AliveSnapshots = atomic.LoadInt64(&db.stats.AliveSnapshots)
	stat.AliveIterators = atomic.LoadInt64(&db.stats.AliveIterators)
	stat.BlockCacheHits = atomic.LoadInt64(&db.stats.BlockCacheHits)
	stat.BlockCacheMisses = atomic.LoadInt64(&db.stats.BlockCacheMisses)
	// table file
	v := db.s.version()
	defer v.release()
	stat.TableFiles = int64(len(v.tfiles))

	return stat, nil
}

type dbStats model.Stats

func (s *dbStats) addFlush() {
	atomic.AddInt64(&s.Flushes, 1)
}

func (s *dbStats) addCompaction() {
	atomic.AddInt64(&s.Compactions, 1)
}

func (s *dbStats) addGetCount() {
	atomic.AddInt64(&s.GetCount, 1)
}

func (s *dbStats) addPutCount() {
	atomic.AddInt64(&s.PutCount, 1)
}

func (s *dbStats) addAliveSnapshot() {
	atomic.AddInt64(&s.AliveSnapshots, 1)
}

func (s *dbStats) subAliveSnapshot() {
	atomic.AddInt64(&s.AliveSnapshots, -1)
}

func (s *dbStats) addAliveIterator() {
	atomic.AddInt64(&s.AliveIterators, 1)
}

func (s *dbStats) subAliveIterator() {
	atomic.AddInt64(&s.AliveIterators, -1)
}

func (s *dbStats) addBlockCacheHit() {
	atomic.AddInt64(&s.BlockCacheHits, 1)
}

func (s *dbStats) addBlockCacheMiss() {
	atomic.AddInt64(&s.BlockCacheMisses, 1)
}
