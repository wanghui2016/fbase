package server

import (
	"time"
	"sync/atomic"
	"model/pkg/statspb"
	"sync"
)

type Stats struct {
	totalUsec int64
	parseUsec int64
	callUsec int64
	slowlog *statspb.SqlSlowlog
	tp *statspb.SqlTp
}

var slowlogLock sync.RWMutex

func NewStats(conf *Config) *Stats {
	return &Stats{
		slowlog: &statspb.SqlSlowlog{
			Len: uint32(conf.SlowlogMaxLen),
			Than: int32(conf.SlowlogSlowerThanUsec),
			Log: make([]*statspb.SqlSlow, conf.SlowlogMaxLen),
		},
		tp: &statspb.SqlTp{
			Tp: make([]int64, statspb.TpArgs_min_index),
		},
	}
}

func (p *Proxy) sqlStats(sql string, TotalUsec, CallUsec time.Duration) {
	atomic.AddInt64(&p.stats.totalUsec, int64(TotalUsec))
	atomic.AddInt64(&p.stats.callUsec, int64(CallUsec))

	// slowlog
	if TotalUsec > time.Duration(p.config.SlowlogSlowerThanUsec) {
		//slowlogLock.Lock()
		index := atomic.AddUint32(&p.stats.slowlog.Idx, 1)
		p.stats.slowlog.Log[index % p.stats.slowlog.Len] = &statspb.SqlSlow{
			TimeSec: int64(time.Now().Unix()),
			Sql: sql,
			TotalUsec: int64(TotalUsec),
			CallUsec: int64(CallUsec),
		}
		//p.stats.slowlog.Idx++
		//p.stats.slowlog.Idx %= p.stats.slowlog.Len
		//slowlogLock.Unlock()
	}

	// trip
	atomic.AddInt64(&p.stats.tp.Calls, 1)
	TotalMsec := TotalUsec/1000
	Sec := TotalMsec/1000
	Msec := TotalMsec%1000
	Min := Sec/60
	if Min > 0 {
		atomic.AddInt64(&p.stats.tp.Tp[statspb.TpArgs_min_index-1], 1)
	} else if Sec > 0 {
		atomic.AddInt64(&p.stats.tp.Tp[int64(statspb.TpArgs_msec_index)+ int64(Sec) - 1], 1)
	} else {
		atomic.AddInt64(&p.stats.tp.Tp[int64(Msec)], 1)
	}
	if int64(TotalUsec) > p.stats.tp.DelayMax {
		p.stats.tp.DelayMax = int64(TotalUsec)
	}
}
