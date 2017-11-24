package server

import (
	"sync/atomic"
	"time"
)

type rangeMetric struct {
	id              uint64
	requestCounter  uint64
	bytesInCounter  uint64
	bytesOutCounter uint64
	last            time.Time
}

func newRangeMetric(id uint64) *rangeMetric {
	return &rangeMetric{
		id:   id,
		last: time.Now(),
	}
}

func (m *rangeMetric) addRequest() {
	atomic.AddUint64(&m.requestCounter, 1)
}

func (m *rangeMetric) addInBytes(in uint64) {
	atomic.AddUint64(&m.bytesInCounter, in)
}

func (m *rangeMetric) addOutBytes(out uint64) {
	atomic.AddUint64(&m.bytesOutCounter, out)
}

func (m *rangeMetric) collect() (ops, bytesInPerSec, bytesOutPerSec uint64) {
	now := time.Now()
	du := now.Sub(m.last) / time.Millisecond
	if du <= 0 {
		return 0, 0, 0
	}

	ops = uint64(float64(atomic.SwapUint64(&m.requestCounter, 0)) / float64(du) * 1000)
	bytesInPerSec = uint64(float64(atomic.SwapUint64(&m.bytesInCounter, 0)) / float64(du) * 1000)
	bytesOutPerSec = uint64(float64(atomic.SwapUint64(&m.bytesOutCounter, 0)) / float64(du) * 1000)
	m.last = now
	return
}
