package server

import (
	"fmt"
	"time"
)

type storeStatus struct {
	snapApplying    bool
	snapStart       time.Time
	snapPutCount    int64
	snapDelCount    int64
	snapAppliedSize int64
}

func (s *storeStatus) startSnapshot() {
	s.snapApplying = true
	s.snapPutCount = 0
	s.snapDelCount = 0
	s.snapAppliedSize = 0
	s.snapStart = time.Now()
}

func (s *storeStatus) endSnapshot() {
	s.snapApplying = false
}

func (s *storeStatus) addSnapPutCount() {
	s.snapPutCount++
}

func (s *storeStatus) addSnapDelCout() {
	s.snapDelCount++
}

func (s *storeStatus) addSnapApplySize(kvsize int64) {
	s.snapAppliedSize += kvsize
}

func (s *storeStatus) String() string {
	if !s.snapApplying {
		return fmt.Sprintf("{\"snaping\": false, \"last_start:\": \"%s\", \"last_put\": %d, \"last_del\": %d, \"last_size\": %d}",
			s.snapStart.String(), s.snapPutCount, s.snapDelCount, s.snapAppliedSize)
	}
	return fmt.Sprintf("{\"snaping\": true, \"start\":\"%s\", \"put\": %d, \"del\": %d, \"size\": %d}",
		s.snapStart.String(), s.snapPutCount, s.snapDelCount, s.snapAppliedSize)
}
