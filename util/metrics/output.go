package metrics

import "time"

type Output interface {
	ReportInterval() time.Duration
	Report(data []byte) error
}
