package model

import (
	"errors"
)

var ErrCompactionBalanced = errors.New("db is balanaced")

// CompactionStat stats
type CompactionStat struct {
	BytesWritten int64
	BytesRead    int64
}

// CompactionTask  compaction task
type CompactionTask interface {
	// GetDBID get db id
	GetDBId() uint64

	// Next  执行compaction中的一小步，返回本次compaction是否完成、错误
	//       并通过stat返回本步中的一些统计信息
	Next(stat *CompactionStat) (finish bool, err error)
}
