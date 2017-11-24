package model

// Stats db 统计信息
type Stats struct {
	Flushes          int64 `json:"flushes"`     // flush了多少次
	Compactions      int64 `json:"compactions"` // compaction了多少次
	TableFiles       int64 `json:"table-files"` // 有多少表文件
	GetCount         int64 `json:"get-count"`   // Get操作次数
	PutCount         int64 `json:"put-count"`   // Put操作次数
	AliveSnapshots   int64 `json:"alive-snaps"`
	AliveIterators   int64 `json:"alive-iters"`
	BlockCacheHits   int64 `json:"cache-hits"`
	BlockCacheMisses int64 `json:"cache-misses"`
}
