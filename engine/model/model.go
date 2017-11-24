package model

import ts "model/pkg/timestamp"

// Store store
type Store interface {
	Get(key []byte, timestamp ts.Timestamp) (value []byte, err error)
	Put(key []byte, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) error
	Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64) error
	Close() error
	Destroy() error

	NewIterator(startKey, endKey []byte, timestamp ts.Timestamp) Iterator

	// 批量写入，提交时保证batch里的修改同时对外可见
	NewWriteBatch() WriteBatch

	// OpenTransaction 打开事务
	// 事务与WriteBatch的区别：
	// 1) 提交时都保证事务执行期间或batch里的写入同时对外可见，不会出现部分写入被外部看见
	//	  但事务提交后所有修改都会持久化，WriteBatch不保证，有可能还在memtable里
	// 2) 对于大批量的写入，WriteBatch会全部暂存到内存里，事务不会，最多只占用一个memtable的内存
	// 3) 一个db同时只能有一个事务，WriteBatch可以有多个；事务执行期间会阻塞写，commit或discard后才释放
	// 4) 事务可以做读操作，可以获取到事务执行期间写入以及db里的数据,
	// 5) 过大的（超过一个memtable的大小）WriteBatch写入时会转换为事务执行
	OpenTransaction() (Transaction, error)

	GetSnapshot() (Snapshot, error)

	// Applied return current applied raft index(已持久化的)
	Applied() uint64

	// 统计
	Size() uint64              // 包含memtable的统计信息
	DiskUsage() uint64         // 只包含磁盘文件的统计信息
	SharedFilesNum() int       // 共享文件数
	GetStats() (*Stats, error) //存储的其他统计信息

	// 分裂
	FindMiddleKey() ([]byte, error)
	PrepareForSplit()
	Split(splitKey []byte, leftDir, rightDir string, leftDBId, rightDBId uint64) (left, right Store, err error)

	// Compaction
	GetCompactionScore() (float64, error)
	NewCompactionTask() (CompactionTask, error)
}

// Iterator iterator
type Iterator interface {
	// return false if over or error
	Next() bool

	Key() []byte
	Value() []byte

	Error() error

	// Release iterator使用完需要释放
	Release()
}

// MVCCIterator  带版本的迭代器
type MVCCIterator interface {
	// return false if over or error
	Next() bool

	Key() []byte
	Timestamp() ts.Timestamp
	IsDelete() bool
	Value() []byte
	ExpireAt() int64

	Error() error

	// Release iterator使用完需要释放
	Release()
}

// Snapshot snapshot
type Snapshot interface {
	NewIterator(startKey, endKey []byte) Iterator
	Get(key []byte) ([]byte, error)

	NewMVCCIterator(startKey, endKey []byte) MVCCIterator
	// apply index
	ApplyIndex() uint64

	// Release snapshot使用完需要释放
	Release()
}

// WriteBatch write batch
type WriteBatch interface {
	Put(key []byte, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64)
	Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64)

	Commit() error
}

// Transaction 事务
type Transaction interface {
	// TODO:
	// Get(key []byte, timestamp uint64) (value []byte, err error)
	// TODO:
	// NewIterator(startKey, endKey []byte, timestamp uint64) Iterator
	Put(key []byte, value []byte, expireAt int64, timestamp ts.Timestamp, raftIndex uint64) error
	Delete(key []byte, timestamp ts.Timestamp, raftIndex uint64) error

	// 提交事务, 如果提交失败，需要调用Discard，释放写锁，废弃table文件
	Commit() error

	// Discard 丢弃事务, 释放写锁
	Discard()
}
