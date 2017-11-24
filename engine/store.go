package engine

import (
	"engine/model"
	"engine/rowstore"
	"engine/rowstore/opt"
)

// NewRowStore new row store
// dbID: 				区分不同的store，例如在日志中等
// path: 				store目录
// startKey, endKey: 	store key的范围
// o: 					其他选项
//
// 返回值：store, applied index, error
func NewRowStore(dbID uint64, path string, startKey, endKey []byte, o *opt.Options) (model.Store, uint64, error) {
	return rowstore.OpenStore(dbID, path, startKey, endKey, o)
}
