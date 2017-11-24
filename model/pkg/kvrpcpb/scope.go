package kvrpcpb
//
//import (
//	"github.com/google/btree"
//	"model/pkg/metapb"
//)
//
//func (s *Scope) Less(item btree.Item) bool {
//	var start *metapb.Key
//	if s.Start == nil {
//		start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinite}
//	} else {
//		start = &metapb.Key{Key: s.Start, Type: metapb.KeyType_KT_Ordinary}
//	}
//	switch it := item.(type) {
//	case *metapb.Route:
//		if metapb.Compare(start, it.GetRange().GetStartKey()) < 0 {
//			return true
//		} else {
//			return false
//		}
//	case *Scope:
//		var _start *metapb.Key
//		if it.Start == nil {
//			_start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinite}
//		} else {
//			_start = &metapb.Key{Key: it.Start, Type: metapb.KeyType_KT_Ordinary}
//		}
//
//		if metapb.Compare(start, _start) < 0 {
//			return true
//		} else {
//			return false
//		}
//	}
//	return false
//}
