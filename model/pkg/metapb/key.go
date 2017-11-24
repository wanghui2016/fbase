package metapb

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
)

var (
	Key_NegativeInfinity = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	Key_PositiveInfinity = &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
)

func (k *Key) Clone() *Key {
	key := &Key{Key: nil, Type: k.Type}
	if k.Type == KeyType_KT_Ordinary {
		_k := make([]byte, len(k.Key))
		copy(_k, k.Key)
		key.Key = _k
	}
	return key
}

func (k *Key) IsNegativeInfinity() bool {
	return k.GetType() == KeyType_KT_NegativeInfinity
}

func (k *Key) IsPositiveInfinity() bool {
	return k.GetType() == KeyType_KT_PositiveInfinity
}

func (k *Key) Less (item btree.Item) bool {
	switch it := item.(type) {
	case *Route:
		if Compare(k, it.GetStartKey()) < 0 {
			return true
		} else {
			return false
		}
	case *Key:
		if Compare(k, it) < 0 {
			return true
		} else {
			return false
		}
	}
	return false
}

func (r *Route) Less(item btree.Item) bool {
	switch it := item.(type) {
	case *Route:
		if Compare(r.GetStartKey(), it.GetStartKey()) < 0 {
			return true
		} else {
			return false
		}
	case *Key:
		if Compare(r.GetEndKey(), it) <= 0 {
			return true
		} else {
			return false
		}
	}
	return false
}

func (r *Route) UpdateLeader(leader *Leader) {
	r.Leader = leader
}

func (r *Route) GetRangeId() uint64 {
	return r.GetId()
}

func (r *Route) SString() string {
	return fmt.Sprintf("%d", r.GetId())
}
// Compare returns an integer comparing two Key lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// A nil argument is equivalent to an empty slice.
func Compare(a, b *Key) int {
	if a == nil || b == nil {
		return 0
	}
	switch a.Type {
	case KeyType_KT_PositiveInfinity:
		if b.Type == KeyType_KT_PositiveInfinity {
			return 0
		}
		return 1
	case KeyType_KT_NegativeInfinity:
		if b.Type == KeyType_KT_NegativeInfinity {
			return 0
		}
		return -1
	case KeyType_KT_Ordinary:
		if b.Type == KeyType_KT_PositiveInfinity {
			return -1
		} else if b.Type == KeyType_KT_NegativeInfinity {
			return 1
		}
		return bytes.Compare(a.Key, b.Key)
	}
	return 0
}

func HasPrefix(a, b *Key) bool {
	switch a.Type {
	case KeyType_KT_PositiveInfinity:
		if b.Type == KeyType_KT_PositiveInfinity {
			return true
		}
		return false
	case KeyType_KT_NegativeInfinity:
		if b.Type == KeyType_KT_NegativeInfinity {
			return true
		}
		return false
	case KeyType_KT_Ordinary:
		if b.Type != KeyType_KT_Ordinary {
			return false
		}
		return bytes.HasPrefix(a.Key, b.Key)
	}
	return false
}
