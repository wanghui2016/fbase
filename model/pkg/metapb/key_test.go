package metapb

import (
	"testing"
	"github.com/google/btree"
	"fmt"
)

func TestCompare(t *testing.T) {
	a := &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
	b := &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
	if Compare(a, b) != 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
	b = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	if Compare(a, b) <= 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
	b = &Key{Key: []byte("a"), Type: KeyType_KT_Ordinary}
	if Compare(a, b) <= 0 {
		t.Error("test failed for infinity")
	}

	c := a.Clone()
	if Compare(a, c) != 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	b = &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
	if Compare(a, b) >= 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	b = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	if Compare(a, b) != 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	b = &Key{Key: []byte("a"), Type: KeyType_KT_Ordinary}
	if Compare(a, b) >= 0 {
		t.Error("test failed for infinity")
	}

	c = a.Clone()
	if Compare(a, c) != 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: []byte("b"), Type: KeyType_KT_Ordinary}
	b = &Key{Key: nil, Type: KeyType_KT_PositiveInfinity}
	if Compare(a, b) >= 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: []byte("b"), Type: KeyType_KT_Ordinary}
	b = &Key{Key: nil, Type: KeyType_KT_NegativeInfinity}
	if Compare(a, b) <= 0 {
		t.Error("test failed for infinity")
	}

	a = &Key{Key: []byte("b"), Type: KeyType_KT_Ordinary}
	b = &Key{Key: []byte("a"), Type: KeyType_KT_Ordinary}
	if Compare(a, b) <= 0 {
		t.Error("test failed for infinity")
	}

	c = a.Clone()
	if Compare(a, c) != 0 {
		t.Error("test failed for infinity")
	}
}

func TestLess(t *testing.T) {
	route0 := &Route{
		Range: &Range{
			StartKey: &Key{Key: nil, Type: KeyType_KT_NegativeInfinity},
			EndKey: &Key{Key: []byte("a"), Type: KeyType_KT_Ordinary},
		},
	}
	route1 := &Route{
		Range: &Range{
			StartKey: &Key{Key: []byte("a"), Type: KeyType_KT_Ordinary},
			EndKey: &Key{Key: []byte("c"), Type: KeyType_KT_Ordinary},
		},
	}
	key1 := &Key{Key: []byte("a"), Type: KeyType_KT_Ordinary}
	if route1.Less(key1) || key1.Less(route1) {
		t.Error("test failed")
	}
	route2 := &Route{
		Range: &Range{
			StartKey: &Key{Key: []byte("c"), Type: KeyType_KT_Ordinary},
			EndKey: &Key{Key: []byte("e"), Type: KeyType_KT_Ordinary},
		},
	}
	//route3 := &Route{
	//	Range: &Range{
	//		StartKey: &Key{Key: []byte("e"), Type: KeyType_KT_Ordinary},
	//		EndKey: &Key{Key: nil, Type: KeyType_KT_PositiveInfinity},
	//	},
	//}
	if route2.Less(key1) || !key1.Less(route2) {
		t.Error("test failed")
	}
	tr := btree.New(10)
	tr.ReplaceOrInsert(route1)
	tr.ReplaceOrInsert(route2)
	tr.ReplaceOrInsert(route0)
	//tr.ReplaceOrInsert(route3)
	it1 := tr.Get(key1)
	if it1 == nil {
		t.Error("test failed")
	}
	key2 := &Key{Key: []byte("b"), Type: KeyType_KT_Ordinary}
	it2 := tr.Get(key2)
	if it2 == nil {
		t.Error("test failed")
	}
	if it1 != it2 {
		t.Error("test failed")
	}
	if it1.Less(route1) || route1.Less(it1) {
		t.Error("test failed")
	}
	fmt.Println(it1)


	key3 := &Key{Key: []byte("w"), Type: KeyType_KT_Ordinary}
	it3 := tr.Get(key3)
	if it3 != nil {
		t.Errorf("test failed %v", it3)
	}
}