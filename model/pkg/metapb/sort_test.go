package metapb

import (
	"testing"
	"sort"
)

func TestPeerSort(t *testing.T) {
	peers := make([]*Peer, 3)
	peers[0] = &Peer{Id:   1, NodeId: 2}
	peers[1] = &Peer{Id:   1, NodeId: 1}
	peers[2] = &Peer{Id:   1, NodeId: 3}
	//ss := PeersByNodeIdSlice(peers)
	sort.Sort(PeersByNodeIdSlice(peers))
	//for i, p := range ss {
	//	if i + 1 < len(ss) && p.GetNodeId() > ss[i + 1].GetNodeId() {
	//		t.Error("test failed")
	//		return
	//	}
	//}
	for i, p := range peers {
		if i + 1 < len(peers) && p.GetNodeId() > peers[i + 1].GetNodeId() {
			t.Error("test failed")
			return
		}
	}
}

func TestRangeSort(t *testing.T) {
	ranges := make([]*Range, 3)
	ranges[0] = &Range{Id:   2}
	ranges[1] = &Range{Id:   3}
	ranges[2] = &Range{Id:   1}
	//ss := PeersByNodeIdSlice(peers)
	sort.Sort(RangeByIdSlice(ranges))
	//for i, p := range ss {
	//	if i + 1 < len(ss) && p.GetNodeId() > ss[i + 1].GetNodeId() {
	//		t.Error("test failed")
	//		return
	//	}
	//}
	for i, p := range ranges {
		if i + 1 < len(ranges) && p.GetId() > ranges[i + 1].GetId() {
			t.Error("test failed")
			return
		}
	}
}