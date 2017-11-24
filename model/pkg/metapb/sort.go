package metapb

type PeersByNodeIdSlice []*Peer

func (p PeersByNodeIdSlice) Len() int {
	return len(p)
}

func (p PeersByNodeIdSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p PeersByNodeIdSlice) Less(i int, j int) bool {
	return p[i].GetNodeId() < p[j].GetNodeId()
}

type RangeByIdSlice []*Range

func (r RangeByIdSlice) Len() int {
	return len(r)
}

func (r RangeByIdSlice) Swap(i int, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RangeByIdSlice) Less(i int, j int) bool {
	return r[i].GetId() < r[j].GetId()
}