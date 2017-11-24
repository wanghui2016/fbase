package server

import (
	"errors"

	"raft"
)

type Resolver struct {
	cluster         map[uint64]*Point
}

func NewResolver(nodes map[uint64]*Point) *Resolver {
	return &Resolver{cluster: nodes}
}

func (r *Resolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	switch stype {
	case raft.HeartBeat:
		node := r.cluster[nodeID]
		if node == nil {
			return "", errors.New("invalid node")
		}
		return node.HeartbeatAddr, nil
	case raft.Replicate:
		node := r.cluster[nodeID]
		if node == nil {
			return "", errors.New("invalid node")
		}
		return node.ReplicateAddr, nil
	default:
		return "", errors.New("unknown socket type")
	}

	return "", nil
}
