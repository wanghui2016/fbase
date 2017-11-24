package server

import (
	"errors"
	"sync"
	"time"

	"master-server/client"
	"model/pkg/metapb"
	"raft"
)

type NodeObj struct {
	node     *metapb.Node
	refcount int
}

type Cluster struct {
	cli   client.Client
	lock  sync.RWMutex
	nodes map[uint64]*NodeObj
}

func NewCluster(cli client.Client) *Cluster {
	return &Cluster{
		cli:   cli,
		nodes: make(map[uint64]*NodeObj, 1000),
	}
}

func (c *Cluster) addNode(node *metapb.Node) {
	if node == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if obj, ok := c.nodes[node.Id]; ok {
		obj.refcount++
	} else {
		c.nodes[node.Id] = &NodeObj{node: node, refcount: 1}
	}
}

func (c *Cluster) deleteNode(id uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if obj, ok := c.nodes[id]; ok {
		obj.refcount--
		if obj.refcount <= 0 {
			delete(c.nodes, id)
		}
	}
}

func (c *Cluster) getNode(id uint64) *metapb.Node {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if obj, ok := c.nodes[id]; ok {
		return obj.node
	}
	return nil
}

func (c *Cluster) GetNode(id uint64) (*metapb.Node, error) {
	var err error
	var node *metapb.Node
	if node = c.getNode(id); node == nil {
		for i := 0; i < 3; i++ {
			node, err = c.cli.GetNode(id)
			if err != nil {
				time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
				continue
			}
			break
		}
		if err != nil {
			return nil, err
		}
		c.addNode(node)
	}
	return node, nil
}

type Resolver struct {
	cluster *Cluster
}

func NewResolver(cluster *Cluster) *Resolver {
	return &Resolver{cluster: cluster}
}

func (r *Resolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	var node *metapb.Node
	switch stype {
	case raft.HeartBeat:
		node, err = r.cluster.GetNode(nodeID)
		if err != nil {
			return "", err
		}
		return node.GetRaftAddrs().GetHeartbeatAddr(), nil
	case raft.Replicate:
		node, err = r.cluster.GetNode(nodeID)
		if err != nil {
			return "", err
		}
		return node.GetRaftAddrs().GetReplicateAddr(), nil
	default:
		return "", errors.New("unknown socket type")
	}

	return "", nil
}
