package server

import (
	"fmt"

	"model/pkg/metapb"
	"util/log"
	"util/deepcopy"
	"model/pkg/statspb"
	"model/pkg/taskpb"
)

func (c *Cluster) AddInstance(serverIp string, serverPort, raftHeartbeatPort, raftReplicaPort uint64, role string) (*Instance, error) {
	if !c.IsLeader() {
		return nil, ErrNotLeader
	}
	serverAddr := fmt.Sprintf("%s:%d", serverIp, serverPort)
	raftHeartbeatAddr := fmt.Sprintf("%s:%d", serverIp, raftHeartbeatPort)
	raftReplicaAddr := fmt.Sprintf("%s:%d", serverIp, raftReplicaPort)
    c.lock.Lock()
	defer c.lock.Unlock()
	//id := inet.GenAddrId(serverIp, serverPort)
	var mac *Machine
	var find bool
	// TODO machines gray upgrade
	mac, find = c.machines.FindMachine(serverIp)
	if !find {
		//return nil, ErrNotExistMac
	}
	ins, find := c.instancesAddr.FindInstance(serverAddr)
	if find {
		// TODO to comment
		if mac != nil {
			mac.AddInstance(ins)
		}
		return ins, nil
	}
	id, err := c.nodeIdGener.GenID()
	if err != nil {
		log.Error("gen Node ID failed, err[%v]", err)
		return nil, err
	}
	n := &metapb.Node{
		Id: id,
		Address: serverAddr,
		State: metapb.NodeState_N_Initial,
		RaftAddrs: &metapb.RaftAddrs{
			HeartbeatAddr: raftHeartbeatAddr,
			ReplicateAddr: raftReplicaAddr,
		},
	}
	err = c.storeNode(n)
	if err != nil {
		log.Error("store node[%s] failed", serverAddr)
		return nil, err
	}
	ins = NewInstance(n)
	if mac != nil {
		mac.AddInstance(ins)
	}
	c.instances.Add(ins)
	c.instancesAddr.Add(ins)
	return ins, nil
}

func (c *Cluster) DeleteNode(n *Instance) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, n.GetId()))
	if err := c.store.Delete(key); err != nil {
		return err
	}
	c.instances.Delete(n.GetId())
	c.instancesAddr.Delete(n.GetAddress())
	return nil
}

func (c *Cluster) FindInstance(id uint64) (*Instance, bool) {
	return c.instances.FindInstance(id)
}

func (c *Cluster) GetAllInstances() []*Instance {
	return c.instances.GetAllInstances()
}

func (c *Cluster) GetAllActiveInstances() []*Instance {
	return c.instances.GetAllActiveInstances()
}

func (c *Cluster) NodeFailOver(node *Instance) error {
	tasks := node.AllTask()
	// 故障节点上没有任务了，failOver已经完成
	if len(tasks) == 0 {
		return nil
	}
	// 故障节点上最多只能有一个任务
	if len(tasks) > 1 {
		log.Error("more than one task in fail node[%s]", node.GetAddress())
		return ErrInternalError
	}
	// 检查任务是否是failOver任务
	task := tasks[0]
	if task.Type != taskpb.TaskType_NodeFailOver {
		log.Error("not exported task[%d: %d] in fail node[%s]",
			task.ID(), task.GetType(), node.GetAddress())
	}
    // 任务完成, 需要删除任务
	batch := c.store.NewBatch()
	n := deepcopy.Iface(node.Node).(*metapb.Node)
	n.State = metapb.NodeState_N_Logout
	data, err := n.Marshal()
	if err != nil {
		log.Error("node marshal failed, err[%v]", err)
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, n.GetId()))
	batch.Put(key, data)
	key = []byte(fmt.Sprintf("%s%d", PREFIX_TASK, task.ID()))
	batch.Delete(key)
	err = batch.Commit()
	if err != nil {
		log.Error("update node[%s] state failed, err[%v]", n.GetAddress(), err)
		return err
	}
	c.tasks.Delete(task.ID())
	node.DeleteTask(task.ID())
	node.Offline()
	// TODO push task log
	return nil
}

func (c *Cluster) UpdateNode(node *Instance, stats *statspb.NodeStats) {
	node.UpdateStats(stats)
	return
}

func (c *Cluster) UpdateNodeState(node *Instance, state metapb.NodeState) error {
	if err := c.storeNode(node.Node); err != nil {
		return err
	}
	node.State = state
	return nil
}

func (c *Cluster) cancelDestNodeTask(id uint64) {
	var dstNodeId uint64
	for _, t := range c.GetAllTask() {
		switch t.Type{
		case taskpb.TaskType_RangeTransfer:
			dstNodeId = t.GetRangeTransfer().GetUpPeer().GetNodeId()
		case taskpb.TaskType_RangeFailOver:
			dstNodeId = t.GetRangeFailover().GetUpPeer().GetNodeId()
		case taskpb.TaskType_RangeAddPeer:
			dstNodeId = t.GetRangeAddPeer().GetPeer().GetNodeId()
		case taskpb.TaskType_NodeLogout:
			dstNodeId = t.GetNodeLogout().GetNodeId()
		case taskpb.TaskType_NodeLogin:
			dstNodeId = t.GetNodeLogin().GetNodeId()
		case taskpb.TaskType_NodeDeleteRanges:
			dstNodeId = t.GetNodeDeleteRanges().GetNodeId()
		case taskpb.TaskType_NodeCreateRanges:
			dstNodeId = t.GetNodeCreateRanges().GetNodeId()
		case taskpb.TaskType_NodeFailOver:
			dstNodeId = t.GetNodeFailover().GetNodeId()
		}
		if id != dstNodeId {
			continue
		}
		if t.SwapAndSaveState(taskpb.TaskState_TaskRunning, taskpb.TaskState_TaskCancel, c) == nil ||
			t.SwapAndSaveState(taskpb.TaskState_TaskTimeout, taskpb.TaskState_TaskCancel, c) == nil ||
			t.SwapAndSaveState(taskpb.TaskState_TaskHangUp, taskpb.TaskState_TaskCancel, c) == nil ||
			t.SwapAndSaveState(taskpb.TaskState_TaskPause, taskpb.TaskState_TaskCancel, c) == nil {
			continue
		}
	}
}

func (c *Cluster) NodeInit(nodeId uint64) error {
	log.Warn("node %v init", nodeId)
	node, find := c.FindInstance(nodeId)
	if !find {
		return ErrNotExistNode
	}
	if !(node.GetState() == metapb.NodeState_N_Logout && len(node.GetAllRanges()) == 0) {
		return fmt.Errorf("node[%v] state is not logout or len(ranges) != 0")
	}

	log.Info("node[%v] is to re-init")
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.GetState() == metapb.NodeState_N_Logout {
		node_ := deepcopy.Iface(node.Node).(*metapb.Node)
		node_.State = metapb.NodeState_N_Initial
		if err := c.storeNode(node_); err != nil {
			return err
		}
		node.Node = node_
	}
	return nil
}

func (c *Cluster) LogoutNode(nodeId uint64) error {
	log.Warn("node %v logout", nodeId)
	node, find := c.FindInstance(nodeId)
	if !find {
		return ErrNotExistNode
	}

	if node.GetState() == metapb.NodeState_N_Logout {
		return fmt.Errorf("node[%v] state is already logout", node.GetId())
	}
	if !(node.GetState() == metapb.NodeState_N_Login || node.GetState() == metapb.NodeState_N_Tombstone) {
		return fmt.Errorf("node[%v] state is not login or tombstone", node.GetId())
	}

	for _, r := range node.GetAllRanges() {
		for _, p := range r.GetPeers() {
			if p.GetNodeId() == node.GetId() {
				continue
			}

			var nodeOther *Instance
			var found bool
			if nodeOther, found = c.FindInstance(p.GetNodeId()); !found {
				return fmt.Errorf("peer range[%v]node[%v] isnot found", p.GetId(), p.GetNodeId())
			}
			if nodeOther.GetState() == metapb.NodeState_N_Logout || nodeOther.GetState() == metapb.NodeState_N_Tombstone {
				return fmt.Errorf("peer range[%v]node[%v] state %v", nodeOther.GetState())
			}
		}
	}

	// cancel tasks which dstnode is input nodeid
	c.cancelDestNodeTask(nodeId)

	node.lock.Lock()
	defer node.lock.Unlock()
	if node.GetState() == metapb.NodeState_N_Login || node.GetState() == metapb.NodeState_N_Tombstone {
		node_ := deepcopy.Iface(node.Node).(*metapb.Node)
		node_.State = metapb.NodeState_N_Logout
		if err := c.storeNode(node_); err != nil {
			return err
		}
		node.Node = node_
	}

	return nil
}
