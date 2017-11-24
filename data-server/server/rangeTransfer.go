package server

import (
	"time"
	"errors"

	"model/pkg/metapb"
	"util/log"
	"model/pkg/schpb"
	"util/deepcopy"
	"model/pkg/eventpb"
)

func (r *Range) AddPeer(taskId uint64, peer *metapb.Peer) error {
	if !r.raftGroup.IsLeader() {
		return errNotLeader
	}
	log.Debug("range[%s] add peer!!!!", r.String())
	startTime := time.Now().UnixNano();
	defer r.reportEventForStatistics(startTime, eventpb.StatisticsType_PeerAdd)
	if r.region.State != metapb.RangeState_R_Normal {
		log.Error("range [%s] state not allow do this task", r.String())
		return errors.New("invalid range state")
	}
	// 检查是否满足分裂条件
	downs := r.raftGroup.GetDownPeers(r.region.GetId())
	pendings := r.raftGroup.GetPendingPeers(r.region.GetId())
	if len(downs) > 0 || len(pendings) > 0 {
		return errRangeBusy
	}
	var err error
	// step 1. check peers
	if r.checkPeer(peer){
		r.lastTaskId = taskId
		log.Info("range[%s] add peer success", r.String())
		return nil
	}

	// step 2. create new range peer
	err = r.createNewPeer(peer)
	if err != nil {
		log.Error("range [%s] create new range peer failed", r.String())
		return err
	}

	err = r.ChangeMember(schpb.RangePeerOp_Op_Add, peer.GetNodeId())
	if err != nil {
		log.Error("range [%s] add peer failed", r.String())
		return err
	}
	r.lastTaskId = taskId
	log.Info("range[%s] add peer success", r.String())
	return nil
}

func (r *Range) DelPeer(taskId uint64, peer *metapb.Peer) error {
	if !r.raftGroup.IsLeader() {
		return errNotLeader
	}
	log.Debug("range[%s] del peer!!!!", r.String())
	startTime := time.Now().UnixNano()
	defer r.reportEventForStatistics(startTime, eventpb.StatisticsType_PeerRemove)
	if r.region.State != metapb.RangeState_R_Normal {
		log.Error("range [%s] state not allow do this task", r.String())
		return errors.New("invalid range state")
	}

	var err error
	// step 1. check peers
	if !r.checkPeer(peer){
		r.lastTaskId = taskId
		log.Info("range[%s] del peer success", r.String())
		return nil
	}
	if r.leader == peer.GetNodeId() {
		log.Error("range[%s] deleted peer is leader", r.String())
		return errors.New("raft leader can not delete self")
	}
	err = r.ChangeMember(schpb.RangePeerOp_Op_Remove, peer.GetNodeId())
	if err != nil {
		log.Error("range[%s] del peer failed", r.String(), err)
		return err
	}
	r.deletePeer(r.server.dsCli, peer, taskId)
	r.lastTaskId = taskId
	log.Info("range[%s] del peer success", r.String())
	return nil
}

func (r *Range) checkPeer(peer *metapb.Peer) bool {
	var find bool
	for _, p := range r.region.Peers {
		if peer.NodeId == p.NodeId {
			find = true
			break
		}
	}
	return find
}

func (r *Range) createNewPeer(peer *metapb.Peer) error {
	_range := deepcopy.Iface(r.region).(*metapb.Range)
	_range.Peers = append(_range.Peers, peer)

	node, err := r.server.cluster.GetNode(peer.GetNodeId())
	if err != nil {
		log.Error("found node[%d] failed", peer.GetNodeId())
		return err
	}
	addr := node.GetAddress()
	for i := 0; i < 3; i++ {
		err = r.server.dsCli.CreateRange(addr, _range)
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10 * (i + 1)))
			continue
		}
		break
	}
	if err != nil {
		log.Error("range[%s] create range failed, err[%v]", _range.String(), err)
		return err
	}

	return nil
}
