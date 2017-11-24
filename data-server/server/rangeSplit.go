package server

import (
	"sync"
	"sync/atomic"
	"time"
	"errors"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/raft_cmdpb"
	"util/deepcopy"
	"util/log"
	"model/pkg/eventpb"
)

func (r *Range) prepareSplit(peer *metapb.Peer) bool {
	node, err := r.server.cluster.GetNode(peer.GetNodeId())
	if err != nil {
		log.Error("node[%d] not found", peer.GetNodeId())
		return false
	}
	addr := node.GetAddress()
	var ready bool
	for i := 0; i < 3; i++ {
		ready, err = r.server.dsCli.SplitRangePrepare(addr, peer.GetId())
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
			continue
		}
		break
	}
	if err != nil {
		log.Error("range[%s] split prepare failed, err[%v]", r.String(), err)
		return false
	}
	if ready {
		return true
	}
	log.Warn("range[%s] peer[%s] not ready for split", r.String(), addr)
	return false
}

func (r *Range) PrepareSplit() bool {
	var readyCount int32
	var wg sync.WaitGroup
	for _, p := range r.region.GetPeers() {
		wg.Add(1)
		go func(peer *metapb.Peer) {
			defer wg.Done()
			if r.prepareSplit(peer) {
				atomic.AddInt32(&readyCount, 1)
			}
		}(p)
	}
	wg.Wait()
	if readyCount != int32(len(r.region.GetPeers())) {
		return false
	}
	// 检查复制进度是否满足分裂条件
	if status := r.server.raftServer.Status(r.GetRangeId()); status != nil {
		for nodeId, rep := range status.Replicas {
			if status.Index > rep.Match && status.Index - rep.Match > uint64(r.server.conf.RaftMaxAllocLogBackward) {
				node, err := r.server.cluster.GetNode(nodeId)
				if err != nil {
					log.Error("get node[%d] failed, err[%v]", nodeId, err)
					return false
				}
				log.Warn("range[%s] peer[%s] raft log backward(%d, %d)!!!",
					r.String(), node.GetAddress(), status.Index, rep.Match)
				return false
			}
		}
	}
	return true
}

func (r *Range) Split(taskId uint64, splitKey []byte, leftRangeId, rightRangeId uint64) error {
	if !r.raftGroup.IsLeader() {
		return errNotLeader
	}
	if !r.PrepareSplit() {
		log.Warn("range[%s] not ready for split", r.String())
		return errRangeBusy
	}
	log.Debug("range[%s] split[-> %d,%d]!!!!", r.String(), leftRangeId, rightRangeId)

	// 检查是否满足分裂条件
	downs := r.raftGroup.GetDownPeers(r.region.GetId())
	pendings := r.raftGroup.GetPendingPeers(r.region.GetId())
	if len(downs) > 0 || len(pendings) > 0 {
		log.Error("range[%s] split[-> %d,%d]!!!! is stopped downs:%d,pendings:%d", r.String(), leftRangeId, rightRangeId, len(downs), len(pendings))
		return errRangeBusy
	}
	var err error
	if len(splitKey) == 0 {
		splitKey, err = r.GetSplitKey()
		if err != nil {
			log.Warn("range[%s] get split key failed", r.String())
			return err
		}
		// 不会出现，做一个保护，以防万一
		if len(splitKey) == 0 {
			log.Error("range[%s] invalid split key", r.String())
			return errors.New("invalid split key")
		}
		err = r.reportSplitKey(taskId, splitKey)
		// 一般不会失败，如果失败，
		if err != nil {
			log.Error("range[%s] report split key failed, err[%v]", r.String(), err)
			return err
		}
		log.Info("range[%s] report split key success", r.String())
		return nil
	}
	// 检查是否已经分裂
	if r.left != nil && r.right != nil {
		// 再次上报
		err = r.reportSplitAck(taskId, r.left, r.right)
		// 一般不会失败，如果失败，
		if err != nil {
			log.Error("report range[%d] split ack failed, err[%v]", r.region.GetId(), err)
			return err
		}
		r.lastTaskId = taskId
		log.Warn("range[%s] already split!!!!", r.String())
		return nil
	}

	state := r.region.State
	//r.lock.Lock()
	if r.region.State == metapb.RangeState_R_Normal {
		r.region.State = metapb.RangeState_R_Split
	} else {
		// 遇到节点重启，仍然需要重试分裂
		if r.left != nil && r.right != nil {
			// 再次上报
			err = r.reportSplitAck(taskId, r.left, r.right)
			// 一般不会失败，如果失败，
			if err != nil {
				log.Error("report range[%d] split ack failed, err[%v]", r.region.GetId(), err)
				return err
			}
			r.lastTaskId = taskId
			log.Warn("range[%s] already split!!!!", r.String())
			return nil
		}
	}
	//r.lock.Unlock()

	//TODO check here with split prepare status
	req := &raft_cmdpb.RaftCmdRequest{
		Type: raft_cmdpb.MessageType_Admin,
		AdminRequest: &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			SplitReq: &raft_cmdpb.SplitRequest{
				SplitKey:     splitKey,
				LeftRangeId:  leftRangeId,
				RightRangeId: rightRangeId,
			},
		},
	}
	resp, err := r.raftGroup.SubmitCommand(r.quitCtx, req)
	if err != nil {
		// rollback
		log.Error("range[%s] split[-> %d,%d]!!!! error %s", r.String(), leftRangeId, rightRangeId, err.Error())
		r.region.State = state
		return err
	}
	// 至此可以任务执行成功，master server等待所有的节点都split
	_resp := resp.GetAdminResponse().GetSplitResp()
	left := _resp.GetLeftRange()
	right := _resp.GetRightRange()
	err = r.reportSplitAck(taskId, left, right)
	// 一般不会失败，如果失败，
	if err != nil {
		log.Error("report range[%d] split ack failed, err[%v]", r.region.GetId(), err)
		return err
	}
	r.lastTaskId = taskId
	log.Info("range[%s] split[-> %d,%d] success!!!", r.String(), leftRangeId, rightRangeId)
	return nil

}

func (r *Range) Merge() error {
	return nil
}

func (r *Range) GetSplitKey() ([]byte, error) {
	if !r.raftGroup.IsLeader() {
		return nil, errNotLeader
	}
	splitKey, err := r.rowstore.GetSplitKey()
	if err != nil {
		return nil, err
	}
	return DecodeKey(splitKey), nil
}

func (r *Range) reportSplitAck(taskId uint64, left, right *metapb.Range) error {
	req := &mspb.ReportEventRequest{
		Event: &eventpb.Event{
			Type: eventpb.EventType_RangeSplitAck,
			EventRangeSplitAck: &eventpb.EventRangeSplitAck{
				TaskId:     taskId,
				Range:      deepcopy.Iface(r.region).(*metapb.Range),
				LeftRange:  deepcopy.Iface(left).(*metapb.Range),
				RightRange: deepcopy.Iface(right).(*metapb.Range),
			},
		},
	}
	var err error
	for i := 0; i < 10; i++ {
		_, err = r.server.mscli.ReportEvent(req)
		if err != nil {
			log.Error("report event failed, err[%v]", err)
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
			continue
		}
		break
	}
	return err
}

func (r *Range) reportSplitKey(taskId uint64, splitKey []byte) error {
	req := &mspb.ReportEventRequest{
		Event: &eventpb.Event{
			Type: eventpb.EventType_RangeSplitKey,
			EventRangeSplitKey: &eventpb.EventRangeSplitKey{
				TaskId:     taskId,
				Range:      deepcopy.Iface(r.region).(*metapb.Range),
				SplitKey: splitKey,
			},
		},
	}
	var err error
	for i := 0; i < 10; i++ {
		_, err = r.server.mscli.ReportEvent(req)
		if err != nil {
			log.Error("report event failed, err[%v]", err)
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
			continue
		}
		break
	}
	return err
}
