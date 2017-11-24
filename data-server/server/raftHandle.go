package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"engine/model"
	"model/pkg/errorpb"
	"model/pkg/eventpb"
	"model/pkg/metapb"
	"model/pkg/raft_cmdpb"
	"raft"
	raftproto "raft/proto"
	"raftgroup"
	"util/deepcopy"
	"util/log"
)

func (r *Range) HandleCmd(req *raft_cmdpb.RaftCmdRequest, raftIndex uint64) (res *raft_cmdpb.RaftCmdResponse, err error) {
	start := time.Now()
	defer func() {
		end := time.Now()
		r.server.metricMeter.AddApiWithDelay("raft_apply", true, end.Sub(start))
	}()
	select {
	case <-r.quitCtx.Done():
		err = errRangeShutDown
		return
	default:
	}
	atomic.AddInt64(&r.referenceCount, 1)
	defer atomic.AddInt64(&r.referenceCount, -1)
	resp := new(raft_cmdpb.RaftCmdResponse)
	resp.Type = req.GetType()
	switch req.GetType() {
	case raft_cmdpb.MessageType_Data:
		kvResp, err := r.handleRaftKv(req.GetRequest(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.Response = kvResp
		res = resp
	case raft_cmdpb.MessageType_Admin:
		adminResp, err := r.handleRaftAdmin(req.GetAdminRequest())
		if err != nil {
			return nil, err
		}
		resp.AdminResponse = adminResp
		res = resp
	/*case raft_cmdpb.MessageType_Task:*/
	default:
		res, err = nil, ErrUnknownCommandType
	}
	return
}

func (r *Range) HandlePeerChange(confChange *raftproto.ConfChange) (res interface{}, err error) {
	switch confChange.Type {
	case raftproto.ConfAddNode:
		log.Debug("add range peer")
		//r.lock.Lock()
		//defer r.lock.Unlock()
		r.region.RangeEpoch.ConfVer++
		for _, p := range r.region.GetPeers() {
			//　副本已经存在，直接返回
			if confChange.Peer.ID == p.GetNodeId() {
				res, err = nil, nil
				return
			}
		}
		peers := make([]*metapb.Peer, len(r.region.Peers))
		copy(peers, r.region.Peers)
		peers = append(peers, &metapb.Peer{Id: r.region.Id, NodeId: confChange.Peer.ID})
		sort.Sort(metapb.PeersByNodeIdSlice(peers))
		r.region.Peers = peers
		//r.server.cluster.addNode(node)
		res, err = nil, nil
	case raftproto.ConfRemoveNode:
		//r.lock.Lock()
		//defer r.lock.Unlock()
		r.region.RangeEpoch.ConfVer++
		log.Debug("delete range peer")
		peers := make([]*metapb.Peer, 0, len(r.region.Peers))
		for _, p := range r.region.Peers {
			if p.NodeId != confChange.Peer.ID {
				peers = append(peers, p)
			}
		}
		sort.Sort(metapb.PeersByNodeIdSlice(peers))
		r.region.Peers = peers
		res, err = nil, nil
	case raftproto.ConfUpdateNode:
		log.Debug("update range peer")
		res, err = nil, nil
	default:
		res, err = nil, ErrUnknownCommandType
	}

	return
}

func (r *Range) HandleGetSnapshot() (model.Snapshot, error) {
	log.Info("[range: %s] get snapshot", r.String())
	return r.rowstore.GetSnapshot()
}

func (r *Range) HandleApplySnapshot(peers []raftproto.Peer, iter *raftgroup.SnapshotKVIterator) error {
	log.Info("range[%s] apply snapshot", r.String())
	start := time.Now()
	defer r.reportEventForStatistics(start.UnixNano(), eventpb.StatisticsType_ApplySnapshot)
	state := r.region.State
	r.region.State = metapb.RangeState_R_LoadSnap
	defer func() {
		r.region.State = state
	}()
	var _peers []*metapb.Peer
	for _, p := range peers {
		peer := &metapb.Peer{
			Id:     r.GetRangeId(),
			NodeId: p.ID,
		}
		_peers = append(_peers, peer)
	}
	sort.Sort(metapb.PeersByNodeIdSlice(_peers))
	log.Info("[range: %d] applay snapshot peers(%d)", r.GetRangeId(), len(peers))

	if err := r.rowstore.ApplySnapshot(r.quitCtx, iter); err != nil {
		return err
	}

	r.region.Peers = _peers
	log.Info("range[%s] apply snapshot success, used %f s", r.String(), time.Since(start).Seconds())
	return nil
}

func (r *Range) HandleLeaderChange(leader uint64) {
	// TODO lock
	//r.lock.Lock()
	//defer r.lock.Unlock()
	//r.region.RangeEpoch.Version++
	log.Info("range[%s] epoch[%v] leader[%d] change!!!", r.String(), r.region.GetRangeEpoch(), leader)
	// 重复当选leader
	if r.leader != 0 && r.leader == leader {
		_, term := r.raftGroup.LeaderTerm()
		if r.region.RangeEpoch.Version < term {
			r.region.RangeEpoch.Version = term
		}
		return
	}
	if leader == 0 {
		r.leader = leader
		r.latestLeaderChange = time.Now()
		log.Debug("no leader now!!!!!")
		return
	}
	_, term := r.raftGroup.LeaderTerm()
	if r.region.RangeEpoch.Version < term {
		r.region.RangeEpoch.Version = term
	}
	// leader发生了变更, 本节点是leader
	if leader == r.server.node.GetId() {
		// 统计丢失leader的时长
		if r.leader == 0 {
			log.Debug("leader change!!!!, finish lost leader")
			if !r.latestLeaderChange.IsZero() {
				r.reportEventForStatistics(r.latestLeaderChange.UnixNano(), eventpb.StatisticsType_LeaderLose)
			}
		}
		r.latestLeaderChange = time.Now()
		r.lastActiveTime = time.Now()
		r.leader = leader
		select {
		case r.leaderChangeChan <- leader:
		default:
			// drop this event, do nothing
		}
	} else if r.leader == r.server.node.GetId() && r.leader != leader {
		if !r.latestLeaderChange.IsZero() {
			r.reportEventForStatistics(r.latestLeaderChange.UnixNano(), eventpb.StatisticsType_LeaderReign)
		}
		r.latestLeaderChange = time.Now()
	}
	r.leader = leader

	if r.region.State == metapb.RangeState_R_Init {
		r.region.State = metapb.RangeState_R_Normal
	}
	return
}

func (r *Range) HandleFatalEvent(err *raft.FatalError) {
	// report error event to master server
	log.Error("range[%s] raft fatal: id[%d], err[%v]", r.String(), err.ID, err.Err)
	event := &eventpb.Event{
		Type: eventpb.EventType_RaftErr,
		EventRaftErr: &eventpb.EventRaftErr{
			NodeId:  r.server.node.GetId(),
			RangeId: r.GetRangeId(),
			Error:   err.Err.Error(),
		},
	}
	r.server.EventCollect(event)
}

func (r *Range) updateEpoch(epoch *metapb.RangeEpoch) {
	//r.lock.Lock()
	//defer r.lock.Unlock()
	//if r.region.RangeEpoch.ConfVer < epoch.ConfVer {
	//	r.region.RangeEpoch.ConfVer = epoch.ConfVer
	//}
	//if r.region.RangeEpoch.Version < epoch.Version {
	//	r.region.RangeEpoch.Version = epoch.Version
	//}
}

func (r *Range) handleRaftKv(req *raft_cmdpb.Request, raftIndex uint64) (resp *raft_cmdpb.Response, err error) {
	//epoch := req.GetRangeEpoch()
	//r.updateEpoch(epoch)

	resp = new(raft_cmdpb.Response)
	resp.CmdType = req.GetCmdType()

	// check range state
	//r.lock.RLock()
	//defer r.lock.RUnlock()
	switch r.region.State {
	case metapb.RangeState_R_Offline:
		resp.Error = &errorpb.Error{RangeOffline: &errorpb.RangeOffline{RangeId: r.GetRangeId()}}
		return
	case metapb.RangeState_R_Split:
		resp.Error = &errorpb.Error{RangeSplit: &errorpb.RangeSplit{RangeId: r.GetRangeId()}}
		return
	}

	// TODO check split status
	switch req.GetCmdType() {
	case raft_cmdpb.CmdType_RawGet:
		_resp, err := r.rowstore.RawGet(req.GetKvRawGetReq())
		if err != nil {
			return nil, err
		}
		resp.KvRawGetResp = _resp
	case raft_cmdpb.CmdType_RawPut:
		_resp, err := r.rowstore.RawPut(req.GetKvRawPutReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvRawPutResp = _resp
	case raft_cmdpb.CmdType_RawDelete:
		_resp, err := r.rowstore.RawDelete(req.GetKvRawDeleteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvRawDeleteResp = _resp

	case raft_cmdpb.CmdType_Select:
		_resp, err := r.rowstore.Select(req.GetKvSelectReq())
		if err != nil {
			return nil, err
		}
		resp.KvSelectResp = _resp
	case raft_cmdpb.CmdType_Insert:
		_resp, err := r.rowstore.Insert(req.GetKvInsertReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvInsertResp = _resp
	case raft_cmdpb.CmdType_Update:
		_resp, err := r.rowstore.Update(req.GetKvUpdateReq())
		if err != nil {
			return nil, err
		}
		resp.KvUpdateResp = _resp
	case raft_cmdpb.CmdType_Replace:
		_resp, err := r.rowstore.Replace(req.GetKvReplaceReq())
		if err != nil {
			return nil, err
		}
		resp.KvReplaceResp = _resp
	case raft_cmdpb.CmdType_Delete:
		_resp, err := r.rowstore.Delete(req.GetKvDeleteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvDeleteResp = _resp
	case raft_cmdpb.CmdType_BatchInsert:
		_resp, err := r.rowstore.BatchInsert(req.GetKvBatchInsertReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.KvBatchInsertResp = _resp
	default:
		resp, err = nil, ErrUnknownCommandType
	}
	return
}

func (r *Range) handleRaftAdmin(req *raft_cmdpb.AdminRequest) (res *raft_cmdpb.AdminResponse, err error) {
	resp := new(raft_cmdpb.AdminResponse)
	resp.CmdType = req.GetCmdType()
	switch req.GetCmdType() {
	/*case raft_cmdpb.AdminCmdType_ChangePeer:*/
	case raft_cmdpb.AdminCmdType_Merge:
		// TODO
	case raft_cmdpb.AdminCmdType_Split:
		splitResp, err := r.handleRaftSplit(req.GetSplitReq())
		if err != nil {
			return nil, err
		}
		resp.SplitResp = splitResp
		res = resp
	default:
		res, err = nil, ErrUnknownCommandType
	}
	return
}

func (r *Range) handleRaftSplit(req *raft_cmdpb.SplitRequest) (res *raft_cmdpb.SplitResponse, err error) {
	splitKey := req.GetSplitKey()
	leftRangeId := req.GetLeftRangeId()
	rightRangeId := req.GetRightRangeId()
	var left, right *metapb.Range
	left, right, err = r.rangeLocalSplit(leftRangeId, rightRangeId, splitKey)
	if err != nil {
		log.Error("range[%s] local split failed, err[%v]", r.String(), err)
		return nil, err
	}
	// 已经分裂完成，不可重复分裂
	if r.region.State == metapb.RangeState_R_Offline {
		res = new(raft_cmdpb.SplitResponse)
		log.Debug("range[%s] split, left[%v], right[%v]", r.String(), left.String(), right.String())
		res.LeftRange = left
		res.RightRange = right
		return
	}
	startTime := time.Now().UnixNano()
	//r.lock.Lock()
	r.region.State = metapb.RangeState_R_Split
	//r.lock.Unlock()
	// split

	log.Info("range[%s] split, key[%v]", r.String(), splitKey)
	var leftPath, rightPath string
	var leftStore, rightStore model.Store
	leftPath = filepath.Join(r.server.conf.DataPath, r.GetDbName(), r.GetTableName(), fmt.Sprintf("range_%d", req.GetLeftRangeId()), "data")
	rightPath = filepath.Join(r.server.conf.DataPath, r.GetDbName(), r.GetTableName(), fmt.Sprintf("range_%d", req.GetRightRangeId()), "data")

	_splitKey := EncodeKey(splitKey)
	leftStore, rightStore, err = r.rowstore.Split(_splitKey, leftPath, rightPath, req.GetLeftRangeId(), req.GetRightRangeId())
	if err != nil {
		log.Error("range[%s] split failed, err[%v]", r.String(), err)
		// TODO report
		event := &eventpb.Event{
			Type: eventpb.EventType_StoreErr,
			EventStoreErr: &eventpb.EventStoreErr{
				NodeId:  r.server.node.GetId(),
				RangeId: r.GetRangeId(),
				Error:   err.Error(),
			},
		}
		r.server.EventCollect(event)
		return nil, err
	}

	rng, err := NewRange(left, r.server, leftStore, true)
	if err != nil {
		log.Error("create split range[%s:%s:%d] failed, err[%v]",
			r.GetDbName(), r.GetTableName(), left.GetId(), err)
		return nil, err
	}
	r.server.AddRange(rng)
	rng, err = NewRange(right, r.server, rightStore, true)
	if err != nil {
		log.Error("create split range[%s:%s:%d] failed, err[%v]",
			r.GetDbName(), r.GetTableName(), right.GetId(), err)
		return nil, err
	}
	r.server.AddRange(rng)
	//r.lock.Lock()
	res = new(raft_cmdpb.SplitResponse)
	res.LeftRange = left
	res.RightRange = right
	r.region.State = metapb.RangeState_R_Offline
	r.reportEventForStatistics(startTime, eventpb.StatisticsType_RaftSplit)
	//r.lock.Unlock()
	log.Info("range[%s] split, left[%v], right[%v]", r.String(), left.String(), right.String())

	return res, nil
}

func (r *Range) rangeLocalSplit(leftRangeId, rightRangeId uint64, splitKey []byte) (*metapb.Range, *metapb.Range, error) {
	// new left range
	var peers []*metapb.Peer
	var ok bool
	if peers, ok = deepcopy.Iface(r.region.Peers).([]*metapb.Peer); !ok {
		return nil, nil, errors.New("internal error")
	}
	for i := 0; i < len(peers); i++ {
		peers[i].Id = leftRangeId
	}
	sort.Sort(metapb.PeersByNodeIdSlice(peers))
	var lStartKey, lEndKey, rStartKey, rEndKey *metapb.Key

	left := &metapb.Range{
		Id:         leftRangeId,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		Peers:      peers,
		State:      metapb.RangeState_R_Init,
		DbId:       r.region.DbId,
		TableId:    r.region.TableId,
		DbName:     r.GetDbName(),
		TableName:  r.GetTableName(),
		CreateTime: time.Now().Unix(),
	}
	lStartKey = r.region.StartKey.Clone()
	_splitKey := make([]byte, len(splitKey))
	copy(_splitKey, splitKey)
	lEndKey = &metapb.Key{Key: _splitKey, Type: metapb.KeyType_KT_Ordinary}
	left.StartKey = lStartKey
	left.EndKey = lEndKey

	// new right range
	if peers, ok = deepcopy.Iface(r.region.Peers).([]*metapb.Peer); !ok {
		return nil, nil, errors.New("internal error")
	}
	for i := 0; i < len(peers); i++ {
		peers[i].Id = rightRangeId
	}
	sort.Sort(metapb.PeersByNodeIdSlice(peers))
	right := &metapb.Range{
		Id:         rightRangeId,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		Peers:      peers,
		State:      metapb.RangeState_R_Init,
		DbId:       r.region.DbId,
		TableId:    r.region.TableId,
		DbName:     r.GetDbName(),
		TableName:  r.GetTableName(),
		CreateTime: time.Now().Unix(),
	}
	rEndKey = r.region.EndKey.Clone()
	_splitKey = make([]byte, len(splitKey))
	copy(_splitKey, splitKey)
	rStartKey = &metapb.Key{Key: _splitKey, Type: metapb.KeyType_KT_Ordinary}
	right.StartKey = rStartKey
	right.EndKey = rEndKey

	r.left = left
	r.right = right
	return left, right, nil
}
