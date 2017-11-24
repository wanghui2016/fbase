package server

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"data-server/client"
	"engine"
	"engine/model"
	"engine/rowstore/opt"
	"model/pkg/errorpb"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/schpb"
	"model/pkg/statspb"
	"raft"
	raftproto "raft/proto"
	"raft/storage/wal"
	"raftgroup"
	"util/deepcopy"
	"util/log"
	"util"
	"model/pkg/taskpb"
	"model/pkg/eventpb"
)

var (
	ErrUnknownCommandType = errors.New("unknown command type")
)

var (
	errStoreOutOfBound = errors.New("store num is out of bound")

	errRangeAlreadyExist  = errors.New("range already exist")
	errRangeMoved         = errors.New("range maybe moved")
	errRangeBusy          = errors.New("range busy with other task")
	errRangeSplit         = errors.New("range busy with split task")
	errRangeShutDown      = errors.New("range is shutdown")
	errNotLeader          = errors.New("raft peer is not leader")
	errUnknownResponeType = errors.New("unknown repsonse type")
	ErrStaleCmd           = errors.New("stale command.")

	DefaultRaftLogCount uint64 = 100000
)

const (
	READ  = 1
	WRITE = 2
)

type Range struct {
	//lock   sync.RWMutex
	region *metapb.Range
	option *metapb.Option
	stats  *statspb.RangeStats
	metric *rangeMetric

	latestLeaderChange time.Time
	leader             uint64
	leaderChangeChan   chan uint64
	insertQueue        chan *InsertBatch

	raftGroup *raftgroup.RaftGroup

	referenceCount int64
	rowstore       *RowStore
	startKey       []byte
	limitKey       []byte

	server *Server

	// 分裂完成
	left           *metapb.Range
	right          *metapb.Range
	lastActiveTime time.Time
	lastTaskId     uint64
	lastCleanRaftLogTime time.Time

	debug          bool

	quitCtx  context.Context
	quitFunc context.CancelFunc
}

func NewRange(c *metapb.Range, server *Server, store model.Store, splitFlag bool) (*Range, error) {
	if c == nil || server == nil {
		return nil, errors.New("invalid param")
	}
	log.Debug("new range [%v]", c)

	// open store
	var _store model.Store
	var err error
	var startKey, endKey []byte
	if c.StartKey.IsNegativeInfinity() {
		startKey = RowPrefix
	} else {
		startKey = EncodeKey(c.StartKey.GetKey())
	}
	if c.EndKey.IsPositiveInfinity() {
		rng := util.BytesPrefix(RowPrefix)
		endKey = rng.Limit
	} else {
		endKey = EncodeKey(c.EndKey.GetKey())
	}
	if store == nil {
		path := filepath.Join(server.conf.DataPath, c.GetDbName(), c.GetTableName(), fmt.Sprintf("range_%d", c.Id), "data")
		storeOpt := &opt.Options{
			BlockCache: server.blockCache,
		}
		_store, _, err = engine.NewRowStore(c.Id, path, startKey, endKey, storeOpt)
		if err != nil {
			return nil, err
		}
	} else {
		_store = store
	}
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("=======range[%d] store applyID %d", c.Id, _store.Applied())
	}
	// new row store
	rs := NewRowStore(c.Id, _store, startKey, endKey, server.clock, server.metricMeter)

	r := &Range{
		region:           c,
		stats:            &statspb.RangeStats{},
		rowstore:         rs,
		startKey:         startKey,
		limitKey:         endKey,
		server:           server,
		metric:           newRangeMetric(c.Id),
		leaderChangeChan: make(chan uint64, 10),
		lastCleanRaftLogTime: time.Now(),
		insertQueue:      make(chan *InsertBatch, 10000),
	}

	r.quitCtx, r.quitFunc = context.WithCancel(context.Background())

	// setup raft log storage
	path := filepath.Join(server.conf.DataPath, c.GetDbName(), c.GetTableName(), fmt.Sprintf("range_%d", c.Id), "raft")
	var wc *wal.Config
	if splitFlag {
		wc = &wal.Config{TruncateFirstDummy: true}
	}
	raftStorage, err := wal.NewStorage(c.Id, path, wc)
	if err != nil {
		_store.Close()
		return nil, err
	}
	var raftPeers []raftproto.Peer
	raftPeers = make([]raftproto.Peer, 0, len(c.Peers))
	for _, p := range c.Peers {
		peer := raftproto.Peer{Type: raftproto.PeerNormal, ID: p.NodeId}
		raftPeers = append(raftPeers, peer)
	}
	raftGroup := raftgroup.NewRaftGroup(r.region.Id, server.raftServer, startKey, endKey)
	raftGroup.RegisterApplyHandle(r.HandleCmd)
	raftGroup.RegisterPeerChangeHandle(r.HandlePeerChange)
	raftGroup.RegisterGetSnapshotHandle(r.HandleGetSnapshot)
	raftGroup.RegisterApplySnapshotHandle(r.HandleApplySnapshot)
	raftGroup.RegisterLeaderChangeHandle(r.HandleLeaderChange)
	raftGroup.RegisterFatalEventHandle(r.HandleFatalEvent)
	var raftConfig *raft.RaftConfig
	// 分裂时指定一个leader
	if splitFlag {
		index := int(r.region.Id) % len(r.region.Peers)
		raftConfig = &raft.RaftConfig{
			ID:           r.region.Id,
			Applied:      _store.Applied(),
			Peers:        raftPeers,
			Storage:      raftStorage,
			StateMachine: raftGroup,
			Term:         1,
			// 默认一个leader
			Leader: raftPeers[index].ID,
		}
	} else {
		raftConfig = &raft.RaftConfig{
			ID:           r.region.Id,
			Applied:      _store.Applied(),
			Peers:        raftPeers,
			Storage:      raftStorage,
			StateMachine: raftGroup,
		}
	}

	r.raftGroup = raftGroup
	err = raftGroup.Create(raftConfig)
	if err != nil {
		_store.Close()
		raftStorage.Close()
		return nil, err
	}
	go r.Heartbeat()
	go r.BatchInsertWorker()
	log.Info("create range[%s] success", r.String())
	return r, nil
}

func (r *Range) rangeRaftStatus(status *raft.Status) *statspb.RaftStatus {
	return &statspb.RaftStatus{
		ID:                status.ID,
		NodeID:            status.NodeID,
		Leader:            status.Leader,
		Term:              status.Term,
		Index:             status.Index,
		Commit:            status.Commit,
		Applied:           status.Applied,
		Vote:              status.Vote,
		PendQueue:         int32(status.PendQueue),
		RecvQueue:         int32(status.RecvQueue),
		AppQueue:          int32(status.AppQueue),
		Stopped:           status.Stopped,
		RestoringSnapshot: status.RestoringSnapshot,
		State:             status.State,
		Replicas: func() []*statspb.ReplicaStatus {
			var repl []*statspb.ReplicaStatus
			for id, r := range status.Replicas {
				repl = append(repl, &statspb.ReplicaStatus{
					Match:       r.Match,
					Commit:      r.Commit,
					Next:        r.Next,
					State:       r.State,
					Snapshoting: r.Snapshoting,
					Paused:      r.Snapshoting,
					Active:      r.Active,
					LastActive:  r.LastActive.Unix(),
					Inflight:    int32(r.Inflight),
					ID:          id,
				})
			}
			return repl
		}(),
	}
}

func (r *Range) heartbeat() {
	defer func() {
		if r := recover(); r != nil {
			fn := func() string {
				n := 10000
				var trace []byte
				for i := 0; i < 5; i++ {
					trace = make([]byte, n)
					nbytes := runtime.Stack(trace, false)
					if nbytes < len(trace) {
						return string(trace[:nbytes])
					}
					n *= 2
				}
				return string(trace)
			}
			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
	if !r.IsLeader() {
		return
	}

	interval := r.server.conf.HeartbeatInterval
	if !r.lastActiveTime.IsZero() {
		t := time.Since(r.lastActiveTime)
		if t > time.Second*time.Duration(interval+interval/2) {
			log.Warn("range[%s] heartbeat interval to long[%s]!!!", r.String(), t.String())
		} else if t < time.Second {
			log.Warn("range[%s] heartbeat too frequently")
			time.Sleep(time.Second)
			return
		}
	}
	if r.debug {
		log.Info("range[%s] start heartbeat!!!!", r.String())
	}
	start := time.Now()

	r.stats.Size_ = r.Size()
	r.stats.Ops, r.stats.BytesInPerSec, r.stats.BytesOutPerSec = r.metric.collect()

	downs := r.raftGroup.GetDownPeers(r.region.GetId())
	pendings := r.raftGroup.GetPendingPeers(r.region.GetId())
	var downPeers []*mspb.PeerStats
	var pendingPeers []*metapb.Peer
	for _, d := range downs {
		p := r.GetPeer(d.NodeID)
		if p == nil {
			log.Error("invalid peer[%d:%d]", r.region.GetId(), d.NodeID)
			continue
		}
		peer := &mspb.PeerStats{Peer: p, DownSeconds: uint64(d.DownSeconds)}
		downPeers = append(downPeers, peer)
	}
	for _, id := range pendings {
		p := r.GetPeer(id)
		if p == nil {
			log.Error("invalid peer[%d:%d]", r.region.GetId(), id)
			continue
		}
		pendingPeers = append(pendingPeers, p)
	}
	leader := r.GetLeader()
	if leader == nil {
		log.Error("range[%s] no leader", r.String())
		return
	}
	raftStatus := r.rangeRaftStatus(r.server.raftServer.Status(r.GetRangeId()))
	//TODO:RangeState_Offline: are you sure that is splitted
	//if r.region.State == metapb.RangeState_R_Offline && r.left != nil && r.right != nil {
	//	rhb = &mspb.RangeHeartbeatRequest{
	//		Range:      deepcopy.Iface(r.region).(*metapb.Range),
	//		Leader:     leader,
	//		LeftRange:  deepcopy.Iface(r.left).(*metapb.Range),
	//		RightRange: deepcopy.Iface(r.right).(*metapb.Range),
	//		LastTaskId: r.lastTaskId,
	//		RaftStatus: raftStatus,
	//	}
	//} else {
	req := &mspb.RangeHeartbeatRequest{
		Range:        deepcopy.Iface(r.region).(*metapb.Range),
		Leader:       leader,
		DownPeers:    downPeers,
		PendingPeers: pendingPeers,
		LastTaskId:   r.lastTaskId,
		Status:       raftStatus,
		Stats:        r.stats,
	}
	resp, err := r.server.mscli.RangeHeartbeat(req)
	if err != nil {
		log.Warn("range[%s] heartbeat failed, err[%v]", r.String(), err)
		return
	}
	r.lastActiveTime = time.Now()
	t := time.Since(start)
	if t > time.Second*2 {
		log.Warn("range[%s] heartbeat time consuming[%s]", r.String(), t.String())
	}
	if r.debug {
		log.Info("range[%s] end heartbeat, consuming[%s]!!!!", r.String(), t.String())
	}
	r.doTask(resp.GetTask())
}

func (r *Range) doTask(task *taskpb.Task) {
	if task == nil {
		return
	}
	log.Info("range[%s] do task[%s]", r.String(), task.String())
	start := time.Now()
	switch task.Type {
	case taskpb.TaskType_RangeSplit:
		split := task.GetRangeSplit()
		err := r.Split(task.GetMeta().GetTaskId(), split.GetSplitKey(), split.GetLeftRangeId(), split.GetRightRangeId())
		if err != nil {
			log.Error("range[%s] split failed, err[%v]", r.String(), err)
		}
	case taskpb.TaskType_RangeAddPeer:
		addPeer := task.GetRangeAddPeer()
		err := r.AddPeer(task.GetMeta().GetTaskId(), addPeer.GetPeer())
		if err != nil {
			log.Error("range[%s] add peer failed, err[%v]", r.String(), err)
		}
	case taskpb.TaskType_RangeDelPeer:
		delPeer := task.GetRangeDelPeer()
		err := r.DelPeer(task.GetMeta().GetTaskId(), delPeer.GetPeer())
		if err != nil {
			log.Error("range[%s] del peer failed, err[%v]", r.String(), err)
		}
	//case mspb.TaskType_RangeDelete:
	//	err := r.Destroy(task.GetMeta())
	//	if err != nil {
	//		log.Error("range[%s] transfer failed, err[%v]", r.String(), err)
	//	}
	case taskpb.TaskType_RangeLeaderTransfer:
		change := task.GetRangeLeaderTransfer()
		err := r.LeaderTransfer(task.GetMeta().GetTaskId(), change.GetExpLeader())
		if err != nil {
			log.Error("range[%s] transfer failed, err[%v]", r.String(), err)
		}
	case taskpb.TaskType_RangeOffline:
		err := r.Offline(task.GetMeta().GetTaskId())
		if err != nil {
			log.Error("range[%s] offline failed, err[%v]", r.String(), err)
		}
	}
	t := time.Since(start)
	if r.debug || t > time.Second {
		log.Info("range[%s] do task time consuming[%s]", t.String())
	}
}

func (r *Range) Heartbeat() {
	interval := r.server.conf.HeartbeatInterval
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := random.Intn(interval)
	ticker := time.NewTimer(time.Duration(index) * time.Second)
	for {
		select {
		case <-r.quitCtx.Done():
			return
		case <-r.leaderChangeChan:
			// 防止连续多次触发心跳，归并操作
		POP_ALL:
			for {
				select {
				case <-r.leaderChangeChan:
				default:
					break POP_ALL
				}
			}
			r.heartbeat()
		case <-ticker.C:
		    if r.debug {
			    log.Info("range[%s] start clean up raft log", r.String())
		    }
		    r.raftLogCleanup(r.server.raftServer)
			if r.debug {
				log.Info("range[%s] end clean up raft log", r.String())
			}
			r.heartbeat()
			ticker.Reset(time.Second * time.Duration(interval))
		}
	}
}

func (r *Range) wait() {
	// 先等待100ms
	time.Sleep(time.Millisecond * 100)
	for {
		if atomic.LoadInt64(&r.referenceCount) == 0 {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (r *Range) Stats() *statspb.RangeStats {
	return r.stats
}

func (r *Range) Clean() {
	r.Close()

	// clean
	path := filepath.Join(r.server.conf.DataPath, r.GetDbName(), r.GetTableName(), fmt.Sprintf("range_%d", r.GetRangeId()))
	err := os.RemoveAll(path)
	if err != nil {
		log.Error("remove store file[%s] failed, err[%v]", path, err)
	}
	log.Info("clean range[%s] success", r.String())
	return
}

// Close close range when server stopped
func (r *Range) Close() {
	select {
	case <-r.quitCtx.Done():
		return
	default:
		r.quitFunc()
	}
	r.wait()
	LOOP:
	for {
		select {
		case <-r.insertQueue:
		default:
			break LOOP
		}
	}
	r.region.State = metapb.RangeState_R_Offline
	r.raftGroup.Release()
	r.rowstore.Close()
	r.rowstore = nil
}

func (r *Range) String() string {
	return fmt.Sprintf("%s:%s:%d", r.region.DbName, r.region.TableName, r.region.GetId())
}

func (r *Range) GetDbName() string {
	return r.region.DbName
}

func (r *Range) GetTableName() string {
	return r.region.TableName
}

func (r *Range) Offline(taskId uint64) error {
	peers := deepcopy.Iface(r.region.Peers).([]*metapb.Peer)
	log.Debug("range[%v] offline ...", r.GetRangeId())
	for _, peer := range peers {
		if err := r.offline(r.server.dsCli, peer, taskId); err != nil {
			log.Info("range[%v] offline error: %v", r.GetRangeId(), err)
			return err
		}
	}

	r.lastTaskId = taskId
	log.Info("range[%s] offline success", r.String())
	return nil
}

func (r *Range) reportDeleteAck(taskId uint64) error {
	var req *mspb.ReportEventRequest
	req = &mspb.ReportEventRequest{
		Event: &eventpb.Event{
			Type: eventpb.EventType_RangeDeleteAck,
			EventRangeDeleteAck: &eventpb.EventRangeDeleteAck{
				TaskId: taskId,
				Range:  deepcopy.Iface(r.region).(*metapb.Range),
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

func (r *Range) deletePeer(cli client.SchClient, peer *metapb.Peer, schId uint64) error {
	var err error
	node, err := r.server.cluster.GetNode(peer.GetNodeId())
	if err != nil {
		log.Error("get node[%d] failed, err[%v]", err)
		return err
	}
	addr := node.GetAddress()
	for i := 0; i < 3; i++ {
		err = cli.DeleteRange(addr, peer.GetId())
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
			continue
		}
		break
	}
	if err != nil {
		log.Error("delete range[%s] in node[%s] failed, err[%v]", r.String(), addr, err)
		return err
	}

	return nil
}

func (r *Range) offline(cli client.SchClient, peer *metapb.Peer, schId uint64) error {
	var err error
	node, err := r.server.cluster.GetNode(peer.GetNodeId())
	if err != nil {
		log.Error("get node[%d] failed, err[%v]", err)
		return err
	}
	addr := node.GetAddress()
	for i := 0; i < 3; i++ {
		err = cli.OfflineRange(addr, peer.GetId())
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
			continue
		}
		break
	}
	if err != nil {
		log.Error("offline range[%s] in node[%s] failed, err[%v]", r.String(), addr, err)
		return err
	}

	return nil
}

func (r *Range) LeaderTransfer(taskId uint64, peer *metapb.Peer) error {
	if !r.raftGroup.IsLeader() {
		return errNotLeader
	}
	// 检查是否满足分裂条件
	downs := r.raftGroup.GetDownPeers(r.region.GetId())
	pendings := r.raftGroup.GetPendingPeers(r.region.GetId())
	if len(downs) > 0 || len(pendings) > 0 {
		return errRangeBusy
	}
	r.lastTaskId = taskId
	// 检查是否可以迁移leader
	status := r.server.raftServer.Status(r.GetRangeId())
	// 根据副本的日志落后情况决定是否截断raft日志
	var preLeaderLogIndex, expLeaderLogIndex uint64
	for nodeId, rep := range status.Replicas {
		if nodeId == r.leader {
			preLeaderLogIndex = rep.Next
		}
		if nodeId == peer.GetNodeId() {
			expLeaderLogIndex = rep.Next
		}
	}
	if preLeaderLogIndex < expLeaderLogIndex {
		log.Error("leader raft log index invalid!!!!")
		return nil
	}
	// TODO 10000 ????
	if preLeaderLogIndex - expLeaderLogIndex < 10000 {
		log.Warn("exp peer raft log backward to mach[%d]", preLeaderLogIndex - expLeaderLogIndex)
		return nil
	}

	var err error
	node, err := r.server.cluster.GetNode(peer.GetNodeId())
	if err != nil {
		log.Error("node[%d] not found, err[%v]", peer.GetNodeId(), err)
		return err
	}
	addr := node.GetAddress()
	for i := 0; i < 3; i++ {
		err = r.server.dsCli.TransferRangeLeader(addr, peer.GetId())
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
			continue
		}
		break
	}
	if err != nil {
		log.Error("range[%s] peer in node[%s] try to leader failed, err[%v]", r.String(), addr, err)
		return err
	}
	log.Info("range[%s] peer in node[%s] try to leader success", r.String(), addr)

	return nil
}

func (r *Range) RaftLeaderTransfer() error {
	return r.raftGroup.LeaderTransfer(r.quitCtx, r.GetRangeId())
}

func (r *Range) GetRangeId() uint64 {
	return r.region.GetId()
}

func (r *Range) raftLogCleanup(raftServer *raft.RaftServer) {
	if time.Since(r.lastCleanRaftLogTime) < time.Minute * 5 {
		return
	}
	// 分裂的range不删除raft log
	if r.region.State == metapb.RangeState_R_Offline || r.region.State == metapb.RangeState_R_Split {
		return
	}
	// 有副本补快照，暂不删除raft log
	pendings := r.raftGroup.GetPendingPeers(r.region.GetId())
	if len(pendings) > 0 {
		r.lastCleanRaftLogTime = time.Now()
		return
	}
	applyId := r.rowstore.Applied()
	if applyId < DefaultRaftLogCount {
		return
	}
	status := r.server.raftServer.Status(r.GetRangeId())
	// 根据副本的日志落后情况决定是否截断raft日志
	var logIndex []uint64
	for _, rep := range status.Replicas {
		logIndex = append(logIndex, rep.Next)
	}
	var maxOffset uint64
	for i := 0; i < len(logIndex); i++ {
		for j := i; j < len(logIndex) - 1; j++ {
			var offset uint64
			if logIndex[j] > logIndex[j + 1] {
				offset = logIndex[j] - logIndex[j + 1]
			} else {
				offset = logIndex[j + 1] - logIndex[j]
			}
			if offset > maxOffset {
				maxOffset = offset
			}
		}
	}
	// 落后太多了，等待......
	if maxOffset > DefaultRaftLogCount {
		// 等待副本追上进度
	} else {
		raftServer.Truncate(r.GetRangeId(), applyId - DefaultRaftLogCount + maxOffset)
	}

	r.lastCleanRaftLogTime = time.Now()
}

func (r *Range) GetPeers() []*metapb.Peer {
	return r.region.GetPeers()
}

func (r *Range) GetPeer(nodeId uint64) *metapb.Peer {
	for _, p := range r.region.GetPeers() {
		if p.GetNodeId() == nodeId {
			return p
		}
	}
	return nil
}

func (r *Range) ChangeMember(op schpb.RangePeerOp, nodeId uint64) error {
	if r.raftGroup.IsLeader() {
		return r.raftGroup.ChangePeer(r.quitCtx, op, nodeId)
	}
	return errNotLeader
}

func (r *Range) IsLeader() bool {
	return r.raftGroup.IsLeader()
}

func (r *Range) CheckKey(key []byte) *errorpb.Error {
	if len(key) <= 0 {
		return nil
	}
	_key := &metapb.Key{Key: key, Type: metapb.KeyType_KT_Ordinary}
	if metapb.Compare(_key, r.region.StartKey) < 0 || metapb.Compare(_key, r.region.EndKey) >= 0 {
		return &errorpb.Error{
			KeyNotInRange: &errorpb.KeyNotInRange{
				Key:      key,
				RangeId:  r.GetRangeId(),
				StartKey: r.region.StartKey,
				EndKey:   r.region.EndKey,
			},
		}
	}
	return nil
	//var ret1, ret2 int
	//ret1 = 1
	//ret2 = -1
	////log.Info("id: %d, start: %s, end: %s", r.Id, string(r.StartKey), string(r.EndKey))
	//if len(r.region.StartKey) > 0 {
	//	ret1 = bytes.Compare(key, r.region.StartKey)
	//}
	//if len(r.region.EndKey) > 0 {
	//	ret2 = bytes.Compare(key, r.region.EndKey)
	//}
	//if ret1 >= 0 && ret2 < 0 {
	//	return nil
	//}
	//return &errorpb.Error{
	//	KeyNotInRange: &errorpb.KeyNotInRange{
	//		Key:      key,
	//		RangeId:  r.GetRangeId(),
	//		StartKey: r.region.StartKey,
	//		EndKey:   r.region.EndKey,
	//	},
	//}
}

func (r *Range) GetLeader() *metapb.Leader {
	for _, p := range r.region.Peers {
		if p.GetNodeId() == r.leader {
			node, err := r.server.cluster.GetNode(p.GetNodeId())
			if err != nil {
				log.Error("node[%d] not found, err[%v]", p.GetNodeId(), err)
				return nil
			}
			return &metapb.Leader{RangeId: r.GetRangeId(), NodeId: p.GetNodeId(), NodeAddr: node.GetAddress()}
		}
	}
	return nil
}

//
func (r *Range) CheckWriteAble() *errorpb.Error {
	//r.lock.RLock()
	//defer r.lock.RUnlock()
	if !r.IsLeader() {
		leader := r.GetLeader()
		// TODO lock???
		if leader == nil {
			log.Debug("no leader")
			return &errorpb.Error{
				NoLeader: &errorpb.NoLeader{
					RangeId: r.GetRangeId(),
				},
			}
		} else {
			log.Debug("not leader")
			return &errorpb.Error{
				NotLeader: &errorpb.NotLeader{
					RangeId: r.GetRangeId(),
					Epoch:   r.region.RangeEpoch,
					Leader:  leader,
				},
			}
		}
	}

	if r.region.State == metapb.RangeState_R_Offline {
		log.Debug("range offline[%d]", r.GetRangeId())
		return &errorpb.Error{
			RangeOffline: &errorpb.RangeOffline{
				RangeId: r.GetRangeId(),
			},
		}
	}
	if r.region.State == metapb.RangeState_R_Abnormal {
		log.Debug("range damage[%d]", r.GetRangeId())
		return &errorpb.Error{
			RangeDamage: &errorpb.RangeDamage{
				RangeId: r.GetRangeId(),
			},
		}
	}
	if r.region.State == metapb.RangeState_R_Split {
		log.Debug("range offline[%d]", r.GetRangeId())
		return &errorpb.Error{
			RangeSplit: &errorpb.RangeSplit{
				RangeId: r.GetRangeId(),
			},
		}
	}
	return nil
}

func (r *Range) CheckReadAble(repReadAble bool) *errorpb.Error {
	//r.lock.RLock()
	//defer r.lock.RUnlock()
	if !repReadAble {
		if !r.IsLeader() {
			leader := r.GetLeader()
			// TODO lock???
			if leader == nil {
				log.Debug("no leader")
				return &errorpb.Error{
					NoLeader: &errorpb.NoLeader{
						RangeId: r.GetRangeId(),
					},
				}
			} else {
				log.Debug("not leader")
				return &errorpb.Error{
					NotLeader: &errorpb.NotLeader{
						RangeId: r.GetRangeId(),
						Epoch:   r.region.RangeEpoch,
						Leader:  leader,
					},
				}
			}
		}
	}

	if r.region.State == metapb.RangeState_R_Offline {
		log.Debug("range offline[%d]", r.GetRangeId())
		return &errorpb.Error{
			RangeOffline: &errorpb.RangeOffline{
				RangeId: r.GetRangeId(),
			},
		}
	}

	return nil
}

func (r *Range) Size() uint64 {
	select {
	case <-r.quitCtx.Done():
		return 0
	default:
	}
	atomic.AddInt64(&r.referenceCount, 1)
	defer atomic.AddInt64(&r.referenceCount, -1)
	return r.rowstore.Size()
}

func (r *Range) RangeSplitPrepare() bool {
	return r.rowstore.PrepareSplit()
}

func mapCode(err error) int32 {
	var code int32
	if err == raft.ErrNotLeader {
		code = ER_NOT_LEADER
	} else if err == raft.ErrSnapping {
		code = ER_SERVER_BUSY
	} else if err == raft.ErrStopped {
		code = ER_SERVER_STOP
	} else {
		code = ER_UNKNOWN
	}
	return code
}

func (r *Range) reportEventForStatistics(startTime int64, statisticsType eventpb.StatisticsType) error {
	event := &eventpb.Event{
		Type: eventpb.EventType_EventStatistics,
		EventEventStatistics: &eventpb.EventEventStatistics{
			DbName:         r.GetDbName(),
			TableName:      r.GetTableName(),
			RangeId:        r.GetRangeId(),
			NodeId:         r.server.node.GetId(),
			StartTime:      startTime,
			EndTime:        time.Now().UnixNano(),
			StatisticsType: statisticsType,
		},
	}
	r.server.EventCollect(event)
	return nil
}
