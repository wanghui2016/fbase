package server

import (
	"time"
	"fmt"
	"compress/flate"
	"io/ioutil"
	"bytes"

	"model/pkg/mspb"
	"github.com/juju/errors"
	"util/log"
	"util/deepcopy"
	"model/pkg/metapb"
	"golang.org/x/net/context"
	"model/pkg/statspb"
	"model/pkg/taskpb"
	"model/pkg/eventpb"
	"github.com/golang/protobuf/proto"
)

func (service *Server) handleGetTopologyEpoch(ctx context.Context, req *mspb.GetTopologyEpochRequest) (resp *mspb.GetTopologyEpochResponse, err error) {
	if !service.IsLeader() {
		return nil, ErrNotLeader
	}
	cluster := service.cluster
	dbId := req.GetDbId()
	tId := req.GetTableId()
	epoch := req.GetCurEpoch()
	if dbId == 0 || tId == 0 {
		log.Error("invalid param")
		return nil, ErrInvalidParam
	}
	t, find := cluster.FindCurTableById(dbId, tId)
	if !find {
		log.Warn("table not exist table[%d:%d]", dbId, tId)
		return nil, ErrNotExistTable
	}
	topo, err := t.GetTopologyEpoch(cluster, epoch)
	if err != nil {
		log.Warn("table[%d:%d] get topology failed, err[%v]", dbId, tId, err)
		return nil, err
	}
	resp = new(mspb.GetTopologyEpochResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.Routes = topo.GetRoutes()
	resp.Epoch = topo.GetEpoch()
	resp.RwPolicy = topo.GetRwPolicy()
	return
}

func (service *Server) handleNodeHeartbeat(ctx context.Context, req *mspb.NodeHeartbeatRequest) (resp *mspb.NodeHeartbeatResponse) {
	cluster := service.cluster

	nodeId := req.GetNodeId()
	log.Debug("node heartbeat[%d]", nodeId)
	resp = new(mspb.NodeHeartbeatResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.NodeId = nodeId
	node, find := cluster.FindInstance(nodeId)
	if !find {
		log.Error("invalid node[%d], node not in resources pool", nodeId)
		return
	}

	// set node stats score
	nodeStats := req.GetStats()
	clsAllLeaderSize := service.cluster.instances.LeaderSize()
	node.caculateNodeScore(nodeStats, clsAllLeaderSize, service.cluster.ranges, nodeId)

	node.NotWorkingRanges = req.GetRangesOffline()
	if len(node.NotWorkingRanges) > 0 {
		log.Info("node[%s] not workding ranges[%v]", node.GetAddress(), node.NotWorkingRanges)
	}
	node.RangeSplitCount = req.GetRangeSplitCount()
	node.RangeCount = req.GetRangeCount()
	node.RangeLeaderCount = req.GetRangeLeaderCount()
	node.ApplyingSnapCount = req.GetApplyingSnapCount()
	node.SendingSnapCount = req.GetSendingSnapCount()
	node.AllRanges = req.GetRanges()
	node.Stats = req.GetStats()
	node.ProcessStats = req.GetProcessStats()
	node.LastHbTime = time.Now()

	var task *taskpb.Task
	switch req.GetNodeState() {
	case metapb.NodeState_N_Logout:
		if node.GetState() != metapb.NodeState_N_Initial {
			break
		}
		log.Info("node[%v] re-init")
		fallthrough
	case metapb.NodeState_N_Initial:
		if node.GetState() == metapb.NodeState_N_Logout {

			break
		}
		log.Info("node[%s] first login", node.GetAddress())
		ranges := node.GetAllRanges()
		var rngs []*metapb.Range
		for _, r := range ranges {
			rng := deepcopy.Iface(r.Range).(*metapb.Range)
			//log.Debug("range[%v]", rng)
			rngs = append(rngs, rng)
		}
		taskId, err := cluster.GenTaskId()
		if err != nil {
			log.Error("gen task ID failed, err[%v]", err)
			return
		}
		task =  &taskpb.Task{
			Type:     taskpb.TaskType_NodeLogin,
			Meta:   &taskpb.TaskMeta{
				TaskId:   taskId,
				CreateTime: time.Now().Unix(),
				State: taskpb.TaskState_TaskWaiting,
				Timeout: DefaultChangeLeaderTaskTimeout,
			},
			NodeLogin: &taskpb.TaskNodeLogin{Ranges: rngs},
		}
		resp.Task = task
	// 正常心跳
	case metapb.NodeState_N_Login:
		switch node.State {
		case metapb.NodeState_N_Initial:
			log.Info("node[%s] login success!!", node.GetAddress())
			err := cluster.UpdateNodeState(node, metapb.NodeState_N_Login)
			if err != nil {
				log.Error("update node[%s] state failed, err[%v]", node.GetAddress(), err)
			}
		case metapb.NodeState_N_Upgrade:
			log.Info("node[%s] upgrade success!!", node.GetAddress())
			err := cluster.UpdateNodeState(node, metapb.NodeState_N_Login)
			if err != nil {
				log.Error("update node[%s] state failed, err[%v]", node.GetAddress(), err)
			}
		//　已经被认定未故障的节点心跳又恢复了,暂时仍然认为故障
		case metapb.NodeState_N_Tombstone:
			log.Warn("node[%s] failover and recover heartbeat, we need doubt the node", node.GetAddress())
			//taskId, err := service.cluster.GenTaskId()
			//if err != nil {
			//	log.Error("gen task ID failed, err[%v]", err)
			//	return
			//}
			//task = &taskpb.Task{
			//	Type:     taskpb.TaskType_NodeLogout,
			//	Meta:   &taskpb.TaskMeta{
			//		TaskId:   taskId,
			//		CreateTime: time.Now().Unix(),
			//		State: taskpb.TaskState_TaskWaiting,
			//		SrcNodeId: node.GetId(),
			//		DstNodeId: node.GetId(),
			//		Timeout: DefaultChangeLeaderTaskTimeout,
			//	},
			//	NodeLogout: new(taskpb.TaskNodeLogout),
			//}
			//resp.Task = task
			err := cluster.UpdateNodeState(node, metapb.NodeState_N_Doubt)
			if err != nil {
				log.Error("update node[%s] state failed, err[%v]", node.GetAddress(), err)
			}
		case metapb.NodeState_N_Doubt:
			log.Info("node[%s] remove doubt!!", node.GetAddress())
			err := cluster.UpdateNodeState(node, metapb.NodeState_N_Login)
			if err != nil {
				log.Error("update node[%s] state failed, err[%v]", node.GetAddress(), err)
			}
		case metapb.NodeState_N_Login:
			// 领取任务
		    task := node.Dispatch(service.cluster)
			if task != nil {
				resp.Task = task.Task
				log.Info("node[%s] try to delete ranges, task[%s]", node.GetAddress(), task.Describe())
			}
		}
	}
	return
}

func (service *Server) handleRangeHeartbeat(ctx context.Context, req *mspb.RangeHeartbeatRequest) (resp *mspb.RangeHeartbeatResponse) {
	cluster := service.cluster
	alarm := service.alarm
	//store := service.store
	//router := service.router
	coordinator := service.coordinator
    // update stats
	r := req.GetRange()

	applyTaskId := req.GetLastTaskId()
	resp = new(mspb.RangeHeartbeatResponse)
	resp.Header = &mspb.ResponseHeader{}
	resp.RangeId = r.GetId()
	if req.GetLeader() == nil {
		log.Error("invalid range[%s:%s:%d] leader", r.GetDbName(), r.GetTableName(), r.GetId())
		return
	}
	metric := make(map[string]time.Duration)
	sstart := time.Now()
	start := sstart
	region, find := cluster.FindRange(r.GetId())
	if !find {
		if _, ok := cluster.FindRangeInMissCache(r.GetId()); ok {
			return
		} else {
			cluster.AddRangeInMissCache(NewRange(r))
		}
		log.Error("range[%s:%s:%d] not found", r.GetDbName(), r.GetTableName(), r.GetId())
		return
	}
	if region.Trace || log.IsEnableDebug() {
		log.Info("[HB] range[%s] heartbeat", region.SString())
	}

	// TODO open set stats score
	// region.caculateRangeScore(req.GetStats())
	t := time.Since(start)
	metric["find_range"] = t
	var saveStore, saveCache bool
	// Range meta is stale, return.
	//if r.GetRangeEpoch().GetVersion() < region.GetRangeEpoch().GetVersion() ||
	//	r.GetRangeEpoch().GetConfVer() < region.GetRangeEpoch().GetConfVer() {
	if r.GetRangeEpoch().GetVersion() < region.GetRangeEpoch().GetVersion() {
		if region.Trace || log.IsEnableDebug() {
			log.Info("[HB] range[%s] stale epoch", region.SString())
		}
		message := fmt.Sprintf("range[%s] meta is stale[%v, %v], leader[%s] stale leader[%s]",
			region.SString(), r.GetRangeEpoch(), region.GetRangeEpoch(), region.GetLeader().GetNodeAddr(), req.GetLeader().GetNodeAddr())
		log.Error(message)
		// 一定是异常了，触发一次version校正
		if time.Since(region.LastHbTime) > time.Minute * 5 {
			alarm.Alarm("分片心跳异常", message)
			if len(req.GetDownPeers()) == 0 && len(req.GetPendingPeers()) == 0 {
				region.RangeEpoch = r.GetRangeEpoch()
				saveCache = true
			}
		} else {
			if region.Trace || log.IsEnableDebug() {
				log.Info("[HB] range[%s] stale epoch and back", region.SString())
			}
			return
		}
	}

	// Range meta is updated, update store and cache.
	//if r.GetRangeEpoch().GetVersion() > region.GetRangeEpoch().GetVersion() ||
	//	r.GetRangeEpoch().GetConfVer() > region.GetRangeEpoch().GetConfVer() {
	if r.GetRangeEpoch().GetVersion() > region.GetRangeEpoch().GetVersion() {
		saveCache = true
		saveStore = true
	}
	if r.State != region.State {
		switch region.State {
		case metapb.RangeState_R_Init:
			saveStore = true
			saveCache = true
		case metapb.RangeState_R_Normal:
			saveStore = true
			saveCache = true
		default:
			switch r.State {
			// 状态异常，比如副本分裂失败，后又当选为leader
			case metapb.RangeState_R_Split:
				log.Warn("range[%s] state[%s, %s] error!!", region.SString(), region.State, r.State)
			case metapb.RangeState_R_Offline:
				log.Warn("range[%s] state[%s, %s] error!!", region.SString(), region.State, r.State)
				// 路由有变化，需要主动推送一次路由表
			default:
				saveStore = true
				saveCache = true
			}
		}
	}
	// 校验一次
	if !saveCache {
		if region.Trace || log.IsEnableDebug() {
			log.Info("[HB] range[%s] check peers!!!", region.SString())
		}
		// 成员不一致
		if len(r.GetPeers()) != len(region.GetPeers()) {
			saveCache = true
			saveStore = true
		} else {
			for _, p := range r.GetPeers() {
				if region.GetPeer(p.GetNodeId()) == nil {
					saveCache = true
					saveStore = true
					break
				}
			}
			if !saveCache {
				for _, p := range region.GetPeers() {
					find := false
					for _, _p := range r.GetPeers() {
						if p.GetNodeId() == _p.GetNodeId() {
							find = true
							break
						}
					}
					if !find {
						saveCache = true
						saveStore = true
						break
					}
				}
			}
		}
		//region.LastCheckTime = time.Now()
	}

	if req.GetLeader().GetNodeId() != region.Leader.GetNodeId() {
		if region.Trace || log.IsEnableDebug() {
			log.Info("[HB] update range[%s] leader", region.SString())
		}
		// just log error for this case
		if r.GetRangeEpoch().GetVersion() == region.GetRangeEpoch().GetVersion() {
			if region.Leader.GetNodeId() != 0 {
				log.Error("range[%s] epoch[remote %d, local %d] remote leader[%s] local leader[%s] has error",
					region.SString(), r.GetRangeEpoch().GetVersion(), region.GetRangeEpoch().GetVersion(),
					req.GetLeader().GetNodeAddr(), region.Leader.GetNodeAddr())
			}
		}

		region.Leader = req.GetLeader()
		saveCache = true
	}
	if region.Trace || log.IsEnableDebug() {
		log.Info("[HB] range[%s] update info", region.SString())
	}
	region.Stats = req.GetStats()
	region.DownPeers = req.GetDownPeers()
	region.PendingPeers = req.GetPendingPeers()
	region.LastHbTime = time.Now()
	region.RaftStatus = req.GetStatus()
	if saveStore {
		start = time.Now()
		err := cluster.storeRange(r)
		if err != nil {
			log.Error("store range[%s] failed, err[%v]", region.SString(), err)
			return
		}
		t = time.Since(start)
		metric["save_range"] = t
	}
	if saveCache {
		// 更新node的分片副本信息
		start = time.Now()
		for _, p := range region.GetPeers() {
			node, ok := cluster.FindInstance(p.GetNodeId())
			if !ok {
				log.Error("node[%d] not found", p.GetNodeId())
				continue
			}
			if region.Trace || log.IsEnableDebug() {
				log.Info("node[%s] delete range[%s] peer in cache",
					node.GetAddress(), region.SString())
			}
			node.DeleteRange(p.GetId())
		}
		region.Range = r
		for _, p := range r.GetPeers() {
			node, ok := cluster.FindInstance(p.GetNodeId())
			if !ok {
				log.Error("node[%d] not found", p.GetNodeId())
				continue
			}
			if region.Trace || log.IsEnableDebug() {
				log.Info("node[%s] add range[%s] peer in cache", node.GetAddress(), region.SString())
			}
			node.AddRange(region)
		}
		if region.State == metapb.RangeState_R_Offline && region.OfflineTime.IsZero(){
			region.OfflineTime = time.Now()
		}
		t = time.Since(start)
		metric["save_range_cache"] = t
	}
	start = time.Now()
	if region.Trace || log.IsEnableDebug() {
		log.Info("[HB] range[%s] dispatch task", region.SString())
	}
	task := coordinator.Dispatch(region, applyTaskId)
	if task != nil {
		if region.Trace || log.IsEnableDebug() {
			log.Info("[HB] range[%s] leader[%s] dispatch task[%s]",
				region.SString(), region.Leader.GetNodeAddr(), task.Describe())
		}
		resp.Task = deepcopy.Iface(task.Task).(*taskpb.Task)
	}
	t = time.Since(start)
	metric["dispatch"] = t
	t = time.Since(sstart)
	if t > time.Second {
		var details string
		for item, delayed := range metric {
			details += fmt.Sprintf("item[%s] delayed[%s] ", item, delayed.String())
		}
		log.Warn("[handleRangeHeartbeat] do time out[%s], details[%s]", t.String(), details)
	}
	return
}

func tpXX(tp *statspb.SqlTp, n float64) int64 {
	M := int64(float64(tp.GetCalls())*n)
	var m int64
	var i statspb.TpArgs

	for i = 0; i < statspb.TpArgs_min_index; i++ {
		log.Error("===== %v", i)
		m += tp.Tp[i]
		if m >= M {
			break
		}
	}
	if i == statspb.TpArgs_min_index {
		log.Error("tp list out of index")
		return -1
	}

	if i < statspb.TpArgs_msec_index {
		return int64(i)// msec
	} else if i < statspb.TpArgs_sec_index {
		return int64(i)*1000 // sec
	} else {
		return int64(i)*1000*60 // min
	}
}

func (service *Server) handleGatewayHeartbeat(ctx context.Context, req *mspb.GatewayHeartbeatRequest) (resp *mspb.GatewayHeartbeatResponse) {
	if !service.IsLeader() {
		log.Error("handle gateway heartbeat: %v", ErrNotLeader)
		return nil
	}
	resp = new(mspb.GatewayHeartbeatResponse)
	resp.Header = &mspb.ResponseHeader{}

	//log.Debug("handleGatewayHeartbeat ...: %v", req.GetStats().GetTp())
	result, err := ioutil.ReadAll(flate.NewReader(bytes.NewReader([]byte(req.GetStats().GetTp()))))
	if err != nil {
		log.Error("handle gateway heartbeat: %v", ErrNotLeader)
		return nil
	}
	tp := new(statspb.SqlTp)
	if err := proto.Unmarshal(result, tp); err != nil {
		log.Error("tp unmarshal error: %v", err)
		return nil
	}
	//log.Debug("--------- stats: %v", *tp)

	//trips := func() (tpxx string) {
	//	//tp := st.GetTrip()
	//	//if tp == nil {
	//	//	return
	//	//}
	//	//tp999 := tpXX(tp, 0.999)
	//	//tp99 := tpXX(tp, 0.99)
	//	//tp50 := tpXX(tp, 0.5)
	//	//tpxx += "tp999: " + fmt.Sprint(tp999) + "\r\n"
	//	//tpxx += "tp99: " + fmt.Sprint(tp99) + "\r\n"
	//	//tpxx += "tp50: " + fmt.Sprint(tp50) + "\r\n"
	//	return
	//}()
	//if err := service.pusher.Push(GatewayMonitorSQL, time_, address, command, calls, totalUsec, parseUsec, callUsec, hits, misses, slowlogs, trips, (service.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
	//	log.Error("pusher gateway info error: %v", err)
	//} else {
	//	log.Debug(GatewayMonitorSQL, time_, address, command, calls, totalUsec, parseUsec, callUsec, hits, misses, slowlogs, trips, (service.conf.MonitorTTL+time.Now().UnixNano())/1000000)
	//}
	return resp
}

func (service *Server) handleReportEvent(ctx context.Context, req *mspb.ReportEventRequest) (resp *mspb.ReportEventResponse, err error) {
	if !service.IsLeader() {
		return nil, ErrNotLeader
	}
	cluster := service.cluster
	pusher := service.pusher
	router := service.router
	event := req.GetEvent()
	switch event.GetType() {
	case eventpb.EventType_RangeSplitAck:
		splitAck := event.GetEventRangeSplitAck()
		err = cluster.RangeSplitAck(splitAck, router)
		if err != nil {
			return
		}
	case eventpb.EventType_RangeSplitKey:
		splitKey := event.GetEventRangeSplitKey()
		err = cluster.RangeSplitKey(splitKey)
		if err != nil {
			return
		}
	case eventpb.EventType_RangeDeleteAck:
		deleteAck := event.GetEventRangeDeleteAck()
		err = cluster.RangeDeleteAck(deleteAck, pusher)
		if err != nil {
			return
		}
	case eventpb.EventType_RaftErr:
		err = service.cluster.RaftError(event.GetEventRaftErr())
		if err != nil {
			return
		}
	case eventpb.EventType_StoreErr:
		err = service.cluster.StoreError(event.GetEventStoreErr(), service.pusher)
		if err != nil {
			return
		}
	case eventpb.EventType_NodeBuildAck:
		buildIp := event.GetEventNodeBuildAck().GetIp()
		buildKey := event.GetEventNodeBuildAck().GetKey()
		buildErr := event.GetEventNodeBuildAck().GetErr()
		if buildErr == "" {
			log.Info("event report: build node %s version %s success", buildIp, buildKey)
		} else {
			log.Error("event report: build node %s version %s failed: %s", buildIp, buildKey, buildErr)
		}
	case eventpb.EventType_EventStatistics:
		statistics := event.GetEventEventStatistics()
		if err := pusher.Push(EventStatisticsSQL, cluster.GetClusterId(), statistics.GetDbName(), statistics.GetTableName(), statistics.GetRangeId(), statistics.GetNodeId(),
			statistics.GetStartTime(), statistics.GetEndTime(),statistics.GetStatisticsType().String(), (service.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Warn("pusher event statistics error: %s", err.Error())
		}
		log.Info("range[%s:%s:%d] event[%s] report success!!!!", statistics.GetDbName(), statistics.GetTableName(),
			statistics.GetRangeId(), statistics.GetStatisticsType().String())
	}
	resp = new(mspb.ReportEventResponse)
	resp.Header = &mspb.ResponseHeader{}
	return
}

func (service *Server) handleGetMsLeader(ctx context.Context, req *mspb.GetMSLeaderRequest) (resp *mspb.GetMSLeaderResponse, err error) {
	resp = &mspb.GetMSLeaderResponse{Header: &mspb.ResponseHeader{}}
	point := service.GetLeader()
	if point == nil {
		err = errors.New("no leader at this time")
		log.Error(err.Error())
		return nil, err
	}
	resp.Leader = &mspb.MSLeader{
		Id: point.Id,
		Address: point.RpcAddress,
	}

	return
}

func (service *Server) handleGetDb(ctx context.Context, req *mspb.GetDBRequest) (resp *mspb.GetDBResponse, err error) {
	resp = new(mspb.GetDBResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbname := req.GetName()
	if dbname == ""{
		return nil, ErrInvalidParam
	}
	db, ok := service.cluster.FindDatabase(dbname)
	if !ok {
		log.Error("invalid database[%s]", dbname)
		return nil, ErrNotExistDatabase
	}
	resp.Db = db.DataBase

	return
}

func (service *Server) handleGetTable(ctx context.Context, req *mspb.GetTableRequest) (resp *mspb.GetTableResponse, err error) {
	resp = new(mspb.GetTableResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tname := req.GetTableName()

	if dbId == 0 || tname == "" {
		return nil, ErrInvalidParam
	}
	d, ok := service.cluster.FindDatabaseById(dbId)
	if !ok {
		log.Error("invalid database[%d]", dbId)
		return nil, ErrNotExistDatabase
	}
	t, ok := d.FindCurTable(tname)
	if !ok {
		log.Error("invalid table[%s:%s]", d.GetName(), tname)
		return nil, ErrNotExistTable
	}
	if t.GetTask() != nil {
		log.Warn("table[%s:%s] not ready", d.GetName(), tname)
		return nil, ErrNotExistTable
	}
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	resp.Table = table

	return
}

func (service *Server) handleGetTableById(ctx context.Context, req *mspb.GetTableByIdRequest) (resp *mspb.GetTableByIdResponse, err error) {
	resp = new(mspb.GetTableByIdResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()

	if dbId == 0 || tId == 0 {
		return nil, ErrInvalidParam
	}
	d, ok := service.cluster.FindDatabaseById(dbId)
	if !ok {
		log.Error("invalid database[%d]", dbId)
		return nil, ErrNotExistDatabase
	}
	t, ok := d.FindTableById(tId)
	if !ok {
		log.Error("invalid table[%s:%d]", d.GetName(), tId)
		return nil, ErrNotExistTable
	}
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	resp.Table = table

	return
}

func (service *Server) handleGetColumns(ctx context.Context, req *mspb.GetColumnsRequest) (resp *mspb.GetColumnsResponse, err error) {
	resp = new(mspb.GetColumnsResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()

	if dbId == 0 || tId == 0 {
		return nil, ErrInvalidParam
	}
	t, ok := service.cluster.FindCurTableById(dbId, tId)
	if !ok {
		return nil, ErrNotExistTable
	}
	resp.Columns = t.GetColumns()

	return
}

func (service *Server) handleAddColumns(ctx context.Context, req *mspb.AddColumnRequest) (resp *mspb.AddColumnResponse, err error) {
	resp = new(mspb.AddColumnResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()
	columns := req.GetColumns()

	if dbId == 0 || tId == 0 || columns == nil || len(columns) == 0{
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindCurTableById(dbId, tId)
	if !ok {
		return nil, errors.New("table is not existed")
	}

	cols,err := t.UpdateSchema(columns,service.cluster.store)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("column add err %s",err.Error()))
	}
	resp.Columns=cols

	return
}

func (service *Server) handleGetColumnByName(ctx context.Context, req *mspb.GetColumnByNameRequest) (resp *mspb.GetColumnByNameResponse, err error) {
	resp = new(mspb.GetColumnByNameResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()
	name := req.GetColName()

	if dbId == 0 || tId == 0 || name == ""{
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindCurTableById(dbId, tId)
	if !ok {
		return nil, errors.New("table is not existed")
	}
	c, ok := t.GetColumnByName(name)
	if !ok {
		return nil, errors.New("column is not existed")
	}
	resp.Column = c
	return
}

func (service *Server) handleGetColumnById(ctx context.Context, req *mspb.GetColumnByIdRequest) (resp *mspb.GetColumnByIdResponse, err error) {
	resp = new(mspb.GetColumnByIdResponse)
	resp.Header = &mspb.ResponseHeader{}
	dbId := req.GetDbId()
	tId := req.GetTableId()
	id := req.GetColId()

	if dbId == 0 || tId == 0 {
		return nil, errors.New("parameter is nil")
	}
	t, ok := service.cluster.FindCurTableById(dbId, tId)
	if !ok {
		return nil, errors.New("table is not existed")
	}
	c, ok := t.GetColumnById(id)
	if !ok {
		return nil, errors.New("column is not existed")
	}
	resp.Column = c
	return
}

func (service *Server) handleGetNode(ctx context.Context, req *mspb.GetNodeRequest) (resp *mspb.GetNodeResponse, err error) {
	resp = new(mspb.GetNodeResponse)
	resp.Header = &mspb.ResponseHeader{}
	id := req.GetId()

	n, ok := service.cluster.FindInstance(id)
	if !ok {
		return nil, ErrNotExistNode
	}
	node := deepcopy.Iface(n.Node).(*metapb.Node)
	resp.Node = node
	return
}

func (n *Instance) caculateNodeScore(stats *statspb.NodeStats, clsAllLeaderSize int, ranges *RangeCache, nodeId uint64){
	if stats == nil{
		return
	}

	cpuScore := stats.CpuProcRate * MasterConf.NodeCpuWeight
	if stats.CpuProcRate > MasterConf.NodeCpuProcRateThreshold {
		cpuScore = cpuScore * MasterConf.NodeScoreMulti
	}
	memScore := stats.MemoryUsedPercent * MasterConf.NodeMemoryWeight
	if stats.MemoryUsedPercent > MasterConf.NodeMemoryUsedPercentThreshold {
		memScore = memScore * MasterConf.NodeScoreMulti
	}
	diskScore := stats.DiskProcRate * MasterConf.NodeDiskWeight
	if stats.DiskProcRate > MasterConf.NodeDiskProcRateThreshold {
		diskScore = diskScore * MasterConf.NodeScoreMulti
	}
	swapScore := stats.SwapMemoryUsedPercent * MasterConf.NodeSwapWeight
	if stats.SwapMemoryUsedPercent > MasterConf.NodeSwapUsedPercentThreshold {
		swapScore = swapScore * MasterConf.NodeScoreMulti
	}

	diskReadPerc := (float64(stats.DiskReadBytePerSec) / MasterConf.NodeTotalDiskRead)*100
	diskReadScore := diskReadPerc * MasterConf.NodeDiskReadWeight
	if diskReadPerc > MasterConf.NodeDiskReadPercThreshold {
		diskReadScore = diskReadScore * MasterConf.NodeScoreMulti
	}

	diskWritePerc := (float64(stats.DiskWriteBytePerSec) / MasterConf.NodeTotalDiskWrite)*100
	diskWriteScore := diskWritePerc * MasterConf.NodeDiskWriteWeight
	if diskWritePerc > MasterConf.NodeDiskWritePercThreshold {
		diskWriteScore = diskWriteScore * MasterConf.NodeScoreMulti
	}

	netInPerc := (float64(stats.NetIoInBytePerSec) / MasterConf.NodeTotalNetIn)*100
	netInScore := netInPerc * MasterConf.NodeNetInWeight
	if netInPerc > MasterConf.NodeNetInPercThreshold {
		netInScore = netInScore * MasterConf.NodeScoreMulti
	}

	netOutPerc := (float64(stats.NetIoOutBytePerSec)/ MasterConf.NodeTotalNetOut)*100
	netOutScore := netOutPerc * MasterConf.NodeNetOutWeight
	if netOutPerc > MasterConf.NodeNetOutPercThreshold {
		netOutScore = netOutScore * MasterConf.NodeScoreMulti
	}

	rangeCountScore := 0.0
	clsAllRangeSize := ranges.Size()
	if clsAllRangeSize > 0{
		rangeCountPerc := (float64(n.RangeCount)/float64(clsAllRangeSize*3))*100
		rangeCountScore = rangeCountPerc * MasterConf.NodeRangeCountWeight
		if rangeCountPerc > MasterConf.NodeRangeCountPercThreshold {
			rangeCountScore = rangeCountScore * MasterConf.NodeScoreMulti
		}
	}

	leaderCountScore := 0.0
	if clsAllLeaderSize > 0{
		leaderCountPerc := (float64(n.RangeLeaderCount)/float64(clsAllLeaderSize))*100
		leaderCountScore := leaderCountPerc * MasterConf.NodeLeaderCountWeight
		if leaderCountPerc > MasterConf.NodeLeaderCountPercThreshold {
			leaderCountScore = leaderCountScore * MasterConf.NodeScoreMulti
		}
	}

	totalLeaderScore := 0.0
	for _,rng := range ranges.GetAllRange(){
		if rng.Leader != nil && rng.Leader.GetNodeId() == nodeId{
			totalLeaderScore += rng.Score
		}
	}
	n.TotalLeaderScore = totalLeaderScore
	n.Score = cpuScore + memScore + diskScore + swapScore + diskReadScore + diskWriteScore + netInScore + netOutScore + rangeCountScore + leaderCountScore
}

func (r *Range) caculateRangeScore(stats *statspb.RangeStats) {
	if stats == nil{
		return
	}
	sizePerc := (float64(stats.Size_)/MasterConf.RangeTotalSize)*100
	opsPerc := (float64(stats.Ops)/MasterConf.RangeTotalOps)*100
	inPerc := (float64(stats.BytesInPerSec)/MasterConf.RangeTotalNetInBytes)*100
	outPerc := (float64(stats.BytesOutPerSec)/MasterConf.RangeTotalNetOutBytes)*100

	sizeScore := sizePerc*MasterConf.RangeSizeWeight
	if sizePerc > MasterConf.RangeSizePercThreshold {
		sizeScore = sizeScore  * MasterConf.RangeScoreMulti
	}
	opsScore := opsPerc*MasterConf.RangeOpsWeight
	if opsPerc > MasterConf.RangeOpsPercThreshold {
		opsScore = opsScore * MasterConf.RangeScoreMulti
	}
	inScore := inPerc * MasterConf.RangeNetInWeight
	if inPerc > MasterConf.RangeNetInPercThreshold {
		inScore = inScore * MasterConf.RangeScoreMulti
	}
	outScore := outPerc * MasterConf.RangeNetOutWeight
	if outPerc > MasterConf.RangeNetOutPercThreshold {
		outScore = outScore * MasterConf.RangeScoreMulti
	}
	r.Score = sizeScore + opsScore + inScore + outScore
}
