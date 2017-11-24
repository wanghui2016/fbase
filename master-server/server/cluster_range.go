package server

import (
	"time"
	"errors"
	"sort"
	"fmt"

	"model/pkg/metapb"
	"util/log"
	"util/deepcopy"
	"golang.org/x/net/context"
	"model/pkg/eventpb"
	"model/pkg/taskpb"
)

func (c *Cluster) GetAllRanges() []*Range {
	return c.ranges.GetAllRange()
}

func (c *Cluster) RangeSplitAck(ack *eventpb.EventRangeSplitAck, router Router) error {
	dbId := ack.GetRange().GetDbId()
	tableId := ack.GetRange().GetTableId()
	rangeId := ack.GetRange().GetId()
	taskId := ack.GetTaskId()
	_, find := c.tasks.FindTask(taskId)
	if !find {
		log.Warn("invalid task, maybe repeated report")
		return ErrNotExistTask
	}

	table, find := c.FindTableById(dbId, tableId)
	if !find {
		return ErrNotExistTable
	}
	r, find := table.FindRange(rangeId)
	if !find {
		return ErrNotExistRange
	}
	r.OfflineTime = time.Now()
	leftRange := ack.GetLeftRange()
	rightRange := ack.GetRightRange()
	// 检查是否已经上报成功了
	if _, find := c.FindRange(leftRange.GetId()); find {
		log.Info("range[%s] split[-> %d, %d] already report success!!!",
			r.SString(), leftRange.GetId(), rightRange.GetId())
		return nil
	}
	left := NewRange(leftRange)
	right := NewRange(rightRange)
	ins := make([]*Range, 2)
	ins[0] = left
	ins[1] = right
	err := table.RangeSplit(r, ins, c)
	if err != nil {
		log.Error("table update routes failed, err[%v]", err)
		return err
	}
	topo, err := table.GetTopologyEpoch(c, &metapb.TableEpoch{})
	if err != nil {
		// 表删除或者创建，告警即可，没有更新路由的必要
		log.Warn("table get topology failed, err[%v]", err)
	} else {
		router.Publish(ROUTE, topo)
		log.Debug("publish routes[%v] version[%d] update!!!!", topo.GetRoutes(), topo.GetEpoch().GetVersion())
	}

	//强制进行一次路由检查
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	table.RoutesCheck(ctx, c, c.alarm)
	log.Info("range[%s] split[-> %s, %s] success", r.SString(), left.String(), right.String())
	return nil
}

func (c *Cluster) RangeSplitKey(key *eventpb.EventRangeSplitKey) error {
	rangeId := key.GetRange().GetId()
	taskId := key.GetTaskId()
	t, find := c.tasks.FindTask(taskId)
	if !find {
		log.Warn("invalid task, maybe repeated report")
		return ErrNotExistTask
	}

	r, find := c.FindRange(rangeId)
	if !find {
		return ErrNotExistRange
	}
	task := deepcopy.Iface(t.Task).(*taskpb.Task)
	task.GetRangeSplit().SplitKey = key.GetSplitKey()
	err := c.storeTask(task)
	if err != nil {
		log.Error("store taksk failed, err[%v]", err)
		return err
	}
	t.Task = task
	//c.ranges.Delete(r.ID())
	log.Info("range[%s] update split key success", r.SString())
	return nil
}

func (c *Cluster) RaftError(event *eventpb.EventRaftErr) error {
	rangeId := event.GetRangeId()
	nodeId := event.GetNodeId()
	rng, find := c.FindRange(rangeId)
	if !find {
		log.Warn("range[%d] not found", rangeId)
		return ErrNotExistRange
	}
	peer := rng.GetPeer(nodeId)
	if peer == nil {
		log.Warn("range[%s:%s:%d] has no peer in node[%d]",
			rng.SString(), nodeId)
		return ErrNotExistPeer
	}
	node, find := c.FindInstance(nodeId)
	if !find {
		log.Warn("node[%d] not found", nodeId)
		return ErrNotExistNode
	}
	message := fmt.Sprintf("分片[%s] 副本[%s] raft 异常, 异常信息[%s]",
		rng.SString(), node.GetAddress(), event.GetError())
	c.alarm.Alarm("分片异常", message)
	// 生成range failover task
	task := FailOverSchedule(peer, rng, c, c.alarm)
	if task != nil {
		err := c.AddTask(task)
		if err != nil {
			log.Error("add task[%s] failed, err[%v]", task.Describe(), err)
			return err
		}
		log.Info("range[%s] raft error in node[%s] report success",
			rng.SString(), node.GetAddress())
	} else {
		log.Warn("range[%s] raft error in node[%s] report failed, failover schedule failed",
			rng.SString(), node.GetAddress())
	}
	return nil
}

func (c *Cluster) StoreError(event *eventpb.EventStoreErr, pusher Pusher) error {
	rangeId := event.GetRangeId()
	nodeId := event.GetNodeId()
	rng, find := c.FindRange(rangeId)
	if !find {
		log.Warn("range[%d] not found", rangeId)
		return ErrNotExistRange
	}
	peer := rng.GetPeer(nodeId)
	if peer == nil {
		log.Warn("range[%s] has no peer in node[%d]",
			rng.SString(), nodeId)
		return ErrNotExistPeer
	}
	node, find := c.FindInstance(nodeId)
	if !find {
		log.Warn("node[%d] not found", nodeId)
		return ErrNotExistNode
	}
	message := fmt.Sprintf("分片[%s] 副本[%s] store 异常, 异常信息[%s]",
		rng.SString(), node.GetAddress(), event.GetError())
	c.alarm.Alarm("分片异常", message)
	// 如果分片有分裂任务，说明是某一个副本分裂失败了
	t := rng.GetTask()
	if t == nil {
		rng.AutoSchUnable = true
		defer func() {
			rng.AutoSchUnable = false
		}()
		task := FailOverSchedule(peer, rng, c, c.alarm)
		if task != nil {
			err := c.AddTask(task)
			if err != nil {
				log.Error("add task[%s] failed, err[%v]", task.Describe(), err)
				return err
			}
			log.Info("range[%s] raft error in node[%s] report success",
				rng.SString(), node.GetAddress())
		} else {
			log.Warn("range[%s] raft error in node[%s] report failed, failover schedule failed",
				rng.SString(), node.GetAddress())
		}
		return nil
	}
	switch t.GetType() {
	case taskpb.TaskType_RangeSplit:
		// split ack 还没有上报
		if rng.State == metapb.RangeState_R_Normal {
			log.Warn("range[%s] wait for split ACK report", rng.SString())
			return errors.New("range wait for split ACK report")
		}
		rng.AutoSchUnable = true
		defer func() {
			rng.AutoSchUnable = false
		}()
		// TODO 任务描述日志
		t.GetMeta().State = taskpb.TaskState_TaskFail
		err := c.storeTask(t.Task)
		if err != nil {
			log.Warn("store task failed, err[%v]", err)
			return err
		}
		if err = pusher.Push(TaskSQL, c.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		message := t.Describe() + " 任务状态: " + t.StateString()
		c.alarm.Alarm("任务失败", message)
	case taskpb.TaskType_RangeTransfer:
		fallthrough
	case taskpb.TaskType_RangeAddPeer:
		fallthrough
	case taskpb.TaskType_RangeDelPeer:
		fallthrough
	case taskpb.TaskType_RangeLeaderTransfer:
		rng.AutoSchUnable = true
		defer func() {
			rng.AutoSchUnable = false
		}()
		// 取消分裂任务,分裂任务raft log已经提交成功了，因此可以取消
		err := c.DeleteTask(t.ID())
		if err != nil {
			log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
			return err
		}
		// TODO 任务描述日志
		t.GetMeta().State = taskpb.TaskState_TaskCancel
		if err := pusher.Push(TaskSQL, c.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		message := t.Describe() + " 任务状态: " + t.StateString()
		c.alarm.Alarm("任务取消", message)
		task := FailOverSchedule(peer, rng, c, c.alarm)
		if task != nil {
			err := c.AddTask(task)
			if err != nil {
				log.Error("add task[%s] failed, err[%v]", task.Describe(), err)
				return err
			}
			log.Info("range[%s] raft error in node[%s] report success",
				rng.SString(), node.GetAddress())
		} else {
			log.Warn("range[%s] raft error in node[%s] report failed, failover schedule failed",
				rng.SString(), node.GetAddress())
		}
	}
	return nil
}

func (c *Cluster) RangeDeleteAck(ack *eventpb.EventRangeDeleteAck, pusher Pusher) error {
	//dbName := ack.GetRange().GetDbName()
	//tableName := ack.GetRange().GetTableName()
	//rangeId := ack.GetRange().GetId()
	//taskId := ack.GetTaskId()
	//task, find := c.tasks.FindTask(taskId)
	//if !find {
	//	log.Warn("invalid task, maybe repeated report")
	//	return ErrNotExistTask
	//}
	//
	//r, find := c.FindRange(rangeId)
	//if !find {
	//	return ErrNotExistRange
	//}
	//
	//err := c.DeleteTask(taskId)
	//if err != nil {
	//	log.Error("delete task[%s] failed, err[%v]", task.Describe(), err)
	//	return err
	//}
	//// TODO 任务描述日志
	//task.GetMeta().State = taskpb.TaskState_TaskSuccess
	//if err := pusher.Push(TaskSQL, c.GetClusterId(), time.Now().Unix(), task.ID(),
	//	task.GetMeta().GetCreateTime(), time.Since(task.CreateTime()), task.StateString(), task.Describe()); err != nil {
	//	log.Error("pusher task info error: %s", err.Error())
	//}
	//log.Info("range[%s:%s:%d] task[%s] do success", r.GetDbName(), r.GetTableName(), r.ID(), task.Describe())
	//err = c.DeleteRangeLocal(r)
	//if err != nil {
	//	log.Error("range[%s:%s:%d] delete failed, err[%v]", dbName, tableName, r.GetId(), err)
	//	return err
	//}
	//log.Info("range[%s:%s:%d] delete success", dbName, tableName, r.GetId())
	return nil
}

func (c *Cluster) prepareCreateRange(dbName, tableName string, startKey, limitKey []byte, nodes []*Instance) (*metapb.Range,  error) {
	if !c.IsLeader() {
		return nil, ErrNotLeader
	}

	id, err := c.rangeIdGener.GenID()
	if err != nil {
		log.Error("cannot generte range[%s:%s] ID, err[%v]", dbName, tableName, err)
		return nil, ErrGenID
	}
	var peers []*metapb.Peer
	for _, n := range nodes {
		peer := &metapb.Peer{Id: id, NodeId: n.GetId()}
		peers = append(peers, peer)
	}
	var start, end *metapb.Key
	if startKey == nil {
		start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
	} else {
		start = &metapb.Key{Key: startKey, Type: metapb.KeyType_KT_Ordinary}
	}
	if limitKey == nil {
		end = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
	} else {
		end = &metapb.Key{Key: limitKey, Type: metapb.KeyType_KT_Ordinary}
	}
	sort.Sort(metapb.PeersByNodeIdSlice(peers))
	_range := &metapb.Range{
		Id:     id,
		StartKey: start,
		EndKey: end,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		Peers: peers,
		DbName: dbName,
		TableName: tableName,
		State: metapb.RangeState_R_Init,
		CreateTime: time.Now().Unix(),
	}
	return _range, nil
}

func (c *Cluster) createRangeRemote(r *metapb.Range) error {
	for _, p := range r.GetPeers() {
		node, find := c.FindInstance(p.GetNodeId())
		if !find {
			continue
		}
		err := c.cli.CreateRange(node.GetAddress(), r)
		if err != nil {
			log.Warn("create range[%v] in node[%s] failed, err[%v]", r.String(), node.GetAddress(), err)
			return err
		}
	}
	return nil
}

// For Test case
func (c *Cluster) CreateRangeForTest(dbName, tableName string, startKey, limitKey []byte, nodes []*Instance) (*Range, error) {
	if !c.IsLeader() {
		return nil, ErrNotLeader
	}
	id, err := c.rangeIdGener.GenID()
	if err != nil {
		log.Error("cannot generte range[%s:%s] ID, err[%v]", dbName, tableName, err)
		return nil, ErrGenID
	}
	var peers []*metapb.Peer
	for _, n := range nodes {
		peer := &metapb.Peer{Id: id, NodeId: n.GetId()}
		peers = append(peers, peer)
	}
	var start, end *metapb.Key
	if startKey == nil {
		start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
	} else {
		start = &metapb.Key{Key: startKey, Type: metapb.KeyType_KT_Ordinary}
	}
	if limitKey == nil {
		end = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
	} else {
		end = &metapb.Key{Key: limitKey, Type: metapb.KeyType_KT_Ordinary}
	}
	sort.Sort(metapb.PeersByNodeIdSlice(peers))
	_range := &metapb.Range{
		Id:     id,
		StartKey: start,
		EndKey: end,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		Peers: peers,
		DbName: dbName,
		TableName: tableName,
		State: metapb.RangeState_R_Init,
	}

	return NewRange(_range), nil
}

func (c *Cluster) DeleteRangeRemote(r *metapb.Range) error {
	for _, p := range r.GetPeers() {
		node, find := c.FindInstance(p.GetNodeId())
		if !find {
			continue
		}
		err := c.cli.DeleteRange(node.GetAddress(), r.GetId())
		if err != nil {
			log.Warn("delete range[%v] in node[%s] failed, err[%v]", r.String(), node.GetAddress(), err)
			// 如果节点已经宕机，那么忽略
			if node.IsFault() {
				continue
			}
			return err
		}
	}
	return nil
}

func (c *Cluster) DeleteRangeLocal(r *Range) error {
	// TODO delete range
	err := c.deleteRange(r.ID())
	if err != nil {
		log.Error("delete range[%d] failed, err[%v]", r.ID(), err)
		return err
	}
	// 删除table
	table, find := c.FindTableById(r.GetDbId(), r.GetTableId())
	if find {
		table.DeleteRange(r.ID())
	}

	c.ranges.Delete(r.ID())
	for _, p := range r.GetPeers() {
		node, find := c.FindInstance(p.GetNodeId())
		if !find {
			continue
		}
		node.DeleteRange(r.ID())
	}
	return nil
}

func (c *Cluster) DeleteRangeLocalCache(r *Range) error {
	// 删除table
	table, find := c.FindTableById(r.GetDbId(), r.GetTableId())
	if find {
		table.DeleteRange(r.ID())
	}

	c.ranges.Delete(r.ID())
	for _, p := range r.GetPeers() {
		node, find := c.FindInstance(p.GetNodeId())
		if !find {
			continue
		}
		node.DeleteRange(r.ID())
	}
	return nil
}

func (c *Cluster) FindRange(id uint64) (*Range, bool) {
	return c.ranges.FindRange(id)
}

func (c *Cluster) AddRangeInMissCache(r *Range) {
	c.missRanges.Add(r)
}

func (c *Cluster) DeleteRangeInMissCache(id uint64) {
    c.missRanges.Delete(id)
}

func (c *Cluster) FindRangeInMissCache(id uint64) (*Range, bool) {
	return c.missRanges.FindRange(id)
}

func (c *Cluster) GetAllMissRanges() []*Range {
	return c.missRanges.GetAllRange()
}

type MissRangesScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
}

func NewMissRangesScheduler(interval time.Duration, cluster *Cluster) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &MissRangesScheduler{
		name:     "range_miss_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster:  cluster,
	}
}

func (rgc *MissRangesScheduler) GetName() string {
	return rgc.name
}

func (rgc *MissRangesScheduler) Schedule() *Task {
	ranges := rgc.cluster.GetAllMissRanges()
	num := len(ranges)
	if num == 0 {
		return nil
	}
	for _, r := range ranges {
		select {
		case <-rgc.ctx.Done():
			return nil
		default:
		}
		// 已经正确接收
		if _, find := rgc.cluster.FindRange(r.GetId()); find {
			rgc.cluster.DeleteRangeInMissCache(r.GetId())
		}
	}
	return nil
}

func (rgc *MissRangesScheduler) AllowSchedule() bool {
	return true
}

func (rgc *MissRangesScheduler) GetInterval() time.Duration {
	return rgc.interval
}

func (rgc *MissRangesScheduler) Ctx() context.Context {
	return rgc.ctx
}

func (rgc *MissRangesScheduler) Stop() {
	rgc.cancel()
}

