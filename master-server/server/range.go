package server

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/statspb"
	"util/deepcopy"
	"util/log"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
)

const (
	DefaultPeersNum                     = 3
	DefaultMaxRangeSize                 = 1 * GB
	RangeSizeStep                       = 64 * MB
	DefaultHeartbeatPeriod              = time.Second * time.Duration(10)
	DefaultRangeSplitProtectTime        = time.Minute * time.Duration(5)
)

type Range struct {
	lock sync.RWMutex
	*metapb.Range
	Leader       *metapb.Leader
	Stats        *statspb.RangeStats
	DownPeers    []*mspb.PeerStats
	PendingPeers []*metapb.Peer
	LastHbTime   time.Time
	LastAlarmTime time.Time
	//LastCheckTime time.Time
	LastTaskTime time.Time
	// 分片下线时间
	OfflineTime time.Time
	tasks       *TaskCache
	RaftStatus  *statspb.RaftStatus
	// 管理端手动执行任务时使用
	AutoSchUnable bool
	Score float64

	Trace  bool
}

func NewRange(r *metapb.Range) *Range {
	region := &Range{
		Range:        r,
		Stats:        new(statspb.RangeStats),
		LastHbTime:   time.Now(),
		LastAlarmTime: time.Now(),
		LastTaskTime: time.Time{},
		//LastCheckTime: time.Now(),
		tasks:         NewTaskCache(),
		AutoSchUnable: false,
	}
	if region.State == metapb.RangeState_R_Offline {
		region.OfflineTime = time.Now()
	}
	return region
}

func (r *Range) SString() string {
	return fmt.Sprintf("%s:%s:%d", r.GetDbName(), r.GetTableName(), r.GetId())
}

func (r *Range) ID() uint64 {
	return r.GetId()
}

func (r *Range) UpdateLeader(leader *metapb.Leader) {
	r.Leader = leader
}

func (r *Range) GetLeader() *metapb.Leader {
	if r.Leader == nil {
		return nil
	}
	return r.Leader
}

func (r *Range) GetStats() *statspb.RangeStats {
	return r.Stats
}

func (r *Range) GetScore() float64 {
	return r.Score
}

func (r *Range) GetPeer(nodeId uint64) *metapb.Peer {
	for _, p := range r.Peers {
		if p.GetNodeId() == nodeId {
			return p
		}
	}
	return nil
}

func (r *Range) AddTask(task *Task) {
	r.tasks.Add(task)
}

func (r *Range) SetNxTask(task *Task) bool {
	return r.tasks.SetNx(task)
}

func (r *Range) DeleteTask(id uint64) {
	r.tasks.Delete(id)
}

func (r *Range) AllowSch(cluster *Cluster) bool {
	if r.AutoSchUnable {
		return false
	}
	if r.State != metapb.RangeState_R_Normal {
		log.Debug("AllowSch: range[%s:%s:%d] state is not normal", r.GetDbName(), r.GetTableName(), r.GetId())
		return false
	}

	if r.tasks.Size() > 0 {
		log.Debug("AllowSch: tasks size >0")
		return false
	}
	if len(r.PendingPeers) > 0 {
		log.Warn("AllowSch: range[%s:%s:%d] pendingpeers[%v] >0",
			r.GetDbName(), r.GetTableName(), r.GetId(), r.PendingPeers)
		return false
	}

	// 检查副本是否完整
	for _, p := range r.Peers {
		node, find := cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("node[%d] not found", p.GetNodeId())
			return false
		}
		// 节点没有激活
		if node.State != metapb.NodeState_N_Login {
			log.Warn("node %v state is not login: %v", node.GetId(), node.GetState())
			return false
		}
		find = false
		for _, id := range node.AllRanges {
			if id == p.GetId() {
				find = true
				break
			}
		}
		// 由于分裂没有完成，缺少副本，不能执行任务
		if !find {
			log.Warn("AllowSch range[%s:%s:%d]缺少副本[%s]，不能执行任务",
				r.GetDbName(), r.GetTableName(), r.GetId(), node.GetAddress())
			return false
		}
	}
	return true
}

func (r *Range) AllowFailoverSch(cluster *Cluster) bool {
	if r.AutoSchUnable {
		log.Debug("r.AutoSchUnable is true")
		return false
	}
	if r.State != metapb.RangeState_R_Normal {
		log.Debug("AllowFailoverSch: range[%s:%s:%d] state is not normal", r.GetDbName(), r.GetTableName(), r.GetId())
		return false
	}
	// 刚刚分裂的分片会挂在空任务，不会触发failover任务
	if r.tasks.Size() > 0 {
		log.Debug("AllowSch: tasks size >0")
		return false
	}
	if len(r.PendingPeers) > 0 {
		log.Warn("AllowFailoverSch: range[%s:%s:%d] pendingpeers[%v] >0",
			r.GetDbName(), r.GetTableName(), r.GetId(), r.PendingPeers)
		return false
	}
	downPeers := r.DownPeers
	logoutPeers := func() []*mspb.PeerStats {
		var logout []*mspb.PeerStats
		for _, p := range r.GetPeers() {
			var n *Instance
			var found bool
			if n, found = cluster.FindInstance(p.GetNodeId()); !found {
				log.Error("range[%v] node[%v] not found", p.GetId(), p.GetNodeId())
				return nil
			}
			if n.GetState() == metapb.NodeState_N_Logout {
				logout = append(logout, &mspb.PeerStats{p, 0xffffffffffffffff})
			}
		}
		return logout
	}()
	downPeers = append(downPeers, logoutPeers...)
	if len(downPeers) != 1 {
		log.Debug("len(downPeers) != 1 ")
		return false
	}
	downPeer := downPeers[0].GetPeer()
	if downPeers[0].GetDownSeconds() < DefaultDownTimeLimit {
		log.Debug("downPeers[0].GetDownSeconds() < DefaultDownTimeLimit")
		return false
	}
	//　检查副本所在的节点是否已经激活
	var nodes []*Instance
	for _, p := range r.Peers {
		node, find := cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("AllowFailoverSch: node[%d] not found", p.GetNodeId())
			return false
		}
		// 节点没有激活
		if node.State == metapb.NodeState_N_Initial {
			log.Debug("node.State == metapb.NodeState_N_Initial")
			return false
		}
		// 缓存节点，不重复查询
		nodes = append(nodes, node)
	}
	// 检查副本是否完整(所有的节点都已经激活)
	var find bool
	for _, node := range nodes {
		find = false
		for _, id := range node.AllRanges {
			if id == r.GetId() {
				find = true
				break
			}
		}
		// 副本加载失败，需要failover
		if !find && downPeer.GetNodeId() == node.GetId() {
			return true
		}
	}
	return true
}

func (r *Range) IsOffline() bool {
	return r.State == metapb.RangeState_R_Offline
}

func (r *Range) AllowGCRemote(cluster *Cluster) bool {
	if r.AutoSchUnable {
		return false
	}
	if r.State != metapb.RangeState_R_Offline {
		return false
	}
	if r.tasks.Size() > 0 {
		return false
	}
	//if len(r.PendingPeers) > 0 {
	//	log.Warn("AllowGCRemote: range[%s] pendingpeers[%v] >0",
	//		r.SString(), r.PendingPeers)
	//	return false
	//}
	//if len(r.DownPeers) > 0 {
	//	log.Warn("AllowGCRemote: range[%s] downpeers[%v] >0",
	//		r.SString(), r.DownPeers)
	//	return false
	//}

	return true
}

func (r *Range) AllowGCLocal(cluster *Cluster) bool {
	if r.AutoSchUnable {
		return false
	}
	if r.State != metapb.RangeState_R_Offline {
		return false
	}
	task := r.GetTask()
	if task == nil {
		return false
	}
	if task.GetType() != taskpb.TaskType_RangeDelete {
		return false
	}
	if !r.OfflineTime.IsZero() && time.Since(r.OfflineTime) < time.Duration(DefaultRangeGCTimeout) {
		return false
	}

	for _, p := range r.GetPeers() {
		node, find := cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("node[%d] not found", p.GetNodeId())
			return false
		}
		// 如果有节点没有激活，那么直接返回
		if !node.IsLogin() {
			return false
		}
		// 该分片副本还没有删除
		if node.FindRangeFromRemote(r.ID()) != nil {
			return false
		}
	}

	return true
}

func (r *Range) CreateTime() time.Time {
	return time.Unix(r.GetCreateTime(), 0)
}

func (r *Range) AllowSplit(threshold uint64, cluster *Cluster, splitProtect bool) bool {
	if r.AutoSchUnable {
		return false
	}
	if r.Stats.Size_ < threshold {
		//log.Debug("to split: range size %v < threshold", r.GetStats().GetSize_())
		return false
	}
	if r.State != metapb.RangeState_R_Normal {
		log.Debug("allowSplit: range state is not normal")
		return false
	}

	if r.tasks.Size() > 0 {
		return false
	}
	// 分片太大，优先分裂
	if r.Stats.Size_ < DefaultMaxRangeSize * 5 {
		if len(r.GetPeers()) != DefaultPeersNum {
			return false
		}
	}

	if len(r.PendingPeers) > 0 {
		log.Warn("AllowSplit: range[%s] pendingpeers[%v] >0",
			r.SString(), r.PendingPeers)
		return false
	}
	if len(r.DownPeers) > 0 {
		log.Warn("AllowSplit: range[%s] downpeers[%v] >0",
			r.SString(), r.DownPeers)
		return false
	}

	// 检查副本是否完整
	for _, p := range r.Peers {
		node, find := cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("AllowSplit: node[%d] not found", p.GetNodeId())
			return false
		}
		// 节点没有激活
		if node.State != metapb.NodeState_N_Login {
			return false
		}
		find = false
		for _, id := range node.AllRanges {
			if id == p.GetId() {
				find = true
				break
			}
		}
		// 由于分裂没有完成，缺少副本，不能执行任务
		// 由于分裂没有完成，缺少副本，不能执行任务
		if !find {
			log.Warn("AllowSplit: range[%s]缺少副本[%s]，不能执行任务",
				r.SString(), node.GetAddress())
			return false
		}
	}

	if splitProtect && time.Since(r.CreateTime()) < DefaultRangeSplitProtectTime {
		return false
	}
	return true
}

func (r *Range) GetDownPeers() []*mspb.PeerStats {
	return r.DownPeers
}

func (r *Range) GetTask() *Task {
	tasks := r.tasks.GetAllTask()
	if len(tasks) > 1 {
		log.Error("internal error, more than one tasks in range[%s:%s:%d]",
			r.GetDbName(), r.GetTableName(), r.GetId())
		return nil
	}
	if len(tasks) == 1 {
		return tasks[0]
	}
	return nil
}

func (r *Range) GetPendingPeers() []*metapb.Peer {
	return r.PendingPeers
}

type RangeCache struct {
	lock   sync.RWMutex
	ranges map[uint64]*Range
}

func NewRangeCache() *RangeCache {
	return &RangeCache{ranges: make(map[uint64]*Range)}
}

func (rc *RangeCache) Reset() {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	rc.ranges = make(map[uint64]*Range)
}

func (rc *RangeCache) Add(r *Range) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	rc.ranges[r.ID()] = r
}

func (rc *RangeCache) Delete(id uint64) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	delete(rc.ranges, id)
}

func (rc *RangeCache) FindRange(id uint64) (*Range, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if r, find := rc.ranges[id]; find {
		return r, true
	}
	return nil, false
}

func (rc *RangeCache) GetAllRange() []*Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var ranges []*Range
	for _, r := range rc.ranges {
		ranges = append(ranges, r)
	}
	return ranges
}

func (rc *RangeCache) Size() int {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	return len(rc.ranges)
}

func (rc *RangeCache) CollectRangesByNodeId(id uint64) []*Range {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var ranges []*Range
	for _, r := range rc.ranges {
		for _, peer := range r.Peers {
			if peer.NodeId == id {
				ranges = append(ranges, r)
				break
			}
		}
	}
	return ranges
}

func (rc *RangeCache) MarshalRange(id uint64) (*RangeDebug, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	r, ok := rc.ranges[id]
	if !ok {
		return nil, errors.New("range is not existed")
	}

	return &RangeDebug{
		Range:        deepcopy.Iface(r.Range).(*metapb.Range),
		Leader:       deepcopy.Iface(r.Leader).(*metapb.Peer),
		Stats:        deepcopy.Iface(r.Stats).(*statspb.RangeStats),
		DownPeers:    deepcopy.Iface(r.DownPeers).([]*mspb.PeerStats),
		PendingPeers: deepcopy.Iface(r.PendingPeers).([]*metapb.Peer),
		LastHbTime:   r.LastHbTime,
		LastSchTime:  r.LastTaskTime,
	}, nil
}

func RangeOffline(cluster *Cluster, r *Range) *Task {
	taskId, err := cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		return nil
	}
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeOffline,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultRangeDeleteTaskTimeout,
		},
		RangeOffline: &taskpb.TaskRangeOffline{
			Range: deepcopy.Iface(r.Range).(*metapb.Range),
		},
	}
	task := NewTask(t)
	return task
}

func RangeDelPeer(cluster *Cluster, r *Range, downPeer *metapb.Peer) *Task {
	if r.GetLeader().GetNodeId() == downPeer.GetNodeId() {
		return nil
	}
	taskId, err := cluster.GenTaskId()
	if err != nil {
		log.Error("gen taskID failed, err[%v]", err)
		return nil
	}
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeDelPeer,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultRangeDelPeerTaskTimeout,
		},
		RangeDelPeer: &taskpb.TaskRangeDelPeer{
			Range: deepcopy.Iface(r.Range).(*metapb.Range),
			Peer:  downPeer,
		},
	}
	log.Info("range[%s] del peer[%d]!!!", r.SString(), downPeer.GetNodeId())
	return NewTask(t)
}

func RangeLeaderTransfer(cluster *Cluster, r *Range, expLeaderId uint64) *Task {
	if r.GetLeader().GetNodeId() == expLeaderId {
		return nil
	}
	taskId, err := cluster.GenTaskId()
	if err != nil {
		log.Error("gen taskID failed, err[%v]", err)
		return nil
	}
	preLeader := r.GetPeer(r.GetLeader().GetNodeId())
	expLeader := r.GetPeer(expLeaderId)
	if preLeader == nil || expLeader == nil {
		log.Warn("range[%s] invalid param", r.SString())
		return nil
	}
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeLeaderTransfer,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultChangeLeaderTaskTimeout,
		},
		RangeLeaderTransfer: &taskpb.TaskRangeLeaderTransfer{
			Range: deepcopy.Iface(r.Range).(*metapb.Range),
			ExpLeader:  expLeader,
			PreLeader:  preLeader,
		},
	}
	log.Info("range[%s] leader transfer[%d -> %d]!!!", r.SString(), preLeader.GetNodeId(), expLeaderId)
	return NewTask(t)
}

type RangeByRegionSlice []*Range

func (p RangeByRegionSlice) Len() int {
	return len(p)
}

func (p RangeByRegionSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p RangeByRegionSlice) Less(i int, j int) bool {
	if metapb.Compare(p[i].GetStartKey(), p[j].GetStartKey()) < 0 {
		return true
	} else {
		return false
	}
}


type RangeGCRemoteScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
}

func NewRangeGCRemoteScheduler(interval time.Duration, cluster *Cluster) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &RangeGCRemoteScheduler{
		name:     "range_gc_remote_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster:  cluster,
	}
}

func (rgc *RangeGCRemoteScheduler) GetName() string {
	return rgc.name
}

func (rgc *RangeGCRemoteScheduler) Schedule() *Task {
	ranges := rgc.cluster.GetAllRanges()
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
		if r.AllowGCRemote(rgc.cluster) {
			log.Info("range[%s] delete in remote", r.SString())
			return RangeGCRemote(rgc.cluster, r)
		}
	}
	return nil
}

func RangeGCRemote(cluster *Cluster, r *Range) *Task {
	taskId, err := cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		return nil
	}
	t := &taskpb.Task{
		Type:   taskpb.TaskType_RangeDelete,
		Meta:   &taskpb.TaskMeta{
			TaskId:   taskId,
			CreateTime: time.Now().Unix(),
			State: taskpb.TaskState_TaskWaiting,
			Timeout: DefaultRangeDeleteTaskTimeout,
		},
		RangeDelete: &taskpb.TaskRangeDelete{
			Range:      deepcopy.Iface(r.Range).(*metapb.Range),
		},
	}
	task := NewTask(t)
	return task
}

func (rgc *RangeGCRemoteScheduler) AllowSchedule() bool {
	return true
}

func (rgc *RangeGCRemoteScheduler) GetInterval() time.Duration {
	return rgc.interval
}

func (rgc *RangeGCRemoteScheduler) Ctx() context.Context {
	return rgc.ctx
}

func (rgc *RangeGCRemoteScheduler) Stop() {
	rgc.cancel()
}

type RangeGCLocalScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	pusher           Pusher
}

func NewRangeGCLocalScheduler(interval time.Duration, cluster *Cluster, pusher Pusher) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &RangeGCLocalScheduler{
		name:     "range_gc_local_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster:  cluster,
		pusher:  pusher,
	}
}

func (rgc *RangeGCLocalScheduler) GetName() string {
	return rgc.name
}

func (rgc *RangeGCLocalScheduler) Schedule() *Task {
	ranges := rgc.cluster.GetAllRanges()
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
		if r.AllowGCLocal(rgc.cluster) {
			RangeGCLocal(rgc.cluster, rgc.pusher, r)
		}
	}
	return nil
}

func RangeGCLocal(cluster *Cluster, pusher Pusher, r *Range) {
	t := r.GetTask()
	if t == nil {
		return
	}
	//首先删除任务
	if t.GetType() != taskpb.TaskType_RangeDelete {
		log.Error("range[%s] local GC invalid, has task[%s]",
			r.SString(), t.Describe())
		return
	}
	// bugfix for range delete remote task add again
    batch := cluster.store.NewBatch()
	tKey := []byte(fmt.Sprintf("%s%d", PREFIX_TASK, t.GetMeta().GetTaskId()))
	batch.Delete(tKey)
	rKey := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
	batch.Delete(rKey)
	err := batch.Commit()
	if err != nil {
		log.Error("delete range %s failed, err[%v]", r.SString(), err)
		return
	}
	err = cluster.DeleteTaskCache(t.ID())
	if err != nil {
		log.Error("delete task %s in cache failed, err[%v]", t.Describe(), err)
		return
	}
	err = cluster.DeleteRangeLocalCache(r)
	if err != nil {
		log.Error("delete range[%s] in cache failed, err[%v]", r.SString(), err)
		return
	}
	t.GetMeta().State = taskpb.TaskState_TaskSuccess
	if err := pusher.Push(TaskSQL, cluster.GetClusterId(), time.Now().Unix(), t.ID(),
		t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
		log.Error("pusher task info error: %s", err.Error())
	}
	log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
	log.Info("delete range[%s] for local success", r.SString())
}

func (rgc *RangeGCLocalScheduler) AllowSchedule() bool {
	return true
}

func (rgc *RangeGCLocalScheduler) GetInterval() time.Duration {
	return rgc.interval
}

func (rgc *RangeGCLocalScheduler) Ctx() context.Context {
	return rgc.ctx
}

func (rgc *RangeGCLocalScheduler) Stop() {
	rgc.cancel()
}

type RangeMonitorScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	alarm            Alarm
}

func NewRangeMonitorScheduler(interval time.Duration, cluster *Cluster, alarm Alarm) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &RangeMonitorScheduler{
		name:     "range_monitor_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster:  cluster,
		alarm:  alarm,
	}
}

func (rgc *RangeMonitorScheduler) GetName() string {
	return rgc.name
}

func (rgc *RangeMonitorScheduler) Schedule() *Task {
	ranges := rgc.cluster.GetAllRanges()
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
		if time.Since(r.LastHbTime) > DefaultFaultTimeout * 10 {
			message := fmt.Sprintf("range[%s] heartbeat delay, last heartbeat time[%s]", r.SString(), r.LastHbTime.String())
			log.Warn(message)
			//rgc.alarm.Alarm("分片心跳延迟报警", message)
			//r.LastAlarmTime = time.Now()
		}
	}
	return nil
}

func (rgc *RangeMonitorScheduler) AllowSchedule() bool {
	return true
}

func (rgc *RangeMonitorScheduler) GetInterval() time.Duration {
	return rgc.interval
}

func (rgc *RangeMonitorScheduler) Ctx() context.Context {
	return rgc.ctx
}

func (rgc *RangeMonitorScheduler) Stop() {
	rgc.cancel()
}