package server

import (
	"sync"
	"time"
	"net"

	"model/pkg/metapb"
	"util/lrucache"
	"util/log"
	"github.com/juju/errors"
	"util/deepcopy"
	"model/pkg/statspb"
	"model/pkg/taskpb"
	"golang.org/x/net/context"
)

var (
	DefaultFaultTimeout = time.Minute
	DefaultAlertTimeout = time.Minute * time.Duration(5)
	DefaultAlertMonitorTimeout = time.Minute * time.Duration(2)
	DefaultMaxBigTaskNum  = 3
	DefaultMaxTaskNum = 50
)

type Instance struct {
	*metapb.Node
	MacIp         string
    lock          sync.RWMutex
	ranges        *RangeCache
	Stats         *statspb.NodeStats
	ProcessStats *statspb.ProcessStats
	RangeCount    uint32
	// Current range split count.
	RangeSplitCount uint32
	// Current sending snapshot count.
	SendingSnapCount uint32
	// Current receiving snapshot count.
	ApplyingSnapCount uint32
	RangeLeaderCount  uint32
	Score            float64
	TotalLeaderScore float64
	NotWorkingRanges []uint64
	// data server上报的分片
	AllRanges        []uint64
	LastHbTime    time.Time
	tasks         *TaskCache

	// for balance
	monitor       *lrucache.LRUCache

	Trace         bool
}

func NewInstance(node *metapb.Node) *Instance {
	ip, _, err := net.SplitHostPort(node.GetAddress())
	if err != nil {
		log.Error("invalid instance %s", node.GetAddress())
		return nil
	}
	return &Instance{
		Node: node,
		MacIp: ip,
		ranges: NewRangeCache(),
		Stats: new(statspb.NodeStats),
		tasks: NewTaskCache(),
		monitor: lrucache.NewLRUCache(DefaultAlertMonitorTimeout),
	}
}

func (n *Instance) Reset() {
	n.ranges = NewRangeCache()
	n.State = metapb.NodeState_N_Login
}

func (n *Instance) Active() {
	n.State = metapb.NodeState_N_Login
}

func (n *Instance) Offline() {
	n.State = metapb.NodeState_N_Logout
}

func (n *Instance) ID() uint64 {
	return n.GetId()
}

func (n *Instance) AddRange(r *Range) {
	n.ranges.Add(r)
}

func (n *Instance) DeleteRange(rangeId uint64) {
	n.ranges.Delete(rangeId)
}

// TODO
func (n *Instance) GetStats() *statspb.NodeStats {
	return nil
}

func (n *Instance) GetRange(id uint64) (*Range, bool) {
	return n.ranges.FindRange(id)
}

func (n *Instance) GetAllRanges() ([]*Range) {
	return n.ranges.GetAllRange()
}

func (n *Instance) GetLeaderRanges() ([]*Range) {
	var leaderRanges []*Range
	for _,r := range n.GetAllRanges(){
		leader := r.Leader
		if leader != nil && leader.GetNodeId() == n.GetId(){
			leaderRanges = append(leaderRanges, r)
		}
	}
	return leaderRanges
}

func (n *Instance) GetRangesSize() (int) {
	return n.ranges.Size()
}

func (n *Instance) GetSlaveRanges() ([]*Range) {
	var slaveRanges []*Range
	for _,r := range n.GetAllRanges(){
		leader := r.Leader
		if leader != nil && leader.GetNodeId() != n.GetId(){
			slaveRanges = append(slaveRanges, r)
		}
	}
	return slaveRanges
}

func (n *Instance) GetRangesNum() int {
	return int(n.RangeCount)
}

func (n *Instance) GetLeaderNum() int {
	return int(n.RangeLeaderCount)
}

func (n *Instance) GetLastHeartbeatTime() time.Time {
	return n.LastHbTime
}

func (n *Instance) AllocSch() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	if n.State != metapb.NodeState_N_Login {
		return false
	}
	count := 0
    for _, t := range n.AllTask() {
	    if t.GetType() == taskpb.TaskType_RangeFailOver ||
	       t.GetType() == taskpb.TaskType_RangeAddPeer ||
	       t.GetType() == taskpb.TaskType_RangeTransfer {
		    count++
	    }
    }
	if count >= DefaultMaxBigTaskNum || n.tasks.Size() > DefaultMaxTaskNum {
		return false
	}
	return true
}

func (n *Instance) IsBusy() bool {
	count := 0
	for _, t := range n.AllTask() {
		if t.GetType() == taskpb.TaskType_RangeFailOver ||
			t.GetType() == taskpb.TaskType_RangeAddPeer ||
			t.GetType() == taskpb.TaskType_RangeTransfer {
			count++
		}
	}
	if count >= DefaultMaxBigTaskNum || n.tasks.Size() > DefaultMaxTaskNum {
		return true
	}
	return false
}

func (n *Instance) AllocSchSplit() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	if n.State != metapb.NodeState_N_Login {
		return false
	}
	if n.tasks.Size() > DefaultMaxTaskNum {
		return false
	}
	return true
}

func (n *Instance) GetScore() float64 {
	return n.Score
}

func (n *Instance) GetTotalLeaderScore() float64 {
	return n.TotalLeaderScore
}

func (n *Instance) GetDiskUsedPercent() float64 {
	return n.Stats.DiskProcRate
}

func (n *Instance) GetDiskFree() uint64 {
	return n.Stats.DiskFree
}

func (n *Instance) GetLoad() float64 {
	return n.Stats.Load5
}

func (n *Instance) GetNetIO() uint64 {
	return n.Stats.NetIoInBytePerSec + n.Stats.NetIoOutBytePerSec
}

func (n *Instance) GetNotWorkingRange(id uint64) (*Range) {
	for _, range_id := range n.NotWorkingRanges {
		if range_id == id {
			r, find := n.ranges.FindRange(id)
			if !find {
				log.Error("range[%d] not found in node[%s]", id, n.GetAddress())
				return nil
			}
			return r
		}
	}
	return nil
}

func (n *Instance) UpdateStats(stats *statspb.NodeStats) {
	n.Stats = stats
	n.LastHbTime = time.Now()
	if stats.CpuProcRate > float64(80) {
		n.monitor.Put("cpu", stats.CpuProcRate)
	}
	if stats.MemoryUsedPercent > float64(60) {
		n.monitor.Put("mem", stats.MemoryUsedPercent)
	}
	if stats.SwapMemoryUsedPercent > float64(3) {
		n.monitor.Put("swap", stats.SwapMemoryUsedPercent)
	}
	if stats.Load5 > float64(1) {
		n.monitor.Put("load", stats.Load5)
	}
	_, createTime, find := n.monitor.Get("cpu")
	if find {
		if time.Since(createTime) > DefaultAlertTimeout {
            //TODO 标记CPU持续很高
			log.Warn("node[%s] CPU rates remain high", n.Address)
			return
		}
	}
	_, createTime, find = n.monitor.Get("mem")
	if find {
		if time.Since(createTime) > DefaultAlertTimeout {
            //TODO 标记内存占用持续很高
			log.Warn("node[%s] memory rates remain high", n.Address)
			return
		}
	}
	_, createTime, find = n.monitor.Get("swap")
	if find {
		if time.Since(createTime) > DefaultAlertTimeout {
            // TODO swap 已经发生了大量的swap
			log.Warn("node[%s] swap rates remain high", n.Address)
			return
		}
	}
	_, createTime, find = n.monitor.Get("load")
	if find {
		if time.Since(createTime) > DefaultAlertTimeout {
            // TODO 负载很高
			log.Warn("node[%s] load rates remain high", n.Address)
			return
		}
	}
	return
}

func (n *Instance) AddTask(task *Task) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.tasks.Add(task)
}

func (n *Instance) DeleteTask(id uint64) {
	n.tasks.Delete(id)
}

func (n *Instance) AllTask() []*Task {
	return n.tasks.GetAllTask()
}

func (n *Instance) IsFault() bool {
	if n.State == metapb.NodeState_N_Tombstone || n.State == metapb.NodeState_N_Logout {
		return true
	}
	return false
}

func (n *Instance) IsLogin() bool {
	if n.State == metapb.NodeState_N_Login {
		return true
	}
	return false
}

func (n *Instance) ForceFault() {
	n.State = metapb.NodeState_N_Tombstone
}

func (n *Instance) genDeleteRangesTask(cluster *Cluster) *Task {
	var deleteRanges []uint64
	for _, r := range n.GetAllRanges() {
		log.Debug("node[%s] range[%s]", n.GetAddress(), r.SString())
		if t := r.GetTask(); t != nil && t.GetType() == taskpb.TaskType_RangeDelete &&
			n.FindRangeFromRemote(r.GetId()) != nil {
			deleteRanges = append(deleteRanges, r.GetId())
		}
	}
	for _, rangeId := range n.NotWorkingRanges {
		if _, find := cluster.FindRange(rangeId); !find {
			deleteRanges = append(deleteRanges, rangeId)
		}
	}
	if len(deleteRanges) > 0 {
		taskId, err := cluster.GenTaskId()
		if err != nil {
			log.Error("gen task ID failed, err[%v]", err)
			return nil
		}

		t := &taskpb.Task{
			Type:   taskpb.TaskType_NodeDeleteRanges,
			Meta:   &taskpb.TaskMeta{
				TaskId:   taskId,
				CreateTime: time.Now().Unix(),
				State: taskpb.TaskState_TaskWaiting,
				Timeout: DefaultTransferTaskTimeout,
			},
			NodeDeleteRanges: &taskpb.TaskNodeDeleteRanges{
				NodeId:      n.GetId(),
				RangeIds:     deleteRanges,
			},
		}
		return NewTask(t)
	}

	return nil
}

func (n *Instance) genCreateRanges(cluster *Cluster) *Task {
	var createRanges []*metapb.Range
	for _, r := range n.GetAllRanges() {
		log.Debug("node[%s] range[%s]", n.GetAddress(), r.SString())
		if t := r.GetTask(); t != nil && t.GetType() == taskpb.TaskType_RangeCreate &&
			n.FindRangeFromRemote(r.GetId()) == nil {
			rng := deepcopy.Iface(r.Range).(*metapb.Range)
			createRanges = append(createRanges, rng)
			// 每次限定50,控制响应的大小
			if len(createRanges) >= 50 {
				break
			}
		}
	}
	if len(createRanges) > 0 {
		taskId, err := cluster.GenTaskId()
		if err != nil {
			log.Error("gen task ID failed, err[%v]", err)
			return nil
		}

		t := &taskpb.Task{
			Type:   taskpb.TaskType_NodeCreateRanges,
			Meta:   &taskpb.TaskMeta{
				TaskId:   taskId,
				CreateTime: time.Now().Unix(),
				State: taskpb.TaskState_TaskWaiting,
				Timeout: DefaultTransferTaskTimeout,
			},
			NodeCreateRanges: &taskpb.TaskNodeCreateRanges{
				NodeId:      n.GetId(),
				Ranges:     createRanges,
			},
		}
		return NewTask(t)
	}

	return nil
}

func (n *Instance) Dispatch(cluster *Cluster) *Task {
	if task := n.genCreateRanges(cluster); task != nil {
		return task
	}
	return n.genDeleteRangesTask(cluster)
}

func (n *Instance) FindRangeFromRemote(rangeId uint64) *Range {
	for _, id := range n.AllRanges {
		if id == rangeId {
			r, find := n.GetRange(rangeId)
			if find {
				return r
			} else {
				log.Error("node[%s] ranges in remote not match the cache", n.GetAddress())
				return nil
			}
		}
	}
	return nil
}

type InstanceCache struct {
	lock      sync.RWMutex
	nodes     map[uint64]*Instance
}

func NewInstanceCache() *InstanceCache {
	return &InstanceCache{nodes: make(map[uint64]*Instance)}
}

func (nc *InstanceCache) Reset() {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.nodes = make(map[uint64]*Instance)
}

func (nc *InstanceCache) Add(n *Instance) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.nodes[n.ID()] = n
}

func (nc *InstanceCache) Delete(id uint64) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	delete(nc.nodes, id)
}

func (nc *InstanceCache) FindInstance(id uint64) (*Instance, bool) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if n, find := nc.nodes[id]; find {
		return n, true
	}
	return nil, false
}

func (nc *InstanceCache) GetAllInstances() []*Instance {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	var nodes []*Instance
	for _, n := range nc.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (nc *InstanceCache) GetAllActiveInstances() []*Instance {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	var nodes []*Instance
	for _, n := range nc.nodes {
		if n.State == metapb.NodeState_N_Login {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (nc *InstanceCache) Size() int {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	return len(nc.nodes)
}

func (nc *InstanceCache) LeaderSize() int {
	nc.lock.RLock()
	defer nc.lock.RUnlock()

	leaderSize := 0
	for _, n := range nc.nodes {
		leaderSize += n.GetLeaderNum()
	}
	return leaderSize
}

func (nc *InstanceCache) MarshalNode(id uint64) (*NodeDebug, error) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	node, ok := nc.nodes[id]
	if !ok {
		return nil, errors.New("node is not existed")
	}

	var ranges []*Range
	for _, r := range node.ranges.ranges {
		ranges = append(ranges, r)
	}
	return &NodeDebug{
		Node: node.Node,
		Ranges: ranges,
		Stats: node.Stats,
		LastHbTime: node.LastHbTime,
		//LastSchTime: node.lastTaskTime,
		//LastOpt: deepcopy.Iface(node.lastTask).(*taskpb.Task),
	}, nil
}


type InstanceAddrCache struct {
	lock      sync.RWMutex
	nodes     map[string]*Instance
}

func NewInstanceAddrCache() *InstanceAddrCache {
	return &InstanceAddrCache{nodes: make(map[string]*Instance)}
}

func (nc *InstanceAddrCache) Reset() {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.nodes = make(map[string]*Instance)
}

func (nc *InstanceAddrCache) Add(n *Instance) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.nodes[n.GetAddress()] = n
}

func (nc *InstanceAddrCache) Delete(addr string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	delete(nc.nodes, addr)
}

func (nc *InstanceAddrCache) FindInstance(addr string) (*Instance, bool) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if n, find := nc.nodes[addr]; find {
		return n, true
	}
	return nil, false
}

func (nc *InstanceAddrCache) GetAllInstances() []*Instance {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	var nodes []*Instance
	for _, n := range nc.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (nc *InstanceAddrCache) GetAllActiveInstances() []*Instance {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	var nodes []*Instance
	for _, n := range nc.nodes {
		if n.State == metapb.NodeState_N_Login {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (nc *InstanceAddrCache) Size() int {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	return len(nc.nodes)
}


type NodeMonitorScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	pusher           Pusher
	alarm            Alarm
}

func NewNodeMonitorScheduler(interval time.Duration, cluster *Cluster, pusher Pusher, alarm Alarm) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &NodeMonitorScheduler{
		name:     "node_monitor_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster: cluster,
		pusher: pusher,
		alarm: alarm,
	}
}

func (s *NodeMonitorScheduler) GetName() string {
	return s.name
}

func (s *NodeMonitorScheduler) Schedule() *Task {
	nodes := s.cluster.GetAllInstances()
	for _, n := range nodes {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}

		if n.State != metapb.NodeState_N_Logout {
			continue
		}

		ranges := n.GetAllRanges()
		if len(ranges) == 0 {
			//delete node
			if err := s.cluster.DeleteNode(n); err != nil {
				log.Error("delete node in memory [%v] error: %v", n.GetId())
				return nil
			}
			log.Info("node[%v] is deleted", n.GetId())
			continue
		}
	//	for _, r := range ranges {
	//		if r.AutoSchUnable {
	//			log.Debug("NodeMonitor: autosch unable")
	//			return nil
	//		}
	//		if r.State != metapb.RangeState_R_Normal {
	//			log.Debug("NodeMonitor: range[%s:%s:%d] state is not normal", r.GetDbName(), r.GetTableName(), r.GetId())
	//			return nil
	//		}
	//		if r.tasks.Size() > 0 {
	//			log.Debug("NodeMonitor: tasks size >0")
	//			return nil
	//		}
	//
	//		if len(r.PendingPeers) > 0 {
	//			log.Warn("NodeMonitor: range[%s:%s:%d] pendingpeers[%v] >0",
	//				r.GetDbName(), r.GetTableName(), r.GetId(), r.PendingPeers)
	//			return nil
	//		}
	//
	//		log.Warn("create range transfer task ..")
	//		if task := RangeTransfer(s.cluster, r, n.GetId()); task != nil {
	//			log.Debug("range[%v] transfer task created", r.GetId(), n.GetId())
	//			return task
	//		}
	//	}
	}

	return nil
}

func (s *NodeMonitorScheduler) AllowSchedule() bool {
	return true
}

func (s *NodeMonitorScheduler) GetInterval() time.Duration {
	return s.interval
}

func (s *NodeMonitorScheduler) Ctx() context.Context {
	return s.ctx
}

func (s *NodeMonitorScheduler) Stop() {
	s.cancel()
}
