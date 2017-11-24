package server

import (
	"time"

	"util/log"
	"util/deepcopy"
	"model/pkg/metapb"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
)

type SplitScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
}

func NewSplitScheduler(interval time.Duration, cluster *Cluster) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &SplitScheduler{
		name: "split_scheduler",
	    ctx: ctx,
	    cancel: cancel,
	    interval: interval,
		cluster: cluster}
}

func (s *SplitScheduler) GetName() string {
	return s.name
}

func (s *SplitScheduler) Schedule() *Task {
	for _, r := range s.cluster.GetAllRanges() {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		t := SplitSchedule(r, s.cluster)
		if t != nil {
			return t
		}
	}
	return nil
}

func (s *SplitScheduler) AllowSchedule() bool {
	if s.cluster.autoShardingUnable {
		return false
	}
	return true
}
func (s *SplitScheduler) GetInterval() time.Duration {
	return s.interval
}
func (s *SplitScheduler) Ctx() context.Context {
	return s.ctx
}
func (s *SplitScheduler) Stop() {
	s.cancel()
}

func SplitSchedule(r *Range, cluster *Cluster) *Task {
	table, find := cluster.FindTableById(r.GetDbId(), r.GetTableId())
	if !find {
		log.Error("table[%s:%s] not exit", r.GetDbName(), r.GetTableName())
		return nil
	}
	num := table.GetRangeNumber()
	threshold := uint64(num + 1 ) * RangeSizeStep
	if threshold > DefaultMaxRangeSize {
		threshold = DefaultMaxRangeSize
	}
	var splitProtect bool
	// 启动分裂保护
	if table.GetEpoch().GetVersion() > 1 {
		splitProtect = true
	}
	if !r.AllowSplit(threshold, cluster, splitProtect) {
		return nil
	}
	// leader所在的节点的任务数量是否达到上限
	nodeId := r.GetLeader().GetNodeId()
	if nodeId == 0 {
		// 没有leader
		return nil
	}
	node, find := cluster.FindInstance(nodeId)
	if !find {
		log.Error("node[%d] not found", nodeId)
		return nil
	}
	if !node.AllocSchSplit() {
		return nil
	}
	leftId, err := cluster.GenRangeId()
	if err != nil {
		log.Error("gen ID failed, err[%v]", err)
		return nil
	}
	rightId, err := cluster.GenRangeId()
	if err != nil {
		log.Error("gen ID failed, err[%v]", err)
		return nil
	}
	taskId, err := cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		return nil
	}
	rng := deepcopy.Iface(r.Range).(*metapb.Range)
	t := &taskpb.Task{
		Type:   taskpb.TaskType_RangeSplit,
		Meta:   &taskpb.TaskMeta{
			TaskId:   taskId,
			CreateTime: time.Now().Unix(),
			State: taskpb.TaskState_TaskWaiting,
			Timeout: DefaultSplitTaskTimeout,
		},
		RangeSplit: &taskpb.TaskRangeSplit{
			Range:       rng,
			LeftRangeId: leftId,
			RightRangeId: rightId,
			RangeSize: r.Stats.Size_,
		},
	}
	task := NewTask(t)
	//r.AddTask(task)
	log.Info("range[%s:%s:%d] split to [%d, %d]",
		r.GetDbName(), r.GetTableName(), r.GetId(), leftId, rightId)
	return task
}
