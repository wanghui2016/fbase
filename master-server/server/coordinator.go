package server

import (
	"sync"
	"time"
	"fmt"

	"golang.org/x/net/context"
	"util/log"
	"model/pkg/metapb"
	"util/deepcopy"
	"model/pkg/taskpb"
)

const (
	maxScheduleRetries = 10
)

type Coordinator struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cluster    *Cluster
	pusher     Pusher
	alarm      Alarm

	lock       sync.RWMutex
	schedulers map[string]Scheduler
}


func NewCoordinator(cluster *Cluster, pusher Pusher, alarm  Alarm) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		ctx:        ctx,
		cancel:     cancel,
		cluster:    cluster,
		pusher:     pusher,
		alarm:      alarm,
		schedulers: make(map[string]Scheduler),
	}
}

func (c *Coordinator) Dispatch(r *Range, applyTaskIndex uint64) *Task {
	// Check existed task.
	if t := c.getTask(r, applyTaskIndex); t != nil {
		return t
	}

	// check range task
	t := c.check(r)
	if t != nil {
		err := c.cluster.AddTask(t)
		if err != nil {
			log.Error("add task[%s] failed, err[%v]", t.Describe(), err)
			return nil
		}
		return c.getTask(r, applyTaskIndex)
	}
	return nil
}

func (c *Coordinator) tryDeletePeerTask(r *Range) *Task {
	// TODO select peer
	log.Warn("range[%s] peers num [%d]", r.SString(), len(r.GetPeers()))
	var downPeer *metapb.Peer
	// 如果有故障的节点，优先选择
	for _, p := range r.GetPeers() {
		node, find := c.cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("node[%d] not found", p.GetNodeId())
			continue
		}
		if node.IsFault() {
			downPeer = p
			break
		}
	}
	// 优先选择pending的副本
	if downPeer == nil {
		pendingPeers := r.GetPendingPeers()
		if len(pendingPeers) > 0 {
			downPeer = pendingPeers[0]
		}
	}
	if downPeer == nil {
		var nodeId uint64
		var offset uint64
		// 如果分片没有写，那么此种场景不能挑选落后出的副本
		if r.RaftStatus != nil {
			for _, rep := range r.RaftStatus.Replicas {
				if rep.Match < r.RaftStatus.Index {
					temp := r.RaftStatus.Index - rep.Match
					if temp > offset {
						offset = temp
						nodeId = rep.ID
					}
				}
			}
			if nodeId > 0 {
				downPeer = r.GetPeer(nodeId)
			}
		}
	}
	if downPeer == nil {
		// 选择节点分片数较多一个副本
		var rangesNum int
		for _, p :=  range r.GetPeers() {
			node, find := c.cluster.FindInstance(p.GetNodeId())
			if !find {
				log.Error("node[%d] not found", p.GetNodeId())
				continue
			}
			ranges := node.GetAllRanges()
			if len(ranges) > rangesNum && p.GetNodeId() != r.GetLeader().GetNodeId() {
				rangesNum = len(ranges)
				downPeer = p
			}
		}
	}
	if downPeer != nil {
		// leader不能删除
		if r.GetLeader().GetNodeId() == downPeer.GetNodeId() {
			return nil
		}
		taskId, err := c.cluster.GenTaskId()
		if err != nil {
			log.Error("gen taskID failed, err[%v]", err)
			return nil
		}
		t := &taskpb.Task{
			Type:   taskpb.TaskType_RangeDelPeer,
			Meta:   &taskpb.TaskMeta{
				TaskId:   taskId,
				CreateTime: time.Now().Unix(),
				State: taskpb.TaskState_TaskWaiting,
				Timeout: DefaultRangeDelPeerTaskTimeout,
			},
			RangeDelPeer: &taskpb.TaskRangeDelPeer{
				Range:      deepcopy.Iface(r.Range).(*metapb.Range),
				Peer:       downPeer,
			},
		}
		log.Info("range[%s] del peer[%d]!!!", r.SString(), downPeer.GetNodeId())
		return NewTask(t)
	}
	return nil
}

func (c *Coordinator) tryAddPeerTask(r *Range) *Task {
	// TODO select peer
	log.Warn("range[%s] peers num [%d]", r.SString(), len(r.GetPeers()))

	expRoom := func() *Room {
		mZoneP := c.cluster.deploy.GetMaster()
		expectRoom := selectExpectRoom(c.cluster, mZoneP, r)
		log.Info("select transfer expect room: %v, mzone: %v", expectRoom, mZoneP)
		if expectRoom == nil {
			for _, sZoneP := range c.cluster.deploy.GetSlaves() {
				expectRoom = selectExpectRoom(c.cluster, sZoneP, r)
				if expectRoom != nil {
					break
				}
			}
		}
		if expectRoom == nil {
			log.Warn("no expect room for range[%s]", r.SString())
			return nil
		}
		return expectRoom
	}()
	upInstance:= c.cluster.SelectTransferInstance(r, expRoom)
	if upInstance == nil {
		log.Error("select transfer peer is nil")
		return nil
	}
	upPeer := &metapb.Peer{
		Id: r.GetId(),
		NodeId: upInstance.GetId(),
	}
	taskId, err := c.cluster.GenTaskId()
	if err != nil {
		log.Error("gen taskID failed, err[%v]", err)
		return nil
	}
	t := &taskpb.Task{
		Type:   taskpb.TaskType_RangeAddPeer,
		Meta:   &taskpb.TaskMeta{
			TaskId:   taskId,
			CreateTime: time.Now().Unix(),
			State: taskpb.TaskState_TaskWaiting,
			Timeout: DefaultRangeAddPeerTaskTimeout,
		},
		RangeAddPeer: &taskpb.TaskRangeAddPeer{
			Range:      deepcopy.Iface(r.Range).(*metapb.Range),
			Peer:       upPeer,
		},
	}
	return NewTask(t)
}

func (c *Coordinator) check(r *Range) *Task {
    //if !r.AllowSch(c.cluster) {
	 //   return nil
    //}
	// 已经有任务了
	if r.GetTask() != nil || r.GetState() != metapb.RangeState_R_Normal {
		return nil
	}
	// NOTE: 如果多个副本都down，则不能选举出leader
	if !c.cluster.autoFailoverUnable {
		for _, down := range r.GetDownPeers() {
			if down.GetDownSeconds() > DefaultDownTimeLimit {
				return FailOverSchedule(down.GetPeer(), r, c.cluster, c.alarm)
			}
		}
	}

	// 检查peer数量
	if !c.cluster.autoTransferUnable {
		if len(r.GetPeers()) > DefaultPeersNum {
			return c.tryDeletePeerTask(r)
		}
		if len(r.GetPeers()) < DefaultPeersNum {
			return c.tryAddPeerTask(r)
		}
	}

	return nil
}

func (c *Coordinator) Run() {
	c.addScheduler(NewFailoverScheduler(time.Second * time.Duration(MasterConf.SchFailoverInterval), c.cluster, c.alarm))
	c.addScheduler(NewDiskBalanceScheduler(time.Second * time.Duration(MasterConf.SchDiskBalanceInterval), c.cluster, c.alarm))
	c.addScheduler(NewRangeGCRemoteScheduler(time.Second * time.Duration(MasterConf.SchRangeGCInterval), c.cluster))
	c.addScheduler(NewSplitScheduler(time.Second * time.Duration(MasterConf.SchSplitInterval), c.cluster))
	c.addScheduler(NewLeaderBalanceScheduler(time.Second * time.Duration(MasterConf.SchLeaderBalanceInterval), c.cluster))
	c.addScheduler(NewRangeGCLocalScheduler(time.Second*10, c.cluster, c.pusher))
	c.addScheduler(NewTaskMonitorScheduler(time.Second*5, c.cluster, c.pusher, c.alarm))
	c.addScheduler(NewNodeMonitorScheduler(time.Second*5, c.cluster, c.pusher, c.alarm))
	c.addScheduler(NewTableMonitorScheduler(time.Second*5, c.cluster, c.pusher))
	c.addScheduler(NewTopologyCheckScheduler(time.Minute * 50, c.cluster, c.alarm))
	c.addScheduler(NewMissRangesScheduler(time.Minute*10, c.cluster))
	//c.addScheduler(NewLeaderScoreBalanceScheduler(time.Second * time.Duration(MasterConf.SchLeaderScoreBalanceInterval), c.cluster))
	c.addScheduler(NewRangesBalanceScheduler(time.Second * time.Duration(MasterConf.SchRangeBalanceInterval), c.cluster))
	//c.addScheduler(NewRangeBalanceByNodeScoreScheduler(time.Second * time.Duration(MasterConf.SchRangeBalanceByNodeScoreInterval), c.cluster))
	c.addScheduler(NewRangeMonitorScheduler(time.Minute*5, c.cluster, c.alarm))
}

func (c *Coordinator) Stop() {
	c.cancel()
	for _, s := range c.schedulers {
		s.Stop()
	}
	c.wg.Wait()
}

func (c *Coordinator) runScheduler(s Scheduler) {
	defer c.wg.Done()

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
		    //if !c.cluster.IsLeader() {
			 //   continue
		    //}
			if !s.AllowSchedule() {
				continue
			}
		    if !c.cluster.IsLeader() {
			    continue
		    }
			for i := 0; i < maxScheduleRetries; i++ {
				t := s.Schedule()
				if t == nil {
					continue
				}
				if err := c.cluster.AddTask(t); err != nil {
					break
				}
			}
		case <-s.Ctx().Done():
			log.Info("%v stopped: %v", s.GetName(), s.Ctx().Err())
			return
		}
	}
}

func (c *Coordinator) addScheduler(s Scheduler) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.schedulers[s.GetName()]; ok {
		return ErrSchedulerExisted
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	return nil
}

func (c *Coordinator) removeScheduler(name string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	s, ok := c.schedulers[name]
	if !ok {
		return ErrSchedulerNotFound
	}

	s.Stop()
	delete(c.schedulers, name)
	return nil
}

func (c *Coordinator) cancelTask(t *Task, r *Range) error {
	state := t.GetMeta().State
	if !t.SwapState(state, taskpb.TaskState_TaskCancel) {
		return fmt.Errorf("task[%d] state alreay changed!!!", t.ID())
	}
	err := c.cluster.DeleteTask(t.ID())
	if err != nil {
		log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
		return err
	}
	// TODO 任务描述日志
	if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
		t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
		log.Error("pusher task info error: %s", err.Error())
	}
	//message := t.Describe() + " 任务状态: " + t.StateString()
	//c.alarm.Alarm("任务取消", message)
	log.Warn("range[%s] task[%s] has no execution environment", r.SString(), t.Describe())
	return nil
}

// 如果存在任务，applyTaskIndex绝对不会比当前的task ID大
func (c *Coordinator) getTask(r *Range, applyTaskIndex uint64) *Task {
	t := r.GetTask()
	if t == nil {
		return nil
	}
	switch t.GetMeta().GetState() {
	case taskpb.TaskState_TaskSuccess:
		return nil
	case taskpb.TaskState_TaskFail:
		return nil
	case taskpb.TaskState_TaskHangUp:
		return nil
	case taskpb.TaskState_TaskPause:
		return nil
	case taskpb.TaskState_TaskCancel:
		return nil
	}
	if applyTaskIndex < t.ID() && t.GetType() != taskpb.TaskType_EmptyRangeTask && t.GetType() != taskpb.TaskType_RangeDelete {
        if len(r.GetDownPeers()) > 1 || len(r.GetPendingPeers()) > 1 {
	        //不满足执行条件，删除任务并报警
	        // change leader结束
	        c.cancelTask(t, r)
	        return nil
        }
	}
	if applyTaskIndex > t.ID() {
		switch t.GetType() {
		// 需要查看子任务
		case taskpb.TaskType_RangeFailOver:
		case taskpb.TaskType_RangeTransfer:
		default:
			log.Error("task[%d: %s] coordinator error, applyTaskId[%d]!!!", t.ID(), t.Describe(), applyTaskIndex)
			return nil
		}
	}
	switch t.GetType() {
	// 需要查看子任务
	case taskpb.TaskType_RangeFailOver:
		return c.getRangeFailOverTask(r, t, applyTaskIndex)
	case taskpb.TaskType_RangeTransfer:
		return c.getRangeTransferTask(r, t, applyTaskIndex)
	case taskpb.TaskType_RangeSplit:
		return c.getRangeSplitTask(r, t, applyTaskIndex)
	case taskpb.TaskType_RangeLeaderTransfer:
		return c.getRangeLeaderTransferTask(r, t, applyTaskIndex)
	// 需要查看子任务
	case taskpb.TaskType_RangeAddPeer:
		return c.getRangeAddPeerTask(r, t, applyTaskIndex)
	// 需要查看子任务
	case taskpb.TaskType_RangeDelPeer:
		return c.getRangeDelPeerTask(r, t, applyTaskIndex)
	case taskpb.TaskType_RangeOffline:
		return c.getRangeOfflineTask(r, t, applyTaskIndex)
		// 由节点执行删除
	case taskpb.TaskType_RangeDelete:
		if t.GetMeta().GetState() == taskpb.TaskState_TaskWaiting {
			err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
			if err != nil {
				log.Warn("update task status failed, err[%v]", err)
				return nil
			}
		}

		return nil
	case taskpb.TaskType_RangeCreate:
		return c.getRangeCreateTask(r, t, applyTaskIndex)
	case taskpb.TaskType_EmptyRangeTask:
		return c.getEmptyTask(r, t, applyTaskIndex)
	}


	return nil
}

func (c *Coordinator) getRangeFailOverTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	for _, subTask := range t.SubTasks {
		// 没有拿到任务，重新发送一次
		if applyTaskIndex < subTask.ID() {
			// 如果删除的是leader节点，那么直接取消任务
			if subTask.GetType() == taskpb.TaskType_RangeDelPeer {
				downNodeId := subTask.GetRangeDelPeer().GetPeer().GetNodeId()
				if r.GetLeader().GetNodeId() == downNodeId {
					c.cancelTask(t, r)
					return nil
				}
				node, find := c.cluster.FindInstance(downNodeId)
				if !find {
					log.Error("node[%s] not found!!", downNodeId)
					return nil
				}
				// 恢复正常了, 如果任务还没有执行，则取消任务
				if !node.IsFault() {
					c.cancelTask(t, r)
					return nil
				}
			}

			err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
			if err != nil {
				log.Warn("update task status failed, err[%v]", err)
				return nil
			}
			return subTask.Clone()
		}
		if applyTaskIndex > subTask.ID() {
			continue
		}
		// 已经处理了任务，检查任务是否已经结束
		if subTask.GetType() == taskpb.TaskType_RangeDelPeer {
			find := false
			for _, p := range r.GetPeers() {
				if p.GetNodeId() == subTask.GetRangeDelPeer().GetPeer().GetNodeId() {
					find = true
					break
				}
			}
			// 删除节点失败, 重试
			if find {
				if r.GetLeader().GetNodeId() == subTask.GetRangeDelPeer().GetPeer().GetNodeId() {
					c.cancelTask(t, r)
					return nil
				}
				log.Info("range[%s] 删除副本失败，重试", r.SString())
				return subTask.Clone()
			} else {
				// 删除节点成功，进行下一个任务
				log.Info("range[%s] 删除副本成功!!!", r.SString())
				continue
			}
		} else if subTask.GetType() == taskpb.TaskType_RangeAddPeer {
			find := false
			for _, p := range r.GetPeers() {
				if p.GetNodeId() == subTask.GetRangeAddPeer().GetPeer().GetNodeId() {
					find = true
					break
				}
			}
			// 添加节点失败, 重试
			if !find {
				fmt.Println("添加副本失败，重试")
				return subTask.Clone()
			}
			// 检查新节点是否完成数据迁移
			if len(r.GetPendingPeers()) == 0 {
				//任务完成，可以删除任务了
				err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
				if err != nil {
					log.Warn("task swap state failed, err[%v]", err)
				}
				err = c.cluster.DeleteTask(t.ID())
				if err != nil {
					log.Warn("delete task[%s] failed, err[%v]", t.Describe(), err)
					return nil
				}
				// TODO push任务描述日志
				if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
					t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
					log.Error("pusher task info error: %s", err.Error())
				}
				log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
			} else {
				// 数据迁移还在进行
				log.Debug("任务仍在进行中......")
				return nil
			}
		}

	}
	return nil
}

func (c *Coordinator) getRangeTransferTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	for _, subTask := range t.SubTasks {
		// 没有拿到任务，重新发送一次
		if applyTaskIndex < subTask.ID() {
			if subTask.GetType() == taskpb.TaskType_RangeDelPeer {
				//任务异常,删除的副本是leader
				downNodeId := subTask.GetRangeDelPeer().GetPeer().GetNodeId()
				if r.GetLeader().GetNodeId() == downNodeId {
					c.cancelTask(t, r)
					return nil
				}
			}
			err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
			if err != nil {
				log.Warn("update task status failed, err[%v]", err)
				return nil
			}
			return subTask.Clone()
		}
		if applyTaskIndex > subTask.ID() {
			continue
		}
		// 已经处理了任务，检查任务是否已经结束
		if subTask.GetType() == taskpb.TaskType_RangeAddPeer {
			find := false
			for _, p := range r.GetPeers() {
				if p.GetNodeId() == subTask.GetRangeAddPeer().GetPeer().GetNodeId() {
					find = true
					break
				}
			}
			// 添加节点失败, 重试
			if !find {
				return subTask.Clone()
			}
			// 检查新节点是否完成数据迁移
			if len(r.GetPendingPeers()) == 0 {
				log.Info("range[%s] finish sub task(add peer) success!!", r.SString())
				//任务完成，继续下一个子任务
				continue
			} else {
				// 数据迁移还在进行
				return nil
			}
		} else if subTask.GetType() == taskpb.TaskType_RangeDelPeer {
			find := false
			for _, p := range r.GetPeers() {
				if p.GetNodeId() == subTask.GetRangeDelPeer().GetPeer().GetNodeId() {
					find = true
					break
				}
			}
			// 删除节点失败, 重试
			if find {
				if r.GetLeader().GetNodeId() == subTask.GetRangeDelPeer().GetPeer().GetNodeId() {
					c.cancelTask(t, r)
					return nil
				}
				return subTask.Clone()
			} else {
				err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
				if err != nil {
					log.Warn("task swap state failed, err[%v]", err)
				}
				// 删除节点成功，任务结束
				err = c.cluster.DeleteTask(t.ID())
				if err != nil {
					log.Warn("delete task[%s] failed, err[%v]", t.Describe(), err)
					return nil
				}
				// TODO 任务描述日志
				if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
					t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
					log.Error("pusher task info error: %s", err.Error())
				}
				log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
			}
		}
	}
	return nil
}

func (c *Coordinator) getRangeSplitTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	// 存在split ack上报失败的情况,此时range的状态已经是offline, 仍然需要通过重试任务达到再次分裂上报
	if applyTaskIndex < t.ID() {
		err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
		if err != nil {
			log.Warn("update task status failed, err[%v]", err)
			return nil
		}
		return t.Clone()
	} else {
		// 分裂任务必须等到所有的副本都分裂完成才算分裂完成
		// leader发生切换，导致记录的最后一次任务ID比当前的小
		// 已经发生了分裂
		if r.State == metapb.RangeState_R_Normal {
			return t.Clone()
		}
		for _, p := range r.Peers {
			node, find := c.cluster.FindInstance(p.GetNodeId())
			if !find {
				log.Error("node[%d] not found", p.GetNodeId())
				return nil
			}
			// 仍然有节点没有开始分裂，或者分裂未完成
			if notWorkingRange := node.GetNotWorkingRange(p.GetId()); notWorkingRange == nil {
				if r.Trace || log.IsEnableDebug() {
					log.Info("range[%s] task[%s] working..., peer[%d] still not split",
						r.SString(), t.Describe(), p.GetNodeId())
				}
				return nil
			}
		}
		// 分裂结束
		err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
		if err != nil {
			log.Warn("task swap task failed, err[%v]", err)
		}
		err = c.cluster.DeleteTask(t.ID())
		if err != nil {
			log.Warn("delete task[%s] failed, err[%v]", t.Describe(), err)
			return nil
		}
		// TODO 任务描述日志
		if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
	}
	return nil
}

func (c *Coordinator) getRangeAddPeerTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	// 没有拿到任务，重新发送一次
	if applyTaskIndex < t.ID() {
		err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
		if err != nil {
			log.Warn("update task status failed, err[%v]", err)
			return nil
		}
		return t.Clone()
	}
	// impossible
	if applyTaskIndex > t.ID() {
		return nil
	}
	// 已经处理了任务，检查任务是否已经结束
	find := false
	for _, p := range r.GetPeers() {
		if p.GetNodeId() == t.GetRangeAddPeer().GetPeer().GetNodeId() {
			find = true
			break
		}
	}
	// 添加节点失败, 重试
	if !find {
		log.Debug("添加副本失败，重试")
		return t.Clone()
	}
	// 检查新节点是否完成数据迁移
	if len(r.GetPendingPeers()) == 0 {
		err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
		if err != nil {
			log.Warn("task swap task failed, err[%v]", err)
		}
		//任务完成，可以删除任务了
		err = c.cluster.DeleteTask(t.ID())
		if err != nil {
			log.Warn("delete task[%s] failed, err[%v]", t.Describe(), err)
			return nil
		}
		// TODO push任务描述日志
		if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
	} else {
		// 数据迁移还在进行
		log.Debug("任务仍在进行中......")
		return nil
	}
	return nil
}

func (c *Coordinator) getRangeDelPeerTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	// 没有拿到任务，重新发送一次
	if applyTaskIndex < t.ID() {
		err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
		if err != nil {
			log.Warn("update task status failed, err[%v]", err)
			return nil
		}
		return t.Clone()
	}
	if applyTaskIndex > t.ID() {
		return nil
	}
	// 已经处理了任务，检查任务是否已经结束
	find := false
	for _, p := range r.GetPeers() {
		if p.GetNodeId() == t.GetRangeDelPeer().GetPeer().GetNodeId() {
			find = true
			break
		}
	}
	// 删除节点失败, 重试
	if find {
		fmt.Println("删除副本失败，重试")
		return t.Clone()
	} else {
		// 删除节点成功，任务结束
		err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
		if err != nil {
			log.Warn("task swap state failed, err[%v]", err)
		}
		err = c.cluster.DeleteTask(t.ID())
		if err != nil {
			log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
			return nil
		}
		// TODO 任务描述日志
		if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
	}
	return nil
}

func (c *Coordinator) getRangeCreateTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	if t.GetMeta().GetState() == taskpb.TaskState_TaskWaiting {
		err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
		if err != nil {
			log.Warn("update task status failed, err[%v]", err)
			return nil
		}
	}
	for _, p := range r.GetPeers() {
		node, find := c.cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("node[%d] not found", p.GetNodeId())
			return nil
		}
		// 仍然有副本没有创建
		if node.FindRangeFromRemote(p.GetId()) == nil {
			return nil
		}
	}
	// 所有副本都创建完成
	err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
	if err != nil {
		log.Warn("task swap state failed, err[%v]", err)
	}
	if err = c.cluster.DeleteTask(t.ID()); err != nil {
		log.Error("delete task [delete table] error: %v", err)
		return nil
	}
	// TODO 任务描述日志
	if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
		t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
		log.Error("pusher task info error: %s", err.Error())
	}
	log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
	return nil
}

func (c *Coordinator) getRangeOfflineTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	if applyTaskIndex < t.ID() {
		err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
		if err != nil {
			log.Warn("update task status failed, err[%v]", err)
			return nil
		}
		log.Debug("get task: range offline")
		return t.Clone()
	}
	if applyTaskIndex == t.ID() {
		log.Debug("delete task: range offline")
		err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
		if err != nil {
			log.Warn("task swap state failed, err[%v]", err)
		}
		if err := c.cluster.DeleteTask(t.ID()); err != nil {
			log.Error("delete task [delete table] error: %v", err)
			return nil
		}
		// TODO 任务描述日志
		if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
		return nil
	}
	return nil
}

func (c *Coordinator) getRangeLeaderTransferTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	if applyTaskIndex < t.ID() && r.GetLeader().GetNodeId() == t.GetRangeLeaderTransfer().GetPreLeader().GetNodeId(){
		err := t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, c.cluster)
		if err != nil {
			log.Warn("update task status failed, err[%v]", err)
			return nil
		}
		return t.Clone()
	} else {
		// change leader结束
		err := t.SwapAndSaveState(t.GetMeta().GetState(), taskpb.TaskState_TaskSuccess, c.cluster)
		if err != nil {
			log.Warn("task swap state failed, err[%v]", err)
		}
		err = c.cluster.DeleteTask(t.ID())
		if err != nil {
			log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
			return nil
		}
		// TODO 任务描述日志
		if err := c.pusher.Push(TaskSQL, c.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
			t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe(), (c.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher task info error: %s", err.Error())
		}
		log.Info("range[%s] task[%s] do success", r.SString(), t.Describe())
	}
	return nil
}

func (c *Coordinator) getEmptyTask(r *Range, t *Task, applyTaskIndex uint64) *Task {
	count := 0
	for _, p := range r.GetPeers() {
		node, find := c.cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("node[%d] not found", p.GetNodeId())
			return nil
		}
		// 仍然有副本没有创建
		if node.FindRangeFromRemote(p.GetId()) == nil {
			count++
		}
	}
	// 多个副本没有分裂完成
	if count > 1 {
		return nil
	}
	// 还有一个副本没有分裂完成，检查是否存在分裂失败任务
	if count == 1 {
		table, find := c.cluster.FindTableById(r.GetDbId(), r.GetTableId())
		if !find {
			log.Error("table[%s:%s] not found", r.GetDbName(), r.GetTableName())
			return nil
		}
		for _, _r := range table.GetAllRanges() {
			if tt := _r.GetTask(); tt != nil {
				// 找到分裂失败的任务
				if tt.GetType() == taskpb.TaskType_RangeSplit && tt.GetMeta().GetState() == taskpb.TaskState_TaskFail {
					// 确实有分裂失败的任务导致新的分片缺失副本, 只缺少一个副本,　可以自动补齐
					if tt.GetRangeSplit().GetLeftRangeId() == r.GetId() || tt.GetRangeSplit().GetRightRangeId() == r.GetId() {
						if err := c.cluster.DeleteTask(t.ID()); err != nil {
							log.Error("delete task [delete table] error: %v", err)
							return nil
						}
					}
				}
			}
		}
	} else {
		// 所有副本都创建完成
		if err := c.cluster.DeleteTask(t.ID()); err != nil {
			log.Error("delete task [delete table] error: %v", err)
			return nil
		}
	}
	return nil
}

func rangesCountInRoom(cluster *Cluster, room *Room, r *Range) int32 {
	var count int32
	for _, peer := range r.GetPeers() {
		_ins, find := cluster.FindInstance(peer.GetNodeId())
		if !find {
			log.Error("instance[%d] not exist", peer.GetNodeId())
			return 0
		}
		_mac, find := cluster.FindMachine(_ins.MacIp)
		if !find {
			log.Error("machine[%s] not exist", _ins.MacIp)
			return 0
		}
		_zone, find := cluster.FindZone(_mac.GetZone().GetZoneName())
		if !find {
			log.Error("zone[%s] not exist", _mac.GetZone().GetZoneName())
			return 0
		}
		_room, find := _zone.FindRoom(_mac.GetRoom().GetRoomName())
		if !find {
			log.Error("room[%s:%s] not exist", _mac.GetZone().GetZoneName(), _mac.GetRoom().GetRoomName())
			return 0
		}
		if _room.GetRoomName() == room.GetRoomName() {
			count++
		}
	}
	return count
}

func selectExpectRoom(cluster *Cluster, zoneP *metapb.ZoneV1Policy, r *Range) *Room {
	zone, find := cluster.FindZone(zoneP.GetZoneName())
	if !find {
		log.Error("zone[%s] not exist", zoneP.GetZoneName())
		return nil
	}
	room, find := zone.FindRoom(zoneP.GetMaster().GetRoomName())
	if !find {
		log.Error("room[%s:%s] not exist", zoneP.GetZoneName(), zoneP.GetMaster().GetRoomName())
		return nil
	}
	count := rangesCountInRoom(cluster, room, r)
	if count < zoneP.GetMaster().GetDsNum() {
		return room
	} else {
		for _, sRoomp := range zoneP.GetSlaves() {
			room, find = zone.FindRoom(sRoomp.GetRoomName())
			if !find {
				log.Error("room[%s:%s] not exist", zoneP.GetZoneName(), sRoomp.GetRoomName())
				return nil
			}
			count = rangesCountInRoom(cluster, room, r)
			if count < sRoomp.GetDsNum() {
				return room
			}
		}
	}
	return nil
}
