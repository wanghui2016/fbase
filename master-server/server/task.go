package server

import (
	"fmt"
	"sync"
	"time"
	"errors"

	"util/deepcopy"
	"util/log"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
	"model/pkg/metapb"
)

var (
	// 单位是秒
	DefaultDownTimeLimit       uint64 = 60
	DefaultTimeFormat                 = "2006-01-02 15:04:05"
	DefaultTransferTaskTimeout uint64 = uint64(time.Minute * time.Duration(20))
	DefaultFailOverTaskTimeout uint64 = uint64(time.Minute * time.Duration(20))
	// 大于一个调度周期+一个心跳周期，预留冗余
	DefaultSplitTaskTimeout        uint64        = uint64(time.Second * time.Duration(300))
	DefaultChangeLeaderTaskTimeout uint64        = uint64(time.Second * time.Duration(300))
	DefaultRangeDeleteTaskTimeout  uint64        = uint64(time.Minute * time.Duration(30))
	DefaultRangeAddPeerTaskTimeout uint64        = uint64(time.Minute * time.Duration(15))
	DefaultRangeDelPeerTaskTimeout uint64        = uint64(time.Second * time.Duration(300))
	DefaultRangeCreateTaskTimeout  uint64        = uint64(time.Second * time.Duration(300))
	DefaultRangeGCTimeout          uint64        = uint64(time.Minute * 5)
	DefaultEmptyTaskTimeout        uint64        = uint64(time.Minute * 5)
	DefaultMaxTaskExecuteTime      uint64        = uint64(time.Minute * 60)
	DefaultTableCreateTaskTimeout  uint64        = uint64(time.Second * time.Duration(10))
	DefaultTableDeleteTaskTimeout  uint64        = uint64(time.Second * time.Duration(300))
)

type Task struct {
	lock sync.RWMutex
	*taskpb.Task
	// range 分裂和迁移才有子任务
	SubTasks      []*Task
	lastAlarmTime time.Time
	nextAlarmTime time.Time
}

func NewTask(t *taskpb.Task) *Task {
	// 检查任务是否合法
	if t.GetMeta().GetTimeout() == 0 {
		log.Error("invalid task, every task has timeout")
		return nil
	}

	return &Task{Task: t, lastAlarmTime: time.Now(), nextAlarmTime: time.Now().Add(DefaultAlarmInterval)}
}

// id1 < id2
func (t *Task) GenSubTasks(c *Cluster) error {
	var subTasks []*Task
	id1, err := c.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		return err
	}
	id2, err := c.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		return err
	}
	switch t.GetType() {
	case taskpb.TaskType_RangeTransfer:
		addPeer := &taskpb.Task{
			Type: taskpb.TaskType_RangeAddPeer,
			Meta: &taskpb.TaskMeta{
				TaskId:     id1,
				CreateTime: time.Now().Unix(),
				State:      taskpb.TaskState_TaskWaiting,
				Timeout:    DefaultRangeAddPeerTaskTimeout,
			},
			RangeAddPeer: &taskpb.TaskRangeAddPeer{
				Range: t.GetRangeTransfer().GetRange(),
				Peer:  t.GetRangeTransfer().GetUpPeer(),
			},
		}
		subTasks = append(subTasks, NewTask(addPeer))
		delPeer := &taskpb.Task{
			Type: taskpb.TaskType_RangeDelPeer,
			Meta: &taskpb.TaskMeta{
				TaskId:     id2,
				CreateTime: time.Now().Unix(),
				State:      taskpb.TaskState_TaskWaiting,
				Timeout:    DefaultRangeDelPeerTaskTimeout,
			},
			RangeDelPeer: &taskpb.TaskRangeDelPeer{
				Range: t.GetRangeTransfer().GetRange(),
				Peer:  t.GetRangeTransfer().GetDownPeer(),
			},
		}
		subTasks = append(subTasks, NewTask(delPeer))
		t.SubTasks = subTasks
	case taskpb.TaskType_RangeFailOver:
		delPeer := &taskpb.Task{
			Type: taskpb.TaskType_RangeDelPeer,
			Meta: &taskpb.TaskMeta{
				TaskId:     id1,
				CreateTime: time.Now().Unix(),
				State:      taskpb.TaskState_TaskWaiting,
				Timeout:    DefaultRangeDelPeerTaskTimeout,
			},
			RangeDelPeer: &taskpb.TaskRangeDelPeer{
				Range: t.GetRangeFailover().GetRange(),
				Peer:  t.GetRangeFailover().GetDownPeer(),
			},
		}
		subTasks = append(subTasks, NewTask(delPeer))
		addPeer := &taskpb.Task{
			Type: taskpb.TaskType_RangeAddPeer,
			Meta: &taskpb.TaskMeta{
				TaskId:     id2,
				CreateTime: time.Now().Unix(),
				State:      taskpb.TaskState_TaskWaiting,
				Timeout:    DefaultRangeAddPeerTaskTimeout,
			},
			RangeAddPeer: &taskpb.TaskRangeAddPeer{
				Range: t.GetRangeFailover().GetRange(),
				Peer:  t.GetRangeFailover().GetUpPeer(),
			},
		}
		subTasks = append(subTasks, NewTask(addPeer))
		t.SubTasks = subTasks
	}
	return nil
}

func (t *Task) ID() uint64 {
	return t.GetMeta().GetTaskId()
}

func (t *Task) Clone() *Task {
	_t := deepcopy.Iface(t.Task).(*taskpb.Task)
	var subTasks []*Task
	for _, sub := range t.SubTasks {
		subTasks = append(subTasks, sub.Clone())
	}
	return &Task{
		Task:          _t,
		SubTasks:      subTasks,
		lastAlarmTime: t.lastAlarmTime,
	}
}

func (t *Task) State() taskpb.TaskState {
	return t.GetMeta().GetState()
}

func (t *Task) CreateTime() time.Time {
	return time.Unix(t.GetMeta().GetCreateTime(), 0)
}

func (t *Task) Timeout() uint64 {
	return t.GetMeta().GetTimeout()
}

func (t *Task) Describe() string {
	if t == nil {
		return ""
	}
	var describe string
	switch t.Type {
	case taskpb.TaskType_EmptyRangeTask:
		rangeId := t.GetRangeEmpty().GetRangeId()
		describe = fmt.Sprintf("空任务，创建于%s, 分片[%d]",
			t.CreateTime().String(), rangeId)
	case taskpb.TaskType_NodeFailOver:
		nodeId := t.GetNodeFailover().GetNodeId()
		describe = fmt.Sprintf("故障恢复任务，创建于%s, 故障节点[%d]",
			t.CreateTime().String(), nodeId)
	case taskpb.TaskType_NodeLogin:
		nodeId := t.GetNodeLogin().GetNodeId()
		describe = fmt.Sprintf("节点激活任务，创建于%s, 故障节点[%d]",
			t.CreateTime().String(), nodeId)
	case taskpb.TaskType_NodeLogout:
		nodeId := t.GetNodeLogout().GetNodeId()
		describe = fmt.Sprintf("节点下线任务，创建于%s, 故障节点[%d]",
			t.CreateTime().String(), nodeId)
	case taskpb.TaskType_NodeDeleteRanges:
		rangeIds := t.GetNodeDeleteRanges().GetRangeIds()
		describe = fmt.Sprintf("节点删除分片副本任务，创建于%s, 分片[%v]",
			t.CreateTime().String(), rangeIds)
	case taskpb.TaskType_RangeLeaderTransfer:
		r := t.GetRangeLeaderTransfer().GetRange()
		preLeader := t.GetRangeLeaderTransfer().GetPreLeader()
		expLeader := t.GetRangeLeaderTransfer().GetExpLeader()
		describe = fmt.Sprintf("分片 raft leader切换任务, 创建于%s, 分片信息[%s:%s:%d], 原leader节点[%d], 期望leader节点[%d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId(), preLeader.GetNodeId(), expLeader.GetNodeId())
	case taskpb.TaskType_RangeDelete:
		r := t.GetRangeDelete().GetRange()
		describe = fmt.Sprintf("分片删除任务, 创建于%s, 分片信息[%s:%s:%d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId())
	case taskpb.TaskType_RangeSplit:
		r := t.GetRangeSplit().GetRange()
		size := t.GetRangeSplit().GetRangeSize()
		left := t.GetRangeSplit().GetLeftRangeId()
		right := t.GetRangeSplit().GetRightRangeId()
		describe = fmt.Sprintf("分片分裂任务, 创建于%s, 分片信息[%s:%s:%d], range大小[%d], 分裂后的range[%d, %d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId(), size, left, right)
	case taskpb.TaskType_RangeTransfer:
		r := t.GetRangeTransfer().GetRange()
		up := t.GetRangeTransfer().GetUpPeer()
		down := t.GetRangeTransfer().GetDownPeer()
		describe = fmt.Sprintf("分片副本迁移任务, 创建于%s, 分片信息[%s:%s:%d], 迁移副本[%d --> %d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId(),
			down.GetNodeId(), up.GetNodeId())
	case taskpb.TaskType_RangeFailOver:
		r := t.GetRangeFailover().GetRange()
		up := t.GetRangeFailover().GetUpPeer()
		down := t.GetRangeFailover().GetDownPeer()
		describe = fmt.Sprintf("分片副本故障恢复任务, 创建于%s, 分片信息[%s:%s:%d], 迁移副本[%d --> %d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId(),
			down.GetNodeId(), up.GetNodeId())
	case taskpb.TaskType_RangeAddPeer:
		r := t.GetRangeAddPeer().GetRange()
		up := t.GetRangeAddPeer().GetPeer()
		describe = fmt.Sprintf("分片副本添加任务, 创建于%s, 分片信息[%s:%s:%d], 添加副本[%d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId(),
			up.GetNodeId())
	case taskpb.TaskType_RangeDelPeer:
		r := t.GetRangeDelPeer().GetRange()
		down := t.GetRangeDelPeer().GetPeer()
		describe = fmt.Sprintf("分片副本删除任务, 创建于%s, 分片信息[%s:%s:%d], 删除副本[%d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId(),
			down.GetNodeId())
	case taskpb.TaskType_RangeOffline:
		r := t.GetRangeOffline().GetRange()
		describe = fmt.Sprintf("分片下线任务, 创建于%s, 分片信息[%s:%s:%d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId())
	case taskpb.TaskType_RangeCreate:
		r := t.GetRangeCreate().GetRange()
		describe = fmt.Sprintf("分片创建任务, 创建于%s, 分片信息[%s:%s:%d]",
			t.CreateTime().String(), r.GetDbName(), r.GetTableName(), r.GetId())
	}
	return fmt.Sprintf("任务ID:%d ", t.GetMeta().GetTaskId()) + "任务详情: " + describe
}

func (t *Task) StateString() string {
	switch t.GetMeta().GetState() {
	case taskpb.TaskState_TaskWaiting:
		return "任务等待执行......"
	case taskpb.TaskState_TaskRunning:
		return fmt.Sprintf("任务执行中......, 已经运行了[%s]", time.Since(t.CreateTime()).String())
	case taskpb.TaskState_TaskFail:
		return "任务执行失败"
	case taskpb.TaskState_TaskTimeout:
		return fmt.Sprintf("任务执行超时, 已经运行了[%s]", time.Since(t.CreateTime()).String())
	case taskpb.TaskState_TaskSuccess:
		return fmt.Sprintf("任务执行成功, 耗时[%s]", time.Since(t.CreateTime()).String())
	case taskpb.TaskState_TaskHangUp:
		return "任务挂起"
	case taskpb.TaskState_TaskPause:
		return "任务暂停"
	case taskpb.TaskState_TaskCancel:
		return "任务取消"
	}
	return "未知状态"
}

func (t *Task) IsTaskTimeout() bool {
	if t.State() == taskpb.TaskState_TaskTimeout {
		return true
	}
	return false
}

func (t *Task) IsTaskFail() bool {
	if t.State() == taskpb.TaskState_TaskFail {
		return true
	}
	return false
}

func (t *Task) UpdateLastAlarmTime() {
	t.lastAlarmTime = time.Now()
}

func (t *Task) GetLastAlarmTime() time.Time {
	return t.lastAlarmTime
}

func (t *Task) UpdateNextAlarmTime(interval time.Duration) {
	t.nextAlarmTime = time.Now().Add(interval)
}

func (t *Task) GetNextAlarmTime() time.Time {
	return t.nextAlarmTime
}

func (t *Task) SwapState(old, new taskpb.TaskState) bool {
	if t.GetMeta().GetState() != old {
		return false
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.GetMeta().GetState() == old {
		t.GetMeta().State = new
		return true
	}
	return false
}

func (t *Task) SwapAndSaveState(old, new taskpb.TaskState, cluster *Cluster) error {
	if t.GetMeta().GetState() == new {
		return nil
	}
	if t.GetMeta().GetState() != old {
		return errors.New("task state already changed")
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.GetMeta().GetState() == old {
		task := deepcopy.Iface(t.Task).(*taskpb.Task)
		task.GetMeta().State = new
		err := cluster.storeTask(task)
		if err != nil {
			return err
		}
		t.Task = task
		return nil
	}
	return errors.New("task state already changed")
}

func (t *Task) Restart(cluster *Cluster) error {
	if t.GetMeta().GetState() == taskpb.TaskState_TaskRunning {
		return nil
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.GetMeta().GetState() == taskpb.TaskState_TaskRunning {
		return nil
	}
	task := deepcopy.Iface(t.Task).(*taskpb.Task)
	// 重置创建时间
	task.Meta.CreateTime = time.Now().Unix()
	task.Meta.State = taskpb.TaskState_TaskRunning
	err := cluster.storeTask(task)
	if err != nil {
		return err
	}
	t.Task = task
	return nil
}

type TaskCache struct {
	lock  sync.RWMutex
	tasks map[uint64]*Task
}

func NewTaskCache() *TaskCache {
	return &TaskCache{tasks: make(map[uint64]*Task)}
}

func (tc *TaskCache) Reset() {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tasks = make(map[uint64]*Task)
}

func (tc *TaskCache) Add(t *Task) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tasks[t.ID()] = t
}

// 只允许存在一个任务
func (tc *TaskCache) SetNx(t *Task) bool {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if len(tc.tasks) > 0 {
		return false
	}
	tc.tasks[t.ID()] = t
	return true
}

func (tc *TaskCache) Delete(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	delete(tc.tasks, id)
}

func (tc *TaskCache) FindTask(id uint64) (*Task, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tasks[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *TaskCache) GetAllTask() []*Task {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tasks []*Task
	for _, t := range tc.tasks {
		tasks = append(tasks, t)
	}
	return tasks
}

func (tc *TaskCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tasks)
}


type TaskMonitorScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	pusher           Pusher
	alarm            Alarm
}

func NewTaskMonitorScheduler(interval time.Duration, cluster *Cluster, pusher Pusher, alarm Alarm) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &TaskMonitorScheduler{
		name:     "task_monitor_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster: cluster,
		pusher: pusher,
		alarm: alarm,
	}
}

func (db *TaskMonitorScheduler) GetName() string {
	return db.name
}

func (db *TaskMonitorScheduler) Schedule() *Task {
	nodes := db.cluster.GetAllInstances()
	logoutNodes := make(map[uint64]*Instance)
	for _, n := range nodes {
		select {
		case <-db.ctx.Done():
			return nil
		default:
		}
		if n.State == metapb.NodeState_N_Logout {
			logoutNodes[n.GetId()] = n
		}
	}

	tasks := db.cluster.GetAllTask()
	for _, t := range tasks {
		select {
		case <-db.ctx.Done():
			return nil
		default:
		}
		if t.GetType() == taskpb.TaskType_EmptyRangeTask {
			continue
		}
		state := t.GetMeta().GetState()

		// dst node is bad, cancel this task
		var dstNodeId uint64
		switch t.GetType() {
		case taskpb.TaskType_RangeTransfer:
			if tsk := t.GetRangeTransfer(); tsk != nil {
				dstNodeId = tsk.GetUpPeer().GetNodeId()
			}
		case taskpb.TaskType_RangeFailOver:
			if tsk := t.GetRangeFailover(); tsk != nil {
				dstNodeId = tsk.GetUpPeer().GetNodeId()
			}
		case taskpb.TaskType_RangeLeaderTransfer:
			if tsk := t.GetRangeLeaderTransfer(); tsk != nil {
				dstNodeId = tsk.GetExpLeader().GetNodeId()
			}
		case taskpb.TaskType_RangeAddPeer:
			if tsk := t.GetRangeAddPeer(); tsk != nil {
				dstNodeId = tsk.GetPeer().GetNodeId()
			}
		case taskpb.TaskType_RangeDelPeer:
			if tsk := t.GetRangeDelPeer(); tsk != nil {
				dstNodeId = tsk.GetPeer().GetNodeId()
			}
		}
		if _, nodeIsLogout := logoutNodes[dstNodeId]; nodeIsLogout {
			state = taskpb.TaskState_TaskCancel
		}

		switch state {
		case taskpb.TaskState_TaskWaiting:
			// 任务没有执行就已经超时,直接取消任务
			if time.Since(t.CreateTime()) > time.Duration(t.Timeout()) {
				err := t.SwapAndSaveState(state, taskpb.TaskState_TaskCancel, db.cluster)
				if err != nil {
					log.Warn("task swap state failed, err[%v]", err)
					return nil
				}
				err = db.cluster.DeleteTask(t.ID())
				if err != nil {
					log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
					return nil
				}
				// TODO 任务描述日志
				t.GetMeta().State = taskpb.TaskState_TaskCancel
				if err := db.pusher.Push(TaskSQL, db.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
					t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe()); err != nil {
					log.Error("pusher task info error: %s", err.Error())
				}
			}
		case taskpb.TaskState_TaskRunning:
			if time.Since(t.CreateTime()) > time.Duration(t.Timeout()) {
				err := t.SwapAndSaveState(state, taskpb.TaskState_TaskTimeout, db.cluster)
				if err != nil {
					log.Warn("task swap state failed, err[%v]", err)
					return nil
				}
				if err := db.pusher.Push(TaskSQL, db.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
					t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe()); err != nil {
					log.Error("pusher task info error: %s", err.Error())
				}
				desc := t.Describe()
				message := desc + " 任务状态: " + t.StateString()
				db.alarm.Alarm("任务超时", message)
				log.Warn("task[%s] 执行超时", desc)
				t.UpdateLastAlarmTime()
				t.UpdateNextAlarmTime(DefaultAlarmInterval)
			}
		case taskpb.TaskState_TaskSuccess:
			err := db.cluster.DeleteTask(t.ID())
			if err != nil {
				log.Warn("delete task failed, err[%v]", err)
				return nil
			}
		case taskpb.TaskState_TaskFail:
		case taskpb.TaskState_TaskTimeout:
			if time.Since(t.GetNextAlarmTime()) > 0 {
				desc := t.Describe()
				log.Warn("task[%s] 执行超时", desc)
				message := desc + " 任务状态: " + t.StateString()
				db.alarm.Alarm("任务超时", message)
				t.UpdateNextAlarmTime(t.GetNextAlarmTime().Sub(t.GetLastAlarmTime()) * 2)
				t.UpdateLastAlarmTime()
			}
			// TODO 执行超时后挂起任务
            if time.Since(t.CreateTime()) > time.Duration(DefaultMaxTaskExecuteTime) {
	            err := t.SwapAndSaveState(state, taskpb.TaskState_TaskHangUp, db.cluster)
	            if err != nil {
		            log.Warn("task swap state failed, err[%v]", err)
		            return nil
	            }
	            if err := db.pusher.Push(TaskSQL, db.cluster.GetClusterId(), time.Now().Unix(), t.ID(),
		            t.GetMeta().GetCreateTime(), time.Since(t.CreateTime()), t.StateString(), t.Describe()); err != nil {
		            log.Error("pusher task info error: %s", err.Error())
	            }
	            desc := t.Describe()
	            message := desc + " 任务状态: " + t.StateString()
	            db.alarm.Alarm("任务挂起", message)
	            log.Warn("task[%s] 挂起", desc)
            }
		case taskpb.TaskState_TaskHangUp:
		case taskpb.TaskState_TaskPause:
		case taskpb.TaskState_TaskCancel:
			// 任务取消但是删除失败，这里可以补充删除任务
			err := db.cluster.DeleteTask(t.ID())
			if err != nil {
				log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
				return nil
			}
		}
	}

	return nil
}

func (db *TaskMonitorScheduler) AllowSchedule() bool {
	return true
}

func (db *TaskMonitorScheduler) GetInterval() time.Duration {
	return db.interval
}

func (db *TaskMonitorScheduler) Ctx() context.Context {
	return db.ctx
}

func (db *TaskMonitorScheduler) Stop() {
	db.cancel()
}
