package server

import (
	"time"

	"util/log"
	"model/pkg/taskpb"
)

var DefaultAlarmInterval = time.Minute * time.Duration(5)

func (c *Cluster) AddTask(t *Task) error {
	if t == nil {
		return nil
	}
	var srcNodeId, dstNodeId, rangeId, dbId, tableId uint64

	switch t.Type{
	case taskpb.TaskType_RangeSplit:
		rangeId = t.GetRangeSplit().GetRange().GetId()
		dbId = t.GetRangeSplit().GetRange().GetDbId()
		tableId = t.GetRangeSplit().GetRange().GetTableId()
	case taskpb.TaskType_RangeLeaderTransfer:
		rangeId = t.GetRangeLeaderTransfer().GetRange().GetId()
		dbId = t.GetRangeLeaderTransfer().GetRange().GetDbId()
		tableId = t.GetRangeLeaderTransfer().GetRange().GetTableId()
	case taskpb.TaskType_RangeTransfer:
		rangeId = t.GetRangeTransfer().GetRange().GetId()
		dbId = t.GetRangeTransfer().GetRange().GetDbId()
		tableId = t.GetRangeTransfer().GetRange().GetTableId()
		srcNodeId = t.GetRangeTransfer().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeTransfer().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeFailOver:
		rangeId = t.GetRangeFailover().GetRange().GetId()
		dbId = t.GetRangeFailover().GetRange().GetDbId()
		tableId = t.GetRangeFailover().GetRange().GetTableId()
		srcNodeId = t.GetRangeFailover().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeFailover().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeCreate:
		rangeId = t.GetRangeCreate().GetRange().GetId()
		dbId = t.GetRangeCreate().GetRange().GetDbId()
		tableId = t.GetRangeCreate().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelete:
		rangeId = t.GetRangeDelete().GetRange().GetId()
		dbId = t.GetRangeDelete().GetRange().GetDbId()
		tableId = t.GetRangeDelete().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelPeer:
		rangeId = t.GetRangeDelPeer().GetRange().GetId()
		dbId = t.GetRangeDelPeer().GetRange().GetDbId()
		tableId = t.GetRangeDelPeer().GetRange().GetTableId()
	case taskpb.TaskType_RangeAddPeer:
		rangeId = t.GetRangeAddPeer().GetRange().GetId()
		dbId = t.GetRangeAddPeer().GetRange().GetDbId()
		tableId = t.GetRangeAddPeer().GetRange().GetTableId()
		dstNodeId = t.GetRangeAddPeer().GetPeer().GetNodeId()
	case taskpb.TaskType_RangeOffline:
		rangeId = t.GetRangeOffline().GetRange().GetId()
		dbId = t.GetRangeOffline().GetRange().GetDbId()
		tableId = t.GetRangeOffline().GetRange().GetTableId()
	case taskpb.TaskType_EmptyRangeTask:
		rangeId = t.GetRangeEmpty().GetRangeId()
	case taskpb.TaskType_NodeLogout:
		dstNodeId = t.GetNodeLogout().GetNodeId()
	case taskpb.TaskType_NodeLogin:
		dstNodeId = t.GetNodeLogin().GetNodeId()
	case taskpb.TaskType_NodeDeleteRanges:
		dstNodeId = t.GetNodeDeleteRanges().GetNodeId()
	case taskpb.TaskType_NodeCreateRanges:
		dstNodeId = t.GetNodeCreateRanges().GetNodeId()
	case taskpb.TaskType_NodeFailOver:
		dstNodeId = t.GetNodeFailover().GetNodeId()
	case taskpb.TaskType_TableCreate:
		dbId = t.GetTableCreate().GetDbId()
		tableId = t.GetTableCreate().GetTableId()
	case taskpb.TaskType_TableDelete:
		dbId = t.GetTableDelete().GetDbId()
		tableId = t.GetTableDelete().GetTableId()
	}

	var table *Table
	var find bool
	if dbId > 0 && tableId > 0 {
		table, find = c.FindTableById(dbId, tableId)
		if !find {
			log.Warn("table[%d:%d] not exit", dbId, tableId)
			return nil
		}
	}

	var rng *Range
	if rangeId > 0 {
		// 一个分片任何时候最多只能有一个任务存在
		rng, find = c.FindRange(rangeId)
		if !find {
			// must bug!!!!!
			log.Error("range[%d] not found", rangeId)
			return ErrNotExistRange
		}
		if table == nil {
			table, find = c.FindTableById(rng.GetDbId(), rng.GetTableId())
			if !find {
				log.Error("table[%s:%s] not exit", rng.GetDbName(), rng.GetTableName())
				return nil
			}
		}

		switch t.Type{
		case taskpb.TaskType_RangeSplit:
			if table.autoShardingUnable{
				return nil
			}
		case taskpb.TaskType_RangeMerge:
			if table.autoShardingUnable{
				return nil
			}
		case taskpb.TaskType_RangeLeaderTransfer:
			if table.autoTransferUnable {
				return nil
			}
		case taskpb.TaskType_RangeTransfer:
			if table.autoTransferUnable {
				return nil
			}
		case taskpb.TaskType_RangeFailOver:
			if table.autoFailoverUnable{
				return nil
			}
		case taskpb.TaskType_RangeAddPeer:
			if table.autoTransferUnable {
				return nil
			}
		case taskpb.TaskType_RangeDelPeer:
			if table.autoTransferUnable {
				return nil
			}
		}
		rng.lock.Lock()
		defer rng.lock.Unlock()
		// 已经存在任务了
		if rng.GetTask() != nil {
			return nil
		}
		if err := c.storeTask(t.Task); err != nil {
			log.Error("store task[%s] failed, err[%v]", t.String(), err)
			return err
		}
		if !rng.SetNxTask(t) {
			return nil
		}
	} else if table != nil {
		table.lock.Lock()
		defer table.lock.Unlock()
		if table.GetTask() != nil {
			return nil
		}
		if err := c.storeTask(t.Task); err != nil {
			log.Error("store task[%s] failed, err[%v]", t.String(), err)
			return err
		}
		table.AddTask(t)
	} else {
		if err := c.storeTask(t.Task); err != nil {
			log.Error("store task[%s] failed, err[%v]", t.String(), err)
			return err
		}
	}

	c.tasks.Add(t)
	if srcNodeId > 0 {
		node, find := c.FindInstance(srcNodeId)
		if find {
			node.AddTask(t)
		}
	}
	if dstNodeId > 0 {
		node, find := c.FindInstance(dstNodeId)
		if find {
			node.AddTask(t)
		}
	}

	return nil
}

func (c *Cluster) AddTaskCache(t *Task) error {
	if t == nil {
		return nil
	}
	var srcNodeId, dstNodeId, rangeId, dbId, tableId uint64

	switch t.Type{
	case taskpb.TaskType_RangeSplit:
		rangeId = t.GetRangeSplit().GetRange().GetId()
		dbId = t.GetRangeSplit().GetRange().GetDbId()
		tableId = t.GetRangeSplit().GetRange().GetTableId()
	case taskpb.TaskType_RangeLeaderTransfer:
		rangeId = t.GetRangeLeaderTransfer().GetRange().GetId()
		dbId = t.GetRangeLeaderTransfer().GetRange().GetDbId()
		tableId = t.GetRangeLeaderTransfer().GetRange().GetTableId()
	case taskpb.TaskType_RangeTransfer:
		rangeId = t.GetRangeTransfer().GetRange().GetId()
		dbId = t.GetRangeTransfer().GetRange().GetDbId()
		tableId = t.GetRangeTransfer().GetRange().GetTableId()
		srcNodeId = t.GetRangeTransfer().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeTransfer().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeFailOver:
		rangeId = t.GetRangeFailover().GetRange().GetId()
		dbId = t.GetRangeFailover().GetRange().GetDbId()
		tableId = t.GetRangeFailover().GetRange().GetTableId()
		srcNodeId = t.GetRangeFailover().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeFailover().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeCreate:
		rangeId = t.GetRangeCreate().GetRange().GetId()
		dbId = t.GetRangeCreate().GetRange().GetDbId()
		tableId = t.GetRangeCreate().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelete:
		rangeId = t.GetRangeDelete().GetRange().GetId()
		dbId = t.GetRangeDelete().GetRange().GetDbId()
		tableId = t.GetRangeDelete().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelPeer:
		rangeId = t.GetRangeDelPeer().GetRange().GetId()
		dbId = t.GetRangeDelPeer().GetRange().GetDbId()
		tableId = t.GetRangeDelPeer().GetRange().GetTableId()
	case taskpb.TaskType_RangeAddPeer:
		rangeId = t.GetRangeAddPeer().GetRange().GetId()
		dbId = t.GetRangeAddPeer().GetRange().GetDbId()
		tableId = t.GetRangeAddPeer().GetRange().GetTableId()
		dstNodeId = t.GetRangeAddPeer().GetPeer().GetNodeId()
	case taskpb.TaskType_RangeOffline:
		rangeId = t.GetRangeOffline().GetRange().GetId()
		dbId = t.GetRangeOffline().GetRange().GetDbId()
		tableId = t.GetRangeOffline().GetRange().GetTableId()
	case taskpb.TaskType_EmptyRangeTask:
		rangeId = t.GetRangeEmpty().GetRangeId()
	case taskpb.TaskType_NodeLogout:
		dstNodeId = t.GetNodeLogout().GetNodeId()
	case taskpb.TaskType_NodeLogin:
		dstNodeId = t.GetNodeLogin().GetNodeId()
	case taskpb.TaskType_NodeDeleteRanges:
		dstNodeId = t.GetNodeDeleteRanges().GetNodeId()
	case taskpb.TaskType_NodeCreateRanges:
		dstNodeId = t.GetNodeCreateRanges().GetNodeId()
	case taskpb.TaskType_NodeFailOver:
		dstNodeId = t.GetNodeFailover().GetNodeId()
	case taskpb.TaskType_TableCreate:
		dbId = t.GetTableCreate().GetDbId()
		tableId = t.GetTableCreate().GetTableId()
	case taskpb.TaskType_TableDelete:
		dbId = t.GetTableDelete().GetDbId()
		tableId = t.GetTableDelete().GetTableId()
		if dbId == 0 || tableId == 0 {
			log.Error("invalid table delete task!!!!")
		}
	}

	var table *Table
	var find bool
	if dbId > 0 && tableId > 0 {
		table, find = c.FindTableById(dbId, tableId)
		if !find {
			// 在删除的ｔａｂｌｅ中查找
			log.Warn("table[%d:%d] not exit", dbId, tableId)
			return nil
		}
	}

	var rng *Range
	if rangeId > 0 {
		// 一个分片任何时候最多只能有一个任务存在
		rng, find = c.FindRange(rangeId)
		if !find {
			// must bug!!!!!
			log.Error("range[%d] not found", rangeId)
			return ErrNotExistRange
		}
		if table == nil {
			table, find = c.FindTableById(rng.GetDbId(), rng.GetTableId())
			if !find {
				log.Error("table[%s:%s] not exit", rng.GetDbName(), rng.GetTableName())
				return nil
			}
		}

		switch t.Type{
		case taskpb.TaskType_RangeSplit:
			if table.autoShardingUnable{
				return nil
			}
		case taskpb.TaskType_RangeMerge:
			if table.autoShardingUnable{
				return nil
			}
		case taskpb.TaskType_RangeLeaderTransfer:
			if table.autoTransferUnable {
				return nil
			}
		case taskpb.TaskType_RangeTransfer:
			if table.autoTransferUnable {
				return nil
			}
		case taskpb.TaskType_RangeFailOver:
			if table.autoFailoverUnable{
				return nil
			}
		case taskpb.TaskType_RangeAddPeer:
			if table.autoTransferUnable {
				return nil
			}
		case taskpb.TaskType_RangeDelPeer:
			if table.autoTransferUnable {
				return nil
			}
		}
		rng.lock.Lock()
		defer rng.lock.Unlock()
		// 已经存在任务了
		if rng.GetTask() != nil {
			return nil
		}
		if !rng.SetNxTask(t) {
			return nil
		}
	} else if table != nil {
		table.lock.Lock()
		defer table.lock.Unlock()
		if table.GetTask() != nil {
			return nil
		}
		table.AddTask(t)
	}

	c.tasks.Add(t)
	if srcNodeId > 0 {
		node, find := c.FindInstance(srcNodeId)
		if find {
			node.AddTask(t)
		}
	}
	if dstNodeId > 0 {
		node, find := c.FindInstance(dstNodeId)
		if find {
			node.AddTask(t)
		}
	}

	return nil
}

func (c *Cluster) UpdateTask(t *Task) error {
	if err := c.storeTask(t.Task); err != nil {
		log.Error("store task[%s] failed, err[%v]", t.String(), err)
		return err
	}
	return nil
}

func (c *Cluster) PauseTask(id uint64) error {
	t, find := c.tasks.FindTask(id)
	if !find {
		log.Error("task[%d] not exit", id)
		return ErrNotExistTask
	}
	switch t.GetMeta().GetState() {
	case taskpb.TaskState_TaskRunning:
		return t.SwapAndSaveState(taskpb.TaskState_TaskRunning, taskpb.TaskState_TaskPause, c)
	case taskpb.TaskState_TaskWaiting:
		return t.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskPause, c)
	case taskpb.TaskState_TaskSuccess:
		return ErrTaskConflictState
	case taskpb.TaskState_TaskFail:
		return ErrTaskConflictState
	case taskpb.TaskState_TaskTimeout:
		return t.SwapAndSaveState(taskpb.TaskState_TaskTimeout, taskpb.TaskState_TaskPause, c)
	case taskpb.TaskState_TaskHangUp:
		return ErrTaskConflictState
	case taskpb.TaskState_TaskPause:
		return nil
	case taskpb.TaskState_TaskCancel:
		return ErrTaskConflictState
	}
	return ErrTaskConflictState
}

func (c *Cluster) RestartTask(id uint64) error {
	t, find := c.tasks.FindTask(id)
	if !find {
		log.Error("task[%d] not exit", id)
		return ErrNotExistTask
	}
	switch t.GetMeta().GetState() {
	case taskpb.TaskState_TaskRunning:
		return nil
	case taskpb.TaskState_TaskWaiting:
		return nil
	case taskpb.TaskState_TaskSuccess:
		return ErrTaskConflictState
	case taskpb.TaskState_TaskFail:
		return ErrTaskConflictState
	case taskpb.TaskState_TaskTimeout:
		return nil
	case taskpb.TaskState_TaskHangUp:
		return t.Restart(c)
	case taskpb.TaskState_TaskPause:
		return t.Restart(c)
	case taskpb.TaskState_TaskCancel:
		return ErrTaskConflictState
	}
	return ErrTaskConflictState
}

func (c *Cluster) DeleteTask(id uint64) error {
	t, find := c.tasks.FindTask(id)
	if !find {
		log.Error("task[%d] not exit", id)
		return ErrNotExistTask
	}
	err := c.deleteTask(id)
	if err != nil {
		log.Error("delete task failed, err[%v]", err)
		return err
	}
	var srcNodeId, dstNodeId, rangeId, dbId, tableId uint64

	switch t.Type{
	case taskpb.TaskType_RangeSplit:
		rangeId = t.GetRangeSplit().GetRange().GetId()
		dbId = t.GetRangeSplit().GetRange().GetDbId()
		tableId = t.GetRangeSplit().GetRange().GetTableId()
	case taskpb.TaskType_RangeLeaderTransfer:
		rangeId = t.GetRangeLeaderTransfer().GetRange().GetId()
		dbId = t.GetRangeLeaderTransfer().GetRange().GetDbId()
		tableId = t.GetRangeLeaderTransfer().GetRange().GetTableId()
	case taskpb.TaskType_RangeTransfer:
		rangeId = t.GetRangeTransfer().GetRange().GetId()
		dbId = t.GetRangeTransfer().GetRange().GetDbId()
		tableId = t.GetRangeTransfer().GetRange().GetTableId()
		srcNodeId = t.GetRangeTransfer().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeTransfer().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeFailOver:
		rangeId = t.GetRangeFailover().GetRange().GetId()
		dbId = t.GetRangeFailover().GetRange().GetDbId()
		tableId = t.GetRangeFailover().GetRange().GetTableId()
		srcNodeId = t.GetRangeFailover().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeFailover().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeCreate:
		rangeId = t.GetRangeCreate().GetRange().GetId()
		dbId = t.GetRangeCreate().GetRange().GetDbId()
		tableId = t.GetRangeCreate().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelete:
		rangeId = t.GetRangeDelete().GetRange().GetId()
		dbId = t.GetRangeDelete().GetRange().GetDbId()
		tableId = t.GetRangeDelete().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelPeer:
		rangeId = t.GetRangeDelPeer().GetRange().GetId()
		dbId = t.GetRangeDelPeer().GetRange().GetDbId()
		tableId = t.GetRangeDelPeer().GetRange().GetTableId()
	case taskpb.TaskType_RangeAddPeer:
		rangeId = t.GetRangeAddPeer().GetRange().GetId()
		dbId = t.GetRangeAddPeer().GetRange().GetDbId()
		tableId = t.GetRangeAddPeer().GetRange().GetTableId()
		dstNodeId = t.GetRangeAddPeer().GetPeer().GetNodeId()
	case taskpb.TaskType_RangeOffline:
		rangeId = t.GetRangeOffline().GetRange().GetId()
		dbId = t.GetRangeOffline().GetRange().GetDbId()
		tableId = t.GetRangeOffline().GetRange().GetTableId()
	case taskpb.TaskType_EmptyRangeTask:
		rangeId = t.GetRangeEmpty().GetRangeId()
	case taskpb.TaskType_NodeLogout:
		dstNodeId = t.GetNodeLogout().GetNodeId()
	case taskpb.TaskType_NodeLogin:
		dstNodeId = t.GetNodeLogin().GetNodeId()
	case taskpb.TaskType_NodeDeleteRanges:
		dstNodeId = t.GetNodeDeleteRanges().GetNodeId()
	case taskpb.TaskType_NodeCreateRanges:
		dstNodeId = t.GetNodeCreateRanges().GetNodeId()
	case taskpb.TaskType_NodeFailOver:
		dstNodeId = t.GetNodeFailover().GetNodeId()
	case taskpb.TaskType_TableCreate:
		dbId = t.GetTableCreate().GetDbId()
		tableId = t.GetTableCreate().GetTableId()
	case taskpb.TaskType_TableDelete:
		dbId = t.GetTableDelete().GetDbId()
		tableId = t.GetTableDelete().GetTableId()
	}

	var table *Table
	if dbId > 0 && tableId > 0 {
		table, find = c.FindTableById(dbId, tableId)
		if !find {
			// 在删除的ｔａｂｌｅ中查找
			log.Warn("table[%d:%d] not exit", dbId, tableId)
			return nil
		}
	}

	if table != nil {
		table.lock.Lock()
		defer table.lock.Unlock()
		table.DelTask(id)
	}
	if srcNodeId > 0 {
		node, find := c.FindInstance(srcNodeId)
		if find {
			node.DeleteTask(id)
		}
	}
	if dstNodeId > 0 {
		node, find := c.FindInstance(dstNodeId)
		if find {
			node.DeleteTask(id)
		}
	}
	if rangeId > 0 {
		rng, find := c.FindRange(rangeId)
		if find {
			rng.DeleteTask(id)
		}
	}

	c.tasks.Delete(id)
	return nil
}

func (c *Cluster) DeleteTaskCache(id uint64) error {
	t, find := c.tasks.FindTask(id)
	if !find {
		log.Error("task[%d] not exit", id)
		return ErrNotExistTask
	}

	var srcNodeId, dstNodeId, rangeId, dbId, tableId uint64

	switch t.Type{
	case taskpb.TaskType_RangeSplit:
		rangeId = t.GetRangeSplit().GetRange().GetId()
		dbId = t.GetRangeSplit().GetRange().GetDbId()
		tableId = t.GetRangeSplit().GetRange().GetTableId()
	case taskpb.TaskType_RangeLeaderTransfer:
		rangeId = t.GetRangeLeaderTransfer().GetRange().GetId()
		dbId = t.GetRangeLeaderTransfer().GetRange().GetDbId()
		tableId = t.GetRangeLeaderTransfer().GetRange().GetTableId()
	case taskpb.TaskType_RangeTransfer:
		rangeId = t.GetRangeTransfer().GetRange().GetId()
		dbId = t.GetRangeTransfer().GetRange().GetDbId()
		tableId = t.GetRangeTransfer().GetRange().GetTableId()
		srcNodeId = t.GetRangeTransfer().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeTransfer().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeFailOver:
		rangeId = t.GetRangeFailover().GetRange().GetId()
		dbId = t.GetRangeFailover().GetRange().GetDbId()
		tableId = t.GetRangeFailover().GetRange().GetTableId()
		srcNodeId = t.GetRangeFailover().GetDownPeer().GetNodeId()
		dstNodeId = t.GetRangeFailover().GetUpPeer().GetNodeId()
	case taskpb.TaskType_RangeCreate:
		rangeId = t.GetRangeCreate().GetRange().GetId()
		dbId = t.GetRangeCreate().GetRange().GetDbId()
		tableId = t.GetRangeCreate().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelete:
		rangeId = t.GetRangeDelete().GetRange().GetId()
		dbId = t.GetRangeDelete().GetRange().GetDbId()
		tableId = t.GetRangeDelete().GetRange().GetTableId()
	case taskpb.TaskType_RangeDelPeer:
		rangeId = t.GetRangeDelPeer().GetRange().GetId()
		dbId = t.GetRangeDelPeer().GetRange().GetDbId()
		tableId = t.GetRangeDelPeer().GetRange().GetTableId()
	case taskpb.TaskType_RangeAddPeer:
		rangeId = t.GetRangeAddPeer().GetRange().GetId()
		dbId = t.GetRangeAddPeer().GetRange().GetDbId()
		tableId = t.GetRangeAddPeer().GetRange().GetTableId()
		dstNodeId = t.GetRangeAddPeer().GetPeer().GetNodeId()
	case taskpb.TaskType_RangeOffline:
		rangeId = t.GetRangeOffline().GetRange().GetId()
		dbId = t.GetRangeOffline().GetRange().GetDbId()
		tableId = t.GetRangeOffline().GetRange().GetTableId()
	case taskpb.TaskType_EmptyRangeTask:
		rangeId = t.GetRangeEmpty().GetRangeId()
	case taskpb.TaskType_NodeLogout:
		dstNodeId = t.GetNodeLogout().GetNodeId()
	case taskpb.TaskType_NodeLogin:
		dstNodeId = t.GetNodeLogin().GetNodeId()
	case taskpb.TaskType_NodeDeleteRanges:
		dstNodeId = t.GetNodeDeleteRanges().GetNodeId()
	case taskpb.TaskType_NodeCreateRanges:
		dstNodeId = t.GetNodeCreateRanges().GetNodeId()
	case taskpb.TaskType_NodeFailOver:
		dstNodeId = t.GetNodeFailover().GetNodeId()
	case taskpb.TaskType_TableCreate:
		dbId = t.GetTableCreate().GetDbId()
		tableId = t.GetTableCreate().GetTableId()
	case taskpb.TaskType_TableDelete:
		dbId = t.GetTableDelete().GetDbId()
		tableId = t.GetTableDelete().GetTableId()
	}

	var table *Table
	if dbId > 0 && tableId > 0 {
		table, find = c.FindTableById(dbId, tableId)
		if !find {
			// 在删除的ｔａｂｌｅ中查找
			log.Warn("table[%d:%d] not exit", dbId, tableId)
			return nil
		}
	}

	if table != nil {
		table.lock.Lock()
		defer table.lock.Unlock()
		table.DelTask(id)
	}
	if srcNodeId > 0 {
		node, find := c.FindInstance(srcNodeId)
		if find {
			node.DeleteTask(id)
		}
	}
	if dstNodeId > 0 {
		node, find := c.FindInstance(dstNodeId)
		if find {
			node.DeleteTask(id)
		}
	}
	if rangeId > 0 {
		rng, find := c.FindRange(rangeId)
		if find {
			rng.DeleteTask(id)
		}
	}

	c.tasks.Delete(id)
	return nil
}


func (c *Cluster) FindTask(id uint64) (*Task, bool) {
	return c.tasks.FindTask(id)
}

func (c *Cluster) GetAllTask() []*Task {
	return c.tasks.GetAllTask()
}

func (c *Cluster) GetAllFailTask() []*Task {
	var tasks []*Task
	for _, t := range c.tasks.GetAllTask() {
		if t.State() == taskpb.TaskState_TaskFail {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (c *Cluster) GetAllTimeoutTask() []*Task {
	var tasks []*Task
	for _, t := range c.tasks.GetAllTask() {
		if t.State() == taskpb.TaskState_TaskFail {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (c *Cluster) GetAllDBTask(db string) []*Task {
	var tasks []*Task
	database, find := c.FindDatabase(db)
	if !find {
		log.Error("database[%s] not found", db)
		return nil
	}
	for _, t := range database.GetAllTable() {
		_tasks := c.GetAllTableTask(db, t.GetName())
		tasks = append(tasks, _tasks...)
	}
	return tasks
}

func (c *Cluster) GetAllDBFailTask(db string) []*Task {
	var tasks []*Task
	database, find := c.FindDatabase(db)
	if !find {
		log.Error("database[%s] not found", db)
		return nil
	}
	for _, t := range database.GetAllTable() {
		_tasks := c.GetAllTableFailTask(db, t.GetName())
		tasks = append(tasks, _tasks...)
	}
	return tasks
}

func (c *Cluster) GetAllDBTimeoutTask(db string) []*Task {
	var tasks []*Task
	database, find := c.FindDatabase(db)
	if !find {
		log.Error("database[%s] not found", db)
		return nil
	}
	for _, t := range database.GetAllTable() {
		_tasks := c.GetAllTableTimeoutTask(db, t.GetName())
		tasks = append(tasks, _tasks...)
	}
	return tasks
}

func (c *Cluster) GetAllTableTask(dbName, tableName string) []*Task {
	table, find := c.FindCurTable(dbName, tableName)
	if !find {
		log.Error("table[%s:%s] not found", dbName, tableName)
		return nil
	}
	var tasks []*Task
	for _, r := range table.GetAllRanges() {
		task := r.GetTask()
		if task != nil {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (c *Cluster) GetAllTableFailTask(dbName, tableName string) []*Task {
	table, find := c.FindCurTable(dbName, tableName)
	if !find {
		log.Error("table[%s:%s] not found", dbName, tableName)
		return nil
	}
	var tasks []*Task
	for _, r := range table.GetAllRanges() {
		task := r.GetTask()
		if task != nil && task.State() == taskpb.TaskState_TaskFail{
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (c *Cluster) GetAllTableTimeoutTask(dbName, tableName string) []*Task {
	table, find := c.FindCurTable(dbName, tableName)
	if !find {
		log.Error("table[%s:%s] not found", dbName, tableName)
		return nil
	}
	var tasks []*Task
	for _, r := range table.GetAllRanges() {
		task := r.GetTask()
		if task != nil && task.State() == taskpb.TaskState_TaskTimeout {
			tasks = append(tasks, task)
		}
	}
	return tasks
}
