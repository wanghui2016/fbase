package server
//
//import (
//	"testing"
//	"time"
//
//	"model/pkg/metapb"
//	"model/pkg/taskpb"
//)
//
//func TestSwapAndSaveState(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//	tsk := &taskpb.Task{
//		Type:   taskpb.TaskType_RangeFailOver,
//		Meta:   &taskpb.TaskMeta{
//			TaskId:   1,
//			CreateTime: time.Now().Unix(),
//			State: taskpb.TaskState_TaskWaiting,
//			Timeout: DefaultFailOverTaskTimeout,
//		},
//		RangeEmpty: &taskpb.TaskRangeEmpty{RangeId: 1},
//	}
//
//	task := NewTask(tsk)
//	err := task.SwapAndSaveState(taskpb.TaskState_TaskWaiting, taskpb.TaskState_TaskRunning, cluster)
//	if err != nil {
//		t.Error("test failed")
//		return
//	}
//	if task.GetMeta().GetState() != taskpb.TaskState_TaskRunning {
//		t.Error("test failed")
//		return
//	}
//}
//
//func TestChangeTaskState(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	n1, _ := cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	n1.State = metapb.NodeState_N_Login
//	n1.Stats.DiskFree = MinDiskFree * 5
//	n1.Stats.Load5 = 0.1
//	n2, _ := cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	n2.State = metapb.NodeState_N_Login
//	n2.Stats.DiskFree = MinDiskFree * 5
//	n2.Stats.Load5 = 0.2
//	n3, _ := cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	n3.State = metapb.NodeState_N_Login
//	n3.Stats.DiskFree = MinDiskFree * 5
//	n3.Stats.Load5 = 0.3
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "ssize", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 5
//	}
//	_nodes := nodes[:3]
//	r, err := cluster.CreateRange("db0", "table0", nil, nil, _nodes)
//	if err != nil {
//		t.Errorf("create range failed, err[%v]", err)
//		return
//	}
//	t.Logf("create range[%s:%s:%d] success", r.GetDbName(), r.GetTableName(), r.ID())
//	table.AddRange(r)
//	cluster.ranges.Add(r)
//	r.State = metapb.RangeState_R_Normal
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//	}
//
//	tsk := &taskpb.Task{
//		Type:   taskpb.TaskType_EmptyRangeTask,
//		Meta:   &taskpb.TaskMeta{
//			TaskId:   1,
//			CreateTime: time.Now().Unix(),
//			State: taskpb.TaskState_TaskWaiting,
//			Timeout: DefaultFailOverTaskTimeout,
//		},
//		RangeEmpty: &taskpb.TaskRangeEmpty{RangeId: 1},
//	}
//	task := NewTask(tsk)
//	cluster.AddTask(task)
//	cluster.PauseTask(task.GetMeta().GetTaskId())
//	if task.GetMeta().GetState() != taskpb.TaskState_TaskPause {
//		t.Error("test failed")
//		return
//	}
//	cluster.RestartTask(task.GetMeta().GetTaskId())
//	if task.GetMeta().GetState() != taskpb.TaskState_TaskRunning {
//		t.Error("test failed")
//		return
//	}
//}
