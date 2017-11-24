package server
//
//import (
//	"time"
//	"testing"
//
//	"model/pkg/metapb"
//	"model/pkg/taskpb"
//)
//
//func TestFailOver(t *testing.T) {
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
//	n4, _ := cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	n4.State = metapb.NodeState_N_Login
//	n4.Stats.DiskFree = MinDiskFree * 5
//	n4.Stats.Load5 = 0.4
//	n5, _ := cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	n5.State = metapb.NodeState_N_Login
//	n5.Stats.DiskFree = MinDiskFree * 5
//	n5.Stats.Load5 = 0.5
//	n2.State = metapb.NodeState_N_Tombstone
//	failOverScheduler := NewFailoverScheduler(time.Second, cluster, alarm)
//	task := failOverScheduler.Schedule()
//	if task == nil || task.GetType() != taskpb.TaskType_RangeFailOver {
//		t.Errorf("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	upNodeId := task.GetRangeFailover().GetUpPeer().GetNodeId()
//	for _, p := range r.Peers {
//		if upNodeId == p.GetNodeId() {
//			t.Errorf("tast failed")
//			return
//		}
//	}
//
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
//func TestFailOver1(t *testing.T) {
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
//	allRanges := make([]uint64, 1)
//	allRanges[0] = r.GetId()
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//		node.AllRanges = allRanges
//	}
//	n4, _ := cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	n4.State = metapb.NodeState_N_Tombstone
//	n4.Stats.DiskFree = MinDiskFree * 5
//	n4.Stats.Load5 = 0.4
//	n5, _ := cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	n5.State = metapb.NodeState_N_Login
//	n5.Stats.DiskFree = MinDiskFree * 5
//	n5.Stats.Load5 = 0.5
//	n2.State = metapb.NodeState_N_Tombstone
//	failOverScheduler := NewFailoverScheduler(time.Second, cluster, alarm)
//	task := failOverScheduler.Schedule()
//	if task == nil || task.GetType() != taskpb.TaskType_RangeFailOver {
//		t.Errorf("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	upNodeId := task.GetRangeFailover().GetUpPeer().GetNodeId()
//	for _, p := range r.Peers {
//		if upNodeId == p.GetNodeId() {
//			t.Errorf("tast failed")
//			time.Sleep(time.Second)
//			return
//		}
//	}
//	if upNodeId == n4.GetId() {
//		t.Errorf("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
