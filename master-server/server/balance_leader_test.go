package server
//
//import (
//	"encoding/json"
//	"fmt"
//	"testing"
//	"time"
//
//	"model/pkg/metapb"
//	"util/log"
//	"model/pkg/taskpb"
//)
//
//var (
//	waitTime = 118
//)
//
////go test -v -run=TestChangeLeaderTest -config=/home/xiaolumm/sharkstore/src/master-server/cmd/app.conf > ~/tmp/a.txt
//
///**
//11 89 0 0 0
//均衡结果:
//11 85 4 0 0
//*/
//func TestChangeLeaderTest(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.SchLeaderBalanceInterval=60
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm, conf)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//	coordinator.removeScheduler("failover_scheduler")
//	coordinator.removeScheduler("disk_balance_sheduler")
//	coordinator.removeScheduler("range_gc_remote_sheduler")
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("ranges_balance_by_node_score_scheduler")
//	//coordinator.removeScheduler("leader_balance_scheduler")
//	coordinator.removeScheduler("leader_score_balance_scheduler")
//
//	cluster.AddInstance("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "mytime", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "mysize", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Error("create table failed, err[%v]", err)
//		return
//	}
//	cluster.UpdateAutoScheduleInfo(true, true, false)
//	table.UpdateAutoScheduleInfo(cluster,true,true,false)
//
//	nodes := cluster.GetAllInstances()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//	_nodes := nodes[:3]
//	for i, j := 0, 0; i < 100; i++ {
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		r, err := cluster.CreateRangeForTest("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//		//randomTmp := rand.New(rand.NewSource(time.Now().UnixNano()))
//		//r.UpdateLeader(r.Peers[randomTmp.Intn(3)])
//		if i < 10 {
//			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: _nodes[0].GetAddress()}
//			r.UpdateLeader(leader)
//		} else {
//			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: _nodes[1].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//
//		table.AddRange(r)
//		cluster.ranges.Add(r)
//		r.State = metapb.RangeState_R_Normal
//		r.Stats.Size_ = 60000
//		for _, p := range r.Peers {
//			node, _ := cluster.FindInstance(p.GetNodeId())
//			node.AddRange(r)
//			node.AllRanges = append(node.AllRanges, r.Id)
//		}
//
//		for _, p := range r.Peers {
//			node, _ := cluster.FindInstance(p.GetNodeId())
//			if r.Leader.NodeId == node.Id {
//				node.RangeLeaderCount += 1
//				node.Score += 1
//			}
//		}
//	}
//	ranges := table.GetAllRanges()
//	if len(ranges) == 0 {
//		t.Error("test failed")
//		return
//	}
//
//	t.Logf("==========start================")
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]",
//			n.Address, n.GetLeaderNum(), n.ranges.Size(), n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(waitTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		//t.Errorf("cluster sw: %v\ntable sw: %v\n", cluster.autoTransferUnable, table.autoTransferUnable)
//		return
//	}
//
//	for _, r := range ranges {
//		vvv, _ := json.Marshal(r)
//		t.Logf("range ++++++%s", string(vvv))
//		// 领取任务－－－－change leader
//		t1 := coordinator.Dispatch(r, 0)
//		if t1 == nil || t1.GetType() != taskpb.TaskType_RangeLeaderTransfer {
//			//t.Error("invalid change leader task %s", t1.Describe())
//			time.Sleep(time.Second)
//			continue
//		}
//		t2 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//		if t2 == nil || t2.GetType() != taskpb.TaskType_RangeLeaderTransfer {
//			//t.Error("invalid change leader task %s", t1.Describe())
//			time.Sleep(time.Second)
//			continue
//		}
//
//		t.Logf("get task success")
//		// 修改range的leader，模拟源节点leader数减1,目标节点leader数加1
//		exp := t1.RangeLeaderTransfer.GetExpLeader()
//		r.Leader = &metapb.Leader{RangeId: exp.GetId(), NodeId: exp.GetNodeId()}
//
//		t3 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//		if t3 != nil {
//			t.Error("invalid task %s", t3.Describe())
//		}
//		srcNode, _ := cluster.FindInstance(t1.SrcNodeId())
//		dstNode, _ := cluster.FindInstance(t1.DstNodeId())
//		t.Logf("leader blance range[%s->%d:%s->%d:avg->%f]",
//			srcNode.Address, srcNode.GetLeaderNum(), dstNode.Address, dstNode.GetLeaderNum())
//	}
//
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]",
//			n.Address, n.GetLeaderNum(), n.ranges.Size(), n.tasks.Size())
//	}
//	t.Logf("test success!!!")
//}
//
///**
//50 0 0 30 20
//均衡结果:
//46 4 4 26 20
//*/
//func Test2ChangeLeaderTest(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.SchLeaderBalanceInterval=60
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm, conf)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//	coordinator.removeScheduler("failover_scheduler")
//	coordinator.removeScheduler("disk_balance_sheduler")
//	coordinator.removeScheduler("range_gc_remote_sheduler")
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("ranges_balance_by_node_score_scheduler")
//	//coordinator.removeScheduler("leader_balance_scheduler")
//	coordinator.removeScheduler("leader_score_balance_scheduler")
//
//	cluster.AddInstance("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Error("create table failed, err[%v]", err)
//		return
//	}
//	table.UpdateAutoScheduleInfo(cluster,false,false,false)
//	nodes := cluster.GetAllInstances()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//
//	for i, j := 0, 0; i < 100; i++ {
//		/*randomTmp := rand.New(rand.NewSource(time.Now().UnixNano()))
//		inx := randomTmp.Intn(3)*/
//		inx := 0
//		if i < 50 {
//			inx = 0
//		}
//		if i >= 50 && i < 80 {
//			inx = 1
//		}
//		if i >= 80 {
//			inx = 2
//		}
//		_nodes := nodes[inx : inx+3]
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//
//		if i < 50 {
//			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: _nodes[0].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 50 && i < 80 {
//			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: _nodes[1].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 80 {
//			leader := &metapb.Leader{RangeId: r.Peers[2].GetId(), NodeId: r.Peers[2].GetNodeId(), NodeAddr: _nodes[2].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//
//		table.AddRange(r)
//		cluster.ranges.Add(r)
//		r.State = metapb.RangeState_R_Normal
//		r.Stats.Size_ = 60000
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			node.AddRange(r)
//			node.AllRanges = append(node.AllRanges, r.Id)
//		}
//
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			if r.Leader.NodeId == node.Id {
//				node.RangeLeaderCount += 1
//				node.Score += 1
//			}
//		}
//	}
//	ranges := table.GetAllRanges()
//	if len(ranges) == 0 {
//		t.Error("test failed")
//		return
//	}
//
//	t.Logf("==========start================")
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]",
//			n.Address, n.GetLeaderNum(), n.ranges.Size(), n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(waitTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//
//	for _, r := range ranges {
//		vvv, _ := json.Marshal(r)
//		t.Logf("range ++++++%s", string(vvv))
//		// 领取任务－－－－change leader
//		t1 := coordinator.Dispatch(r, 0)
//		if t1 == nil || t1.GetType() != taskpb.TaskType_RangeLeaderTransfer {
//			//t.Error("invalid change leader task %s", t1.Describe())
//			time.Sleep(time.Second)
//			continue
//		}
//		t2 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//		if t2 == nil || t2.GetType() != taskpb.TaskType_RangeLeaderTransfer {
//			//t.Error("invalid change leader task %s", t1.Describe())
//			time.Sleep(time.Second)
//			continue
//		}
//
//		t.Logf("get task success")
//		// 修改range的leader，模拟源节点leader数减1,目标节点leader数加1
//		exp := t1.RangeLeaderTransfer.GetExpLeader()
//		r.Leader = &metapb.Leader{RangeId: exp.GetId(), NodeId: exp.GetNodeId()}
//
//		t3 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//		if t3 != nil {
//			t.Error("invalid task %s", t3.Describe())
//		}
//		srcNode, _ := cluster.FindInstance(t1.SrcNodeId())
//		dstNode, _ := cluster.FindInstance(t1.DstNodeId())
//		t.Logf("leader blance range[%s->%d:%s->%d:avg->%f]",
//			srcNode.Address, srcNode.GetLeaderNum(), dstNode.Address, dstNode.GetLeaderNum())
//	}
//
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]",
//			n.Address, n.GetLeaderNum(), n.ranges.Size(), n.tasks.Size())
//	}
//	t.Logf("test success!!!")
//}
//
///**
//50 30 20 5 3 2
//一共六个节点
//前三个节点一个集群,集群有100个分片,leader副本都在前三个节点
//后三个节点一个集群,集群有10个分片,leader副本都在后三个节点
//均衡结果:
//46 30 24 5 3 2
//*/
//func Test3ChangeLeaderTest(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.SchLeaderBalanceInterval=60
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm, conf)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//
//	coordinator.Run()
//	coordinator.removeScheduler("failover_scheduler")
//	coordinator.removeScheduler("disk_balance_sheduler")
//	coordinator.removeScheduler("range_gc_remote_sheduler")
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("ranges_balance_by_node_score_scheduler")
//	//coordinator.removeScheduler("leader_balance_scheduler")
//	coordinator.removeScheduler("leader_score_balance_scheduler")
//
//	cluster.AddInstance("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.AddInstance("127.0.0.1", uint64(6065), uint64(1244), uint64(1245), "data-server")
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "timeTmp", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "sizeTmp", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Error("create table failed, err[%v]", err)
//		return
//	}
//	table.UpdateAutoScheduleInfo(cluster,false,false,false)
//	nodes := cluster.GetAllInstances()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//
//	nodes1 := nodes[:3]
//	nodes2 := nodes[3:6]
//	for i, j := 0, 0; i < 110; i++ {
//		/*randomTmp := rand.New(rand.NewSource(time.Now().UnixNano()))
//		inx := randomTmp.Intn(3)*/
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		var _nodes []*Instance
//		if i < 100 {
//			_nodes = nodes1
//		} else {
//			_nodes = nodes2
//		}
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//
//		if i < 50 {
//			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: _nodes[0].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 50 && i < 80 {
//			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: _nodes[1].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 80 && i < 100 {
//			leader := &metapb.Leader{RangeId: r.Peers[2].GetId(), NodeId: r.Peers[2].GetNodeId(), NodeAddr: _nodes[2].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 100 && i < 105 {
//			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: _nodes[0].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 105 && i < 108 {
//			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: _nodes[1].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 108 && i < 110 {
//			leader := &metapb.Leader{RangeId: r.Peers[2].GetId(), NodeId: r.Peers[2].GetNodeId(), NodeAddr: _nodes[2].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//
//		table.AddRange(r)
//		cluster.ranges.Add(r)
//		r.State = metapb.RangeState_R_Normal
//		r.Stats.Size_ = 60000
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			node.AddRange(r)
//			node.AllRanges = append(node.AllRanges, r.Id)
//		}
//
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			if r.Leader.NodeId == node.Id {
//				node.RangeLeaderCount += 1
//				node.Score += 1
//			}
//		}
//	}
//	ranges := table.GetAllRanges()
//	if len(ranges) == 0 {
//		t.Error("test failed")
//		return
//	}
//
//	t.Logf("==========start================")
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]",
//			n.Address, n.GetLeaderNum(), len(n.GetAllRanges()), n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(waitTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		time.Sleep(time.Second)
//		//return
//	}
//	for _, r := range ranges {
//		//vvv,_ := json.Marshal(r)
//		//t.Logf("range ++++++%s",string(vvv))
//		// 领取任务－－－－change leader
//		t1 := coordinator.Dispatch(r, 0)
//		if t1 == nil || t1.GetType() != taskpb.TaskType_RangeLeaderTransfer {
//			//t.Error("invalid change leader task %s", t1.Describe())
//			time.Sleep(time.Second)
//			continue
//		}
//		t2 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//		if t2 == nil || t2.GetType() != taskpb.TaskType_RangeLeaderTransfer {
//			//t.Error("invalid change leader task %s", t1.Describe())
//			time.Sleep(time.Second)
//			continue
//		}
//
//		t.Logf("get task success")
//		// 修改range的leader，模拟源节点leader数减1,目标节点leader数加1
//		exp := t1.RangeLeaderTransfer.GetExpLeader()
//		r.Leader = &metapb.Leader{RangeId: exp.GetId(), NodeId: exp.GetNodeId()}
//
//		t3 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//		if t3 != nil {
//			t.Error("invalid task %s", t3.Describe())
//		}
//		srcNode, _ := cluster.FindNode(t1.SrcNodeId())
//		dstNode, _ := cluster.FindNode(t1.DstNodeId())
//		t.Logf("leader blance range[%s->%d:%s->%d:avg->%f]",
//			srcNode.Address, srcNode.GetLeaderNum(), dstNode.Address, dstNode.GetLeaderNum())
//	}
//	t.Logf("==========end================")
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]",
//			n.Address, n.GetLeaderNum(), n.ranges.Size(), n.tasks.Size())
//	}
//	t.Logf("test success!!!")
//}
//
//func Test5ChangeLeaderTest(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.SchLeaderBalanceInterval=60
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//	coordinator.removeScheduler("failover_scheduler")
//	coordinator.removeScheduler("disk_balance_sheduler")
//	coordinator.removeScheduler("range_gc_remote_sheduler")
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("ranges_balance_by_node_score_scheduler")
//	//coordinator.removeScheduler("leader_balance_scheduler")
//	coordinator.removeScheduler("leader_score_balance_scheduler")
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "timeTmp", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "sizeTmp", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Error("create table failed, err[%v]", err)
//		return
//	}
//	table.UpdateAutoScheduleInfo(cluster,false,false,false)
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//
//	for i, j := 0, 0; i < 10; i++ {
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//		leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId()}
//		r.UpdateLeader(leader)
//		table.AddRange(r)
//		cluster.ranges.Add(r)
//		r.State = metapb.RangeState_R_Normal
//		r.Stats.Size_ = 60000
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			node.AddRange(r)
//			node.AllRanges = append(node.AllRanges, r.Id)
//		}
//
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			if r.Leader.NodeId == node.Id {
//				node.RangeLeaderCount += 1
//				node.Score += 1
//			}
//		}
//	}
//	ranges := table.GetAllRanges()
//	if len(ranges) == 0 {
//		t.Error("test failed")
//		return
//	}
//
//	t.Log("==========start================")
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]", n.Address, n.GetLeaderNum(), len(n.GetAllRanges()), n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(waitTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		time.Sleep(time.Second)
//		//return
//	}
//	t.Log("==========end================")
//	for _, n := range nodes {
//		t.Logf("[%s->leaderNum:%d->rangesSize:%d->taskSize:%d]", n.Address, n.GetLeaderNum(), n.ranges.Size(), n.tasks.Size())
//	}
//	t.Logf("test success!!!")
//}
//
///**
//初始环境:
//2017-08-02 18:37:50,181 :0: [DEBUG] [127.0.0.1:6063->leaderNum:11->totalLeaderScore:110.000000->taskSize:0]^M
//2017-08-02 18:37:50,181 :0: [DEBUG] [127.0.0.1:6064->leaderNum:89->totalLeaderScore:890.000000->taskSize:0]^M
//2017-08-02 18:37:50,181 :0: [DEBUG] [127.0.0.1:6060->leaderNum:0->totalLeaderScore:0.000000->taskSize:0]^M
//2017-08-02 18:37:50,181 :0: [DEBUG] [127.0.0.1:6061->leaderNum:0->totalLeaderScore:0.000000->taskSize:0]^M
//2017-08-02 18:37:50,181 :0: [DEBUG] [127.0.0.1:6062->leaderNum:0->totalLeaderScore:0.000000->taskSize:0]
//均衡结果:
//        balance_leader_test.go:727: [127.0.0.1:6060->leaderNum:11->totalLeaderScore:110.000000->taskSize:0]
//        balance_leader_test.go:727: [127.0.0.1:6061->leaderNum:85->totalLeaderScore:850.000000->taskSize:4]
//        balance_leader_test.go:727: [127.0.0.1:6062->leaderNum:4->totalLeaderScore:40.000000->taskSize:4]
//        balance_leader_test.go:727: [127.0.0.1:6063->leaderNum:0->totalLeaderScore:0.000000->taskSize:0]
//        balance_leader_test.go:727: [127.0.0.1:6064->leaderNum:0->totalLeaderScore:0.000000->taskSize:0]
//
//
// */
//func TestBalanceLeaderScore(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.SchLeaderScoreBalanceInterval=60
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//	coordinator.removeScheduler("failover_scheduler")
//	coordinator.removeScheduler("disk_balance_sheduler")
//	coordinator.removeScheduler("range_gc_remote_sheduler")
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("ranges_balance_by_node_score_scheduler")
//	coordinator.removeScheduler("leader_balance_scheduler")
//	//coordinator.removeScheduler("leader_score_balance_scheduler")
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "mytime", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "mysize", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Error("create table failed, err[%v]", err)
//		return
//	}
//	table.UpdateAutoScheduleInfo(cluster,false,false,false)
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//	_nodes := nodes[:3]
//	for i, j := 0, 0; i < 100; i++ {
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//		//randomTmp := rand.New(rand.NewSource(time.Now().UnixNano()))
//		//r.UpdateLeader(r.Peers[randomTmp.Intn(3)])
//		if i <= 10{
//			r.UpdateLeader(&metapb.Leader{RangeId: r.ID(), NodeId: _nodes[0].ID(), NodeAddr: _nodes[0].Address})
//		}
//		if i > 10 && i<=100{
//			r.UpdateLeader(&metapb.Leader{RangeId: r.ID(), NodeId: _nodes[1].ID(), NodeAddr: _nodes[1].Address})
//		}
//
//		table.AddRange(r)
//		cluster.ranges.Add(r)
//		r.State = metapb.RangeState_R_Normal
//		r.Stats.Size_ = 60000
//		r.Score = 10
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			node.AddRange(r)
//			node.AllRanges = append(node.AllRanges, r.Id)
//		}
//
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			if r.Leader.NodeId == node.Id{
//				node.RangeLeaderCount += 1
//				node.TotalLeaderScore += r.Score
//			}
//		}
//	}
//	ranges := table.GetAllRanges()
//	if len(ranges) == 0 {
//		t.Error("test failed")
//		return
//	}
//
//	t.Logf("==========start================")
//	for _,n := range nodes{
//		log.Debug("[%s->leaderNum:%d->totalLeaderScore:%f->taskSize:%d]", n.Address,n.GetLeaderNum(),n.GetTotalLeaderScore(),n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(waitTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//
//	t.Logf("==========end================")
//	for _,n := range nodes{
//		t.Logf("[%s->leaderNum:%d->totalLeaderScore:%f->taskSize:%d]", n.Address,n.GetLeaderNum(),n.GetTotalLeaderScore(),n.tasks.Size())
//	}
//	t.Logf("test success!!!")
//}
