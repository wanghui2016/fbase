package server

import (
	"testing"
	"time"
	"sort"

	"model/pkg/metapb"
)

//go test -v -run=TestBalanceRangesByNum -config=/home/xiaolumm/sharkstore/src/master-server/cmd/app.conf > ~/tmp/a.txt

/**
均衡前:
2017-08-02 11:22:42,690 :0: [DEBUG] [127.0.0.1:6063->rangeNum:20->rangesSize:20->taskSize:0]^M
2017-08-02 11:22:42,690 :0: [DEBUG] [127.0.0.1:6064->rangeNum:20->rangesSize:20->taskSize:0]^M
2017-08-02 11:22:42,690 :0: [DEBUG] [127.0.0.1:6060->rangeNum:20->rangesSize:20->taskSize:0]^M
2017-08-02 11:22:42,690 :0: [DEBUG] [127.0.0.1:6061->rangeNum:0->rangesSize:0->taskSize:0]^M
2017-08-02 11:22:42,690 :0: [DEBUG] [127.0.0.1:6062->rangeNum:0->rangesSize:0->taskSize:0]^M
均衡后结果:
        balance_ranges_test.go:171: [127.0.0.1:6063->rangeNum:17->rangesSize:20->taskSize:3]
        balance_ranges_test.go:171: [127.0.0.1:6064->rangeNum:18->rangesSize:20->taskSize:2]
        balance_ranges_test.go:171: [127.0.0.1:6060->rangeNum:17->rangesSize:20->taskSize:3]
        balance_ranges_test.go:171: [127.0.0.1:6061->rangeNum:4->rangesSize:0->taskSize:4]
        balance_ranges_test.go:171: [127.0.0.1:6062->rangeNum:4->rangesSize:0->taskSize:4]
 */
func TestBalanceRangesByNum(t *testing.T) {
	genStep = 10000
	conf := new(Config)
	MasterConf = conf
	MasterConf.SchRangesBalancePercent = 0.3
	store := NewLocalStore()
	alarm := NewTestAlarm()
	cluster := NewCluster(uint64(1), uint64(1), store, alarm, conf)
	cluster.cli = NewLocalClient()
	cluster.UpdateLeader(uint64(1))
	err := cluster.LoadCache()
	if err != nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	deploy := &metapb.DeployV1Policy{
		Master: &metapb.ZoneV1Policy{
			ZoneName:  "华北",
			Master: &metapb.RoomV1Policy{
				RoomName:   "廊坊",
				DsNum:  int32(2),
				Sync:   int32(1),
			},
			Slaves: []*metapb.RoomV1Policy{&metapb.RoomV1Policy{
				RoomName:   "马驹桥",
				DsNum:  int32(1),
				Sync:   int32(1),
			}},
		},
	}
	err = cluster.SetDeploy(deploy)
	if err != nil {
		t.Errorf("test failed %v, deploy %v", err, cluster.deploy)
		time.Sleep(time.Second)
		return
	}
	zone := &metapb.Zone{
		ZoneName:  "华北",
		ZoneId:    1,
	}
	lRoom := &metapb.Room{
		RoomName: "廊坊",
		RoomId:  1,
	}
	mRoom := &metapb.Room{
		RoomName: "马驹桥",
		RoomId:  2,
	}
	machines := make([]*metapb.Machine, 8)
	machines[0] = &metapb.Machine{
		Ip:       "192.168.0.1",
		Zone:     zone,
		Room:     lRoom,
		SwitchIp: "192.168.1.1",
		FrameId:  1,
	}
	machines[1] = &metapb.Machine{
		Ip:       "192.168.0.2",
		Zone:     zone,
		Room:     lRoom,
		SwitchIp: "192.168.1.1",
		FrameId:  1,
	}
	machines[2] = &metapb.Machine{
		Ip:       "192.168.0.3",
		Zone:     zone,
		Room:     lRoom,
		SwitchIp: "192.168.1.2",
		FrameId:  2,
	}
	machines[3] = &metapb.Machine{
		Ip:       "192.168.0.4",
		Zone:     zone,
		Room:     lRoom,
		SwitchIp: "192.168.1.2",
		FrameId:  2,
	}

	//
	machines[4] = &metapb.Machine{
		Ip:       "172.10.0.1",
		Zone:     zone,
		Room:     mRoom,
		SwitchIp: "172.10.1.1",
		FrameId:  1,
	}
	machines[5] = &metapb.Machine{
		Ip:       "172.10.0.2",
		Zone:     zone,
		Room:     mRoom,
		SwitchIp: "172.10.1.1",
		FrameId:  1,
	}
	machines[6] = &metapb.Machine{
		Ip:       "172.10.0.3",
		Zone:     zone,
		Room:     mRoom,
		SwitchIp: "172.10.1.2",
		FrameId:  2,
	}
	machines[7] = &metapb.Machine{
		Ip:       "172.10.0.4",
		Zone:     zone,
		Room:     mRoom,
		SwitchIp: "172.10.1.2",
		FrameId:  2,
	}
	cluster.AddMachines(machines)
	for _, mac := range machines {
		ip := mac.GetIp()
		ins1, err := cluster.AddInstance(ip, uint64(6060), uint64(1234), uint64(1235), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins1.State = metapb.NodeState_N_Login
		ins1.Stats.DiskFree = MinDiskFree
		ins2, err := cluster.AddInstance(ip, uint64(6061), uint64(1236), uint64(1237), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins2.State = metapb.NodeState_N_Login
		ins2.Stats.DiskFree = MinDiskFree
		ins3, err := cluster.AddInstance(ip, uint64(6062), uint64(1238), uint64(1239), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins3.State = metapb.NodeState_N_Login
		ins3.Stats.DiskFree = MinDiskFree
		ins4, err := cluster.AddInstance(ip, uint64(6063), uint64(1240), uint64(1241), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins4.State = metapb.NodeState_N_Login
		ins4.Stats.DiskFree = MinDiskFree
	}
	selecter := NewDeployV1Select(cluster.GetAllZones(), deploy)
	f := NewDeployInstanceFilter()
	for i := 0; i < 100; i++ {
		inss := selecter.SelectInstance(3, f)
		if len(inss) != 3 {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		switchIps := make(map[string]*Machine)
		for _, ins := range inss {
			if mac, find := cluster.FindMachine(ins.MacIp); find {
				switchIps[mac.GetSwitchIp()] = mac
			} else {
				t.Error("test failed")
				time.Sleep(time.Second)
				return
			}
		}
		if len(switchIps) != 3 {
			t.Errorf("test failed %d %v", i, inss)
			time.Sleep(time.Second)
			return
		}
		var start, end *metapb.Key
		start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
		end = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
		var peers []*metapb.Peer
		rangeId := uint64(i + 1)
		for _, ins := range inss {
			peer := &metapb.Peer{
				Id:     rangeId,
				NodeId: ins.GetId(),
			}
			peers = append(peers, peer)
		}
		sort.Sort(metapb.PeersByNodeIdSlice(peers))
		r := &metapb.Range{
			Id:     rangeId,
			StartKey: start,
			EndKey: end,
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers: peers,
			DbName: dbName,
			TableName: tName,
			DbId:  dbId,
			TableId: tId,
			State: metapb.RangeState_R_Normal,
		}
		rng := NewRange(r)
		rng.Stats.Size_ = DefaultMaxRangeSize
		cluster.ranges.Add(rng)
		for _, peer := range peers {
			ins, find := cluster.FindInstance(peer.GetNodeId())
			if !find {
				t.Error("test failed")
				time.Sleep(time.Second)
				return
			}
			ins.AddRange(rng)
		}
		if i <= 3{
			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: inss[0].GetAddress()}
			rng.UpdateLeader(leader)
		}
		if i > 3 && i<=7{
			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: inss[1].GetAddress()}
			rng.UpdateLeader(leader)
		}
		if i > 7{
			leader := &metapb.Leader{RangeId: r.Peers[2].GetId(), NodeId: r.Peers[2].GetNodeId(), NodeAddr: inss[2].GetAddress()}
			rng.UpdateLeader(leader)
		}
		switchIps = make(map[string]*Machine)
		for _, peer := range r.GetPeers() {
			if ins, find := cluster.FindInstance(peer.GetNodeId()); find {
				if mac, find := cluster.FindMachine(ins.MacIp); find {
					switchIps[mac.GetSwitchIp()] = mac
				} else {
					t.Error("test failed")
					time.Sleep(time.Second)
					return
				}
			} else {
				t.Error("test failed")
				time.Sleep(time.Second)
				return
			}
		}
		if len(switchIps) != 3 {
			t.Errorf("test failed %d %v", i, inss)
			time.Sleep(time.Second)
			return
		}
	}
	// 模拟节点磁盘占用空间
	for i, ins := range cluster.GetAllInstances() {
		ins.Stats.DiskProcRate = float64(((i + 1) % 10) * 10)
		if ins.Stats.DiskProcRate >= float64(90.0) {
			ins.Stats.DiskProcRate += float64(0.1)
		}
		var ranges []uint64
		var leaderCount uint32
		for _, r := range ins.GetAllRanges() {
			ranges = append(ranges, r.GetId())
			if r.GetLeader().GetNodeId() == ins.GetId() {
				leaderCount++
			}
		}
		ins.AllRanges = ranges
		ins.RangeCount = uint32(len(ranges))
		ins.RangeLeaderCount = leaderCount
	}

	balancer := NewRangesBalanceScheduler(time.Second, cluster)
	task := balancer.Schedule()
	if task == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	r := task.GetRangeTransfer().GetRange()
	switchIps := make(map[string]*Machine)
	for _, peer := range r.GetPeers() {
		if ins, find := cluster.FindInstance(peer.GetNodeId()); find {
			if mac, find := cluster.FindMachine(ins.MacIp); find {
				switchIps[mac.GetSwitchIp()] = mac
			} else {
				t.Error("test failed")
				time.Sleep(time.Second)
				return
			}
		} else {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
	}
	if len(switchIps) != 3 {
		t.Errorf("test failed %v", switchIps)
		time.Sleep(time.Second)
		return
	}

	down := task.GetRangeTransfer().GetDownPeer()
	if ins, find := cluster.FindInstance(down.GetNodeId()); !find {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	} else {
		if mac, find := cluster.FindMachine(ins.MacIp); find {
			delete(switchIps, mac.GetSwitchIp())
		}
	}
	up := task.GetRangeTransfer().GetUpPeer()
	if ins, find := cluster.FindInstance(up.GetNodeId()); !find {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	} else {
		if mac, find := cluster.FindMachine(ins.MacIp); find {
			switchIps[mac.GetSwitchIp()] = mac
		}
	}
	if len(switchIps) != 3 {
		t.Errorf("test failed %v", switchIps)
		time.Sleep(time.Second)
		return
	}

	time.Sleep(time.Second)
}

/**
均衡前
2017-08-02 15:27:37,657 :0: [DEBUG] [127.0.0.1:6060->rangeNum:10->nodeScore:%!d(float64=100)->taskSize:0]^M
2017-08-02 15:27:37,657 :0: [DEBUG] [127.0.0.1:6061->rangeNum:20->nodeScore:%!d(float64=200)->taskSize:0]^M
2017-08-02 15:27:37,657 :0: [DEBUG] [127.0.0.1:6062->rangeNum:20->nodeScore:%!d(float64=200)->taskSize:0]^M
2017-08-02 15:27:37,657 :0: [DEBUG] [127.0.0.1:6063->rangeNum:10->nodeScore:%!d(float64=100)->taskSize:0]^M
2017-08-02 15:27:37,657 :0: [DEBUG] [127.0.0.1:6064->rangeNum:0->nodeScore:%!d(float64=0)->taskSize:0]^M
均衡后结果:
        balance_ranges_test.go:265: [127.0.0.1:6060->rangeNum:12->nodeScore:%!d(float64=120)->taskSize:2]
        balance_ranges_test.go:265: [127.0.0.1:6061->rangeNum:16->nodeScore:%!d(float64=160)->taskSize:4]
        balance_ranges_test.go:265: [127.0.0.1:6062->rangeNum:16->nodeScore:%!d(float64=160)->taskSize:4]
        balance_ranges_test.go:265: [127.0.0.1:6063->rangeNum:12->nodeScore:%!d(float64=120)->taskSize:2]
        balance_ranges_test.go:265: [127.0.0.1:6064->rangeNum:4->nodeScore:%!d(float64=40)->taskSize:4]

 */
//func TestBalanceRangesByNodeScore(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.NodeDiskMustFreeSize = 0
//	MasterConf.SchRangeBalanceByNodeScoreInterval=60
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
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("leader_balance_scheduler")
//	coordinator.removeScheduler("leader_score_balance_scheduler")
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
//
//	table.UpdateAutoScheduleInfo(cluster,false,false,false)
//
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//
//	for i, j := 0, 0; i < 20; i++ {
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//
//		_nodes := nodes[:3]
//		if i>=10{
//			_nodes = nodes[1:4]
//		}
//
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//
//		if i <= 3{
//			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: _nodes[0].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i > 3 && i<=7{
//			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: _nodes[1].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i > 7{
//			leader := &metapb.Leader{RangeId: r.Peers[2].GetId(), NodeId: r.Peers[2].GetNodeId(), NodeAddr: _nodes[2].GetAddress()}
//			r.UpdateLeader(leader)
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
//			node.RangeCount += 1
//			node.Score += r.Score
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
//		t.Logf("[%s->rangeNum:%d->nodeScore:%f->taskSize:%d]", n.Address,n.GetRangesNum(),n.Score,n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(wtTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//
//	t.Logf("==========end================")
//	for _,n := range nodes{
//		t.Logf("[%s->rangeNum:%d->nodeScore:%f->taskSize:%d]", n.Address,n.GetRangesNum(),n.Score,n.tasks.Size())
//	}
//}


/**
n1 n2 n3 n4 n5
10 10 10 0  0
   10 10 10 0
n2上20个副本都是leader
期望结果,n2上没有副本可迁移
均衡前
2017-08-02 20:05:20,067 :0: [DEBUG] [127.0.0.1:6061->rangeNum:10->nodeScore:100.000000->taskSize:0]^M
2017-08-02 20:05:20,067 :0: [DEBUG] [127.0.0.1:6062->rangeNum:20->nodeScore:200.000000->taskSize:0]^M
2017-08-02 20:05:20,067 :0: [DEBUG] [127.0.0.1:6063->rangeNum:20->nodeScore:200.000000->taskSize:0]^M
2017-08-02 20:05:20,067 :0: [DEBUG] [127.0.0.1:6064->rangeNum:10->nodeScore:100.000000->taskSize:0]^M
2017-08-02 20:05:20,067 :0: [DEBUG] [127.0.0.1:6060->rangeNum:0->nodeScore:0.000000->taskSize:0]^M
均衡后结果:
        balance_ranges_test.go:406: [127.0.0.1:6061->rangeNum:10->nodeScore:100.000000->taskSize:0]
        balance_ranges_test.go:406: [127.0.0.1:6062->rangeNum:20->nodeScore:200.000000->taskSize:0]
        balance_ranges_test.go:406: [127.0.0.1:6063->rangeNum:16->nodeScore:160.000000->taskSize:4]
        balance_ranges_test.go:406: [127.0.0.1:6064->rangeNum:10->nodeScore:100.000000->taskSize:0]
        balance_ranges_test.go:406: [127.0.0.1:6060->rangeNum:4->nodeScore:40.000000->taskSize:4]
 */
//func TestBalanceRangesByNodeScore2(t *testing.T) {
//	genStep = 10000
//	conf := new(Config)
//	conf.LoadConfig()
//	MasterConf = conf
//	MasterConf.NodeDiskMustFreeSize = 0
//	MasterConf.SchRangeBalanceByNodeScoreInterval=60
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
//	coordinator.removeScheduler("range_gc_sheduler")
//	coordinator.removeScheduler("split_scheduler")
//
//	coordinator.removeScheduler("range_gc_local_sheduler")
//	coordinator.removeScheduler("task_monitor_sheduler")
//	coordinator.removeScheduler("table_delete_sheduler")
//
//	coordinator.removeScheduler("ranges_balance_scheduler")
//	coordinator.removeScheduler("leader_balance_scheduler")
//	coordinator.removeScheduler("leader_score_balance_scheduler")
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
//
//	table.UpdateAutoScheduleInfo(cluster,false,false,false)
//
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//
//	for i, j := 0, 0; i < 20; i++ {
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//
//		_nodes := nodes[:3]
//		if i>=10{
//			_nodes = nodes[1:4]
//		}
//
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Error("create range failed, err[%v]", err)
//			return
//		}
//
//		if i < 10{
//			leader := &metapb.Leader{RangeId: r.Peers[1].GetId(), NodeId: r.Peers[1].GetNodeId(), NodeAddr: _nodes[1].GetAddress()}
//			r.UpdateLeader(leader)
//		}
//		if i >= 10{
//			leader := &metapb.Leader{RangeId: r.Peers[0].GetId(), NodeId: r.Peers[0].GetNodeId(), NodeAddr: _nodes[0].GetAddress()}
//			r.UpdateLeader(leader)
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
//			node.RangeCount += 1
//			node.Score += r.Score
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
//		t.Logf("[%s->rangeNum:%d->nodeScore:%f->taskSize:%d]", n.Address,n.GetRangesNum(),n.Score,n.tasks.Size())
//	}
//
//	time.Sleep(time.Second * time.Duration(wtTime))
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Error("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//
//	t.Logf("==========end================")
//	for _,n := range nodes{
//		t.Logf("[%s->rangeNum:%d->nodeScore:%f->taskSize:%d]", n.Address,n.GetRangesNum(),n.Score,n.tasks.Size())
//	}
//}