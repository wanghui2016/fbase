package server

import (
	"time"
	"testing"
	"sort"

	"model/pkg/metapb"
	"encoding/json"
	"fmt"
)

func TestMachJson(t *testing.T) {
	zone := &metapb.Zone{
		ZoneName:  "华北",
		ZoneId:    1,
	}
	lRoom := &metapb.Room{
		RoomName: "廊坊",
		RoomId:  1,
	}
	machine := &metapb.Machine{
		Ip:       "192.168.0.1",
		Zone:     zone,
		Room:     lRoom,
		SwitchIp: "192.168.1.1",
		FrameId:  1,
	}
	ms, _ := json.Marshal(machine)
	fmt.Println(string(ms))

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
	d, _ := json.Marshal(deploy)
	fmt.Println(string(d))
}


// > 90%
func TestBalanceDisk1(t *testing.T) {
	genStep = 10000
	conf := new(Config)
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
		for _, r := range ins.GetAllRanges() {
			ranges = append(ranges, r.GetId())
		}
		ins.AllRanges = ranges
	}
	balancer := NewDiskBalanceScheduler(time.Second, cluster, NewTestAlarm())
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
		if ins.Stats.DiskProcRate < float64(90.0) {
			t.Errorf("test failed %f", ins.Stats.DiskProcRate)
			time.Sleep(time.Second)
			return
		}
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
		if ins.Stats.DiskProcRate > float64(70.0) {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
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
	return
}

// 三档和一档
func TestBalanceDisk2(t *testing.T) {
	genStep = 10000
	conf := new(Config)
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
		ins, err := cluster.AddInstance(ip, uint64(6060), uint64(1234), uint64(1235), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
		ins, err = cluster.AddInstance(ip, uint64(6061), uint64(1236), uint64(1237), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
		ins, err = cluster.AddInstance(ip, uint64(6062), uint64(1238), uint64(1239), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
		ins, err = cluster.AddInstance(ip, uint64(6063), uint64(1240), uint64(1241), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
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
	}
	// 模拟节点磁盘占用空间
	for i, ins := range cluster.GetAllInstances() {
		tt := i + 1 % 10
		if tt < 7 && tt >= 5 {
			tt = tt - 2
		}
		ins.Stats.DiskProcRate = float64(tt * 10)
		if ins.Stats.DiskProcRate >= float64(90.0) {
			ins.Stats.DiskProcRate = float64(80.1)
		}
		var ranges []uint64
		for _, r := range ins.GetAllRanges() {
			ranges = append(ranges, r.GetId())
		}
		ins.AllRanges = ranges
	}
	balancer := NewDiskBalanceScheduler(time.Second, cluster, NewTestAlarm())
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
			}
		}
	}
	down := task.GetRangeTransfer().GetDownPeer()
	if ins, find := cluster.FindInstance(down.GetNodeId()); !find {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	} else {
		if ins.Stats.DiskProcRate < float64(50.0) {
			t.Errorf("test failed %f", ins.Stats.DiskProcRate)
			time.Sleep(time.Second)
			return
		}
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
		if ins.Stats.DiskProcRate > float64(70.0) {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		if mac, find := cluster.FindMachine(ins.MacIp); find {
			switchIps[mac.GetSwitchIp()] = mac
		}
	}
	if len(switchIps) != 3 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	time.Sleep(time.Second)
	return
}

// 三档和二档
func TestBalanceDisk3(t *testing.T) {
	genStep = 10000
	conf := new(Config)
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
		ins, err := cluster.AddInstance(ip, uint64(6060), uint64(1234), uint64(1235), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
		ins, err = cluster.AddInstance(ip, uint64(6061), uint64(1236), uint64(1237), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
		ins, err = cluster.AddInstance(ip, uint64(6062), uint64(1238), uint64(1239), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
		ins, err = cluster.AddInstance(ip, uint64(6063), uint64(1240), uint64(1241), "data-server")
		if err != nil {
			t.Error("test failed ", err)
			time.Sleep(time.Second)
			return
		}
		ins.State = metapb.NodeState_N_Login
		ins.Stats.DiskFree = MinDiskFree
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
	}
	// 模拟节点磁盘占用空间
	for i, ins := range cluster.GetAllInstances() {
		tt := i + 1 % 10
		if tt < 5 {
			tt = tt + 5
			if tt >= 9 {
				tt -= 2
			}
		}
		ins.Stats.DiskProcRate = float64(tt * 10)
		if ins.Stats.DiskProcRate >= float64(90.0) {
			ins.Stats.DiskProcRate = float64(80.1)
		}
		var ranges []uint64
		for _, r := range ins.GetAllRanges() {
			ranges = append(ranges, r.GetId())
		}
		ins.AllRanges = ranges
	}
	balancer := NewDiskBalanceScheduler(time.Second, cluster, NewTestAlarm())
	task := balancer.Schedule()
	if task != nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	time.Sleep(time.Second)
	return
}
