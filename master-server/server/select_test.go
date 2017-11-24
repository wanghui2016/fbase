package server

import (
	"testing"
	"math/rand"
	"fmt"
	"time"
	"sort"

	"model/pkg/metapb"
)

func TestNewRandomSelect(t *testing.T) {
	nodes := make([]*Instance,0)
	node := &Instance{
		Node:&metapb.Node{
			Id:1,
			Address:"172.0.0.1",
		},
		Score:0.9,
	}

	nodes = append(nodes,node)

	randNodes := randSlic(nodes)
	if len(nodes) != len(randNodes) {
		t.Error("size no equal %v %v \n",nodes,randNodes)
	}else{
		fmt.Printf("%v,%v \n",nodes,randNodes)
	}
	nodes2 := make([]*Instance,0)
	for i:=1; i<=10; i++ {
		node2 := &Instance{
			Node:&metapb.Node{
				Id:uint64(i),
				Address:fmt.Sprint("127.0.0.",i+i%2),
			},
			Score:0.9,
		}

		nodes2 = append(nodes2,node2)
	}

	randNodes2 := randSlic(nodes2)
	if len(nodes2) != len(randNodes2) {
		t.Error("size no equal %v \n %v \n",nodes2,randNodes2)
	}else{
		fmt.Printf("%v,\n %v \n",nodes2,randNodes2)
	}

	MasterConf = new(Config)
	MasterConf.Mode="debug"
	rs := NewDebugDeploySelect(nodes2)
	sns := rs.SelectInstance(3)
	if len(sns) != 3 {
		t.Error("size no equal %v \n %v \n",sns,nodes2)
	}
	for _,n:= range sns {
		fmt.Println(n.Id)
	}
}

func randSlic(nodes []*Instance) []*Instance{
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	offset := r.Intn(len(nodes))
	fmt.Printf("\n %d \n",offset)
	randomNodes := make([]*Instance,0,len(nodes))
	randomNodes = append(randomNodes,nodes[offset:]...)
	randomNodes = append(randomNodes,nodes[0:offset]...)

	return randomNodes
}

func TestDeploySelect(t *testing.T) {
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
	inss := selecter.SelectInstance(3, f)
	if len(inss) != 3 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	t.Logf("select 3 instances!!!!!!!")
	macsMap := make(map[string]*Machine)
	switchsMap := make(map[string]*Machine)
	zoneMap := make(map[string]*Zone)
	for _, ins := range inss {
		mac, find := cluster.FindMachine(ins.MacIp)
		if !find {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		if _, ok := macsMap[mac.GetIp()]; ok {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		if _, ok := switchsMap[mac.GetSwitchIp()]; ok {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		macsMap[mac.GetIp()] = mac
		switchsMap[mac.GetIp()] = mac
		zone, find := cluster.FindZone(mac.GetZone().GetZoneName())
		if !find {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		t.Logf("zone name %s", zone.GetZoneName())
		zoneMap[zone.GetZoneName()] = zone
	}
	if len(zoneMap) != 1 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	time.Sleep(time.Second)
	return
}

func TestTransferSelect(t *testing.T) {
	conf := new(Config)
	store := NewLocalStore()
	alarm := NewTestAlarm()
	cluster := NewCluster(uint64(1), uint64(1), store, alarm, conf)
	cluster.cli = NewLocalClient()
	cluster.UpdateLeader(uint64(1))
	cluster.LoadCache()
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
	cluster.SetDeploy(deploy)
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
	inss := selecter.SelectInstance(3, f)
	if len(inss) != 3 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	macsMap := make(map[string]struct{})
	switchsMap := make(map[string]struct{})
	zoneMap := make(map[string]*Zone)
	for _, ins := range inss {
		mac, find := cluster.FindMachine(ins.MacIp)
		if !find {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		if _, ok := macsMap[mac.GetIp()]; ok {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		if _, ok := switchsMap[mac.GetSwitchIp()]; ok {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		macsMap[mac.GetIp()] = struct{}{}
		switchsMap[mac.GetIp()] = struct{}{}
		zone, find := cluster.FindZone(mac.GetZone().GetZoneName())
		if !find {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
		zoneMap[zone.GetZoneName()] = zone
	}
	if len(zoneMap) != 1 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	var start, end *metapb.Key
	start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
	end = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
	var peers []*metapb.Peer
	for _, ins := range inss {
		peer := &metapb.Peer{
			Id:     1,
			NodeId: ins.GetId(),
		}
		peers = append(peers, peer)
	}
	sort.Sort(metapb.PeersByNodeIdSlice(peers))
	r := &metapb.Range{
		Id:     1,
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
	selecter = NewTransferSelect(cluster.GetAllZones(), cluster.deploy)
	z, find := cluster.FindZone("华北")
	if !find {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	room, find := z.FindRoom("廊坊")
	if !find {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	f = NewTransferFilter(cluster, room, switchsMap, macsMap)
	inss = selecter.SelectInstance(1, f)
	if len(inss) != 1 {
		t.Errorf("test failed %v", inss)
		time.Sleep(time.Second)
		return
	}
	ins := inss[0]
	mac, find := cluster.FindMachine(ins.MacIp)
	if !find {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	for ip, _ := range macsMap {
		if ip == mac.GetIp() {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
	}
	for ip, _ := range switchsMap {
		if ip == mac.GetSwitchIp() {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
	}
	if mac.GetZone().GetZoneName() != z.GetZoneName() {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
}