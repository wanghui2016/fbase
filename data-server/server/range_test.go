package server

import (
	"math/rand"
	"testing"
	"time"

	"model/pkg/metapb"
	"model/pkg/eventpb"
)

func TestEventStatics(t *testing.T) {
	conf := &Config{
		AppName:       "data-server",
		AppVersion:    "v1",
		AppManagePort: 6060,
		DataPath:      "/export/Data/data-server",
		NodeID:        1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   &metapb.Key{Key: []byte("a"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("z"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	defer rng1.Clean()
	svr.AddRange(rng1)
	start := time.Now().Unix()
	time.Sleep(time.Second)
	rng1.reportEventForStatistics(start, eventpb.StatisticsType_LeaderLose)
	event := <-svr.eventChan
	if event.GetEventEventStatistics().GetEndTime() <= start {
		t.Error("test failed")
		return
	}
	t.Log("test success")
}

func TestEventStatics1(t *testing.T) {
	conf := &Config{
		AppName:    "data-server",
		AppVersion: "v1",
		AppRpcPort: 6060,
		DataPath:   "/export/Data/data-server",
		NodeID:     1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   &metapb.Key{Key: []byte("a"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("z"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	defer rng1.Clean()
	svr.AddRange(rng1)
	start := time.Now().Unix()
	go func(s int64) {
		defer rng1.reportEventForStatistics(s, eventpb.StatisticsType_LeaderLose)
		time.Sleep(time.Second)
	}(start)

	event := <-svr.eventChan
	if event.GetEventEventStatistics().GetEndTime() <= start {
		t.Error("test failed")
		return
	}
	t.Log("test success")
}

func TestRangeMetric(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	m := newRangeMetric(1)
	var ops, in, out, actualOps, actualIns, actualOuts uint64
	ops = rnd.Uint64() % 1000000
	in = rnd.Uint64()
	out = rnd.Uint64()
	for i := uint64(0); i < ops; i++ {
		m.addRequest()
	}
	m.addInBytes(in)
	m.addOutBytes(out)

	time.Sleep(time.Second * 2)
	actualOps, actualIns, actualOuts = m.collect()
	t.Log(ops, in, out)
	t.Log(actualOps, actualIns, actualOuts)

	ops = rnd.Uint64() % 1000000
	in = rnd.Uint64()
	out = rnd.Uint64()
	for i := uint64(0); i < ops; i++ {
		m.addRequest()
	}
	m.addInBytes(in)
	m.addOutBytes(out)

	time.Sleep(time.Second * 3)
	actualOps, actualIns, actualOuts = m.collect()
	t.Log(ops, in, out)
	t.Log(actualOps, actualIns, actualOuts)
}
