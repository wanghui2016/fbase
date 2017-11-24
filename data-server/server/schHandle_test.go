package server

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	engineSch "engine/scheduler"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"model/pkg/schpb"
	"raft"
	"util/encoding"
	"util/metrics"
)

var id uint64 = 0
var rowId uint64 = 0

const dbName = "test_Db1"
const tableName = "test_tAble1"

var nodes map[uint64]*metapb.Node = make(map[uint64]*metapb.Node, 3)
var s *Server
var localNode *metapb.Node

func ParseClusterInfo() {

	nodeInfos := strings.Split("1-127.0.0.1-8887-8877-8867;", ";")
	for _, nodeInfo := range nodeInfos {
		if nodeInfo == "" {
			continue
		}
		elems := strings.Split(nodeInfo, "-")
		if len(elems) != 5 {
			fmt.Errorf("clusterInfos format{nodeId-ip-port-raftHeartbeatPort-raft.replicaPort[;nodeId-ip-port-raftHeartbeatPort-raft.replicaPort] }is error %s", nodeInfo)
		}
		node := new(metapb.Node)
		node.Id, _ = strconv.ParseUint(elems[0], 10, 64)
		ip := elems[1]
		node.Address = fmt.Sprintf("%s:%s", ip, elems[2])

		node.RaftAddrs = &metapb.RaftAddrs{
			HeartbeatAddr: fmt.Sprintf("%s:%s", ip, elems[3]),
			ReplicateAddr: fmt.Sprintf("%s:%s", ip, elems[4]),
		}

		nodes[node.Id] = node
	}
	var ok bool
	localNode, ok = nodes[1]
	if !ok {
		fmt.Println("node info is error")
	}

}

func startDataServer() {
	ParseClusterInfo()
	/**
		app.name = data-node
	app.isprod = true
	app.port = 6061
	app.id = 1
	app.version = v1

	log.dir = /tmp/sharkstore/log
	log.module = node
	log.level = debug

	raft.heartbeatPort = 1234
	raft.replicaPort = 1235

	db.path = /sharkstore/data

	master.addrs = 127.0.0.1:8887;127.0.0.1:8888;127.0.0.1:8889

	ping.frequency = 10
	*/

	//ip := strings.Split(localNode.GetAddress(), ":")[0]
	portStr := strings.Split(localNode.GetAddress(), ":")[1]
	port, _ := strconv.Atoi(portStr)
	list := strings.Split("127.0.0.1:8887;127.0.0.1:8888;127.0.0.1:8889", ";")

	conf := &Config{
		AppName:    "data-1",
		AppVersion: "1.0",
		AppRpcPort: port,
		DataPath:   "/tmp/fbase/datanode/data",
		NodeID:     localNode.Id,

		LogDir:    "/tmp/fbase/datanode/logs",
		LogModule: "datanode",
		LogLevel:  "debug",

		RaftHeartbeatAddr:        localNode.GetRaftAddrs().HeartbeatAddr,
		RaftReplicaAddr:          localNode.GetRaftAddrs().ReplicateAddr,
		HeartbeatInterval:        10,
		CompactionConcurrency:    15,
		CompactionWriteRateLimit: 30 * 1024 * 1024,
	}
	conf.MasterServerAddrs = list
	cluster := NewCluster(nil)

	// conf raft
	rc := raft.DefaultConfig()
	rc.HeartbeatAddr = conf.RaftHeartbeatAddr
	rc.ReplicateAddr = conf.RaftReplicaAddr
	rc.Resolver = NewResolver(cluster)
	rc.NodeID = uint64(conf.NodeID)
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		fmt.Printf("boot raft server failed, err[%v]", err)
	}
	esc := &engineSch.Config{
		Concurrency:    conf.CompactionConcurrency,
		WriteRateLimit: conf.CompactionWriteRateLimit,
	}
	engineSch.StartScheduler(esc)
	s = new(Server)
	s.conf = conf
	s.raftConfig = rc
	s.raftServer = rs
	s.cluster = cluster
	s.node = localNode

	s.quit = make(chan struct{})
	s.ranges = make(map[uint64]*Range, 1000)
	s.metricMeter = metrics.NewMetricMeter("data-server", &Report{})
}

func TestCreateRange(t *testing.T) {
	startDataServer()

	os.RemoveAll(s.conf.LogDir)
	os.RemoveAll(s.conf.DataPath)
	os.MkdirAll(s.conf.LogDir, 0777)
	os.MkdirAll(s.conf.DataPath, 0777)

	for i := 0; i < 100; i++ {
		createRange(t)
	}

}

func TestDeleteRange(t *testing.T) {
	TestCreateRange(t)
	ranges := s.GetAllRanges()
	size := len(ranges)
	for _, r := range ranges {
		req := &schpb.DeleteRangeRequest{
			RangeId: r.GetRangeId(),
		}
		_, err := s.DeleteRange(context.Background(), req)
		if err != nil {
			t.Error(err)
		}
		if len(s.GetAllRanges()) != size-1 {
			t.Error("delete range error")
		} else {
			size--
		}
	}

}

func createRange(t *testing.T) {
	rangeId := atomic.AddUint64(&id, 1)

	peer := &metapb.Peer{Id: rangeId, NodeId: 1}

	_range := &metapb.Range{
		Id:         rangeId,
		StartKey:   nil,
		EndKey:     nil,
		RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
		Peers:      []*metapb.Peer{peer},
		DbName:     dbName,
		TableName:  tableName,
		State:      metapb.RangeState_R_Init,
	}

	req := &schpb.CreateRangeRequest{
		Range: _range,
	}
	_, err := s.CreateRange(context.Background(), req)
	if err != nil {
		t.Error(err)
	}

	r, exist := s.FindRange(rangeId)
	if !exist || r == nil {
		t.Error("add range error")
	}
}

func TestRange_Insert(t *testing.T) {
	TestCreateRange(t)
	rid := atomic.AddUint64(&rowId, 1)
	buff1 := make([]byte, 0)
	buff1 = encoding.EncodeUvarintAscending(buff1, rid)

	buff2 := make([]byte, 0)
	buff2 = encoding.EncodeBytesValue(buff2, uint32(2), []byte("abcdefgchijijgalfjoa21421343288"))

	row := &kvrpcpb.KeyValue{
		Key:   buff1,
		Value: buff2,
	}
	//curTime := time.Now()
	req := &kvrpcpb.KvInsertRequest{
		Rows: []*kvrpcpb.KeyValue{
			row,
		},
		//Version:uint64(curTime.UnixNano() ),

	}
	ranges := s.GetAllRanges()
	if len(ranges) == 0 {
		t.Error("create range error")
	}
	for _, ran := range ranges {
		s.handleInsert(context.Background(), ran.GetRangeId(), req)
	}

	// for _, ran := range ranges {
	// 	reqKey := &kvrpcpb.KvSelectRequest{
	// 		Key: buff1,
	// 		//Version:req.Version,
	// 	}

	// TODO: fix
	// resp := ran.Get(context.Background(), reqKey)
	// if bytes.Compare(resp.Row.Fields[0].Value, buff1) != 0 {
	// 	t.Error("put row error ,maybe get error")
	// }

	// if bytes.Compare(resp.Row.Fields[1].Value, buff2) != 0 {
	// 	t.Error("put row error ,maybe get error")
	// }
	// }

}

type Keys struct {
	Start []byte
	End   []byte
}

func TestRangesRecover1(t *testing.T) {
	startDataServer()

	os.RemoveAll(s.conf.LogDir)
	os.RemoveAll(s.conf.DataPath)
	os.MkdirAll(s.conf.LogDir, 0777)
	os.MkdirAll(s.conf.DataPath, 0777)

	var rrs = []Keys{Keys{[]byte("a"), []byte("e")}, Keys{[]byte("e"), []byte("h")},
		Keys{[]byte("h"), []byte("l")}, Keys{[]byte("l"), []byte("n")},
		Keys{[]byte("n"), []byte("q")}, Keys{[]byte("q"), []byte("t")},
		Keys{[]byte("t"), []byte("w")}, Keys{[]byte("w"), []byte("xa")},
		Keys{[]byte("xa"), []byte("xz")}, Keys{[]byte("xz"), []byte("ya")},
		Keys{[]byte("ya"), []byte("ye")}, Keys{[]byte("ye"), []byte("yz")},
		Keys{[]byte("yz"), []byte("za")}, Keys{[]byte("za"), []byte("zh")},
		Keys{[]byte("a"), []byte("h")}, Keys{[]byte("q"), []byte("w")},
		Keys{[]byte("xa"), []byte("ya")}, Keys{[]byte("t"), []byte("xa")},

		Keys{[]byte("zh"), []byte("zla")}, Keys{[]byte("zla"), []byte("zlb")},
		Keys{[]byte("zlb"), []byte("zlg")}, Keys{[]byte("zlg"), []byte("zlw")},
		Keys{[]byte("zlw"), []byte("zlz")}, Keys{[]byte("zlz"), []byte("zma")},
		Keys{[]byte("zma"), []byte("zmb")}, Keys{[]byte("zmb"), []byte("zmc")},
		Keys{[]byte("zmc"), []byte("zmd")}, Keys{[]byte("zmd"), []byte("zmh")},
		Keys{[]byte("zmh"), []byte("zmz")}, Keys{[]byte("zmz"), []byte("zzz")}}
	var ranges []*metapb.Range
	for _, key := range rrs {
		rangeId := atomic.AddUint64(&id, 1)

		peer := &metapb.Peer{Id: rangeId, NodeId: 1}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   &metapb.Key{Key: key.Start, Type: metapb.KeyType_KT_Ordinary},
			EndKey:     &metapb.Key{Key: key.End, Type: metapb.KeyType_KT_Ordinary},
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      []*metapb.Peer{peer},
			DbName:     dbName,
			TableName:  tableName,
			State:      metapb.RangeState_R_Init,
		}
		ranges = append(ranges, r)
	}
	s.RangesRecover(ranges)
	if len(s.ranges) != len(ranges) {
		t.Errorf("test failed, %d %d", len(s.ranges), len(ranges))
		time.Sleep(time.Second)
		return
	}
	var _ranges []*metapb.Range
	for _, r := range s.ranges {
		_ranges = append(_ranges, r.region)
	}
	defer func() {
		for _, r := range s.ranges {
			r.Clean()
		}
	}()
	sort.Sort(metapb.RangeByIdSlice(_ranges))
	sort.Sort(metapb.RangeByIdSlice(ranges))
	for i := 0; i < len(ranges); i++ {
		if ranges[i].GetId() != _ranges[i].GetId() {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
	}
}

func TestRangesRecover2(t *testing.T) {
	startDataServer()

	os.RemoveAll(s.conf.LogDir)
	os.RemoveAll(s.conf.DataPath)
	os.MkdirAll(s.conf.LogDir, 0777)
	os.MkdirAll(s.conf.DataPath, 0777)

	var rrs = []Keys{Keys{[]byte("a"), []byte("e")}, Keys{[]byte("e"), []byte("h")},
		Keys{[]byte("h"), []byte("l")}, Keys{[]byte("l"), []byte("n")},
		Keys{[]byte("n"), []byte("q")}, Keys{[]byte("q"), []byte("t")},
		Keys{[]byte("t"), []byte("w")}, Keys{[]byte("w"), []byte("xa")},
		Keys{[]byte("xa"), []byte("xz")}, Keys{[]byte("xz"), []byte("ya")},
		Keys{[]byte("ya"), []byte("ye")}, Keys{[]byte("ye"), []byte("yz")},
		Keys{[]byte("yz"), []byte("za")}, Keys{[]byte("za"), []byte("zh")},
		Keys{[]byte("a"), []byte("h")}, Keys{[]byte("q"), []byte("w")},
		Keys{[]byte("xa"), []byte("ya")}, Keys{[]byte("t"), []byte("xa")}}

	//Keys{[]byte("zh"), []byte("zla")}, Keys{[]byte("zla"), []byte("zlb")},
	//Keys{[]byte("zlb"), []byte("zlg")}, Keys{[]byte("zlg"), []byte("zlw")},
	//Keys{[]byte("zlw"), []byte("zlz")}, Keys{[]byte("zlz"), []byte("zma")},
	//Keys{[]byte("zma"), []byte("zmb")}, Keys{[]byte("zmb"), []byte("zmc")},
	//Keys{[]byte("zmc"), []byte("zmd")}, Keys{[]byte("zmd"), []byte("zmh")},
	//Keys{[]byte("zmh"), []byte("zmz")}, Keys{[]byte("zmz"), []byte("zzz")},}
	var ranges []*metapb.Range
	for _, key := range rrs {
		rangeId := atomic.AddUint64(&id, 1)

		peer := &metapb.Peer{Id: rangeId, NodeId: 1}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   &metapb.Key{Key: key.Start, Type: metapb.KeyType_KT_Ordinary},
			EndKey:     &metapb.Key{Key: key.End, Type: metapb.KeyType_KT_Ordinary},
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      []*metapb.Peer{peer},
			DbName:     dbName,
			TableName:  tableName,
			State:      metapb.RangeState_R_Init,
		}
		ranges = append(ranges, r)
	}
	s.RangesRecover(ranges)
	if len(s.ranges) != len(ranges) {
		t.Errorf("test failed, %d %d", len(s.ranges), len(ranges))
		time.Sleep(time.Second)
		return
	}
	var _ranges []*metapb.Range
	for _, r := range s.ranges {
		_ranges = append(_ranges, r.region)
	}
	defer func() {
		for _, r := range s.ranges {
			r.Clean()
		}
	}()
	sort.Sort(metapb.RangeByIdSlice(_ranges))
	sort.Sort(metapb.RangeByIdSlice(ranges))
	for i := 0; i < len(ranges); i++ {
		if ranges[i].GetId() != _ranges[i].GetId() {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
	}
}

func TestRangesRecover3(t *testing.T) {
	startDataServer()

	os.RemoveAll(s.conf.LogDir)
	os.RemoveAll(s.conf.DataPath)
	os.MkdirAll(s.conf.LogDir, 0777)
	os.MkdirAll(s.conf.DataPath, 0777)

	var rrs = []Keys{Keys{[]byte("a"), []byte("e")}, Keys{[]byte("e"), []byte("h")},
		Keys{[]byte("h"), []byte("l")}, Keys{[]byte("l"), []byte("n")},
		Keys{[]byte("n"), []byte("q")}, Keys{[]byte("q"), []byte("t")},
		Keys{[]byte("t"), []byte("w")}, Keys{[]byte("w"), []byte("xa")},
		Keys{[]byte("xa"), []byte("xz")}, Keys{[]byte("xz"), []byte("ya")},
		Keys{[]byte("ya"), []byte("ye")}, Keys{[]byte("ye"), []byte("yz")},
		Keys{[]byte("yz"), []byte("za")}, Keys{[]byte("za"), []byte("zh")},
		Keys{[]byte("a"), []byte("h")}, Keys{[]byte("q"), []byte("w")},
		Keys{[]byte("xa"), []byte("ya")}, Keys{[]byte("znz"), []byte("zzz")},

		Keys{[]byte("zh"), []byte("zla")}, Keys{[]byte("zla"), []byte("zlb")},
		Keys{[]byte("zlb"), []byte("zlg")}, Keys{[]byte("zlg"), []byte("zlw")},
		Keys{[]byte("zlw"), []byte("zlz")}, Keys{[]byte("zlz"), []byte("zma")},
		Keys{[]byte("zma"), []byte("zmb")}, Keys{[]byte("zmb"), []byte("zmc")},
		Keys{[]byte("zmc"), []byte("zmd")}, Keys{[]byte("zmd"), []byte("zmh")},
		Keys{[]byte("zmh"), []byte("zmz")}, Keys{[]byte("zmz"), []byte("znz")}}
	var ranges []*metapb.Range
	for _, key := range rrs {
		rangeId := atomic.AddUint64(&id, 1)

		peer := &metapb.Peer{Id: rangeId, NodeId: 1}
		r := &metapb.Range{
			Id:         rangeId,
			StartKey:   &metapb.Key{Key: key.Start, Type: metapb.KeyType_KT_Ordinary},
			EndKey:     &metapb.Key{Key: key.End, Type: metapb.KeyType_KT_Ordinary},
			RangeEpoch: &metapb.RangeEpoch{ConfVer: uint64(1), Version: uint64(1)},
			Peers:      []*metapb.Peer{peer},
			DbName:     dbName,
			TableName:  tableName,
			State:      metapb.RangeState_R_Init,
		}
		ranges = append(ranges, r)
	}
	s.RangesRecover(ranges)
	if len(s.ranges) != len(ranges) {
		t.Errorf("test failed, %d %d", len(s.ranges), len(ranges))
		time.Sleep(time.Second)
		return
	}
	var _ranges []*metapb.Range
	for _, r := range s.ranges {
		_ranges = append(_ranges, r.region)
	}
	defer func() {
		for _, r := range s.ranges {
			r.Clean()
		}
	}()
	sort.Sort(metapb.RangeByIdSlice(_ranges))
	sort.Sort(metapb.RangeByIdSlice(ranges))
	for i := 0; i < len(ranges); i++ {
		if ranges[i].GetId() != _ranges[i].GetId() {
			t.Error("test failed")
			time.Sleep(time.Second)
			return
		}
	}
}
