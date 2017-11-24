package server

import (
	"data-server/client"
	"engine/model"
	"model/pkg/metapb"
)

type LocalStore struct {
}

func NewLocalStore() Store {
	return &LocalStore{}
}

func (l *LocalStore) Put(key, value []byte) error {

	return nil
}

func (l *LocalStore) Delete(key []byte) error {

	return nil
}

func (l *LocalStore) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (l *LocalStore) Scan(startKey, limitKey []byte) Iterator {
	return NewLocalIter()
}

func (l *LocalStore) NewBatch() Batch {
	return NewLocalBatch()
}

func (l *LocalStore) Close() error {
	return nil
}

func (l *LocalStore) LocalStore() model.Store {
	return nil
}

type LocalIter struct {
}

func NewLocalIter() Iterator {
	return &LocalIter{}
}

func (i *LocalIter) Next() bool {
	return false
}

func (i *LocalIter) Key() []byte {
	return nil
}

func (i *LocalIter) Value() []byte {
	return nil
}

func (i *LocalIter) Error() error {
	return nil
}

// Release iterator使用完需要释放
func (i *LocalIter) Release() {

}

type LocalBatch struct {
}

func NewLocalBatch() Batch {
	return &LocalBatch{}
}

func (b *LocalBatch) Put(key, value []byte) {

}

func (b *LocalBatch) Delete(key []byte) {

}

func (b *LocalBatch) Commit() error {
	return nil
}

type LocalClient struct {
}

func NewLocalClient() client.SchClient {
	return &LocalClient{}
}

// Close should release all data.
func (c *LocalClient) Close() error {
	return nil
}

func (c *LocalClient) StopRange(addr string, rangeId uint64) error {
	return nil
}
func (c *LocalClient) CloseRange(addr string, rangeId uint64) error {
	return nil
}
func (c *LocalClient) CreateRange(addr string, r *metapb.Range) error {
	return nil
}
func (c *LocalClient) DeleteRange(addr string, rangeId uint64) error {
	return nil
}
func (c *LocalClient) OfflineRange(addr string, rangeId uint64) error {
	return nil
}
func (c *LocalClient) SplitRangePrepare(addr string, rangeId uint64) (bool, error) {
	return false, nil
}
func (c *LocalClient) TransferRangeLeader(addr string, rangeId uint64) error {
	return nil
}

type TestAlarm struct {
}

func NewTestAlarm() Alarm {
	return &TestAlarm{}
}

func (a *TestAlarm) Alarm(title, content string) error {
	return nil
}

func (a *TestAlarm) Stop() {

}

type TestPusher struct {
}

func NewTestPusher() Pusher {
	return &TestPusher{}
}

func (p *TestPusher) Close() {

}

func (p *TestPusher) Run() {

}

func (p *TestPusher) Push(format string, v ...interface{}) error {
	return nil
}
//
//type TestRouter struct {
//}
//
//func NewTestRouter() Router {
//	return &TestRouter{}
//}
//
//func (r *TestRouter) GetTable(dbName, tableName string) (*metapb.Table, error) {
//	return nil, nil
//}
//
//func (r *TestRouter) GetRoute(dbName, tableName string, version uint64) (routes []*metapb.Route, curVersion uint64, err error) {
//	return nil, 1, nil
//}
//
//func (r *TestRouter) Publish(channel string, message interface{}) error {
//	return nil
//}
//func (r *TestRouter) Subscribe(channel string) error {
//	return nil
//}
//func (r *TestRouter) UnSubscribe(channel string) error {
//	return nil
//}
//func (r *TestRouter) Receive() (interface{}, error) {
//	return nil, nil
//}
//func (r *TestRouter) AddSession(table string) *Session {
//	return nil
//}
//func (r *TestRouter) DeleteSession(table string, sessionId uint64) {
//	return
//}
//func (r *TestRouter) Notify(data interface{}) {
//	return
//}
//func (r *TestRouter) Close() {
//
//}
//
//func InitConfig() {
//	MasterConf = &Config{
//		SchFailoverInterval:      1,
//		SchDiskBalanceInterval:   2,
//		SchRangeGCInterval:       1,
//		SchSplitInterval:         20,
//		SchLeaderBalanceInterval: 30,
//		SchLeaderBalancePercent:  30.0,
//	}
//}
//
//func TestRangeFailOver(t *testing.T) {
//	genStep = 10000
//	InitConfig()
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm, MasterConf)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt},
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 2
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
//	_nodes[0].State = metapb.NodeState_N_Tombstone
//	time.Sleep(time.Second * 11)
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Errorf("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//	// 领取任务－－－－删除副本任务
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeDelPeer {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 重复领取一次
//	t2 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//	if t2 == nil || t2.GetType() != taskpb.TaskType_RangeDelPeer {
//		t.Errorf("invalid task %s", t2.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 修改range的副本分布，模拟删除副本成功
//	var peers []*metapb.Peer
//	for _, p := range r.Peers {
//		if t2.GetRangeDelPeer().GetPeer().GetNodeId() != p.GetNodeId() {
//			peers = append(peers, p)
//		}
//	}
//	r.Peers = peers
//	t3 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//	if t3 == nil || t3.GetType() != taskpb.TaskType_RangeAddPeer {
//		t.Errorf("invalid task %s", t3.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("删除副本子任务执行成功")
//	// 重复领取一次任务
//	t4 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//	if t4 == nil || t4.GetType() != taskpb.TaskType_RangeAddPeer {
//		t.Errorf("invalid task %s", t4.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 修改range的副本分布，模拟添加副本进行中
//	peers = make([]*metapb.Peer, 0)
//	for _, p := range r.Peers {
//		peers = append(peers, p)
//	}
//	peers = append(peers, t4.GetRangeAddPeer().GetPeer())
//	r.Peers = peers
//	var pendingPeers []*metapb.Peer
//	pendingPeers = append(pendingPeers, t4.GetRangeAddPeer().GetPeer())
//	r.PendingPeers = pendingPeers
//	t5 := coordinator.Dispatch(r, t4.GetMeta().GetTaskId())
//	if t5 != nil {
//		t.Errorf("invalid task %s", t5.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 模拟副本添加成功
//	r.PendingPeers = nil
//	t6 := coordinator.Dispatch(r, t4.GetMeta().GetTaskId())
//	if t6 != nil {
//		t.Errorf("invalid task %s", t6.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	if len(cluster.GetAllTask()) > 0 {
//		t.Errorf("test fail!!!, task still exist")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
//func TestRangeTransfer(t *testing.T) {
//	genStep = 10000
//	InitConfig()
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
//	coordinator.removeScheduler("split_scheduler")
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt},
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	disRate := []float64{90.0, 80.0, 80.0, 66.0, 1.0}
//	for i, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 2
//		n.Stats.DiskProcRate = disRate[i]
//	}
//	_nodes := nodes[:3]
//	for i, j := 0, 0; i < 100; i++ {
//		startKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		endKey := fmt.Sprintf("%d_%d", i, j)
//		j++
//		r, err := cluster.CreateRange("db0", "table0", []byte(startKey), []byte(endKey), _nodes)
//		if err != nil {
//			t.Errorf("create range failed, err[%v]", err)
//			return
//		}
//		table.AddRange(r)
//		cluster.ranges.Add(r)
//		r.State = metapb.RangeState_R_Normal
//		r.Stats.Size_ = 60000
//		for _, p := range r.Peers {
//			node, _ := cluster.FindNode(p.GetNodeId())
//			node.AddRange(r)
//		}
//	}
//	ranges := table.GetAllRanges()
//	if len(ranges) == 0 {
//		t.Errorf("test failed")
//		return
//	}
//	r := ranges[0]
//	// 600M
//	r.Stats.Size_ = 600000000
//	time.Sleep(time.Second * 61)
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Errorf("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//	// 领取任务－－－－添加副本任务
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeAddPeer {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 重复领取一次
//	t2 := coordinator.Dispatch(r, 0)
//	if t2 == nil || t2.GetType() != taskpb.TaskType_RangeAddPeer {
//		t.Errorf("invalid task %s", t2.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 修改range的副本分布，模拟添加副本进行中
//	var peers []*metapb.Peer
//	peers = make([]*metapb.Peer, 0)
//	for _, p := range r.Peers {
//		peers = append(peers, p)
//	}
//	peers = append(peers, t2.GetRangeAddPeer().GetPeer())
//	r.Peers = peers
//	var pendingPeers []*metapb.Peer
//	pendingPeers = append(pendingPeers, t2.GetRangeAddPeer().GetPeer())
//	r.PendingPeers = pendingPeers
//
//	t3 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//	if t3 != nil {
//		t.Errorf("invalid task %s", t3.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 模拟副本添加成功
//	r.PendingPeers = nil
//	t4 := coordinator.Dispatch(r, t2.GetMeta().GetTaskId())
//	if t4 == nil || t4.GetType() != taskpb.TaskType_RangeDelPeer {
//		t.Errorf("invalid task %s", t4.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//
//	t.Log("添加副本子任务执行成功")
//	// 重复领取一次任务
//	t5 := coordinator.Dispatch(r, t4.GetMeta().GetTaskId())
//	if t5 == nil || t5.GetType() != taskpb.TaskType_RangeDelPeer {
//		t.Errorf("invalid task %s", t5.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 修改range的副本分布，模拟删除副本成功
//	peers = make([]*metapb.Peer, 0)
//	for _, p := range r.Peers {
//		if t5.GetRangeDelPeer().GetPeer().GetNodeId() != p.GetNodeId() {
//			peers = append(peers, p)
//		}
//	}
//	r.Peers = peers
//	t6 := coordinator.Dispatch(r, t5.GetMeta().GetTaskId())
//	if t6 != nil {
//		t.Errorf("invalid task %s", t6.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//
//	if len(cluster.GetAllTask()) > 0 {
//		t.Errorf("test fail!!!, task still exist")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
//func genRangeSplitAck(task *Task, r *Range) (*eventpb.EventRangeSplitAck, error) {
//	peers := deepcopy.Iface(r.Peers).([]*metapb.Peer)
//	for _, p := range peers {
//		p.Id = task.GetRangeSplit().GetLeftRangeId()
//	}
//	leftRange := &metapb.Range{
//		Id:         task.GetRangeSplit().GetLeftRangeId(),
//		StartKey:   nil,
//		EndKey:     nil,
//		RangeEpoch: &metapb.RangeEpoch{},
//		Peers:      peers,
//		DbName:     r.GetDbName(),
//		TableName:  r.GetTableName(),
//		State:      metapb.RangeState_R_Init,
//	}
//	peers = deepcopy.Iface(r.Peers).([]*metapb.Peer)
//	for _, p := range peers {
//		p.Id = task.GetRangeSplit().GetRightRangeId()
//	}
//	rightRange := &metapb.Range{
//		Id:         task.GetRangeSplit().GetRightRangeId(),
//		StartKey:   nil,
//		EndKey:     nil,
//		RangeEpoch: &metapb.RangeEpoch{},
//		Peers:      peers,
//		DbName:     r.GetDbName(),
//		TableName:  r.GetTableName(),
//		State:      metapb.RangeState_R_Init,
//	}
//	rng := deepcopy.Iface(r.Range).(*metapb.Range)
//	rng.State = metapb.RangeState_R_Offline
//	return &eventpb.EventRangeSplitAck{
//		TaskId:     task.GetMeta().GetTaskId(),
//		Range:      rng,
//		LeftRange:  leftRange,
//		RightRange: rightRange,
//	}, nil
//}
//
//func genRangeDeleteAck(task *Task, r *Range) (*eventpb.EventRangeDeleteAck, error) {
//	rng := deepcopy.Iface(r.Range).(*metapb.Range)
//	rng.State = metapb.RangeState_R_Offline
//	return &eventpb.EventRangeDeleteAck{
//		TaskId: task.GetMeta().GetTaskId(),
//		Range:  rng,
//	}, nil
//}
//
//func TestRangeSplit(t *testing.T) {
//	genStep = 10000
//	InitConfig()
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	router := NewTestRouter()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt},
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 2
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
//	r.Stats.Size_ = 600000000
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//	}
//	time.Sleep(time.Second * 31)
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Errorf("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//	// 领取任务－－－－删除副本任务
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeSplit {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 重复领取一次
//	t2 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//	if t2 == nil || t2.GetType() != taskpb.TaskType_RangeSplit {
//		t.Errorf("invalid task %s", t2.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 模拟split完成
//	splitAck, err := genRangeSplitAck(t1, r)
//	if err != nil {
//		t.Errorf("gen range split ack failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	err = cluster.RangeSplitAck(splitAck, router)
//	if err != nil {
//		t.Errorf("range split ack failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	if len(table.GetAllRanges()) != 3 {
//		t.Errorf("test failed!!!")
//		time.Sleep(time.Second)
//		return
//	}
//	// range的状态为offline
//	t3 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//	if t3 != nil {
//		t.Errorf("invalid task %s", t3.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 修改部分节点上的副本为offline
//	var notWorkingRanges []uint64
//	notWorkingRanges = append(notWorkingRanges, r.ID())
//	for i, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.NotWorkingRanges = notWorkingRanges
//		if i > 1 {
//			break
//		}
//	}
//	// 重复领取一次任务
//	t4 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//	if t4 != nil {
//		t.Errorf("invalid task %s", t4.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	// 修改所有节点上的副本为offline
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.NotWorkingRanges = notWorkingRanges
//	}
//	t5 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//	if t5 != nil {
//		t.Errorf("invalid task %s", t5.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("range split success!!!")
//	if len(cluster.GetAllTask()) > 0 {
//		t.Errorf("test fail!!!, task still exist")
//		time.Sleep(time.Second)
//		return
//	}
//
//	// 回收range
//	time.Sleep(time.Second * 121)
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Errorf("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//	t6 := coordinator.Dispatch(r, t1.GetMeta().GetTaskId())
//	if t6 == nil || t6.GetType() != taskpb.TaskType_RangeDelete {
//		t.Errorf("invalid task %s", t6.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("range delete task do success!!!")
//	deleteAck, _ := genRangeDeleteAck(t6, r)
//	err = cluster.RangeDeleteAck(deleteAck, pusher)
//	if err != nil {
//		t.Errorf("range delete ack failed, err[%v]", err)
//		return
//	}
//	if len(cluster.GetAllTask()) > 0 {
//		t.Errorf("test fail!!!, task still exist")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
//func TestDeleteTable(t *testing.T) {
//	genStep = 10000
//	InitConfig()
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
//	coordinator.removeScheduler("split_scheduler")
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//	}
//	_nodes := nodes[:3]
//	r, err := cluster.CreateRange("db0", "table0", []byte("a"), []byte("c"), _nodes)
//	if err != nil {
//		t.Errorf("create range failed, err[%v]", err)
//		return
//	}
//	cluster.ranges.Add(r)
//	r.State = metapb.RangeState_R_Normal
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//	}
//	time.Sleep(time.Second * 121)
//	// 检查是否生成任务
//	if len(cluster.GetAllTask()) == 0 {
//		t.Errorf("test fail!!!, no task")
//		time.Sleep(time.Second)
//		return
//	}
//	// 领取任务－－－－添加副本任务
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeDelete {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	deleteAck, _ := genRangeDeleteAck(t1, r)
//	err = cluster.RangeDeleteAck(deleteAck, pusher)
//	if err != nil {
//		t.Errorf("range delete ack failed, err[%v]", err)
//		return
//	}
//	if len(cluster.GetAllTask()) > 0 {
//		t.Errorf("test fail!!!, task still exist")
//		time.Sleep(time.Second)
//		return
//	}
//
//	time.Sleep(time.Minute)
//	if len(cluster.GetAllRanges()) > 0 {
//		t.Errorf("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
//func TestTaskRangeMorePeerInNodeFault(t *testing.T) {
//	genStep = 10000
//	InitConfig()
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	//router := NewTestRouter()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "table_size", DataType: metapb.DataType_BigInt},
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 2
//	}
//	// range has 4 peer
//	r, err := cluster.CreateRange("db0", "table0", nil, nil, nodes)
//	if err != nil {
//		t.Errorf("create range failed, err[%v]", err)
//		return
//	}
//	t.Logf("create range[%s:%s:%d] success", r.GetDbName(), r.GetTableName(), r.ID())
//	table.AddRange(r)
//	cluster.ranges.Add(r)
//	r.State = metapb.RangeState_R_Normal
//	r.Stats.Size_ = 6
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//	}
//	// 模拟一个节点故障了
//	nodes[0].State = metapb.NodeState_N_Tombstone
//
//	// 领取任务－－－－删除副本任务
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeDelPeer {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	if t1.GetRangeDelPeer().GetPeer().GetNodeId() != nodes[0].GetId() {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("test success")
//}
//
//func TestTaskRangeMorePeerInPeerBackward(t *testing.T) {
//	genStep = 10000
//	InitConfig()
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	//router := NewTestRouter()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "table_size", DataType: metapb.DataType_BigInt},
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 2
//	}
//	r, err := cluster.CreateRange("db0", "table0", nil, nil, nodes)
//	if err != nil {
//		t.Errorf("create range failed, err[%v]", err)
//		return
//	}
//	t.Logf("create range[%s:%s:%d] success", r.GetDbName(), r.GetTableName(), r.ID())
//	table.AddRange(r)
//	cluster.ranges.Add(r)
//	r.State = metapb.RangeState_R_Normal
//	r.Stats.Size_ = 6
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//	}
//	// 模拟一个副本日志落后
//	replicas := make([]*statspb.ReplicaStatus, 4)
//	replicas[0] = &statspb.ReplicaStatus{
//		ID:    nodes[0].GetId(),
//		Match: 9000,
//	}
//	replicas[1] = &statspb.ReplicaStatus{
//		ID:    nodes[1].GetId(),
//		Match: 10000,
//	}
//	replicas[2] = &statspb.ReplicaStatus{
//		ID:    nodes[2].GetId(),
//		Match: 9050,
//	}
//	replicas[3] = &statspb.ReplicaStatus{
//		ID:    nodes[3].GetId(),
//		Match: 9001,
//	}
//	r.RaftStatus = &statspb.RaftStatus{
//		Index:    10000,
//		Replicas: replicas,
//	}
//	// 领取任务－－－－删除副本任务
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeDelPeer {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	if t1.GetRangeDelPeer().GetPeer().GetNodeId() != nodes[0].GetId() {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//}
//
//func TestTaskRangeLessPeer(t *testing.T) {
//	genStep = 10000
//	InitConfig()
//	pusher := NewTestPusher()
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	//router := NewTestRouter()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	coordinator := NewCoordinator(cluster, pusher, alarm)
//	coordinator.Run()
//
//	cluster.AddNode("127.0.0.1", uint64(6060), uint64(1234), uint64(1235), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6061), uint64(1236), uint64(1237), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6062), uint64(1238), uint64(1239), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6063), uint64(1240), uint64(1241), "data-server")
//	cluster.AddNode("127.0.0.1", uint64(6064), uint64(1242), uint64(1243), "data-server")
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1},
//		&metapb.Column{Name: "table_size", DataType: metapb.DataType_BigInt},
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		return
//	}
//	nodes := cluster.GetAllNode()
//	for _, n := range nodes {
//		n.State = metapb.NodeState_N_Login
//		n.Stats.DiskFree = MinDiskFree * 2
//	}
//	_nodes := nodes[:2]
//	r, err := cluster.CreateRange("db0", "table0", nil, nil, _nodes)
//	if err != nil {
//		t.Errorf("create range failed, err[%v]", err)
//		return
//	}
//	t.Logf("create range[%s:%s:%d] success", r.GetDbName(), r.GetTableName(), r.ID())
//	table.AddRange(r)
//	cluster.ranges.Add(r)
//	r.State = metapb.RangeState_R_Normal
//	r.Stats.Size_ = 6
//	for _, p := range r.Peers {
//		node, _ := cluster.FindNode(p.GetNodeId())
//		node.AddRange(r)
//	}
//
//	// 领取任务－－－－删除副本任务
//	nodes[2].State = metapb.NodeState_N_Tombstone
//	t1 := coordinator.Dispatch(r, 0)
//	if t1 == nil || t1.GetType() != taskpb.TaskType_RangeAddPeer {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//	if t1.GetRangeAddPeer().GetPeer().GetNodeId() != nodes[3].GetId() {
//		t.Errorf("invalid task %s", t1.Describe())
//		time.Sleep(time.Second)
//		return
//	}
//
//}
