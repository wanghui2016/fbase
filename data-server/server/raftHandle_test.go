package server

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/statspb"
	"model/pkg/eventpb"
)

type LocalClient struct {
	lostCount   int
	changeCount int
}

func (lc *LocalClient) GetRoutes(dbName, tableName string, version uint64) ([]*metapb.Route, uint64, error) {
	return nil, 0, nil
}

func (lc *LocalClient) GetNode(nodeId uint64) (*metapb.Node, error) {
	return nil, nil
}
func (lc *LocalClient) GetDB(dbName string) (*metapb.DataBase, error) {
	return nil, nil
}
func (lc *LocalClient) GetTable(dbName, tableName string) (*metapb.Table, error) {
	return nil, nil
}
func (lc *LocalClient) GetColumns(dbName, tableName string) ([]*metapb.Column, error) {
	return nil, nil
}
func (lc *LocalClient) GetColumnByName(dbName, tableName, columnName string) (*metapb.Column, error) {
	return nil, nil
}
func (lc *LocalClient) GetColumnByID(dbName, tableName string, columnId uint64) (*metapb.Column, error) {
	return nil, nil
}

func (lc *LocalClient) AddColumns(dbName, tableName string, columns []*metapb.Column) ([]*metapb.Column, error) {
	return nil, nil
}

// ReportEvent report event to master server
func (lc *LocalClient) ReportEvent(req *mspb.ReportEventRequest) (*mspb.ReportEventResponse, error) {
	fmt.Println(req.GetEvent().GetEventEventStatistics())
	if req.GetEvent().GetEventEventStatistics().GetStatisticsType() == eventpb.StatisticsType_LeaderLose {
		fmt.Println("lost leader!!!!!")
		lc.lostCount++
	} else {
		fmt.Println("change leader!!!!")
		lc.changeCount++
	}
	return nil, nil
}
func (lc *LocalClient) NodeHeartbeat(*mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error) {
	return nil, nil
}

func (lc *LocalClient) RangeHeartbeat(*mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error) {
	return nil, nil
}

// Close closes the client.
func (lc *LocalClient) Close() {

}

func TestLeaderChangeStatics(t *testing.T) {
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
	node := &metapb.Node{
		Id:      uint64(conf.NodeID),
		Address: fmt.Sprintf(":%d", conf.AppManagePort),
		State:   metapb.NodeState_N_Initial,
		RaftAddrs: &metapb.RaftAddrs{
			HeartbeatAddr: conf.RaftHeartbeatAddr,
			ReplicateAddr: conf.RaftReplicaAddr,
		},
		Version: "",
	}
	cli := &LocalClient{}
	service := new(Server)
	service.conf = conf
	service.node = node
	r := &metapb.Range{
		Id:         1,
		StartKey:   nil,
		EndKey:     nil,
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: service.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng := &Range{
		region: r,
		stats:  &statspb.RangeStats{},
		server: service,
	}
	rng.quitCtx, rng.quitFunc = context.WithCancel(context.Background())
	rng.HandleLeaderChange(0)
	time.Sleep(time.Second)
	rng.HandleLeaderChange(1)
	time.Sleep(time.Second)
	if cli.lostCount != 1 {
		t.Error("test failed")
		return
	}
	rng.HandleLeaderChange(2)
	time.Sleep(time.Second)
	if cli.changeCount != 1 {
		t.Error("test failed")
		return
	}
	rng.HandleLeaderChange(1)
	time.Sleep(time.Second)
	if cli.changeCount != 1 {
		t.Error("test failed")
		return
	}
}
