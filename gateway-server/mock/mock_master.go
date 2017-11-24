package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"util"
	"util/log"
	"util/server"
)

var mockNode = &metapb.Node{
	Id:      1,
	Address: "127.0.0.1:7788",
	State:   metapb.NodeState_N_Login,
	Version: "mock 1.1",
}

// MasterClient : 模拟只有一个表
type MasterClient struct {
	ts     int64
	t      *metapb.Table
	mu     sync.RWMutex
	ranges []*util.Range
}

// NewMasterCli: 创建只包含一个表的master
func NewMasterCli(t *metapb.Table, ranges []*util.Range, listenPort uint16) *MasterClient {
	c := &MasterClient{
		t:      t,
		ranges: ranges,
	}
	svr := server.NewServer()
	config := &server.ServerConfig{
		Port:      fmt.Sprintf("%d", listenPort),
		Version:   "v1",
		ConnLimit: 1000,
	}
	svr.Init("masterserver", config, nil)
	svr.Handle("/route/get", server.ServiceHttpHandler{Handler: c.handleRouteGet})
	go svr.Run()
	time.Sleep(15 * time.Millisecond)
	return c
}

type httpReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`

	TableEpoch *metapb.TableEpoch `json:"table_epoch, omitempty"`
}

func sendReply(w http.ResponseWriter, httpreply *httpReply) {
	reply, err := json.Marshal(httpreply)
	if err != nil {
		log.Error("http reply marshal error: %s", err)
		w.WriteHeader(500)
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("http reply[%s] len[%d] write error: %v", string(reply), len(reply), err)
	}
}

func (c *MasterClient) handleRouteGet(w http.ResponseWriter, r *http.Request) {
	log.Debug("handleRouteGet!!!!!")
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbId, err := strconv.ParseUint(r.FormValue("dbId"), 10, 64)
	if err != nil {
		reply.Code = 1
		reply.Message = "invalid param"
		return
	}
	tableId, err := strconv.ParseUint(r.FormValue("tableId"), 10, 64)
	if err != nil {
		reply.Code = 1
		reply.Message = "invalid param"
		return
	}
	version, err := strconv.ParseUint(r.FormValue("version"), 10, 64)
	if err != nil {
		reply.Code = 1
		reply.Message = "invalid param"
		return
	}

	topo, err := c.GetTopologyEpoch(dbId, tableId, &metapb.TableEpoch{})
	if err != nil {
		log.Error("http get route failed, err[%v]", err)
		reply.Code = 1
		reply.Message = err.Error()
		return
	}
	if topo.GetEpoch().GetVersion() > version && len(topo.GetRoutes()) > 0 {
		reply.Message = fmt.Sprintf("%d", topo.GetEpoch().GetVersion())
		reply.Data = topo.GetRoutes()
		log.Info("[%s:%s:%d]get routes oldVersion[%d] success!!!", dbId, tableId, topo.GetEpoch().GetVersion(), version)
		return
	}

	select {
	case <-w.(http.CloseNotifier).CloseNotify():
		log.Warn("session closed!!!")
	case <-time.After(time.Second * 60):
		log.Debug("session timeout!!!!")
	}
	return
}

func (c *MasterClient) GetTS() (int64, int64, error) {
	return 0, atomic.AddInt64(&c.ts, 1), nil
}

func (c *MasterClient) GetTopologyEpoch(dbId, tableId uint64, epoch *metapb.TableEpoch) (*metapb.TopologyEpoch, error) {
	if len(c.ranges) == 0 {
		route := &metapb.Route{
			Id:  1,
			StartKey:   &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity},
			EndKey:     &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity},
			RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
			Leader: &metapb.Leader{RangeId: mockNode.Id, NodeId: mockNode.GetId(), NodeAddr: mockNode.GetAddress()},
		}
        return &metapb.TopologyEpoch{
	        DbId: dbId,
	        TableId: tableId,
	        Routes: []*metapb.Route{route},
	        Epoch: &metapb.TableEpoch{},
        }, nil
	} else {
		var routes []*metapb.Route
		for i, r := range c.ranges {
			rangeId := uint64(i + 1)
			var start, end *metapb.Key
			if r.Start == nil {
				start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
			} else {
				start = &metapb.Key{Key: r.Start, Type: metapb.KeyType_KT_Ordinary}
			}
			if r.Limit == nil {
				end = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
			} else {
				end = &metapb.Key{Key: r.Limit, Type: metapb.KeyType_KT_Ordinary}
			}
			route := &metapb.Route{
				Id:         rangeId,
				StartKey:   start,
				EndKey:     end,
				RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
				Leader: &metapb.Leader{RangeId: rangeId, NodeId: mockNode.GetId(), NodeAddr: mockNode.GetAddress()},
			}
			routes = append(routes, route)
		}
		log.Debug("+++++routes[%v]", routes)
		return &metapb.TopologyEpoch{
			DbId: dbId,
			TableId: tableId,
			Routes: routes,
			Epoch: &metapb.TableEpoch{},
		}, nil
	}

}

func (c *MasterClient) GetNode(nodeId uint64) (*metapb.Node, error) {
	return mockNode, nil
}

func (c *MasterClient) GetDB(dbName string) (*metapb.DataBase, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if dbName != c.t.DbName {
		return nil, nil
	}
	return &metapb.DataBase{
		Name:    dbName,
		Id:      1,
		Version: 1,
	}, nil
}

func (c *MasterClient) GetTable(dbId uint64, tableName string) (*metapb.Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if dbId != c.t.DbId || tableName != c.t.Name {
		return nil, nil
	}
	return c.t, nil
}

func (c *MasterClient) GetTableById(dbId uint64, tableId uint64) (*metapb.Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if dbId != c.t.DbId || tableId != c.t.Id {
		return nil, nil
	}
	return c.t, nil
}

func (c *MasterClient) GetColumns(dbId, tableId uint64) ([]*metapb.Column, error) {
	t, _ := c.GetTableById(dbId, tableId)
	if t == nil {
		return nil, nil
	}
	return t.Columns, nil
}

func (c *MasterClient) GetColumnByName(dbId, tableId uint64, columnName string) (*metapb.Column, error) {
	cols, _ := c.GetColumns(dbId, tableId)
	for _, c := range cols {
		if c.Name == columnName {
			return c, nil
		}
	}
	return nil, nil
}

func (c *MasterClient) GetColumnByID(dbId, tableId uint64, columnId uint64) (*metapb.Column, error) {
	cols, _ := c.GetColumns(dbId, tableId)
	for _, c := range cols {
		if c.Id == columnId {
			return c, nil
		}
	}
	return nil, nil
}

func (c *MasterClient) AddColumns(dbId, tableId uint64, columns []*metapb.Column) ([]*metapb.Column, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if dbId != c.t.DbId || tableId != c.t.Id {
		return nil, fmt.Errorf("table %d.%d doesn't exist", dbId, tableId)
	}

	c.t.Columns = append(c.t.Columns, columns...)
	return c.t.Columns, nil
}

func (c *MasterClient) GatewayHeartbeat(*mspb.GatewayHeartbeatRequest) (*mspb.GatewayHeartbeatResponse, error) {
	return new(mspb.GatewayHeartbeatResponse), nil
}

func (c *MasterClient) NodeHeartbeat(*mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error) {
	return new(mspb.NodeHeartbeatResponse), nil
}

func (c *MasterClient) RangeHeartbeat(*mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error) {
	return new(mspb.RangeHeartbeatResponse), nil
}

func (c *MasterClient) ReportEvent(req *mspb.ReportEventRequest) (*mspb.ReportEventResponse, error) {
	return new(mspb.ReportEventResponse), nil
}

func (c *MasterClient) TruncateTable(dbId, tableId uint64) error {
	return nil
}

func (c *MasterClient) Close() {}
