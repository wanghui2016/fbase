package server

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"
	"sort"
	"errors"

	"golang.org/x/net/context"
	"model/pkg/metapb"
	"util/deepcopy"
	"util/log"
	"model/pkg/taskpb"
)

var (
	http_error                       string = "bad request"
	http_error_parameter_not_enough  string = "parameter is not enough"
	http_error_invalid_parameter     string = "invalid param"
	http_error_database_find         string = "database is not existed"
	http_error_table_find            string = "table is not existed"
	http_error_table_deleted         string = "table is deleted"
	http_error_range_find            string = "range is not existed"
	http_error_node_find             string = "node is not existed"
	http_error_range_split           string = "range is spliting"
	http_error_node_pick             string = "node pick error"
	http_error_range_create          string = "range create error"
	http_error_cluster_has_no_leader string = "raft cluster has no leader"
	http_error_master_is_not_leader  string = "this master server is not leader node"
	http_error_wrong_sign            string = "sign is wrong"
	http_error_database_exist        string = "database is existed"
	http_error_wrong_cluster         string = "cluster id is wrong"
	http_error_range_busy            string = "range is busy"
)

const (
	HTTP_OK = iota
	HTTP_ERROR
	HTTP_ERROR_PARAMETER_NOT_ENOUGH
	HTTP_ERROR_INVALID_PARAM
	HTTP_ERROR_DATABASE_FIND
	HTTP_ERROR_TABLE_FIND
	HTTP_ERROR_NODE_PICK
	HTTP_ERROR_RANGE_CREATE
	HTTP_ERROR_CLUSTER_HAS_NO_LEADER
	HTTP_ERROR_MASTER_IS_NOT_LEADER
	HTTP_ERROR_WRONG_SIGN
	HTTP_ERROR_RANGE_FIND
	HTTP_ERROR_RANGE_SPLIT
	HTTP_ERROR_DATABASE_EXISTED
	HTTP_ERROR_TASK_FIND
	HTTP_ERROR_CLUSTERID
	HTTP_ERROR_NODE_FIND
	HTTP_ERROR_RANGE_BUSY
)

const (
	HTTP_DB_NAME = "dbName"
	HTTP_DB_ID = "dbId"
	HTTP_TABLE_NAME = "tableName"
	HTTP_TABLE_ID = "tableId"
	HTTP_CLUSTER_ID = "clusterId"
	HTTP_RANGE_ID = "rangeId"
	HTTP_NODE_ID = "nodeId"
	HTTP_PEER_ID = "peerId"
	HTTP_NAME = "name"
	HTTP_PROPERTIES = "properties"
	HTTP_PKDUPCHECK = "pkDupCheck"
	HTTP_RANGEKEYS = "rangeKeys"
	HTTP_POLICY= "policy"
	HTTP_D = "d"
	HTTP_S = "s"
	HTTP_TOKEN = "token"
	HTTP_SQL = "sql"
	HTTP_SERVER_PORT = "serverPort"
	HTTP_RAFT_HEARTBEAT_PORT = "raftHeartbeatPort"
	HTTP_RAFT_REPLICA_PORT = "raftReplicaPort"
	HTTP_TASK_ID = "taskId"
	MACHINES = "machines"
)

const (
	ROUTE_SUBSCRIBE = "route_subscribe"
)

type Proxy struct {
	targetHost string
}

func (p *Proxy) proxy(w http.ResponseWriter, r *http.Request) {
	director := func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = p.targetHost
	}
	proxy := &httputil.ReverseProxy{Director: director}
	proxy.ServeHTTP(w, r)
}

type httpReply struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
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

func (service *Server) verifier(w http.ResponseWriter, r *http.Request) bool {
	id, err := strconv.ParseUint(r.FormValue(HTTP_CLUSTER_ID), 10, 64)
	if err != nil {
		log.Warn("invalid cluster id err: %v", err)
		return false
	}
	if id != uint64(service.conf.ClusterID) {
		log.Warn("unexpected clusterId[%d]", id)
		return false
	}

	d := r.FormValue(HTTP_D)
	sign := r.FormValue(HTTP_S)

	if len(d) == 0 || len(sign) == 0 {
		log.Warn("d/s len = 0")
		return false
	}
	h := md5.New()
	h.Write([]byte(service.conf.Token))
	h.Write([]byte(d))
	h.Sum(nil)
	if sign != fmt.Sprintf("%x", h.Sum(nil)) {
		log.Warn("wrong sign")
		return false
	}
	return true
}

func (service *Server) proxy(w http.ResponseWriter, r *http.Request) bool {
	if is := service.IsLeader(); !is {
		if leader := service.GetLeader(); leader == nil {
			sendReply(w, &httpReply{
				Code: HTTP_ERROR_MASTER_IS_NOT_LEADER,
				Message: http_error_cluster_has_no_leader,
			})
		} else {
			proxy := &Proxy{targetHost: leader.ManageAddress}
			proxy.proxy(w, r)
		}
		return true
	}
	return false
}

func (service *Server) donotProxy(w http.ResponseWriter, r *http.Request) bool {
	if !service.IsLeader() {
		sendReply(w, &httpReply{
			Code: HTTP_ERROR_MASTER_IS_NOT_LEADER,
			Message: ErrNotLeader.Error(),
		})
		return true
	}
	return false
}

func (service *Server) handleDatabaseCreate(w http.ResponseWriter, r *http.Request) {
	reply := new(httpReply)
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_NAME)
	dbProperties := r.FormValue(HTTP_PROPERTIES)
	if len(dbName) == 0 {
		log.Error("http create database: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	if _, err := service.cluster.CreateDatabase(dbName, dbProperties); err != nil {
		log.Error("http create database: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("create database[%s] success", dbName)
}

func (service *Server) handleTableCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	properties := r.FormValue(HTTP_PROPERTIES)
	pkDupCheck := r.FormValue(HTTP_PKDUPCHECK)
	rangeKeys := r.FormValue(HTTP_RANGEKEYS)
	policyStr := r.FormValue(HTTP_POLICY)
	if len(dbName) == 0 || len(tName) == 0 || len(properties) == 0 {
		log.Error("http create table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	if _, ok := service.cluster.FindDatabase(dbName); !ok {
		log.Error("http create table: %s", http_error_database_find)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	// 默认不检查
	if len(pkDupCheck) == 0 {
		pkDupCheck = "false"
	}
	columns, regxs, err := ParseProperties(properties)
	if err != nil {
		log.Error("parse cols[%s] failed, err[%v]", properties, err)
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = err.Error()
		return
	}
	var policy *metapb.TableRwPolicy
	log.Debug("policy str: %v, len: %v", policyStr, len(policyStr))
	if len(policyStr) != 0 {
		policy = new(metapb.TableRwPolicy)
		err = json.Unmarshal([]byte(policyStr), policy)
		if err != nil {
			log.Error("parse cols[%s] failed, err[%v]", properties, err)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
	}
	if policy == nil || policy.GetPolicy() == metapb.RwPolicy_RW_Invalid {
		log.Warn("no rw policy, we need default policy!!!!")
		policy = &metapb.TableRwPolicy {
			Policy: metapb.RwPolicy_RW_OnlyMaster,
			RwOnlyMaster: &metapb.TableOnlyMaster{},
		}
	}
	log.Debug("policy %v", policy)

	var rangeKeys_ []string
	var ranges []*metapb.Range
	if len(rangeKeys) > 0 {
		if rangeKeys_, err = rangeKeysSplit(rangeKeys, ","); err != nil {
			log.Error("invalid rangekeys %s, err[%v]", rangeKeys, err)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
		keys, err := encodeSplitKeys(rangeKeys_, columns)
		if err != nil {
			log.Error("encode table preSplit keys failed(%v), keys: %v", err, rangeKeys_)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
		sort.Sort(ByLetter(keys))

		log.Info("start create table %s.%s, preSplit keys: %v", dbName, tName, keys)

		ranges, err = service.cluster.prepareCreateRanges(dbName, tName, keys)
		if err != nil {
			log.Error("prepare create ranges failed, err[%v]", err)
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
			return
		}
	} else {
		ranges, err = service.cluster.prepareCreateRanges(dbName, tName, nil)
		if err != nil {
			log.Error("prepare create ranges failed, err[%v]", err)
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
			return
		}
	}

	table, err := service.cluster.CreateTable(dbName, tName, columns, regxs, pkDupCheck != "false", policy, ranges)
	if err != nil {
		log.Error("http create table: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("create table[%s:%s] policy[%v] success", dbName, tName, table.RwPolicy)
	return
}

func (service *Server) handleSqlTableCreate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	command := r.FormValue(HTTP_SQL)
	rangeKeys := r.FormValue(HTTP_RANGEKEYS)
	if len(dbName) == 0 || len(tName) == 0 || len(command) == 0 {
		log.Error("sql http create table: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if _, ok := service.cluster.FindDatabase(dbName); !ok {
		log.Error("sql http create table: %s", http_error_database_find)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}

	var table *metapb.Table
	if table = parseCreateTableSql(command); table == nil {
		reply.Code = HTTP_ERROR
		reply.Message = "table is nil, check log for detail"
		return
	}
	if strings.ToLower(tName) != table.GetName() {
		reply.Code = HTTP_ERROR
		reply.Message = "table name error"
		return
	}
	table.Name = tName

	policyStr := r.FormValue("policy")
	var policy *metapb.TableRwPolicy
	if len(policyStr) != 0 {
		policy = new(metapb.TableRwPolicy)
		err := json.Unmarshal([]byte(policyStr), policy)
		if err != nil {
			log.Error("parse policy failed, err[%v]", err)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
	} else {
		log.Warn("no rw policy, we need default policy!!!!")
		policy = &metapb.TableRwPolicy {
			Policy: metapb.RwPolicy_RW_OnlyMaster,
			RwOnlyMaster: &metapb.TableOnlyMaster{},
		}
	}
	log.Debug("policy %v", policy)

	var findPks bool
	for _, col := range table.GetColumns() {
		if col.Id == 0 {
			reply.Code = HTTP_ERROR
			reply.Message = "column Id is invalid"
			log.Error("sql column Id is invalid!!!!")
			return
		}
		if col.PrimaryKey == 1 {
			findPks = true
		}
	}
	if !findPks {
		reply.Code = HTTP_ERROR
		reply.Message = "table must has PK"
		log.Error("sql column Id is invalid!!!!")
		return
	}

	var rangeKeys_ []string
	var ranges []*metapb.Range
	var err error
	if len(rangeKeys) > 0 {
		if rangeKeys_, err = rangeKeysSplit(rangeKeys, ","); err != nil {
			log.Error("sql invalid rangekeys %s, err[%v]", rangeKeys, err)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
		keys, err := encodeSplitKeys(rangeKeys_, table.GetColumns())
		if err != nil {
			log.Error("sql encode table preSplit keys failed(%v), keys: %v", err, rangeKeys_)
			reply.Code = HTTP_ERROR_INVALID_PARAM
			reply.Message = err.Error()
			return
		}
		sort.Sort(ByLetter(keys))

		log.Info("sql start create table %s.%s, preSplit keys: %v", dbName, table.GetName(), keys)

		ranges, err = service.cluster.prepareCreateRanges(dbName, table.GetName(), keys)
		if err != nil {
			log.Error("sql prepare create ranges failed, err[%v]", err)
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
			return
		}
	} else {
		ranges, err = service.cluster.prepareCreateRanges(dbName, table.GetName(), nil)
		if err != nil {
			log.Error("sql prepare create ranges failed, err[%v]", err)
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
			return
		}
	}
	_, err = service.cluster.CreateTable(dbName, table.GetName(), table.GetColumns(), nil, false, policy, ranges)
	if err != nil {
		log.Error("http sql table create : %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("sql table create [%s:%s] success", dbName, table.GetName())
}

func (service *Server) handleNodeActivate(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	serverPort, err := strconv.ParseUint(r.FormValue(HTTP_SERVER_PORT), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = err.Error()
		return
	}
	raftHeartbeatPort, err := strconv.ParseUint(r.FormValue(HTTP_RAFT_HEARTBEAT_PORT), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = err.Error()
		return
	}
	raftReplicaPort, err := strconv.ParseUint(r.FormValue(HTTP_RAFT_REPLICA_PORT), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = err.Error()
		return
	}
	serverIp, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	if serverPort <= 1024 || raftHeartbeatPort <= 1024 || raftReplicaPort <= 1024 {
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	node, err := service.cluster.AddInstance(serverIp, serverPort, raftHeartbeatPort, raftReplicaPort, "data-server")
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Error("add node[%s:%d] failed, err[%v]", serverIp, serverPort, err)
		return
	}
	if node.GetState() == metapb.NodeState_N_Logout {
		reply.Code = HTTP_ERROR
		reply.Message = errors.New("node state is logout").Error()
		return
	}
	reply.Data = fmt.Sprint(node.ID())
	log.Info("node[%s:%d %d] active success", serverIp, serverPort, node.ID())
}

// not use
func (service *Server) handleRangeSplit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil || rangeId == 0 {
		log.Error("http range split: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	rng, found := service.cluster.FindRange(rangeId)
	if !found {
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	t := SplitSchedule(rng, service.cluster)
	if t == nil {
		reply.Code = HTTP_ERROR
		reply.Message = "can not gen split task"
		return
	}

	if err := service.cluster.AddTask(t); err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "can not add task to cluster"
		return
	}

	log.Info("range split success!!!!")
	reply.Code = HTTP_OK
}

func (service *Server) handleDBGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	reply.Data = service.cluster.GetAllDatabase()
}

func (service *Server) handleTableGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	if len(dbName) == 0 {
		log.Error("http table getall: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbName); !ok {
		log.Error("http table getall: db [%s] not found", dbName)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	reply.Data = db.GetAllCurTable()
}

func (service *Server) handleNodeGetAll(w http.ResponseWriter, r *http.Request) {
	log.Debug("node getall")
	reply := &httpReply{}
	defer sendReply(w, reply)

	reply.Data = service.cluster.GetAllInstances()
}

func (service *Server) handleNodeStats(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	id, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		log.Error("http node stats: nodeid is not uint64")
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}

	node, find := service.cluster.FindInstance(id)
	if !find {
		log.Error("http node stats: no such nodeid")
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}

	reply.Data = node.GetStats()
}

func (service *Server) handleRangeGetAll(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	if len(dbname) == 0 || len(tablename) == 0 {
		log.Error("http range getall: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http range getall: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http range getall: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	type Peer struct {
		Id   uint64       `json:"id,omitempty"`
		Node *metapb.Node `json:"node,omitempty"`
	}
	type Range struct {
		Id uint64 `json:"id,omitempty"`
		// Range key range [start_key, end_key).
		StartKey   []byte             `json:"start_key,omitempty"`
		EndKey     []byte             `json:"end_key,omitempty"`
		RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
		Peers      []*Peer            `json:"peers,omitempty"`
		// Range state
		State      int32  `json:"state,omitempty"`
		DbName     string `json:"db_name,omitempty"`
		TableName  string `json:"table_name,omitempty"`
		CreateTime int64  `json:"create_time,omitempty"`
	}

	type Route struct {
		Range  *Range `json:"range,omitempty"`
		Leader *Peer  `json:"leader,omitempty"`
	}
	var _routes []*Route
	//reply.Data = table.GetAllRanges()
	topo, err := table.GetTopologyEpoch(service.cluster, &metapb.TableEpoch{})
	if err != nil {
		log.Warn("table[%s:%s] get topology failed, err[%v]", dbname, tablename, err)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	for _, r := range topo.GetRoutes() {
		var peers []*Peer
		var leader *Peer
		rng, find := service.cluster.FindRange(r.GetId())
		if !find {
			log.Error("invalid range %d", r.GetId())
			continue
		}
		for _, p := range rng.GetPeers() {
			node, find := service.cluster.FindInstance(p.GetNodeId())
			if !find {
				log.Error("node[%d] not found", p.GetNodeId())
			}
			peer := &Peer{
				Id:   p.GetId(),
				Node: node.Node,
			}
			peers = append(peers, peer)
			if r.GetLeader().GetNodeId() == node.ID() {
				leader = peer
			}
		}
		_range := &Range{
			Id: rng.GetId(),
			// Range key range [start_key, end_key).
			StartKey:   rng.GetStartKey().GetKey(),
			EndKey:     rng.GetEndKey().GetKey(),
			RangeEpoch: &metapb.RangeEpoch{ConfVer: rng.GetRangeEpoch().GetConfVer(), Version: rng.GetRangeEpoch().GetVersion()},
			Peers:      peers,
			// Range state
			State:      int32(rng.GetState()),
			DbName:     rng.GetDbName(),
			TableName:  rng.GetTableName(),
			CreateTime: rng.GetCreateTime(),
		}
		route := &Route{
			Range:  _range,
			Leader: leader,
		}
		_routes = append(_routes, route)
	}
	reply.Data = _routes
	return
}

func (service *Server) handleRangeStats(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeid, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	nodeid, err2 := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err1 != nil || err2 != nil {
		log.Error("http range stats: nodeid or rangeid is not uint64")
		reply.Code = HTTP_ERROR
		reply.Message = "wrong args type "
		return
	}

	node, find := service.cluster.FindInstance(nodeid)
	if !find {
		log.Error("http range stats: no such nodeid")
		reply.Code = HTTP_ERROR
		reply.Message = "no sucn nodeid"
		return
	}
	range_, find := node.GetRange(rangeid)
	if !find {
		log.Error("http range stats: no such rangeid")
		reply.Code = HTTP_ERROR
		reply.Message = "no such rangeid"
		return
	}
	reply.Data = range_.GetStats()
}

func (service *Server) handleMasterGetLeader(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	point := service.GetLeader()
	if point == nil {
		log.Error("http get master leader: no leader")
		reply.Code = HTTP_ERROR_CLUSTER_HAS_NO_LEADER
		reply.Message = http_error_cluster_has_no_leader
		return
	}
	reply.Data = point.ManageAddress
}

func (service *Server) handleRangeGetLeader(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)

	if len(dbname) == 0 || len(tablename) == 0 || err != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	range_, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	n := range_.Leader
	if n == nil {
		log.Error("http get range leader: no leader")
		reply.Code = HTTP_ERROR_CLUSTER_HAS_NO_LEADER
		reply.Message = http_error_cluster_has_no_leader
		return
	}
	reply.Data = n.GetNodeAddr()
}

func (service *Server) handleRangeGetPeerInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err1 := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	peerid, err2 := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)

	if len(dbname) == 0 || len(tablename) == 0 || err1 != nil || err2 != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	rang, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	type Peer struct {
		Id   uint64       `json:"id,omitempty"`
		Node *metapb.Node `json:"node,omitempty"`
	}
	type PeerInfo struct {
		Id uint64 `json:"id,omitempty"`
		// Range key range [start_key, end_key).
		StartKey   []byte             `json:"start_key,omitempty"`
		EndKey     []byte             `json:"end_key,omitempty"`
		RangeEpoch *metapb.RangeEpoch `json:"range_epoch,omitempty"`
		Peer       *Peer              `json:"peers,omitempty"`
		// Range state
		State      string `json:"state,omitempty"`
		DbName     string `json:"db_name,omitempty"`
		TableName  string `json:"table_name,omitempty"`
		CreateTime int64  `json:"create_time,omitempty"`
		Size       uint64 `json:"size"`
	}
	node, find := service.cluster.FindInstance(peerid)
	if !find {
		log.Error("node[%d] not found", peerid)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}
	peer := &Peer{
		Id:   rang.GetId(),
		Node: deepcopy.Iface(node.Node).(*metapb.Node),
	}
	info := &PeerInfo{
		Id:         rang.GetId(),
		StartKey:   rang.GetStartKey().GetKey(),
		EndKey:     rang.GetEndKey().GetKey(),
		RangeEpoch: rang.GetRangeEpoch(),
		Peer:       peer,
		State:      rang.GetState().String(),
		DbName:     rang.GetDbName(),
		TableName:  rang.GetTableName(),
		CreateTime: rang.GetCreateTime(),
		Size:       rang.GetStats().GetSize_(),
	}
	reply.Data = info
	log.Info("get range[%s:%s:%d] peer[%s] info success", rang.GetDbName(), rang.GetTableName(), rang.GetId(), peer.Node.GetAddress())
}

func (service *Server) handleGetAllTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeIdStr := r.FormValue(HTTP_RANGE_ID)

	var tasks []*Task
	if len(dbname) == 0 {
		tasks = service.cluster.GetAllTask()
	} else if len(tablename) == 0 {
		tasks = service.cluster.GetAllDBTask(dbname)
	} else if len(rangeIdStr) == 0 {
		tasks = service.cluster.GetAllTableTask(dbname, tablename)
	} else {
		rangeid, err := strconv.ParseUint(rangeIdStr, 10, 64)
		if err != nil {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		rng, find := service.cluster.FindRange(rangeid)
		if !find {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		task := rng.GetTask()
		if task != nil {
			tasks = append(tasks, task)
		}
	}

	type Tsk struct {
		Id         uint64 `json:"id"`
		CreateTime int64  `json:"createtime"`
		Describe   string `json:"describe"`
		State      string `json:"state"`
		Type       string `json:"type"`
	}
	tsks := make([]*Tsk, 0)
	for _, tsk := range tasks {
		if tsk.GetType() == taskpb.TaskType_EmptyRangeTask {
			continue
		}
		tsks = append(tsks, &Tsk{
			Id:         tsk.Meta.GetTaskId(),
			CreateTime: tsk.CreateTime().Unix(),
			Describe:   tsk.Describe(),
			State:      tsk.StateString(),
			Type:       tsk.GetType().String(),
		})
	}

	reply.Data = tsks
	log.Info("get all task success")
}

func (service *Server) handleGetAllFailTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeIdStr := r.FormValue(HTTP_RANGE_ID)

	var tasks []*Task
	if len(dbname) == 0 {
		tasks = service.cluster.GetAllFailTask()
	} else if len(tablename) == 0 {
		tasks = service.cluster.GetAllDBFailTask(dbname)
	} else if len(rangeIdStr) == 0 {
		tasks = service.cluster.GetAllTableFailTask(dbname, tablename)
	} else {
		rangeid, err := strconv.ParseUint(rangeIdStr, 10, 64)
		if err != nil {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		rng, find := service.cluster.FindRange(rangeid)
		if !find {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		task := rng.GetTask()
		if task != nil && task.State() == taskpb.TaskState_TaskFail {
			tasks = append(tasks, task)
		}
	}

	reply.Data = tasks
	log.Info("get all fail task success")
}

func (service *Server) handleGetTimeoutTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeIdStr := r.FormValue(HTTP_RANGE_ID)

	var tasks []*Task
	if len(dbname) == 0 {
		tasks = service.cluster.GetAllTimeoutTask()
	} else if len(tablename) == 0 {
		tasks = service.cluster.GetAllDBTimeoutTask(dbname)
	} else if len(rangeIdStr) == 0 {
		tasks = service.cluster.GetAllTableTimeoutTask(dbname, tablename)
	} else {
		rangeid, err := strconv.ParseUint(rangeIdStr, 10, 64)
		if err != nil {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		rng, find := service.cluster.FindRange(rangeid)
		if !find {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		task := rng.GetTask()
		if task != nil && task.State() == taskpb.TaskState_TaskTimeout {
			tasks = append(tasks, task)
		}
	}

	reply.Data = tasks
	log.Info("get all timeout task success")
}

func (service *Server) handleGetAllEmptyTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeIdStr := r.FormValue(HTTP_RANGE_ID)

	var tasks []*Task
	if len(dbname) == 0 {
		tasks = service.cluster.GetAllTask()
	} else if len(tablename) == 0 {
		tasks = service.cluster.GetAllDBTask(dbname)
	} else if len(rangeIdStr) == 0 {
		tasks = service.cluster.GetAllTableTask(dbname, tablename)
	} else {
		rangeid, err := strconv.ParseUint(rangeIdStr, 10, 64)
		if err != nil {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		rng, find := service.cluster.FindRange(rangeid)
		if !find {
			log.Error("invalid param rangeID[%s]", rangeIdStr)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		task := rng.GetTask()
		if task != nil {
			tasks = append(tasks, task)
		}
	}

	type Tsk struct {
		Id         uint64 `json:"id"`
		CreateTime int64  `json:"createtime"`
		Describe   string `json:"describe"`
		State      string `json:"state"`
		Type       string `json:"type"`
	}
	tsks := make([]*Tsk, 0)
	for _, tsk := range tasks {
		if tsk.GetType() != taskpb.TaskType_EmptyRangeTask {
			continue
		}
		tsks = append(tsks, &Tsk{
			Id:         tsk.Meta.GetTaskId(),
			CreateTime: tsk.CreateTime().Unix(),
			Describe:   tsk.Describe(),
			State:      tsk.StateString(),
			Type:       tsk.GetType().String(),
		})
	}

	reply.Data = tsks
	log.Info("get all task success")
}

func (service *Server) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	taskId, err := strconv.ParseUint(r.FormValue(HTTP_TASK_ID), 10, 64)
	if err != nil {
		log.Error("invalid task: %v", taskId)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	err = service.cluster.RestartTask(taskId)
	if err != nil {
		log.Error("delete task failed, err[%v]", err)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	log.Info("restart task[%d] success", taskId)
	return
}

func (service *Server) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	taskId, err := strconv.ParseUint(r.FormValue(HTTP_TASK_ID), 10, 64)
	if err != nil {
		log.Error("invalid task: %s", r.FormValue(HTTP_TASK_ID))
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	err = service.cluster.DeleteTask(taskId)
	if err != nil {
		log.Error("delete task failed, err[%v]", err)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	log.Info("delete task[%d] success", taskId)
	return
}

func (service *Server) handlePauseTask(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	taskId, err := strconv.ParseUint(r.FormValue(HTTP_TASK_ID), 10, 64)
	if err != nil {
		log.Error("invalid task: %s", r.FormValue(HTTP_TASK_ID))
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	err = service.cluster.PauseTask(taskId)
	if err != nil {
		log.Error("delete task failed, err[%v]", err)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	log.Info("pause task[%d] success", taskId)
	return
}

func (service *Server) handleModifyCfg(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	cfgName := strings.Trim(r.FormValue("k"), " ")
	cfgVal := strings.Trim(r.FormValue("v"), " ")
	cfgUnit := strings.Trim(r.FormValue("unit"), " ")

	if cfgName == "" {
		log.Error("http modify cfg error, param k is required")
		reply.Code = HTTP_ERROR
		reply.Message = "param k is required"
		return
	}

	if cfgVal == "" {
		log.Error("http modify cfg error, param v is nil")
		reply.Code = HTTP_ERROR
		reply.Message = "param v is nil"
		return
	}

	if cfgUnit == "" {
		log.Error("http modify cfg error, param unit is required")
		reply.Code = HTTP_ERROR
		reply.Message = "param unit is required"
		return
	}

	intCfgVal, err := strconv.Atoi(r.FormValue("v"))
	if err != nil {
		log.Error("http modify cfg intCfgVal: parse v error: %v", err)
		reply.Code = HTTP_ERROR
		reply.Message = fmt.Sprintf("http modify cfg intCfgVal: parse v error: %v", err)
		return
	}

	var uint64CfgVal uint64
	if cfgUnit == "M" || cfgUnit == "m" {
		uint64CfgVal = uint64(time.Minute * time.Duration(intCfgVal))
	} else if cfgUnit == "HTTP_S" || cfgUnit == "s" {
		uint64CfgVal = uint64(time.Second * time.Duration(intCfgVal))
	} else {
		uint64CfgVal = uint64(intCfgVal)
	}

	switch cfgName {
	case "DefaultDownTimeLimit":
		DefaultDownTimeLimit = uint64CfgVal
		log.Info("DefaultDownTimeLimit=%d", DefaultDownTimeLimit)
	case "DefaultTransferTaskTimeout":
		DefaultTransferTaskTimeout = uint64CfgVal
		log.Info("DefaultTransferTaskTimeout=%d", DefaultTransferTaskTimeout)
	case "DefaultFailOverTaskTimeout":
		DefaultFailOverTaskTimeout = uint64CfgVal
		log.Info("DefaultFailOverTaskTimeout=%d", DefaultFailOverTaskTimeout)
	case "DefaultSplitTaskTimeout":
		DefaultSplitTaskTimeout = uint64CfgVal
		log.Info("DefaultSplitTaskTimeout=%d", DefaultSplitTaskTimeout)
	case "DefaultChangeLeaderTaskTimeout":
		DefaultChangeLeaderTaskTimeout = uint64CfgVal
		log.Info("DefaultChangeLeaderTaskTimeout=%d", DefaultChangeLeaderTaskTimeout)
	case "DefaultRangeDeleteTaskTimeout":
		DefaultRangeDeleteTaskTimeout = uint64CfgVal
		log.Info("DefaultRangeDeleteTaskTimeout=%d", DefaultRangeDeleteTaskTimeout)
	case "DefaultRangeAddPeerTaskTimeout":
		DefaultRangeAddPeerTaskTimeout = uint64CfgVal
		log.Info("DefaultRangeAddPeerTaskTimeout=%d", DefaultRangeAddPeerTaskTimeout)
	case "DefaultRangeDelPeerTaskTimeout":
		DefaultRangeDelPeerTaskTimeout = uint64CfgVal
		log.Info("DefaultRangeDelPeerTaskTimeout=%d", DefaultRangeDelPeerTaskTimeout)
	case "DefaultRangeGCTimeout":
		DefaultRangeGCTimeout = uint64CfgVal
		log.Info("DefaultRangeGCTimeout=%d", DefaultRangeGCTimeout)
	}
}

func (service *Server) handleManageRangeFailOver(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)

	if len(dbname) == 0 || len(tablename) == 0 || err != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	range_, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	n := range_.Leader
	if n == nil {
		log.Error("http get range leader: no leader")
		reply.Code = HTTP_ERROR_CLUSTER_HAS_NO_LEADER
		reply.Message = http_error_cluster_has_no_leader
		return
	}
	reply.Data = n.GetNodeAddr()
}

// TODO transfer
func (service *Server) handleManageRangeTransfer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	nodeid, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if len(dbname) == 0 || len(tablename) == 0 || rangeid == 0 || nodeid == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	rng, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	// 检验参数
	downPeer := rng.GetPeer(nodeid)
	if downPeer == nil {
		log.Error("invalid param, range[%s:%s:%d] has no peer in the node[%d]", dbname, tablename, rangeid, nodeid)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	downInstance, find := service.cluster.FindInstance(nodeid)
	if !find {
		log.Error("invalid param, node[%d] not exist", nodeid)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	if !rng.AllowSch(service.cluster) {
		log.Error("range[%s:%s:%d] not allow schedule", dbname, tablename, rangeid)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	taskId, err := service.cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	// TODO same room as downPeer, but different switch from others peer in same room
	expRoom := func(c *Cluster, downPeer *metapb.Peer) *Room {
		// expect room
		ins, find := c.FindInstance(downPeer.GetNodeId())
		if !find {
			log.Error("instance[%d] not exist", downPeer.GetNodeId())
			return nil
		}
		mac, find := c.FindMachine(ins.MacIp)
		if !find {
			log.Error("machine[%s] not exist", ins.MacIp)
			return nil
		}
		zone, find := c.FindZone(mac.GetZone().GetZoneName())
		if !find {
			log.Error("zone[%s] not exist", mac.GetZone().GetZoneName())
			return nil
		}
		room, find := zone.FindRoom(mac.GetRoom().GetRoomName())
		if !find {
			log.Error("room[%s:%s] not exist", mac.GetZone().GetZoneName(), mac.GetRoom().GetRoomName())
			return nil
		}
		return room
	}(service.coordinator.cluster, downPeer)
	upInstance := service.coordinator.cluster.SelectTransferInstance(rng, expRoom)
	if upInstance == nil {
		log.Error("manager range transfer: select transfer instance is nil")
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	upPeer := &metapb.Peer{
		Id:     rng.ID(),
		NodeId: upInstance.GetId(),
	}
	downPeer = &metapb.Peer{
		Id:     rng.ID(),
		NodeId: downPeer.GetNodeId(),
	}
	_r := deepcopy.Iface(rng.Range).(*metapb.Range)
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeTransfer,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultTransferTaskTimeout,
		},
		RangeTransfer: &taskpb.TaskRangeTransfer{
			Range:    _r,
			UpPeer:   upPeer,
			DownPeer: downPeer,
		},
	}
	task := NewTask(t)
	err = task.GenSubTasks(service.cluster)
	if err != nil {
		log.Error("gen sub tasks failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	err = service.cluster.AddTask(task)
	if err != nil {
		log.Error("add task failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("range[%s:%s:%d] from [%s] to [%s]",
		rng.GetDbName(), rng.GetTableName(), rng.GetId(),
		downInstance.GetAddress(), upInstance.GetAddress())
	return
}

func (service *Server) handleManageRangeSplit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}

	if len(dbname) == 0 || len(tablename) == 0 || rangeid == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	rng, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	if !rng.AllowSch(service.cluster) {
		log.Error("range[%s:%s:%d] not allow schedule", dbname, tablename, rangeid)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	taskId, err := service.cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	leftId, err := service.cluster.GenRangeId()
	if err != nil {
		log.Error("gen ID failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	rightId, err := service.cluster.GenRangeId()
	if err != nil {
		log.Error("gen ID failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	_r := deepcopy.Iface(rng.Range).(*metapb.Range)
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeSplit,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultSplitTaskTimeout,
		},
		RangeSplit: &taskpb.TaskRangeSplit{
			Range:        _r,
			LeftRangeId:  leftId,
			RightRangeId: rightId,
			RangeSize:    rng.Stats.Size_,
		},
	}
	task := NewTask(t)
	err = service.cluster.AddTask(task)
	if err != nil {
		log.Error("add task failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("range[%s:%s:%d] split task",
		rng.GetDbName(), rng.GetTableName(), rng.GetId())
	return
}

func (service *Server) handleManageRangeAddPeer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	nodeId, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		log.Error("http get node id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if len(dbname) == 0 || len(tablename) == 0 || rangeid == 0 || nodeId == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	rng, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	if !rng.AllowSch(service.cluster) {
		log.Error("range[%s:%s:%d] not allow schedule", dbname, tablename, rangeid)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	// 检查节点是否可用
	if rng.GetPeer(nodeId) != nil {
		log.Error("range[%s:%s:%d] has peer on the node[%d]", dbname, tablename, rangeid, nodeId)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	node, find := service.cluster.FindInstance(nodeId)
	if !find {
		log.Error("node[%d] not exist", nodeId)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	if node.IsFault() {
		log.Error("node[%d] is fault", nodeId)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	taskId, err := service.cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	_r := deepcopy.Iface(rng.Range).(*metapb.Range)
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeAddPeer,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultRangeAddPeerTaskTimeout,
		},
		RangeAddPeer: &taskpb.TaskRangeAddPeer{
			Range: _r,
			Peer: &metapb.Peer{
				Id:     rng.ID(),
				NodeId: node.GetId(),
			},
		},
	}
	task := NewTask(t)
	err = service.cluster.AddTask(task)
	if err != nil {
		log.Error("add task failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("range[%s:%s:%d] add peer task",
		rng.GetDbName(), rng.GetTableName(), rng.GetId())
	return
}

func (service *Server) handleManageRangeDelPeer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	nodeId, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		log.Error("http get node id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if len(dbname) == 0 || len(tablename) == 0 || rangeid == 0 || nodeId == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var db *Database
	var table *Table
	var ok bool
	if db, ok = service.cluster.FindDatabase(dbname); !ok {
		log.Error("http create range: db [%s] is not existed", dbname)
		reply.Code = HTTP_ERROR_DATABASE_FIND
		reply.Message = http_error_database_find
		return
	}
	if table, ok = db.FindCurTable(tablename); !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	rng, ok := table.FindRange(rangeid)
	if !ok {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	if rng.GetLeader().GetNodeId() == nodeId {
		log.Error("http create range: table [%s] is not existed", tablename)
		reply.Code = HTTP_ERROR
		reply.Message = "this is leader node"
		return
	}
	//if !rng.AllowSch(service.cluster) {
	//	log.Error("range[%s:%s:%d] not allow schedule", dbname, tablename, rangeid)
	//	reply.Code = HTTP_ERROR
	//	reply.Message = http_error
	//	return
	//}
	if rng.GetTask() != nil {
		log.Warn("range[%s:%s:%d] already has task[%s]", dbname, tablename, rangeid, rng.GetTask())
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	// 检查节点是否可用
	if rng.GetPeer(nodeId) == nil {
		log.Error("range[%s:%s:%d] has no peer on the node[%d]", dbname, tablename, rangeid, nodeId)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	node, find := service.cluster.FindInstance(nodeId)
	if !find {
		log.Error("node[%d] not exist", nodeId)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	taskId, err := service.cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	_r := deepcopy.Iface(rng.Range).(*metapb.Range)
	t := &taskpb.Task{
		Type: taskpb.TaskType_RangeDelPeer,
		Meta: &taskpb.TaskMeta{
			TaskId:     taskId,
			CreateTime: time.Now().Unix(),
			State:      taskpb.TaskState_TaskWaiting,
			Timeout:    DefaultRangeDelPeerTaskTimeout,
		},
		RangeDelPeer: &taskpb.TaskRangeDelPeer{
			Range: _r,
			Peer: &metapb.Peer{
				Id:     rng.ID(),
				NodeId: node.GetId(),
			},
		},
	}
	task := NewTask(t)
	err = service.cluster.AddTask(task)
	if err != nil {
		log.Error("add task failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	log.Info("range[%s:%s:%d] del peer task",
		rng.GetDbName(), rng.GetTableName(), rng.GetId())
	return
}

func (service *Server) handleManageGetAutoScheduleInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	clusterId, err := strconv.ParseUint(r.FormValue(HTTP_CLUSTER_ID), 10, 64)
	if err != nil || clusterId != service.cluster.GetClusterId() {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	type ClusterAutoScheduleInfo struct {
		// 是否支持数据分片，默认支持数据分片
		AutoShardingUnable bool `json:"autoShardingUnable"`
		// 是否支持数据迁移
		AutoTransferUnable bool `json:"autoTransferUnable"`
		// 是否支持failOver
		AutoFailoverUnable bool `json:"autoFailoverUnable"`
	}
	info := ClusterAutoScheduleInfo{
		AutoShardingUnable: service.cluster.autoShardingUnable,
		AutoTransferUnable: service.cluster.autoTransferUnable,
		AutoFailoverUnable: service.cluster.autoFailoverUnable,
	}
	reply.Data = info
	log.Info("get cluster auto schedule info success!!!")
	return
}

func (service *Server) handleManageSetAutoScheduleInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	clusterId, err := strconv.ParseUint(r.FormValue(HTTP_CLUSTER_ID), 10, 64)
	infoStr := r.FormValue("clusterAutoScheduleInfo")
	if err != nil || clusterId != service.cluster.GetClusterId() || len(infoStr) == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	type ClusterAutoScheduleInfo struct {
		// 是否支持数据分片，默认支持数据分片
		AutoShardingUnable bool `json:"autoShardingUnable"`
		// 是否支持数据迁移
		AutoTransferUnable bool `json:"autoTransferUnable"`
		// 是否支持failOver
		AutoFailoverUnable bool `json:"autoFailoverUnable"`
	}
	info := new(ClusterAutoScheduleInfo)
	err = json.Unmarshal([]byte(infoStr), info)
	if err != nil {
		log.Error("unmarshal cluster auto schedule info[%s] failed, err[%v]", infoStr, err)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	err = service.cluster.UpdateAutoScheduleInfo(info.AutoFailoverUnable, info.AutoShardingUnable, info.AutoTransferUnable)
	if err != nil {
		log.Error("update cluster auto schedule info failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error_parameter_not_enough
		return
	}

	log.Info("set cluster auto schedule info success!!!")
	return
}

func (service *Server) handleManageGetClusterDeployPolicy(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	reply.Data = service.cluster.deploy
	log.Debug("get deply: %v", service.cluster.deploy)
	log.Info("get cluster deploy policy success!!!")
	return
}

func (service *Server) handleManageSetClusterDeployPolicy(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var err error
	//clusterId, err := strconv.ParseUint(r.FormValue("clusterid"), 10, 64)
	infoStr := r.FormValue("clusterAutoScheduleInfo")
	//if err != nil || clusterId != service.cluster.GetClusterId() || len(infoStr) == 0 {
	//	log.Error("http get range leader: %s", http_error_parameter_not_enough)
	//	reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
	//	reply.Message = http_error_parameter_not_enough
	//	return
	//}
	log.Debug("clusterAutoScheduleInfo: %v", infoStr)
	deploy := new(metapb.DeployV1Policy)
	err = json.Unmarshal([]byte(infoStr), deploy)
	if err != nil {
		log.Error("unmarshal cluster auto schedule info[%s] failed, err[%v]", infoStr, err)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	log.Debug("set deploy: %v", deploy)
	err = service.cluster.SetDeploy(deploy)
	if err != nil {
		log.Error("set cluster deploy failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}

	log.Info("set cluster deploy success!!!")
	return
}

func (service *Server) handleManageGetTableAutoScheduleInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tableName := r.FormValue(HTTP_TABLE_NAME)
	table, find := service.cluster.FindCurTable(dbName, tableName)
	if !find {
		log.Error("table[%s:%s] not exit", dbName, tableName)
		return
	}
	type TableAutoScheduleInfo struct {
		// 是否支持数据分片，默认支持数据分片
		AutoShardingUnable bool `json:"autoShardingUnable"`
		// 是否支持数据迁移
		AutoTransferUnable bool `json:"autoTransferUnable"`
		// 是否支持failOver
		AutoFailoverUnable bool `json:"autoFailoverUnable"`
	}
	info := TableAutoScheduleInfo{
		AutoShardingUnable: table.autoShardingUnable,
		AutoTransferUnable: table.autoTransferUnable,
		AutoFailoverUnable: table.autoFailoverUnable,
	}
	reply.Data = info
	log.Info("get table auto schedule info success!!!")
	return
}

func (service *Server) handleManageSetTableAutoScheduleInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tableName := r.FormValue(HTTP_TABLE_NAME)
	infoStr := r.FormValue("tableAutoInfo")
	if len(dbName) == 0 || len(tableName) == 0 || len(infoStr) == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	type TableAutoScheduleInfo struct {
		// 是否支持数据分片，默认支持数据分片
		AutoShardingUnable bool `json:"autoShardingUnable"`
		// 是否支持数据迁移
		AutoTransferUnable bool `json:"autoTransferUnable"`
		// 是否支持failOver
		AutoFailoverUnable bool `json:"autoFailoverUnable"`
	}
	info := new(TableAutoScheduleInfo)
	err := json.Unmarshal([]byte(infoStr), info)
	if err != nil {
		log.Error("unmarshal table auto schedule info[%s] failed, err[%v]", infoStr, err)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	table, find := service.cluster.FindCurTable(dbName, tableName)
	if !find {
		log.Error("table[%s:%s] not exit", dbName, tableName)
		return
	}
	err = table.UpdateAutoScheduleInfo(service.cluster, info.AutoFailoverUnable, info.AutoShardingUnable, info.AutoTransferUnable)
	if err != nil {
		log.Error("update table auto schedule info failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = http_error_parameter_not_enough
		return
	}

	log.Info("set table auto schedule info success!!!")
	return
}

func (service *Server) handleManageRangeStop(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if rangeid == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	rng, ok := service.cluster.FindRange(rangeid)
	if !ok {
		log.Error("find range[%d] is not existed", rangeid)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	table, find := service.cluster.FindCurTable(rng.GetDbName(), rng.GetTableName())
	if !find {
		log.Warn("invalid table[%s:%s]", rng.GetDbName(), rng.GetTableName())
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	// step range
	for _, p := range rng.GetPeers() {
		node, find := service.cluster.FindInstance(p.GetNodeId())
		if find {
			// 有可能控制失败
			err = service.cluster.cli.StopRange(node.GetAddress(), rangeid)
			if err != nil {
				log.Warn("stop range[%s] from remote[%s] failed, err[%v]", rng.SString(), node.GetAddress(), err)
			}
		}
	}
	rng.State = metapb.RangeState_R_Abnormal
	err = table.RoutesUpdate(service.cluster, rng)
	if err != nil {
		log.Warn("update table[%s:%s] routes failed, err[%v]", rng.GetDbName(), rng.GetTableName(), err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	topoEpoch, err:= table.GetTopologyEpoch(service.cluster, &metapb.TableEpoch{})
	if err != nil {
		log.Warn("get table topology epoch [%s:%s] routes failed, err[%v]", rng.GetDbName(), rng.GetTableName(), err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	service.router.Publish(ROUTE, topoEpoch)
	log.Debug("publish routes[%v] version[%d] update!!!!", topoEpoch.GetRoutes(), topoEpoch.GetEpoch())
	log.Info("range[%s] stop success", rng.SString())
	return
}

func (service *Server) handleManageRangeClose(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("http get range id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if rangeid == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	rng, ok := service.cluster.FindRange(rangeid)
	if !ok {
		log.Error("find range[%d] is not existed", rangeid)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	for _, p := range rng.GetPeers() {
		node, find := service.cluster.FindInstance(p.GetNodeId())
		if find {
			// 有可能控制失败
			err = service.cluster.cli.CloseRange(node.GetAddress(), rangeid)
			if err != nil {
				log.Warn("close range[%s] from remote[%s] failed, err[%v]", rng.SString(), node.GetAddress(), err)
			}
		}
	}

	log.Info("range[%s:%s:%d] close",
		rng.GetDbName(), rng.GetTableName(), rng.GetId())
	return
}

func (service *Server) handleManageRangeGenId(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := service.cluster.GenRangeId()
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		log.Warn("gen range ID failed, err[%v]", err)
		return
	}
	ret := fmt.Sprintf("new range ID: %d", rangeId)
	w.Write([]byte(ret))

	log.Info("gen range ID [%d] success", rangeId)
	return
}

func (service *Server) handleManageRangeReplace(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	oldRangeId, err := strconv.ParseUint(r.FormValue("oldRangeId"), 10, 64)
	if err != nil {
		log.Error("http get range id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	newRangeId, err := strconv.ParseUint(r.FormValue("newRangeId"), 10, 64)
	if err != nil {
		log.Error("http get range id: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	if oldRangeId == 0 || newRangeId == 0 {
		log.Error("http get range leader: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	nodesStr := r.FormValue("nodes")
	if len(nodesStr) == 0 {
		log.Error("http get range nodes: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	list := strings.Split(nodesStr, ";")
	if len(list) != 3 {
		log.Error("http get range nodes: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	var peers []*metapb.Peer
	for _, ns := range list {
		nodeId, err := strconv.ParseUint(ns, 10, 64)
		if err != nil {
			log.Error("http get range nodes: %s", http_error_parameter_not_enough)
			reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
			reply.Message = http_error_parameter_not_enough
			return
		}
		if node, find := service.cluster.FindInstance(nodeId); find {
			if node.GetState() != metapb.NodeState_N_Login {
				log.Warn("invalid node[%s]", node.GetAddress())
				reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
				reply.Message = http_error_parameter_not_enough
				return
			}
		} else {
			log.Warn("invalid node[%d]", nodeId)
			reply.Code = HTTP_ERROR_NODE_FIND
			reply.Message = http_error_node_find
			return
		}
		peer := &metapb.Peer{
			Id: newRangeId,
			NodeId: nodeId,
		}
		peers = append(peers, peer)
	}
	rng, ok := service.cluster.FindRange(oldRangeId)
	if !ok {
		log.Error("find range[%d] is not existed", oldRangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	if rng.GetState() != metapb.RangeState_R_Abnormal {
		message := fmt.Sprintf("range[%s] state[%s] is invalid", rng.SString(), rng.GetState().String())
		log.Warn("%s", message)
		reply.Code = HTTP_ERROR
		reply.Message = message
		return
	}
	table, find := service.cluster.FindCurTable(rng.GetDbName(), rng.GetTableName())
	if !find {
		log.Warn("invalid table[%s:%s]", rng.GetDbName(), rng.GetTableName())
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	_r := &metapb.Range{
		Id: newRangeId,
		// Range key range [start_key, end_key).
		StartKey: deepcopy.Iface(rng.GetStartKey()).(*metapb.Key),
		EndKey: deepcopy.Iface(rng.GetEndKey()).(*metapb.Key),
		RangeEpoch: &metapb.RangeEpoch{ConfVer: 1, Version: 1},
		Peers: peers,
		// Range state
		State:      metapb.RangeState_R_Init,
		DbId:       rng.GetDbId(),
		TableId:    rng.GetTableId(),
		DbName:     rng.GetDbName(),
		TableName:  rng.GetTableName(),
		CreateTime: time.Now().Unix(),
	}
	err = service.cluster.createRangeRemote(_r)
	if err != nil {
		log.Warn("create range[%s] failed, err[%v]", _r.String(), err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	err = table.RoutesUpdate(service.cluster, NewRange(_r))
	if err != nil {
		log.Warn("routes update failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	topoEpoch, err := table.GetTopologyEpoch(service.cluster, &metapb.TableEpoch{})
	if err != nil {
		log.Warn("get topology epoch failed, err[%v]", err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}

	service.router.Publish(ROUTE, topoEpoch)
	log.Debug("publish routes[%v] version[%d] update!!!!", topoEpoch.GetRoutes(), topoEpoch.GetEpoch())
	log.Info("range[%s] replace with range[%v] success", rng.SString(), _r)
	return
}

/////////////////////////////////////// TODDO ///////////////////////////////////////////////

func (service *Server) handleDatabaseDelete(w http.ResponseWriter, r *http.Request) {
	//reply := &httpReply{}
	//defer sendReply(w, reply)
	//if !service.IsLeader() {
	//	reply.Code = -1
	//	reply.Message = http_error_master_is_not_leader
	//	return
	//}
	//dbName := r.FormValue("name")
	//
	//if dbName == ""{
	//	log.Error("http delete database: %s", http_error_parameter_not_enough)
	//	reply.Code = -1
	//	reply.Message = http_error_parameter_not_enough
	//	return
	//}
	//
	//if _, err := service.DbMngr.DeleteDatabase(dbName); err != nil {
	//	log.Error("http delete database: %v", err)
	//	reply.Code = -1
	//	reply.Message = err.Error()
	//	return
	//}
	//return
}

func (service *Server) handleTableDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	if len(dbName) == 0 || len(tName) == 0 {
		log.Error("http delete table: %s", http_error_parameter_not_enough)
		reply.Code = -1
		reply.Message = http_error_parameter_not_enough
		return
	}

	if table, err := service.cluster.DeleteCurTable(dbName, tName); err != nil {
		log.Error("http delete table: %v", err)
		reply.Code = -1
		reply.Message = err.Error()
		return
	} else {
		service.router.Publish(ROUTE, &metapb.TopologyEpoch{
			DbId: table.GetDbId(),
			TableId: table.GetId(),
			Epoch: table.GetEpoch()})
	}

}

func (service *Server) handleTableEdit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	properties := r.FormValue(HTTP_PROPERTIES)

	t, find := service.cluster.FindCurTable(dbName, tName)
	if !find {
		log.Warn("table[%s:%s] not exist", dbName, tName)
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		return
	}
	if err := service.cluster.EditTable(t, properties); err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Warn("edit table[%s:%s] failed, properties[%s]", dbName, tName, properties)
		return
	}
	service.router.Publish(ROUTE, &metapb.TopologyEpoch{
		DbId: t.GetDbId(),
		TableId: t.GetId(),
		Epoch: t.GetEpoch()})
	log.Info("edit table[%s:%s] success", dbName, tName)
}

func (service *Server) handleTableGetRwPolicy(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)

	table, find := service.cluster.FindCurTable(dbName, tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		log.Warn("tabel[%s:%s] not exist", dbName, tName)
		return
	}
	reply.Data = table.RwPolicy
	log.Info("get table[%s:%s] rw policy success", dbName, tName)
}

func (service *Server) handleTableSetRwPolicy(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbName := r.FormValue(HTTP_DB_NAME)
	tName := r.FormValue(HTTP_TABLE_NAME)
	policyStr := r.FormValue(HTTP_POLICY)
	if len(policyStr) == 0 {
		reply.Code = HTTP_ERROR
		reply.Message = http_error_parameter_not_enough
		return
	}
	policy := &metapb.TableRwPolicy{}
	err := json.Unmarshal([]byte(policyStr), policy)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Warn("invalid policy!!!!")
		return
	}

	table, find := service.cluster.FindCurTable(dbName, tName)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = ErrNotExistTable.Error()
		log.Warn("tabel[%s:%s] not exist", dbName, tName)
		return
	}
	err = table.UpdateRwPolicy(service.cluster, policy)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Warn("update table[%s:%s] policy failed, err[%v]", dbName, tName, err)
		return
	}
	topo, err := table.GetTopologyEpoch(service.cluster, &metapb.TableEpoch{})
	if err != nil {
		log.Warn("table[%s:%s] get topology failed, err[%v]", dbName, tName, err)
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	service.router.Publish(ROUTE, topo)
	log.Info("set table[%s:%s] rw policy success: %+v", dbName, tName, *topo)
}

//func (service *Server) handleTableColumnRename(w http.ResponseWriter, r *http.Request) {
//	reply := &httpReply{}
//
//	if verifySign(w, reply, r, service.conf.Mode) == false {
//		log.Error("sign verify error: %s", http_error_wrong_sign)
//		reply.Code = HTTP_ERROR_WRONG_SIGN
//		reply.Message = http_error_wrong_sign
//		sendReply(w, reply)
//		return
//	}
//
//	if !service.IsLeader() {
//		if _, addr := service.GetLeader(); addr == "" {
//			reply.Code = HTTP_ERROR_MASTER_IS_NOT_LEADER
//			reply.Message = http_error_cluster_has_no_leader
//			sendReply(w, reply)
//		} else {
//			proxy := &Proxy{targetHost: addr}
//			proxy.proxy(w, r)
//		}
//		return
//	}
//	defer sendReply(w, reply)
//
//	dbName := r.FormValue("dbname")
//	tName := r.FormValue("name")
//	oldColName := r.FormValue("oldColName")
//	newColName := r.FormValue("newColName")
//	cluster := service.cluster
//	router := service.router
//
//	if dbName == "" || tName == "" || len(oldColName) == 0 || len(newColName) == 0 {
//		log.Error("http delete table: %s", http_error_parameter_not_enough)
//		reply.Code = -1
//		reply.Message = http_error_parameter_not_enough
//		return
//	}
//
//	if err := cluster.TableColumnRename(dbName, tName, strings.ToLower(oldColName), strings.ToLower(newColName), router); err != nil {
//		log.Error("http delete table: %v", err)
//		reply.Code = -1
//		reply.Message = err.Error()
//		return
//	}
//	log.Info("table[%s:%s] rename column success", dbName, tName)
//	return
//}

func (service *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	//reply := &httpReply{}
	//defer sendReply(w, reply)
	//if !service.IsLeader() {
	//	reply.Code = -1
	//	reply.Message = http_error_master_is_not_leader
	//	return
	//}
	//
	//var id uint64
	//var err error
	//if id, err = strconv.ParseUint(r.FormValue("id"), 10, 64); err != nil {
	//	log.Error("http delete node: %v", err.Error())
	//	reply.Code = -1
	//	reply.Message = err.Error()
	//	return
	//}
	//if err := service.NodeMngr.DeleteNode(id); err != nil {
	//	log.Error("http delete node: %v", err)
	//	reply.Code = -1
	//	reply.Message = err.Error()
	//	return
	//}
	//return
}
func (service *Server) handleNodeLogin(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}

	if !service.IsLeader() {
		if point := service.GetLeader(); point == nil {
			reply.Code = HTTP_ERROR_MASTER_IS_NOT_LEADER
			reply.Message = http_error_cluster_has_no_leader
			sendReply(w, reply)
		} else {
			proxy := &Proxy{targetHost: point.ManageAddress}
			proxy.proxy(w, r)
		}
		return
	}
	defer sendReply(w, reply)
	nodeAddr, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		return
	}
	sPort := r.FormValue("port")
	if len(sPort) == 0 {
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		log.Warn("bad request[%v]")
		return
	}
	log.Info("node[%s:%s] login success", nodeAddr, sPort)
	return
}

func (service *Server) handleNodeInit(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}

	if !service.IsLeader() {
		if point := service.GetLeader(); point == nil {
			reply.Code = HTTP_ERROR_MASTER_IS_NOT_LEADER
			reply.Message = http_error_cluster_has_no_leader
			sendReply(w, reply)
		} else {
			proxy := &Proxy{targetHost: point.ManageAddress}
			proxy.proxy(w, r)
		}
		return
	}
	defer sendReply(w, reply)
	var id uint64
	var err error
	if id, err = strconv.ParseUint(r.FormValue("id"), 10, 64); err != nil {
		log.Error("http delete node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	if err := service.cluster.NodeInit(id); err != nil {
		log.Error("http delete node: %v", err)
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
}

func (service *Server) handleNodeLogout(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	var id uint64
	var err error
	if id, err = strconv.ParseUint(r.FormValue("id"), 10, 64); err != nil {
		log.Error("http delete node: %v", err.Error())
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	if err := service.cluster.LogoutNode(id); err != nil {
		log.Error("http delete node: %v", err)
		reply.Code = -1
		reply.Message = err.Error()
		return
	}
	return
}

func (service *Server) handleRangeDelete(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("invalid rangeId error: %s", r.FormValue(HTTP_RANGE_ID))
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	cluster := service.cluster
	rng, find := cluster.FindRange(rangeId)
	if !find {
		log.Error("rangeId[%d] not found: %s", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	rng.AutoSchUnable = true
	defer func() {
		rng.AutoSchUnable = false
	}()
	t := rng.GetTask()
	if t != nil {
		switch t.GetType() {
		case taskpb.TaskType_RangeDelete:
			return
		case taskpb.TaskType_RangeCreate:
			reply.Code = HTTP_ERROR_RANGE_FIND
			reply.Message = http_error_range_find
			return
		case taskpb.TaskType_RangeSplit:
			reply.Code = HTTP_ERROR_RANGE_SPLIT
			reply.Message = http_error_range_split
			return
		default:
			err = cluster.DeleteTask(t.ID())
			if err != nil {
				log.Error("delete task[%s] failed, err[%v]", t.Describe(), err)
				reply.Code = HTTP_ERROR
				reply.Message = err.Error()
				return
			}
		}
	}
	task := RangeOffline(cluster, rng)
	if task == nil {
		log.Error("can not do range delete now !!!")
		reply.Code = HTTP_ERROR
		reply.Message = "can not do range delete now"
		return
	}
	err = cluster.AddTask(task)
	if err != nil {
		log.Error("add task[%s] failed, err[%v]", t.Describe(), err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
	}
	log.Info("to delete range[%d] success", rangeId)
	return
}

func (service *Server) handleRangeLeaderTransfer(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		log.Error("invalid rangeId error: %s", r.FormValue(HTTP_RANGE_ID))
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	nodeId, err := strconv.ParseUint(r.FormValue(HTTP_PEER_ID), 10, 64)
	if err != nil {
		log.Error("invalid nodeId error: %s", r.FormValue(HTTP_PEER_ID))
		reply.Code = HTTP_ERROR_INVALID_PARAM
		reply.Message = http_error_invalid_parameter
		return
	}
	cluster := service.cluster
	rng, find := cluster.FindRange(rangeId)
	if !find {
		log.Error("range[%d] not found: %s", rangeId)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	rng.AutoSchUnable = true
	defer func() {
		rng.AutoSchUnable = false
	}()
	t := rng.GetTask()
	if t != nil {
		reply.Code = HTTP_ERROR_RANGE_BUSY
		reply.Message = http_error_range_busy
		return
	}
	_, find = cluster.FindInstance(nodeId)
	if !find {
		log.Error("node[%d] not found: %s", nodeId)
		reply.Code = HTTP_ERROR_NODE_FIND
		reply.Message = http_error_node_find
		return
	}
	task := RangeLeaderTransfer(cluster, rng, nodeId)
	if task == nil {
		log.Error("can not do range delete peer now !!!")
		reply.Code = HTTP_ERROR
		reply.Message = "can not do range delete peer now"
		return
	}
	err = cluster.AddTask(task)
	if err != nil {
		log.Error("add task[%s] failed, err[%v]", t.Describe(), err)
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
	}
	log.Info("to transfer leader range[%s] success", rng.SString())
	return
}

func (service *Server) handleRouteGet(w http.ResponseWriter, r *http.Request) {
	log.Debug("handleRouteGet!!!!!")
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbId, err := strconv.ParseUint(r.FormValue("dbId"), 10, 64)
	if err != nil {
		log.Error("http get route: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	tableId, err := strconv.ParseUint(r.FormValue("tableId"), 10, 64)
	if err != nil {
		log.Error("http get route: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	version, err := strconv.ParseUint(r.FormValue("version"), 10, 64)
	if err != nil {
		log.Error("http get route: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	confVersion, err := strconv.ParseUint(r.FormValue("confVersion"), 10, 64)
	if err != nil {
		log.Error("http get route: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
    epoch := &metapb.TableEpoch{ConfVer: confVersion, Version: version}
	topo, err := service.router.GetTopologyEpoch(dbId, tableId, epoch)
	if err != nil {
		log.Error("http get route failed, err[%v]", err)
		if err.Error() == ErrNotExistTable.Error() {
			reply.Code = HTTP_ERROR_TABLE_FIND
			reply.Message = err.Error()
		} else {
			reply.Code = HTTP_ERROR
			reply.Message = err.Error()
		}
		return
	}
	if topo.GetEpoch().GetVersion() > version || topo.GetEpoch().GetConfVer() > confVersion {
		reply.Data = topo
		log.Info("[%d:%d]get topology success, new epoch[%v] old epoch[%v]!!!",
			dbId, tableId, topo.GetEpoch(), epoch)
		return
	}
	table := fmt.Sprintf("$%d#%d", dbId, tableId)
	session := service.router.AddSession(table)
	defer service.router.DeleteSession(table, session.id)

	select {
	case <-w.(http.CloseNotifier).CloseNotify():
		close(session.quit)
		log.Warn("session closed!!!")
	case data := <-session.notify:
		log.Info("recv notify!!!!!!!")
		reply.Data = data
		log.Debug("[%d:%d:%d]get routes[%v] success!!!", dbId, tableId, version, reply.Data)
	case <-time.After(time.Second * 60):
	// 超时更新路由
	// TODO compress response
		topo, err = service.router.GetTopologyEpoch(dbId, tableId, &metapb.TableEpoch{ConfVer: 1, Version: 1})
		if err != nil {
			log.Error("http get route failed, err[%v]", err)
			if err.Error() == ErrNotExistTable.Error() {
				reply.Code = HTTP_ERROR_TABLE_FIND
				reply.Message = err.Error()
			} else {
				reply.Code = HTTP_ERROR
				reply.Message = err.Error()
			}
			return
		}
		reply.Data = topo
		log.Debug("session timeout!!!!")
	}
	return
}

func (service *Server) handleRouteCheck(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	dbname := r.FormValue(HTTP_DB_NAME)
	tablename := r.FormValue(HTTP_TABLE_NAME)
	if dbname == "" || tablename == "" {
		log.Error("http get route: %s", http_error_parameter_not_enough)
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = http_error_parameter_not_enough
		return
	}
	cluster := service.cluster
	alarm := service.alarm
	table, find := cluster.FindCurTable(dbname, tablename)
	if !find {
		reply.Code = HTTP_ERROR_TABLE_FIND
		reply.Message = http_error_table_find
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	table.RoutesCheck(ctx, cluster, alarm)
	return
}

func (service *Server) handleManageGetMachine(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	ip := r.FormValue("ip")
	mach, _ := service.cluster.FindMachine(ip)
	data, _ := json.Marshal(mach)
	reply.Message = string(data)
}

func (service *Server) handleManageListMachine(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	maches := service.cluster.machines.GetAllMachine()
	data, _ := json.Marshal(maches)
	reply.Message = string(data)
}

func (service *Server) handleManageAddMachine(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	machines := r.FormValue(MACHINES)
	if len(machines) == 0 {
		reply.Code = HTTP_ERROR_PARAMETER_NOT_ENOUGH
		reply.Message = "machines is empty"
		return
	}
	log.Debug("add machines: %v", machines)
	type Machines struct {
		Macs      []*metapb.Machine    `json:"macs"`
	}
	macs := &Machines{}
	err := json.Unmarshal([]byte(machines), macs)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "machines cannot decode"
		return
	}
	log.Debug("macs: %v", macs)
	err = service.cluster.AddMachines(macs.Macs)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = http_error
		log.Warn("add machines failed, err[%v]!!!", err)
		return
	}
	log.Info("add machines success")
}
