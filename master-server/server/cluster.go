package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
	"sync"
	//"net"

	"github.com/golang/protobuf/proto"
	"data-server/client"
	sErr "engine/errors"
	"model/pkg/metapb"
	"util"
	"util/deepcopy"
	"util/log"
	"model/pkg/taskpb"
)

var SCHEMA_SPLITOR string = " "
var PREFIX_DB string = fmt.Sprintf("schema%sdb%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_TABLE string = fmt.Sprintf("schema%stable%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_DELETE_TABLE string = fmt.Sprintf("schema%sdelete%stable%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_ZONE string = fmt.Sprintf("schema%szone%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_MACHINE string = fmt.Sprintf("schema%smachine%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_NODE string = fmt.Sprintf("schema%snode%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_RANGE string = fmt.Sprintf("schema%srange%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var TIMESTAMP string = fmt.Sprintf("$timestamp")
var ID string = fmt.Sprintf("$id")
var DEPLOY_POLICY = fmt.Sprintf("$policy")
var TASK_ID string = fmt.Sprintf("$task_id")
var RANGE_ID string = fmt.Sprintf("$range_id")
var TABLE_ID string = fmt.Sprintf("$table_id")
var DATABASE_ID string = fmt.Sprintf("$database_id")
var NODE_ID string = fmt.Sprintf("$node_id")
var DATASERVER_LATEST_VERSION string = "dataserver_latest_version"
var PREFIX_TASK string = fmt.Sprintf("schema%stask%s", SCHEMA_SPLITOR, SCHEMA_SPLITOR)
var PREFIX_AUTO_SHARDING string = fmt.Sprintf("$auto_sharding_%d")
var PREFIX_AUTO_TRANSFER string = fmt.Sprintf("$auto_transfer_%d")
var PREFIX_AUTO_FAILOVER string = fmt.Sprintf("$auto_failover_%d")

type Cluster struct {
	conf *Config
	clusterId uint64
	nodeId    uint64
	// 是否支持数据分片，默认支持数据分片
	autoShardingUnable bool
	// 是否支持数据迁移
	autoTransferUnable bool
	// 是否支持failOver
	autoFailoverUnable bool

	cli client.SchClient

	taskIdGener     IDGenerator
	nodeIdGener     IDGenerator
	rangeIdGener    IDGenerator
	tableIdGener    IDGenerator
	databaseIDGener IDGenerator

	lock   sync.Mutex
	dbs    *DbCache
	zones  *ZoneCache
	machines *MachineCache
	instances  *InstanceCache
	instancesAddr *InstanceAddrCache
	ranges *RangeCache
	missRanges *RangeCache
	tasks      *TaskCache
	deploy     *metapb.DeployV1Policy
	leader     uint64

	// alarm
	alarm Alarm

	close bool

	store                   Store
	dataserverLatestVersion string
}

func NewCluster(clusterId, nodeId uint64, store Store, alarm Alarm, conf *Config) *Cluster {
	cluster := &Cluster{
		conf: conf,
		clusterId:       clusterId,
		nodeId:          nodeId,
		cli:             client.NewSchClient(),
		taskIdGener:     NewTaskIDGenerator(store),
		nodeIdGener:     NewNodeIDGenerator(store),
		rangeIdGener:    NewRangeIDGenerator(store),
		tableIdGener:    NewTableIDGenerator(store),
		databaseIDGener: NewDatabaseIDGenerator(store),
		alarm:           alarm,
		store:           store,
		//tsoSaveInterval: tsoSaveInterval,

		dbs:        NewDbCache(),
		zones:      NewZoneCache(),
		machines:   NewMachineCache(),
		instances:      NewInstanceCache(),
		instancesAddr:  NewInstanceAddrCache(),
		tasks:      NewTaskCache(),
		ranges:     NewRangeCache(),
		missRanges: NewRangeCache(),

		// stats
		close: true,
	}
	return cluster
}

func (c *Cluster) LoadCache() error {
	c.dbs = NewDbCache()
	c.zones = NewZoneCache()
	c.machines = NewMachineCache()
	c.instances = NewInstanceCache()
	c.instancesAddr = NewInstanceAddrCache()
	c.tasks = NewTaskCache()
	c.ranges = NewRangeCache()
	c.taskIdGener = NewTaskIDGenerator(c.store)
	c.nodeIdGener = NewNodeIDGenerator(c.store)
	c.rangeIdGener = NewRangeIDGenerator(c.store)
	c.tableIdGener = NewTableIDGenerator(c.store)
	c.databaseIDGener = NewDatabaseIDGenerator(c.store)
	c.close = false

	err := c.loadDatabase()
	if err != nil {
		log.Error("load database from store failed, err[%v]", err)
		return err
	}
	err = c.loadTable()
	if err != nil {
		log.Error("load table from store failed, err[%v]", err)
		return err
	}
	err = c.loadDeleteTable()
	if err != nil {
		log.Error("load delete table from store failed, err[%v]", err)
		return err
	}
	err = c.loadRange()
	if err != nil {
		log.Error("load range from store failed, err[%v]", err)
		return err
	}
	// must load at end
	err = c.loadMachine()
	if err != nil {
		log.Error("load machine from store failed, err[%v]", err)
		return err
	}
	err = c.loadInstance()
	if err != nil {
		log.Error("load node from store failed, err[%v]", err)
		return err
	}
	err = c.loadTask()
	if err != nil {
		log.Error("load task from store failed, err[%v]", err)
		return err
	}
	err = c.loadScheduleSwitch()
	if err != nil {
		log.Error("load task from store failed[cluster: %d, leader: %d], err[%v]", c.nodeId, c.leader, err)
		return err
	}

	c.loadUploadVersion()
	err = c.loadDeploy()
	if err != nil {
		log.Error("load deploy from store failed[cluster: %d, leader: %d], err[%v]", c.nodeId, c.leader, err)
		return err
	}
	return nil
}

func (c *Cluster) Close() {
	if c.close {
		return
	}
	c.cli.Close()
	c.close = true
}

func (c *Cluster) GenRangeId() (uint64, error) {
	log.Warn("[GENID] gen rangeId")
	return c.rangeIdGener.GenID()
}

func (c *Cluster) FindDatabase(name string) (*Database, bool) {
	return c.dbs.FindDb(name)
}

func (c *Cluster) FindDatabaseById(id uint64) (*Database, bool) {
	return c.dbs.FindDbById(id)
}

func (c *Cluster) DeleteDatabase(name string) error {
	return nil
}

func (c *Cluster) CreateDatabase(name string, properties string) (*Database, error) {
	if !c.IsLeader() {
		return nil, ErrNotLeader
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.FindDatabase(name); ok {
		log.Error("database name:%s is existed!", name)
		return nil, ErrDupDatabase
	}

	id, err := c.databaseIDGener.GenID()
	if err != nil {
		log.Error("gen database ID failed, err[%v]", err)
		return nil, ErrGenID
	}
	db := &metapb.DataBase{
		Name:       name,
		Id:         id,
		Properties: properties,
		Version:    0,
		CreateTime: time.Now().Unix(),
	}
	err = c.storeDatabase(db)
	if err != nil {
		log.Error("store database[%s] failed", name)
		return nil, err
	}
	database := NewDatabase(db)
	c.dbs.Add(database)
	log.Info("create database[%s] success", name)
	return database, nil
}

func (c *Cluster) GetAllDatabase() []*Database {
	return c.dbs.GetAllDatabase()
}

func (c *Cluster) FindCurTable(dbName, tableName string) (*Table, bool) {
	db, find := c.dbs.FindDb(dbName)
	if !find {
		return nil, false
	}
	return db.FindCurTable(tableName)
}

func (c *Cluster) FindTableById(dbId, tableId uint64) (*Table, bool) {
	db, find := c.dbs.FindDbById(dbId)
	if !find {
		return nil, false
	}
	return db.FindTableById(tableId)
}

func (c *Cluster) FindCurTableById(dbId, tableId uint64) (*Table, bool) {
	db, find := c.dbs.FindDbById(dbId)
	if !find {
		return nil, false
	}
	return db.FindCurTableById(tableId)
}

type ByLetter [][]byte

func (s ByLetter) Len() int           { return len(s) }
func (s ByLetter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByLetter) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) == -1 }

func rangeKeysSplit(keys, sep string) ([]string, error) {
	ks := strings.Split(keys, sep)

	kmap := make(map[string]interface{})
	for _, k := range ks {
		if _, found := kmap[k]; !found {
			kmap[k] = nil
		} else {
			return nil, fmt.Errorf("dup key in split keys: %v", k)
		}
	}
	return ks, nil
}

func encodeSplitKeys(keys []string, columns []*metapb.Column) ([][]byte, error) {
	var ret [][]byte
	for _, c := range columns {
		// 只按照第一主键编码
		if c.GetPrimaryKey() == 1 {
			for _, k := range keys {
				buf, err := util.EncodePrimaryKey(nil, c, []byte(k))
				if err != nil {
					return nil, err
				}
				ret = append(ret, buf)
			}
			// 只按照第一主键编码
			break
		}
	}
	return ret, nil
}

func (c *Cluster) EditTable(t *Table, properties string) error {
	columns, err := EditProperties(properties)
	if err != nil {
		return err
	}
	err = t.MergeColumn(columns, c)
	if err != nil {
		return err
	}
	return nil
}

// step 1. create table
// step 2. create range in remote
// step 3. add range in cache and disk
func (c *Cluster) CreateTable(dbName, tableName string, columns, regxs []*metapb.Column, pkDupCheck bool, policy *metapb.TableRwPolicy, ranges []*metapb.Range) (*Table, error) {
	if !c.IsLeader() {
		return nil, ErrNotLeader
	}
	for _, col := range columns {
		if isSqlReservedWord(col.Name) {
			log.Warn("col[%s] is sql reserved word", col.Name)
			return nil, ErrSqlReservedWord
		}
	}

	// check the table if exist
	db, find := c.FindDatabase(dbName)
	if !find {
		return nil, ErrNotExistDatabase
	}
	_, find = c.FindCurTable(dbName, tableName)
	if find {
		return nil, ErrDupTable
	}
	// create table
	tableId, err := c.tableIdGener.GenID()
	if err != nil {
		log.Error("cannot generte table[%s:%s] ID, err[%v]", dbName, tableName, err)
		return nil, ErrGenID
	}
	t := &metapb.Table{
		Name:   tableName,
		DbName: dbName,
		Id:     tableId,
		DbId:   db.GetId(),
		//Properties: properties,
		Columns:    columns,
		Regxs:      regxs,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
		PkDupCheck: pkDupCheck,
	}
	taskId, err := c.GenTaskId()
	if err != nil {
		log.Error("gen task id failed, err[%v]", err)
		return nil, err
	}
	task := &taskpb.Task{
		Type:   taskpb.TaskType_TableCreate,
		Meta:   &taskpb.TaskMeta{
			TaskId:   taskId,
			CreateTime: time.Now().Unix(),
			State: taskpb.TaskState_TaskRunning,
			Timeout: DefaultTableCreateTaskTimeout,
		},
		TableCreate: &taskpb.TaskTableCreate{
			DbId:      db.GetId(),
			TableId:   tableId,
			Ranges:    ranges,
		},
	}
	table := NewTable(t)
	table.Creating = true
	defer func() {
		table.Creating = false
	}()
	if policy == nil {
		table.RwPolicy = &metapb.TableRwPolicy{
			Policy: metapb.RwPolicy_RW_OnlyMaster,
			RwOnlyMaster: &metapb.TableOnlyMaster{},
		}
	} else {
		table.RwPolicy = policy
	}

	table.AddTask(NewTask(task))
	err = db.CreateTable(table)
	if err != nil {
		log.Error("create table[%s:%s] failed, err[%v]", dbName, tableName, err)
		return nil, err
	}
	batch := c.store.NewBatch()
	tt := deepcopy.Iface(t).(*metapb.Table)
	data, err := tt.Marshal()
	if err != nil {
		//TODO error
		db.DeleteTableById(tableId)
		return nil, err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	batch.Put(key, data)

	key = []byte(fmt.Sprintf("%s%d", PREFIX_RW_POLICY_TABLE, t.GetId()))
	data, err = table.RwPolicy.Marshal()
	if err != nil {
		//TODO error
		db.DeleteTableById(tableId)
		return nil, err
	}
	batch.Put(key, data)

	key = []byte(fmt.Sprintf("%s%d", PREFIX_TASK, taskId))
	data, err = task.Marshal()
	if err != nil {
		//TODO error
		db.DeleteTableById(tableId)
		return nil, err
	}
	batch.Put(key, data)
	err = batch.Commit()
	if err != nil {
		//TODO error
		log.Warn("create table[%s:%s] failed, err[%v]", dbName, tableName, err)
		db.DeleteTableById(tableId)
		return nil, err
	}
	// 暂时认为创建成功,解决range心跳上报产生range不存在的误报
	for _, r := range ranges {
		rng := NewRange(r)
		c.ranges.Add(rng)
		table.AddRange(rng)
		for _, peer := range r.GetPeers() {
			node, find := c.FindInstance(peer.GetNodeId())
			if find {
				node.AddRange(rng)
			}
		}
	}
	// create range remote
	// TODO collateral execution
	for _, r := range ranges {
		// 赋值
		r.DbId = db.GetId()
		r.TableId = tableId
		err = c.createRangeRemote(r)
		if err != nil {
			log.Warn("create range failed, range[%v] err[%v]", r, err)
			// 删除table
			batch = c.store.NewBatch()
			key := []byte(fmt.Sprintf("%s%d", PREFIX_DELETE_TABLE, t.GetId()))
			tbData, _ := tt.Marshal()
			batch.Put(key, tbData)
			key = []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
			batch.Delete(key)
			_err := batch.Commit()
			if _err != nil {
				//TODO error
				log.Warn("create table[%s:%s] failed, err[%v]", dbName, tableName, _err)
				db.DeleteCurTable(tableName)
				return nil, err
			}
			return nil, err
		}
	}

	tt.Epoch.Version++
	data, _ = tt.Marshal()
	key = []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	batch.Put(key, data)
	// 可以删除任务了
	key = []byte(fmt.Sprintf("%s%d", PREFIX_TASK, taskId))
	batch.Delete(key)
	err = batch.Commit()
	if err != nil {
		//TODO error
		db.DeleteCurTable(tableName)
		return nil, err
	}
	table.Table = tt
	table.DelTask(taskId)
	log.Info("create table[%s:%s] success", dbName, tableName)
	return table, nil
}

func (c *Cluster) DeleteCurTable(dbName, tableName string) (*Table, error) {
	db, find  := c.FindDatabase(dbName)
	if !find {
		return nil, ErrNotExistDatabase
	}
	table, find := db.FindCurTable(tableName)
	if !find {
		return nil, ErrNotExistTable
	}
	taskId, err := c.GenTaskId()
	if err != nil {
		log.Error("gen task id failed, err[%v]", err)
		return nil, err
	}
	task := &taskpb.Task{
		Type:   taskpb.TaskType_TableDelete,
		Meta:   &taskpb.TaskMeta{
			TaskId:   taskId,
			CreateTime: time.Now().Unix(),
			State: taskpb.TaskState_TaskRunning,
			Timeout: DefaultTableDeleteTaskTimeout,
		},
		TableDelete: &taskpb.TaskTableDelete{
			DbId:      table.GetDbId(),
			TableId:   table.GetId(),
		},
	}
	table.schemaLock.Lock()
	defer table.schemaLock.Unlock()
	batch := c.store.NewBatch()
	_t := deepcopy.Iface(table.Table).(*metapb.Table)
	_t.Epoch.ConfVer++
	data, _ := _t.Marshal()
	// 移送到删除列表
	key := []byte(fmt.Sprintf("%s%d", PREFIX_DELETE_TABLE, _t.GetId()))
	batch.Put(key, data)
	key = []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, _t.GetId()))
	batch.Delete(key)
	key = []byte(fmt.Sprintf("%s%d", PREFIX_RW_POLICY_TABLE, _t.GetId()))
	batch.Delete(key)

	data, err = task.Marshal()
	if err != nil {
		//TODO error
		return nil, err
	}
	key = []byte(fmt.Sprintf("%s%d", PREFIX_TASK, task.GetMeta().GetTaskId()))
	batch.Put(key, data)

	// close auto switch
	key = []byte(fmt.Sprintf(PREFIX_AUTO_TRANSFER_TABLE, _t.GetId()))
	sData := uint64ToBytes(uint64(1))
	batch.Put(key, sData)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_SHARDING_TABLE, _t.GetId()))
	batch.Put(key, sData)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_FAILOVER_TABLE, _t.GetId()))
	batch.Put(key, sData)
	err = batch.Commit()
	if err != nil {
		log.Warn("store task failed, err[%v]", err)
		return nil, err
	}
	err = c.AddTaskCache(NewTask(task))
	if err != nil {
		log.Error("add table delete task failed, err[%v]", err)
		return nil, err
	}
	if table.GetTask() == nil {
        log.Error("no delete table task !!!!!")
	}
	table.autoTransferUnable = true
	table.autoFailoverUnable = true
	table.autoShardingUnable = true
	table.Table = _t
	db.DeleteCurTable(tableName)
	return table, nil
}

func (c *Cluster) SavePackageVersion(version string) error {
	err := c.store.Put([]byte(DATASERVER_LATEST_VERSION), []byte(version))
	if err != nil {
		log.Error("save package version failed, err[%v]", err)
		return err
	}
	c.dataserverLatestVersion = version
	//c.service.router.Publish(ROUTE, &RouteData{
	//	tableId: TableId(table.GetDbName(), table.GetName()),
	//	data: &Data{
	//		version: table.Epoch.Version,
	//		data: table.routes,
	//	},
	//})
	//for _, p := range r.Peers {
	//	node, find := c.FindNode(p.GetNode().GetId())
	//	if !find {
	//		return ErrNotExistNode
	//	}
	//	node.DeleteRange(r.ID())
	//}
	//for _, p := range left.Peers {
	//	node, find := c.FindNode(p.GetNode().GetId())
	//	if !find {
	//		return ErrNotExistNode
	//	}
	//	node.AddRange(left)
	//}
	//for _, p := range right.Peers {
	//	node, find := c.FindNode(p.GetNode().GetId())
	//	if !find {
	//		return ErrNotExistNode
	//	}
	//	node.AddRange(right)
	//}
	//log.Info("range[%s:%s:%d] split[-> %s, %s] success", dbName, tableName, r.GetId(), left.String(), right.String())
	return nil
}

func (c *Cluster) UpdateAutoScheduleInfo(autoFailoverUnable, autoShardingUnable, autoTransferUnable bool) error {
	batch := c.store.NewBatch()
	var key, value []byte
	key = []byte(fmt.Sprintf(PREFIX_AUTO_TRANSFER, c.clusterId))
	if autoTransferUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_SHARDING, c.clusterId))
	if autoShardingUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_FAILOVER, c.clusterId))
	if autoFailoverUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	err := batch.Commit()
	if err != nil {
		log.Error("batch commit failed, err[%v]", err)
		return err
	}
	c.autoTransferUnable = autoTransferUnable
	c.autoFailoverUnable = autoFailoverUnable
	c.autoShardingUnable = autoShardingUnable
	log.Info("auto[T:%t F:%t S:%t]", c.autoTransferUnable, c.autoFailoverUnable, c.autoShardingUnable)
	return nil
}

func (c *Cluster) AddMachines(macs []*metapb.Machine) error {
	if len(macs) == 0 {
		return nil
	}
	batch := c.store.NewBatch()
	for _, m := range macs {
		key := []byte(fmt.Sprintf("%s%s", PREFIX_MACHINE, m.GetIp()))
		data, _ := m.Marshal()
		batch.Put(key, data)
	}
	err := batch.Commit()
	if err != nil {
		log.Warn("add machines failed, err[%v]", err)
		return err
	}
	var zone *Zone
	var room *Room
	var mac *Machine
	var find bool
	for _, m := range macs {
		if zone, find = c.zones.FindZone(m.GetZone().GetZoneName()); !find {
			zone = NewZone(m.GetZone())
			c.zones.Add(zone)
		}
		if room, find = zone.FindRoom(m.GetRoom().GetRoomName()); !find {
			room = NewRoom(m.GetRoom())
			zone.AddRoom(room)
		}
		if mac, find = room.GetMachine(m.GetIp()); !find {
			mac = NewMachine(m)
			room.AddMachine(mac)
			c.machines.Add(mac)
		}
	}
	return nil
}

func (c *Cluster) FindMachine(ip string) (*Machine, bool) {
	return c.machines.FindMachine(ip)
}

func (c *Cluster) SetDeploy(deploy *metapb.DeployV1Policy) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.deploy != nil {
		return ErrDupDeploy
	}
	err := c.storeDeploy(deploy)
	if err != nil {
		return err
	}
	c.deploy = deploy
	return nil
}

func (c *Cluster) prepareCreateRanges(dbName, tableName string, keys [][]byte) ([]*metapb.Range, error) {
	var err error
	var ranges []*metapb.Range
	var selecter Selecter
	var filter Filter
	//if c.conf.Mode == "debug" || c.deploy == nil {
	//	inss := c.GetAllActiveInstances()
	//	selecter = NewDebugDeploySelect(inss)
	//	filter = NewDebugInstanceFilter(len(inss), DefaultPeersNum)
	//} else {
		selecter = NewDeployV1Select(c.GetAllZones(), c.deploy)
		filter = NewDeployInstanceFilter()
	//}
	for i := 0; i <= len(keys); i++ {
		var nodes []*Instance
		nodes = selecter.SelectInstance(DefaultPeersNum, filter)
		if nodes == nil || len(nodes) < DefaultPeersNum {
			log.Error("cannot apply enough nodes, \n %v  \n %v", nodes,c.GetAllActiveInstances())
			return nil, ErrNotEnoughResources
		}

		var rng *metapb.Range
		if i ==0 && i == len(keys) {
			rng, err = c.prepareCreateRange(dbName, tableName, nil, nil, nodes)
		}else if i == 0 {
			rng, err = c.prepareCreateRange(dbName, tableName, nil, keys[i], nodes)
		} else if i == len(keys) {
			rng, err = c.prepareCreateRange(dbName, tableName, keys[i-1], nil, nodes)
		} else {
			rng, err = c.prepareCreateRange(dbName, tableName, keys[i-1], keys[i], nodes)
		}
		if err != nil {
			log.Error("create range failed, err[%v]", err)
			return nil, err
		}
		ranges = append(ranges, rng)
	}
	return ranges, nil
}

func (c *Cluster) GenTaskId() (uint64, error) {
	return c.taskIdGener.GenID()
}

func (c *Cluster) UpdateLeader(leader uint64) {
	c.leader = leader
}

func (c *Cluster) GetLeader() uint64 {
	return c.leader
}

func (c *Cluster) GetClusterId() uint64 {
	return c.clusterId
}

func (c *Cluster) IsLeader() bool {
	return c.nodeId == c.leader
}

func (c *Cluster) FindZone(name string) (*Zone, bool) {
	return c.zones.FindZone(name)
}

func (c *Cluster)GetAllZones() []*Zone {
	return c.zones.GetAllZones()
}

func (c *Cluster) storeDatabase(db *metapb.DataBase) error {
	database := deepcopy.Iface(db).(*metapb.DataBase)
	data, err := database.Marshal()
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_DB, db.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeTable(t *metapb.Table) error {
	table := deepcopy.Iface(t).(*metapb.Table)
	data, err := table.Marshal()
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) deleteTable(tableId uint64) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, tableId))
	return c.store.Delete(key)
}

func (c *Cluster) deleteRange(rangeId uint64) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, rangeId))
	return c.store.Delete(key)
}

func (c *Cluster) storeRange(r *metapb.Range) error {
	rng := deepcopy.Iface(r).(*metapb.Range)
	data, err := rng.Marshal()
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeNode(n *metapb.Node) error {
	node := deepcopy.Iface(n).(*metapb.Node)
	data, err := node.Marshal()
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_NODE, n.GetId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeTask(t *taskpb.Task) error {
	task := deepcopy.Iface(t).(*taskpb.Task)
	data, err := task.Marshal()
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TASK, t.GetMeta().GetTaskId()))
	return c.store.Put(key, data)
}

func (c *Cluster) storeDeploy(deploy *metapb.DeployV1Policy) error {
	dep := deepcopy.Iface(deploy).(*metapb.DeployV1Policy)
	data, err := dep.Marshal()
	if err != nil {
		//TODO error
		return err
	}
	key := []byte(DEPLOY_POLICY)
	return c.store.Put(key, data)
}

func (c *Cluster) deleteTask(id uint64) error {
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TASK, id))
	return c.store.Delete(key)
}

func (c *Cluster) loadDatabase() error {
	prefix := []byte(PREFIX_DB)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		db := new(metapb.DataBase)
		err := proto.Unmarshal(it.Value(), db)
		if err != nil {
			return err
		}
		database := NewDatabase(db)
		c.dbs.Add(database)
	}
	return nil
}

func (c *Cluster) loadTable() error {
	prefix := []byte(PREFIX_TABLE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		t := new(metapb.Table)
		err := proto.Unmarshal(it.Value(), t)
		if err != nil {
			return err
		}

		db, find := c.FindDatabase(t.DbName)
		if !find {
			log.Error("database[%s] not found", t.DbName)
			return ErrNotExistDatabase
		}

		table := NewTable(t)
		table.loadScheduleSwitch(c)
		table.loadRwPolicy(c)
		db.AddTable(table)
	}
	return nil
}

func (c *Cluster) loadDeleteTable() error {
	prefix := []byte(PREFIX_DELETE_TABLE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		t := new(metapb.Table)
		err := proto.Unmarshal(it.Value(), t)
		if err != nil {
			return err
		}
        log.Debug("load delete table %v", t)
		db, find := c.FindDatabase(t.DbName)
		if !find {
			log.Error("database[%s] not found", t.DbName)
			return ErrNotExistDatabase
		}

		table := NewTable(t)
		table.loadScheduleSwitch(c)
		db.AddDeleteTable(table)
	}
	return nil
}

func (c *Cluster) loadRange() error {
	prefix := []byte(PREFIX_RANGE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		r := new(metapb.Range)
		err := proto.Unmarshal(it.Value(), r)
		if err != nil {
			return err
		}

		t, find := c.FindTableById(r.GetDbId(), r.GetTableId())
		if !find {
			log.Error("table[%d:%d] not found", r.GetDbId(), r.GetTableId())
			return ErrNotExistTable
		}
		rr := NewRange(r)
		t.AddRange(rr)
		c.ranges.Add(rr)
	}

	return nil
}

func (c *Cluster) loadMachine() error {
	prefix := []byte(PREFIX_MACHINE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	var zone *Zone
	var room *Room
	var find bool
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		m := new(metapb.Machine)
		err := proto.Unmarshal(it.Value(), m)
		if err != nil {
			return err
		}
        if zone, find = c.zones.FindZone(m.GetZone().GetZoneName()); !find {
	        zone = NewZone(m.GetZone())
	        c.zones.Add(zone)
        }
		if room, find = zone.FindRoom(m.GetRoom().GetRoomName()); !find {
			room = NewRoom(m.GetRoom())
			zone.AddRoom(room)
		}
		mac := NewMachine(m)
		room.AddMachine(mac)
		c.machines.Add(mac)
	}
	return nil
}

func (c *Cluster) loadInstance() error {
	prefix := []byte(PREFIX_NODE)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		n := new(metapb.Node)
		err := proto.Unmarshal(it.Value(), n)
		if err != nil {
			return err
		}
		if n.State == metapb.NodeState_N_Login {
			n.State = metapb.NodeState_N_Initial
		}
		ins := NewInstance(n)
		//ip, _, err := net.SplitHostPort(n.GetAddress())
		//if err != nil {
		//	log.Error("invalid address %s", n.GetAddress())
		//	return err
		//}
		// TODO machines gray upgrade
		//if mac, find := c.machines.FindMachine(ip); !find {
		//	log.Error("invalid machine %s", ip)
		//	return ErrNotExistMac
		//} else {
		//	mac.AddInstance(ins)
		//}
		c.instances.Add(ins)
		c.instancesAddr.Add(ins)
		for _, db := range c.dbs.GetAllDatabase() {
			for _, table := range db.GetAllTable() {
				ranges := table.CollectRangesByNodeId(n.Id)
				for _, r := range ranges {
					ins.AddRange(r)
				}
			}
		}
	}
	return nil
}

func (c *Cluster) loadTask() error {
	prefix := []byte(PREFIX_TASK)
	startKey, limitKey := bytesPrefix(prefix)
	it := c.store.Scan(startKey, limitKey)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if k == nil {
			continue
		}
		t := new(taskpb.Task)
		err := proto.Unmarshal(it.Value(), t)
		if err != nil {
			return err
		}
		log.Debug("task %v", t)
		if t.GetMeta().GetState() == taskpb.TaskState_TaskSuccess {
			continue
		}
		task := NewTask(t)
		switch task.GetType() {
		case taskpb.TaskType_RangeFailOver:
			err = task.GenSubTasks(c)
			if err != nil {
				log.Error("gen sub tasks failed, err[%v]", err)
				return nil
			}
		case taskpb.TaskType_RangeTransfer:
			err = task.GenSubTasks(c)
			if err != nil {
				log.Error("gen sub tasks failed, err[%v]", err)
				return nil
			}
		case taskpb.TaskType_RangeSplit:
			err = task.GenSubTasks(c)
			if err != nil {
				log.Error("gen sub tasks failed, err[%v]", err)
				return nil
			}
		}
		c.AddTaskCache(task)
	}
	return nil
}

func (c *Cluster) loadAutoTransfer() error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_TRANSFER, c.clusterId)
	value, err := c.store.Get([]byte(key))
	if err != nil {
		if err == sErr.ErrNotFound {
			s = uint64(0)
		} else {
			return err
		}
	}
	var auto bool
	auto = true // enable by default
	if value != nil {
		s, err = bytesToUint64(value)
		if err != nil {
			return err
		}
	}
	if s == 0 {
		auto = false
	}
	c.autoTransferUnable = auto
	return nil
}

func (c *Cluster) loadAutoSharding() error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_SHARDING, c.clusterId)
	value, err := c.store.Get([]byte(key))
	if err != nil {
		if err == sErr.ErrNotFound {
			s = uint64(0)
		} else {
			return err
		}
	}
	var auto bool
	auto = true
	if value != nil {
		s, err = bytesToUint64(value)
		if err != nil {
			return err
		}
	}
	if s == 0 {
		auto = false
	}
	c.autoShardingUnable = auto
	return nil
}

func (c *Cluster) loadAutoFailover() error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_FAILOVER, c.clusterId)
	value, err := c.store.Get([]byte(key))
	if err != nil {
		if err == sErr.ErrNotFound {
			s = uint64(0)
		} else {
			return err
		}
	}
	var auto bool
	auto = true
	if value != nil {
		s, err = bytesToUint64(value)
		if err != nil {
			return err
		}
	}
	if s == 0 {
		auto = false
	}
	c.autoFailoverUnable = auto
	return nil
}

func (c *Cluster) loadScheduleSwitch() error {
	if err := c.loadAutoFailover(); err != nil {
		log.Error("load auto failover failed, err[%v]", err)
		return err
	}
	if err := c.loadAutoSharding(); err != nil {
		log.Error("load auto sharding failed, err[%v]", err)
		return err
	}
	if err := c.loadAutoTransfer(); err != nil {
		log.Error("load auto transfer failed, err[%v]", err)
		return err
	}
	return nil
}

func (c *Cluster) loadUploadVersion() {
	if v, err := c.store.Get([]byte(DATASERVER_LATEST_VERSION)); err == nil {
		c.dataserverLatestVersion = string(v)
	}
}

func (c *Cluster) loadDeploy() error {
	var v []byte
	var err error
	if v, err = c.store.Get([]byte(DEPLOY_POLICY)); err != nil {
		if err == sErr.ErrNotFound {
			return nil
		}
		return err
	}
	if len(v) == 0 {
		return nil
	}
	p := new(metapb.DeployV1Policy)
	err = proto.Unmarshal(v, p)
	if err != nil {
		return err
	}
	log.Debug("load deploy %v", p)
	c.deploy = p
	return nil
}

func bytesPrefix(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}

var SQLReservedWord = []string{
	"abs", "absolute", "action", "add", "all", "allocate", "alter", "analyse", "analyze", "and", "any", "are", "array",
	"array_agg", "array_max_cardinality", "as", "asc", "asensitive", "assertion", "asymmetric", "at", "atomic", "attributes",
	"authorization", "avg", "begin", "begin_frame", "begin_partition", "between", "bigint", "binary", "bit", "bit_length",
	"blob", "boolean", "both", "by", "call", "called", "cardinality", "cascade", "cascaded", "case", "cast", "catalog", "ceil",
	"ceiling", "char", "character", "character_length", "char_length", "check", "clob", "close", "coalesce", "collate",
	"collation", "collect", "column", "commit", "condition", "connect", "connection", "constraint", "constraints", "contains",
	"continue", "convert", "corr", "corresponding", "count", "covar_pop", "covar_samp", "create", "cross", "cube", "cume_dist",
	"current", "current_catalog", "current_date", "current_default_transform_group", "current_path", "current_role",
	"current_row", "current_schema", "current_time", "current_timestamp", "current_transform_group_for_type",
	"current_user", "cursor", "cycle", "datalink", "date", "day", "deallocate", "dec", "decimal", "declare", "default",
	"deferrable", "deferred", "delete", "dense_rank", "deref", "desc", "describe", "descriptor", "deterministic",
	"diagnostics", "disconnect", "distinct", "dlnewcopy", "dlpreviouscopy", "dlurlcomplete", "dlurlcompleteonly",
	"dlurlcompletewrite", "dlurlpath", "dlurlpathonly", "dlurlpathwrite", "dlurlscheme", "dlurlserver", "dlvalue", "do",
	"domain", "double", "drop", "dynamic", "each", "element", "else", "end", "end-exec", "end_frame", "end_partition", "equals",
	"escape", "every", "except", "exception", "exec", "execute", "exists", "external", "extract", "false", "fetch", "filter",
	"first", "first_value", "float", "floor", "for", "foreign", "found", "frame_row", "free", "from", "full", "function",
	"fusion", "get", "global", "go", "goto", "grant", "group", "grouping", "groups", "having", "hold", "hour", "identity",
	"immediate", "import", "in", "indicator", "initially", "inner", "inout", "input", "insensitive", "insert", "int", "integer",
	"intersect", "intersection", "interval", "into", "is", "isolation", "join", "key", "lag", "language", "large", "last",
	"last_value", "lateral", "lead", "leading", "left", "level", "like", "like_regex", "limit", "ln", "local", "localtime",
	"localtimestamp", "lower", "match", "max", "max_cardinality", "member", "merge", "method", "min", "minute", "mod",
	"modifies", "module", "month", "multiset", "names", "national", "natural", "nchar", "nclob", "new", "next", "no", "none",
	"normalize", "not", "nth_value", "ntile", "null", "nullif", "numeric", "occurrences_regex", "octet_length", "of", "offset",
	"old", "on", "only", "open", "option", "or", "order", "out", "outer", "output", "over", "overlaps", "overlay", "pad",
	"parameter", "partial", "partition", "percent", "percentile_cont", "percentile_disc", "percent_rank", "period", "placing",
	"portion", "position", "position_regex", "power", "precedes", "precision", "prepare", "preserve", "primary", "prior",
	"privileges", "procedure", "public", "range", "rank", "read", "reads", "real", "recursive", "ref", "references",
	"referencing", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy",
	"regr_syy", "relative", "release", "restrict", "result", "return", "returned_cardinality", "returning", "returns",
	"revoke", "right", "rollback", "rollup", "row", "rows", "row_number", "savepoint", "schema", "scope", "scroll", "search",
	"second", "section", "select", "sensitive", "session", "session_user", "set", "similar", "size", "smallint", "some", "space",
	"specific", "specifictype", "sql", "sqlcode", "sqlerror", "sqlexception", "sqlstate", "sqlwarning", "sqrt", "start",
	"static", "stddev_pop", "stddev_samp", "submultiset", "substring", "substring_regex", "succeeds", "sum", "symmetric",
	"system", "system_time", "system_user", "table", "tablesample", "temporary", "then", "time", "timestamp", "timezone_hour",
	"timezone_minute", "to", "trailing", "transaction", "translate", "translate_regex", "translation", "treat", "trigger",
	"trim", "trim_array", "true", "truncate", "uescape", "union", "unique", "unknown", "unnest", "update", "upper", "usage",
	"user", "using", "value", "values", "value_of", "varbinary", "varchar", "variadic", "varying", "var_pop", "var_samp",
	"versioning", "view", "when", "whenever", "where", "width_bucket", "window", "with", "within", "without", "work", "write",
	"xml", "xmlagg", "xmlattributes", "xmlbinary", "xmlcast", "xmlcomment", "xmlconcat", "xmldocument", "xmlelement",
	"xmlexists", "xmlforest", "xmliterate", "xmlnamespaces", "xmlparse", "xmlpi", "xmlquery", "xmlserialize", "xmltable",
	"xmltext", "xmlvalidate", "year", "zone",
}

func isSqlReservedWord(col string) bool {
	for _, w := range SQLReservedWord {
		if col == w {
			return true
		}
	}
	return false
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func (c *Cluster) SelectTransferInstance(r *Range, expectRoom *Room) *Instance {
	log.Debug("select transfer instance: expectRoom %v", expectRoom)
	if expectRoom == nil {
		log.Warn("select transfer instance: expectRoom is nil")
		return nil
	}
	var selecter Selecter
	var filter Filter
	//if c.cluster.conf.Mode == "debug" || c.cluster.deploy == nil {
	//	selecter = NewDebugTransferSelect(c.cluster.GetAllActiveInstances())
	//	var except []*Instance
	//	for _, p := range r.Peers {
	//		n, find := c.cluster.FindInstance(p.NodeId)
	//		if !find {
	//			log.Error("invalid node[%d]", p.NodeId)
	//			return nil
	//		}
	//		except = append(except, n)
	//	}
	//	filter = NewDebugTransferFilter(except)
	//} else {

	selecter = NewTransferSelect(c.GetAllZones(), c.deploy)

	var switchsMap, macsMap map[string]struct{}
	switchsMap = make(map[string]struct{})
	macsMap = make(map[string]struct{})
	for _, p := range r.Peers {
		ins, find := c.FindInstance(p.NodeId)
		if !find {
			log.Error("invalid node[%d]", p.NodeId)
			return nil
		}
		mac, find := c.FindMachine(ins.MacIp)
		if !find {
			log.Error("machine[%s] not exist", ins.MacIp)
			return nil
		}
		macsMap[mac.GetIp()] = struct {}{}
		switchsMap[mac.GetSwitchIp()] = struct {}{}
	}
	filter = NewTransferFilter(c, expectRoom, switchsMap, macsMap)
	//}
	nodes := selecter.SelectInstance(1, filter)
	if len(nodes) == 0 {
		var nodesInfo string
		for _, node := range c.GetAllInstances() {
			nodesInfo += fmt.Sprintf(`{ node[%s] state[%s] tasks num[%d] free disk[%d] }`,
				node.GetAddress(), node.GetState().String(), len(node.AllTask()), node.GetDiskFree())
		}
		log.Warn("schedule select node resource failed, all nodes info[%s]", nodesInfo)
		message := fmt.Sprintf("分片[%s], 补充副本, 没有可用的节点资源", r.SString())
		c.alarm.Alarm("分片故障恢复异常", message)
		return nil
	}
	return nodes[0]
}
