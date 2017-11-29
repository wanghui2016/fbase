package server

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"sort"

	sErr "engine/errors"
	"model/pkg/metapb"
	"util/deepcopy"
	"util/log"
	"github.com/juju/errors"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
	"github.com/golang/protobuf/proto"
)

var (
	MAX_COLUMN_NAME_LENGTH = 128
)

var PREFIX_AUTO_SHARDING_TABLE string = fmt.Sprintf("$auto_sharding_table_%d")
var PREFIX_AUTO_TRANSFER_TABLE string = fmt.Sprintf("$auto_transfer_table_%d")
var PREFIX_AUTO_FAILOVER_TABLE string = fmt.Sprintf("$auto_failover_table_%d")
var PREFIX_RW_POLICY_TABLE     string = fmt.Sprintf("$rw_policy_table_%d")

const (
	maxRoutesCheckInternal = time.Minute * 31
)

type Table struct {
	*metapb.Table
	Creating           bool
	dbName  string
	// 表属性锁
	schemaLock sync.RWMutex
	// 路由锁
	lock sync.Mutex
	maxColId uint64
	ranges *RangeCache
	/**columns map[string]*metapb.Column*/

	options *metapb.Option
	//
	//routes []*metapb.Route
	tasks *TaskCache

	LastRoutesCheckTime time.Time
	LastCheckEpoch *metapb.TableEpoch
	//routes []*metapb.Route

	// 是否支持数据分片，默认支持数据分片
	autoShardingUnable bool
	// 是否支持数据迁移
	autoTransferUnable bool
	// 是否支持failOver
	autoFailoverUnable bool

	// 读写策略
	RwPolicy *metapb.TableRwPolicy

	// 缓存的拓扑
	LastTopology *metapb.TopologyEpoch
	LastTopologyUpdateTime time.Time
}

func NewTable(t *metapb.Table) *Table {
	var maxColId uint64
	for _, col := range t.GetColumns() {
		maxColId = col.GetId()
	}
	table := &Table{
		Table:  t,
		maxColId: maxColId,
		ranges: NewRangeCache(),
		tasks: NewTaskCache(),
		LastRoutesCheckTime: time.Now(),
		LastCheckEpoch: &metapb.TableEpoch{},
		//tasks: NewTaskCache(),
	}
	return table
}

func (t *Table)GenColId() uint64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.maxColId++
	return t.maxColId
}

//type Column struct {
//	Name         string `json:"name"`
//	DataType     string `json:"datatype"`
//	Nullable     bool   `json:"nullable"`
//	PrimaryKey   bool   `json:"primarykey"`
//	Index        bool   `json:"index"`
//	DefaultValue []byte `json:"defaultvalue"`
//	Unsigned     bool   `json:"unsigned"`
//}

type TableProperty struct {
	Columns []*metapb.Column `json:"columns"`
	Regxs []*metapb.Column `json:"regxs"`
}

func (t *Table) Name() string {
	return t.GetName()
}

func (t *Table) GetRangeNumber() int {
	return t.ranges.Size()
}

type ByPrimaryKey []*metapb.Column

func (s ByPrimaryKey) Len() int           { return len(s) }
func (s ByPrimaryKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByPrimaryKey) Less(i, j int) bool { return s[i].PrimaryKey < s[j].PrimaryKey }

func (t *Table) GetColumns() []*metapb.Column {
	var columns []*metapb.Column
	_columns := t.Columns
	for _, c := range _columns {
		columns = append(columns, c)
	}
	return columns
}

func (t *Table) GetColumnByName(name string) (*metapb.Column, bool) {
	_columns := t.Columns
	for _, c := range _columns {
		if c.GetName() == name {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) GetColumnById(id uint64) (*metapb.Column, bool) {
	_columns := t.Columns
	for _, c := range _columns {
		if c.Id == id {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) MergeColumn(source []*metapb.Column, cluster *Cluster) (error) {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	table := deepcopy.Iface(t.Table).(*metapb.Table)

	for _, col := range source {
		col.Name = strings.ToLower(col.GetName())
		if len(col.GetName()) > MAX_COLUMN_NAME_LENGTH {
			return ErrColumnNameTooLong
		}
		if isSqlReservedWord(col.GetName()) {
			log.Warn("col[%s] is sql reserved word", col.GetName())
			return ErrSqlReservedWord
		}

		if col.GetId() == 0 { // add column
			if col.PrimaryKey == 1 {
				log.Warn("pk column is not allow change")
				return ErrInvalidColumn
			}
			if _, find := t.GetColumnByName(col.GetName()); find {
				log.Warn("column[%s:%s:%s] is already existed", t.GetDbName(), t.GetName(), col.GetName())
				return ErrDupColumnName
			}
			col.Id = t.GenColId()
			table.Columns = append(table.Columns, col)
			table.Epoch.ConfVer++
		}else { // column maybe rename
			tt := NewTable(table)
			col_, find := tt.GetColumnById(col.GetId())
			if !find {
				log.Warn("column[%s:%s:%s] is not exist", tt.GetDbName(), tt.GetName(), col.GetId())
				return ErrInvalidColumn
			}
			if col_.Name == col.GetName() {
				continue
			}

			col_.Name = col.GetName()
			table.Epoch.ConfVer++
		}
	}
	err := cluster.storeTable(table)
	if err != nil {
		log.Error("store table failed, err[%v]", err)
		return err
	}
	t.Table = table
	return nil
}

func (t *Table) AddTask(task *Task) bool {
	return t.tasks.SetNx(task)
}

func (t *Table) DelTask(id uint64) {
	t.tasks.Delete(id)
}

func (t *Table) GetTask() *Task {
	tasks :=  t.tasks.GetAllTask()
	if len(tasks) > 1 {
		log.Panic("internal error, more than one tasks in table[%s:%s]",
			t.GetDbName(), t.GetName())
	}
	if len(tasks) == 1 {
		return tasks[0]
	}
	return nil
}

func (t *Table) AddRange(r *Range) {
	t.ranges.Add(r)
}

func (t *Table) DeleteRange(id uint64) {
	t.ranges.Delete(id)
}

func (t *Table) FindRange(id uint64) (*Range, bool) {
	return t.ranges.FindRange(id)
}

func (t *Table) GetAllRanges() []*Range {
	return t.ranges.GetAllRange()
}

func (t *Table) CollectRangesByNodeId(nodeId uint64) []*Range {
	return t.ranges.CollectRangesByNodeId(nodeId)
}

func (t *Table) loadScheduleSwitch(c *Cluster) error {
	if err := t.loadAutoFailover(c); err != nil {
		log.Error("table load auto failover failed, err[%v]", err)
		return err
	}
	if err := t.loadAutoSharding(c); err != nil {
		log.Error("table load auto sharding failed, err[%v]", err)
		return err
	}
	if err := t.loadAutoTransfer(c); err != nil {
		log.Error("table load auto transfer failed, err[%v]", err)
		return err
	}
	return nil
}

func (t *Table) loadAutoTransfer(cluster *Cluster) error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_TRANSFER_TABLE, t.GetId())

	value, err := cluster.store.Get([]byte(key))
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
	t.autoTransferUnable = auto
	return nil
}

func (t *Table) loadAutoSharding(cluster *Cluster) error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_SHARDING_TABLE, t.GetId())
	value, err := cluster.store.Get([]byte(key))
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
	t.autoShardingUnable = auto
	return nil
}

func (t *Table) loadAutoFailover(cluster *Cluster) error {
	var s uint64
	s = uint64(1)
	key := fmt.Sprintf(PREFIX_AUTO_FAILOVER_TABLE, t.GetId())
	value, err := cluster.store.Get([]byte(key))
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
	t.autoFailoverUnable = auto
	return nil
}

func (t *Table) loadRwPolicy(cluster *Cluster) error {
	var policy *metapb.TableRwPolicy
	key := fmt.Sprintf(PREFIX_RW_POLICY_TABLE, t.GetId())

	value, err := cluster.store.Get([]byte(key))
	if err != nil {
		if err == sErr.ErrNotFound {
			policy = &metapb.TableRwPolicy{
				Policy: metapb.RwPolicy_RW_OnlyMaster,
				RwOnlyMaster: &metapb.TableOnlyMaster{},
			}
		} else {
			return err
		}
	}
	if value != nil {
		policy = new(metapb.TableRwPolicy)
		err = proto.Unmarshal(value, policy)
		if err != nil {
			log.Error("invalid rw policy!!!!, err[%v]", err)
			return err
		}
	}

	t.RwPolicy = policy
	return nil
}

func (t *Table) UpdateAutoScheduleInfo(cluster *Cluster,autoFailoverUnable, autoShardingUnable, autoTransferUnable bool) error {
	batch := cluster.store.NewBatch()
	var key, value []byte
	key = []byte(fmt.Sprintf(PREFIX_AUTO_TRANSFER_TABLE, t.GetId()))
	if autoTransferUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_SHARDING_TABLE, t.GetId()))
	if autoShardingUnable {
		value = uint64ToBytes(uint64(1))
	} else {
		value = uint64ToBytes(uint64(0))
	}
	batch.Put(key, value)
	key = []byte(fmt.Sprintf(PREFIX_AUTO_FAILOVER_TABLE, t.GetId()))
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
	t.autoTransferUnable = autoTransferUnable
	t.autoFailoverUnable = autoFailoverUnable
	t.autoShardingUnable = autoShardingUnable
	return nil
}

func (t *Table) RoutesUpdate(cluster *Cluster, r *Range) error {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	batch := cluster.store.NewBatch()
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	table.Epoch.Version++
	data, err := table.Marshal()
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	batch.Put(key, data)
	key = []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
	rr := deepcopy.Iface(r.Range).(*metapb.Range)
	data, err = rr.Marshal()
	if err != nil {
		return err
	}
	batch.Put(key, data)
	err = batch.Commit()
	if err != nil {
		return err
	}

	t.ranges.Add(r)
	cluster.ranges.Add(r)
	for _, p := range r.Peers {
		node, find := cluster.FindInstance(p.GetNodeId())
		if !find {
			log.Error("node[%d] not found", p.GetNodeId())
			return ErrNotExistNode
		}
		node.AddRange(r)
	}
	//保证路由的可靠完整
	t.Table.Epoch.Version++
	return nil
}

func (t *Table) RangeSplit(old *Range, news []*Range, cluster *Cluster) error {
	if old == nil || len(news) == 0 {
		return nil
	}
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	batch := cluster.store.NewBatch()
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	table.Epoch.Version++
	data, err := table.Marshal()
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	batch.Put(key, data)
	r := deepcopy.Iface(old.Range).(*metapb.Range)
	r.State = metapb.RangeState_R_Offline
	data, err = r.Marshal()
	if err != nil {
		return err
	}
	key = []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, old.GetId()))
	batch.Put(key, data)
	var tasks []*Task
	for _, new := range news {
		r := deepcopy.Iface(new.Range).(*metapb.Range)
		data, err = r.Marshal()
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, new.GetId()))
		batch.Put(key, data)
		taskId, err := cluster.GenTaskId()
		if err != nil {
			log.Error("gen task ID failed, err[%v]", err)
			return nil
		}
		// 新分裂的分片挂一个空任务，为了调度管理区分failover
		t := &taskpb.Task{
			Type:   taskpb.TaskType_EmptyRangeTask,
			Meta:   &taskpb.TaskMeta{
				TaskId:   taskId,
				CreateTime: time.Now().Unix(),
				State: taskpb.TaskState_TaskRunning,
				Timeout: DefaultEmptyTaskTimeout,
			},
			RangeEmpty: &taskpb.TaskRangeEmpty{
				RangeId: r.GetId(),
			},
		}
		data, err = t.Marshal()
		if err != nil {
			return err
		}
		key = []byte(fmt.Sprintf("%s%d", PREFIX_TASK, t.GetMeta().GetTaskId()))
		batch.Put(key, data)
		tasks = append(tasks, NewTask(t))
	}

	err = batch.Commit()
	if err != nil {
		return err
	}
	//保证路由的可靠完整
	for _, in := range news {
		t.ranges.Add(in)
		cluster.ranges.Add(in)
		for _, p := range in.Peers {
			node, find := cluster.FindInstance(p.GetNodeId())
			if !find {
				log.Error("node[%d] not found", p.GetNodeId())
				return ErrNotExistNode
			}
			node.AddRange(in)
		}
	}
	for _, task := range tasks {
		cluster.AddTaskCache(task)
	}
	old.State = metapb.RangeState_R_Offline
	t.Table.Epoch.Version++
	return nil
}

func checkTTLDataType(dataType metapb.DataType) bool {
	return metapb.DataType_BigInt == dataType
}

func (t *Table) UpdateSchema(columns []*metapb.Column, store Store) ([]*metapb.Column, error) {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	table := deepcopy.Iface(t.Table).(*metapb.Table)
	//not concurrent safy,need in syncronized block
	cols := make([]*metapb.Column, 0)
	allCols := make([]*metapb.Column, 0)
	colMap := make(map[string]*metapb.Column)
	for _, tempCol := range table.Columns {
		colMap[tempCol.GetName()] = tempCol
		allCols = append(allCols, tempCol)
	}

	var match bool
	for _, newCol := range columns {
		_, ok := colMap[newCol.GetName()]
		if ok {
			continue
		}
		//scan template column
		for _, tempCol := range table.Regxs {
			isMatch, err := regexp.Match(tempCol.GetName(), []byte(newCol.Name))
			if err == nil && isMatch {
				if isSqlReservedWord(newCol.Name) {
					log.Warn("col[%s:%s:%s] is sql reserved word",
						table.GetDbName(), table.GetName(), newCol.Name)
					return nil, ErrSqlReservedWord
				}
				addCol := deepcopy.Iface(tempCol).(*metapb.Column)
				addCol.Name = newCol.Name
				addCol.Id = t.GenColId()
				cols = append(cols, addCol)
				allCols = append(allCols, addCol)
				match = true
				break
			}
		}
	}
	if match == false {
		return nil, errors.New("none of columns matches")
	}
	props, err := ToTableProperty(allCols)
	if err != nil {
		return nil, err
	}
	table.Columns = allCols
	table.Properties = props
	table.Epoch.ConfVer++
	data, err := table.Marshal()
	if err != nil {
		return nil, err
	}
	batch := store.NewBatch()
	key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, t.GetId()))
	batch.Put(key, data)
	err = batch.Commit()
	if err != nil {
		return nil, err
	}
	t.Table = table
	//t.routes = t.genRoutes()
	return cols, nil
}

func (t *Table) GetTopologyEpoch(cluster *Cluster, epoch *metapb.TableEpoch) (*metapb.TopologyEpoch, error) {
	t.schemaLock.RLock()
	defer t.schemaLock.RUnlock()
	_epoch := t.Epoch
	policy := t.RwPolicy
	// create or delete
	if t.tasks.Size() > 0 {
        return nil, ErrNotExistTable
	}
	// 第一次更新; 版本有变化; 间隔一分钟
	if t.LastTopology == nil || (_epoch.GetConfVer() > t.LastTopology.GetEpoch().GetConfVer() ||
		_epoch.GetVersion() > t.LastTopology.GetEpoch().GetVersion()) ||
		time.Since(t.LastTopologyUpdateTime) >= time.Second * 60 {
		lastTopologyUpdateTime := t.LastTopologyUpdateTime
		t.lock.Lock()
		defer t.lock.Unlock()
		// 并发更新检查
		if !lastTopologyUpdateTime.IsZero() && !lastTopologyUpdateTime.Equal(t.LastTopologyUpdateTime) {
			return t.LastTopology, nil
		}
		routes := t.genRoutes(cluster)
		t.LastTopology = &metapb.TopologyEpoch{
			DbId: t.GetDbId(),
			TableId: t.GetId(),
			Routes: routes,
			Epoch: _epoch,
			RwPolicy: policy,
		}
		t.LastTopologyUpdateTime = time.Now()
	}

	return t.LastTopology, nil
}

func (t *Table) genRoutes(cluster *Cluster) []*metapb.Route {
	var routes []*metapb.Route
	start := time.Now()
	for _, r := range t.ranges.GetAllRange() {
		if r.State == metapb.RangeState_R_Normal || r.State == metapb.RangeState_R_Init {
			var leader *metapb.Leader
			if r.Leader == nil {
				if len(r.Peers) == 0 {
					log.Error("invalid range[%v]", r.String())
					return nil
				}
				node, find := cluster.FindInstance(r.Peers[0].GetNodeId())
				if !find {
					log.Error("node[%d] not found", r.Peers[0].GetNodeId())
					return nil
				}
				leader = &metapb.Leader{RangeId: r.ID(), NodeId: node.GetId(), NodeAddr: node.GetAddress()}
			} else {
				leader = r.Leader
			}
			rng := deepcopy.Iface(r.Range).(*metapb.Range)
			replicas := make([]*metapb.Replica, 0, len(rng.GetPeers()))
			for _, peer := range rng.GetPeers() {
				ins, find := cluster.FindInstance(peer.GetNodeId())
				if !find {
					log.Error("node[%d] not found", peer.GetNodeId())
					return nil
				}
				var logIndex uint64
				if r.RaftStatus == nil {
					logIndex = 0
				} else {
					for _, rep := range r.RaftStatus.Replicas {
						if rep.GetID() == peer.GetNodeId() {
							logIndex = rep.GetNext()
						}
					}
				}
				replica := &metapb.Replica{
					NodeId: peer.GetNodeId(),
					NodeAddr: ins.GetAddress(),
					LogIndex: logIndex,
				}
				replicas = append(replicas, replica)
			}
			ro := &metapb.Route{
				Id:     rng.GetId(),
				StartKey: rng.GetStartKey(),
				EndKey: rng.GetEndKey(),
				RangeEpoch: rng.GetRangeEpoch(),
				Leader: leader,
				Replicas: replicas,
			}
			routes = append(routes, ro)
		}
	}
	tt := time.Since(start)
	if tt > time.Second {
		log.Warn("table gen routes timeout[%s]", t.String())
	}
	return routes
}

func (t *Table) RoutesCheck(ctx context.Context, cluster *Cluster, alarm Alarm) {
	var conflict bool
	var _ranges []*Range

	// 只优化定时检查
	if t.GetEpoch().GetVersion() == t.LastCheckEpoch.GetVersion() {
		return
	}
	topo, err := t.GetTopologyEpoch(cluster, t.LastCheckEpoch)
	if err != nil {
		return
	}
	if t.LastCheckEpoch.GetVersion() == topo.GetEpoch().GetVersion() {
		return
	}
	t.LastCheckEpoch = topo.GetEpoch()
    routes := topo.GetRoutes()
	t.LastRoutesCheckTime = time.Now()
	// 检查冲突的分片
	for i := 0; i < len(routes); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		rng, find := cluster.FindRange(routes[i].GetRangeId())
		if !find {
			return
		}
		_ranges = append(_ranges, rng)
		for j := i + 1; j < len(routes); j++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// j包含i
			if metapb.Compare(routes[i].StartKey, routes[j].StartKey) >= 0 && metapb.Compare(routes[i].StartKey, routes[j].EndKey) < 0 {
				eR, find := cluster.FindRange(routes[i].GetId())
				if !find {
					return
				}
				cR, find := cluster.FindRange(routes[j].GetId())
				if !find {
					return
				}
				message := fmt.Sprintf("异常分片[%s],　冲突分片[%s], 范围有重叠", eR.SString(), cR.SString())
				log.Warn("路由异常: %s", message)
				alarm.Alarm("路由异常报警", message)
				conflict = true
				break
			} else if metapb.Compare(routes[i].EndKey, routes[j].StartKey) > 0 && metapb.Compare(routes[i].EndKey, routes[j].EndKey) <= 0 {
				eR, find := cluster.FindRange(routes[i].GetId())
				if !find {
					return
				}
				cR, find := cluster.FindRange(routes[j].GetId())
				if !find {
					return
				}
				message := fmt.Sprintf("异常分片[%s],　冲突分片[%s], 范围有重叠", eR.SString(), cR.SString())
				log.Warn("路由异常: %s", message)
				alarm.Alarm("路由异常报警", message)
				conflict = true
				break
			} else if metapb.Compare(routes[i].StartKey, routes[j].StartKey) <= 0 && metapb.Compare(routes[i].EndKey, routes[j].EndKey) >= 0 {
				eR, find := cluster.FindRange(routes[i].GetId())
				if !find {
					return
				}
				cR, find := cluster.FindRange(routes[j].GetId())
				if !find {
					return
				}
				message := fmt.Sprintf("异常分片[%s],　冲突分片[%s], 范围有覆盖", eR.SString(), cR.SString())
				log.Warn("路由异常: %s", message)
				alarm.Alarm("路由异常报警", message)
				conflict = true
				break
			}
		}
	}

	if conflict {
		return
	}

	// 检查分片覆盖范围是否有缺失
    sort.Sort(RangeByRegionSlice(_ranges))
	for i := 0; i < len(_ranges); i++ {
		if (i + 1) == len(_ranges) {
			break
		}
		if metapb.Compare(_ranges[i + 1].StartKey, _ranges[i].EndKey) != 0 {
			eR, find := cluster.FindRange(routes[i].GetId())
			if !find {
				return
			}
			cR, find := cluster.FindRange(routes[i + 1].GetId())
			if !find {
				return
			}
			message := fmt.Sprintf("集群:%d 时间:%s 路由缺失[%s ~ %s]",
				cluster.GetClusterId(), time.Now().String(), eR.SString(), cR.SString())
			log.Warn("路由缺失: %s", message)
			alarm.Alarm("路由缺失报警", message)
		}
	}
}

// TODO push
func (t *Table) UpdateRwPolicy(cluster *Cluster, policy *metapb.TableRwPolicy) error {
	t.schemaLock.Lock()
	defer t.schemaLock.Unlock()
	key := []byte(fmt.Sprintf("%s%d", PREFIX_RW_POLICY_TABLE, t.GetId()))
	data, err := policy.Marshal()
	if err != nil {
		return err
	}
	err = cluster.store.Put(key, data)
	if err != nil {
		return err
	}
	t.RwPolicy = policy
	t.Epoch.ConfVer++
	return nil
}

type TableNCache struct {
	lock   sync.RWMutex
	tableNs map[string]*Table
}

func NewTableNCache() *TableNCache {
	return &TableNCache{
		tableNs: make(map[string]*Table)}
}

func (tc *TableNCache) Add(t *Table) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableNs[t.Name()] = t
}

func (tc *TableNCache) Delete(name string) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if _, find := tc.tableNs[name]; find {
		delete(tc.tableNs, name)
		return
	}
}

func (tc *TableNCache) FindTable(name string) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableNs[name]; find {
		return t, true
	}
	return nil, false
}

func (tc *TableNCache) GetAllTable() []*Table {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*Table
	for _, t := range tc.tableNs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *TableNCache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableNs)
}



type TableICache struct {
	lock   sync.RWMutex
	tableIs map[uint64]*Table
}

func NewTableICache() *TableICache {
	return &TableICache{
		tableIs: make(map[uint64]*Table)}
}

func (tc *TableICache) Add(t *Table) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.tableIs[t.GetId()] = t
}

func (tc *TableICache) Delete(id uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if t, find := tc.tableIs[id]; find {
		delete(tc.tableIs, t.GetId())
		return
	}
}

func (tc *TableICache) FindTable(id uint64) (*Table, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if t, find := tc.tableIs[id]; find {
		return t, true
	}
	return nil, false
}

func (tc *TableICache) GetAllTable() []*Table {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	var tables []*Table
	for _, t := range tc.tableIs {
		tables = append(tables, t)
	}
	return tables
}

func (tc *TableICache) Size() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return len(tc.tableIs)
}

func ToTableProperty(cols []*metapb.Column) (string, error) {
	tp := &TableProperty{
		Columns: make([]*metapb.Column, 0),
	}
	for _, c := range cols {
		tp.Columns = append(tp.Columns, c)
	}
	bytes, err := json.Marshal(tp)
	if err != nil {
		return "", err
	} else {
		return string(bytes), nil
	}
}

func EditProperties(properties string) ([]*metapb.Column, error) {
	tp := new(TableProperty)
	log.Debug("edit properties string: %v", properties)
	if err := json.Unmarshal([]byte(properties), tp); err != nil {
		return nil, err
	}
	log.Debug("edit properties struct: %v", *tp)
	if tp.Columns == nil {
		return nil, ErrInvalidColumn
	}

	return tp.Columns, nil
}

func ParseProperties(properties string) ([]*metapb.Column, []*metapb.Column, error) {
	tp := new(TableProperty)
	if err := json.Unmarshal([]byte(properties), tp); err != nil {
		return nil, nil, err
	}
	if tp.Columns == nil {
		return nil, nil, ErrInvalidColumn
	}

	var hasPk bool
	for _, c := range tp.Columns {
		c.Name = strings.ToLower(c.Name)
		if len(c.GetName()) > MAX_COLUMN_NAME_LENGTH {
			return nil, nil, ErrColumnNameTooLong
		}
		if c.PrimaryKey == 1 {
			if c.Nullable {
				return nil, nil, ErrPkMustNotNull
			}
			if len(c.DefaultValue) > 0 {
				return nil, nil, ErrPkMustNotSetDefaultValue
			}
			hasPk = true
		}
		if c.DataType == metapb.DataType_Invalid {
			return nil, nil, ErrInvalidColumn
		}
	}
	if !hasPk {
		return nil, nil, ErrMissingPk
	}

	for _, c := range tp.Regxs {
		// TODO check regx compile if error or not, error return
		if c.DataType == metapb.DataType_Invalid {
			return nil, nil, ErrInvalidColumn
		}
	}

	columnName := make(map[string]interface{})
	//sort.Sort(ByPrimaryKey(cols))
	var id uint64 = 1
	for _, c := range tp.Columns {

		// check column name
		if _, ok := columnName[c.Name]; ok {
			return nil, nil, ErrDupColumnName
		} else {
			columnName[c.Name] = nil
		}

		// set column id
		c.Id = id
		id++

	}
	return tp.Columns, tp.Regxs, nil
}

func GetTypeByName(name string) metapb.DataType {
	for k, v := range metapb.DataType_name {
		if strings.Compare(strings.ToLower(v), strings.ToLower(name)) == 0 {
			return metapb.DataType(k)
		}
	}
	return metapb.DataType_Invalid
}

type TableMonitorScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	pusher Pusher
}

func NewTableMonitorScheduler(interval time.Duration, cluster *Cluster, pusher Pusher) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &TableMonitorScheduler{
		name:     "table_delete_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster:  cluster,
		pusher: pusher,
	}
}

func (s *TableMonitorScheduler) GetName() string {
	return s.name
}

func (s *TableMonitorScheduler) Schedule() *Task {
	//log.Debug("table delete scheduler ....")
	dbs := s.cluster.dbs.GetAllDatabase()
	for _, db := range dbs {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		tables := db.GetAllTable()
		for _, table := range tables {
			select {
			case <-s.ctx.Done():
				return nil
			default:
			}
			log.Debug("table[%s:%s]", table.GetDbName(), table.GetName())
			task := table.GetTask()
			if task == nil {
				continue
			}
			log.Debug("task[%s]", task.GetType().String())
			switch task.GetType() {
			case taskpb.TaskType_TableCreate:
				// 正在创建
				if table.Creating {
					continue
				}

				ranges := task.GetTableCreate().GetRanges()
				// TODO collateral execution
				batch := s.cluster.store.NewBatch()
				for _, r := range ranges {
					// TODO rollback table
					err := s.cluster.DeleteRangeRemote(r)
					if err != nil {
						log.Error("delete range[%s] from remote failed, err[%v]", r.String(), err)
						return nil
					}
					key := []byte(fmt.Sprintf("%s%d", PREFIX_RANGE, r.GetId()))
					batch.Delete(key)
				}
				key := []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, table.GetId()))
				batch.Delete(key)
				key = []byte(fmt.Sprintf("%s%d", PREFIX_DELETE_TABLE, table.GetId()))
				batch.Delete(key)
				key = []byte(fmt.Sprintf("%s%d", PREFIX_TASK, task.GetMeta().GetTaskId()))
				batch.Delete(key)

				err := batch.Commit()
				if err != nil {
					return nil
				}
				for _, r := range ranges {
					if t, find := s.cluster.FindTableById(r.GetDbId(), r.GetTableId()); find {
						t.DeleteRange(r.GetId())
					}
					for _, p := range r.GetPeers() {
						node, find := s.cluster.FindInstance(p.GetNodeId())
						if !find {
							continue
						}
						node.DeleteRange(r.GetId())
					}
					s.cluster.ranges.Delete(r.GetId())
				}
				s.cluster.DeleteTaskCache(task.GetMeta().GetTaskId())
				db.DeleteTableById(table.GetId())
				log.Info("rollback table[%s:%s] success!!", table.GetDbName(), table.GetName())
				return nil
			case taskpb.TaskType_TableDelete:
				ranges := table.GetAllRanges()
				if len(ranges) == 0 {
					//delete table
					batch := s.cluster.store.NewBatch()
					key := []byte(fmt.Sprintf("%s%d", PREFIX_TASK, task.GetMeta().GetTaskId()))
					batch.Delete(key)
					key = []byte(fmt.Sprintf("%s%d", PREFIX_TABLE, table.GetId()))
					batch.Delete(key)
					key = []byte(fmt.Sprintf("%s%d", PREFIX_DELETE_TABLE, table.GetId()))
					batch.Delete(key)
					err := batch.Commit()
					if err != nil {
						log.Error("delete table in disk [%v: %v] error: %v",
							table.GetDbName(), table.GetName(), err)
						return nil
					}
					s.cluster.DeleteTaskCache(task.GetMeta().GetTaskId())
					db.DeleteDelTable(table.GetId())
					log.Info("table[%v: %v] is deleted", table.GetDbName(), table.GetName())
				} else {
					for _, r := range ranges {
						if !r.AllowSch(s.cluster) {
							continue
						}
						if task := RangeOffline(s.cluster, r); task != nil {
							log.Debug("range[%v] offline task created: leader[%v]", r.GetId(), r.Leader.GetNodeAddr())
							return task
						}
					}
				}
			}
		}
	}
	return nil
}

func (s *TableMonitorScheduler) AllowSchedule() bool {
	return true
}

func (s *TableMonitorScheduler) GetInterval() time.Duration {
	return s.interval
}

func (s *TableMonitorScheduler) Ctx() context.Context {
	return s.ctx
}

func (s *TableMonitorScheduler) Stop() {
	s.cancel()
}

type TopologyCheckScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	alarm            Alarm
}

func NewTopologyCheckScheduler(interval time.Duration, cluster *Cluster, alarm Alarm) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &TopologyCheckScheduler{
		name:     "topology_check_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster:  cluster,
		alarm:    alarm,
	}
}

func (s *TopologyCheckScheduler) GetName() string {
	return s.name
}

func (s *TopologyCheckScheduler) Schedule() *Task {
	//log.Debug("table delete scheduler ....")
	dbs := s.cluster.dbs.GetAllDatabase()
	for _, db := range dbs {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		tables := db.GetAllTable()
		for _, table := range tables {
			select {
			case <-s.ctx.Done():
				return nil
			default:
			}
			if time.Since(table.LastRoutesCheckTime) < maxRoutesCheckInternal {
				continue
			}
			table.RoutesCheck(s.ctx, s.cluster, s.alarm)
		}
	}
	return nil
}

func (s *TopologyCheckScheduler) AllowSchedule() bool {
	return true
}

func (s *TopologyCheckScheduler) GetInterval() time.Duration {
	return s.interval
}

func (s *TopologyCheckScheduler) Ctx() context.Context {
	return s.ctx
}

func (s *TopologyCheckScheduler) Stop() {
	s.cancel()
}
