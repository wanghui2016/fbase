package server

import (
	"sort"
	"sync"
	"math"

	"github.com/google/btree"
	"master-server/client"
	"model/pkg/metapb"
	"util/log"
	"math/rand"
	"time"
)

type RwType uint32

const (
	READ    RwType = 1
	WRITE   RwType = 2

	MaxAllowOffset uint64 = 1000
)


type Table struct {
	*metapb.Table
	primaryKeys []string
	cli         client.Client
	//rLock   sync.RWMutex
	routes  *btree.BTree
	//routes  map[uint64]*Route

	cLock     sync.RWMutex
	columns   map[string]*metapb.Column
	columnIds map[uint64]*metapb.Column

	RwPolicy *metapb.TableRwPolicy

	quit    chan struct{}
	release bool
}

func NewTable(table *metapb.Table, cli client.Client) *Table {
	t := &Table{
		Table:     table,
		cli:       cli,
		columns:   make(map[string]*metapb.Column),
		columnIds: make(map[uint64]*metapb.Column),
	}
	log.Debug("new added Table %s.%s:", t.DbName(), t.Name())
	var pks []string
	for _, c := range table.Columns {
		log.Debug("column: {%v}", c)
		t.columns[c.Name] = c
		t.columnIds[c.Id] = c
		if c.PrimaryKey > 0 {
			pks = append(pks, c.Name)
		}
	}
	t.primaryKeys = pks
	return t
}

func (t *Table) DbName() string {
	return t.GetDbName()
}

func (t *Table) Name() string {
	return t.GetName()
}

func (t *Table) ID() uint64 {
	return t.GetId()
}

func (t *Table) PKS() []string {
	return t.primaryKeys
}
//
//func (t *Table) findRoute(key *metapb.Key) *metapb.Route {
//	routes := t.routes
//	item := routes.Get(key)
//	if item == nil {
//		return nil
//	}
//	if route, ok := item.(*metapb.Route); ok {
//		return route
//	}
//	return nil
//}

// 一定能找到对应的路由信息
func (t *Table) FindRoute(key *metapb.Key) *metapb.Route {
	if key == nil {
		log.Warn("key is empty!!!!!!!")
		return nil
	}
	routes := t.routes
	item := routes.Get(key)
	if item == nil {
		return nil
	}
	if route, ok := item.(*metapb.Route); ok {
		return route
	}
	return nil
}

func GetRouteAddr(route *metapb.Route, policy *metapb.TableRwPolicy, rw RwType) string {
	switch rw {
	case READ:
		if policy == nil {
			return route.GetLeader().GetNodeAddr()
		}
		switch policy.GetPolicy() {
		    // 混合读
		case metapb.RwPolicy_RW_MutliRead:
			leader := route.GetLeader()
			replicas := route.GetReplicas()
			var leaderRep *metapb.Replica
			for _, rep := range replicas {
				if rep.GetNodeId() == leader.GetNodeId() {
					leaderRep = rep
					break
				}
			}
			if leaderRep == nil {
				log.Warn("invalid route[%d]", route.GetRangeId())
				return route.GetLeader().GetNodeAddr()
			}
			rd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < 3; i++ {
				index := rd.Intn(len(replicas))
				if leaderRep.GetLogIndex() >= replicas[index].GetLogIndex() {
					if leaderRep.GetLogIndex() - replicas[index].GetLogIndex() < MaxAllowOffset {
						return replicas[index].GetNodeAddr()
					}
				}
			}
			if log.IsEnableDebug() {
				log.Debug("route[%d] replica log index too much behind [%v]", route.GetRangeId(), replicas)
			}
			return route.GetLeader().GetNodeAddr()
			//读写都访问master
		case metapb.RwPolicy_RW_OnlyMaster:
			return route.GetLeader().GetNodeAddr()
			//读写分离
		case metapb.RwPolicy_RW_Split:
			leader := route.GetLeader()
			replicas := route.GetReplicas()
			var leaderRep *metapb.Replica
			for _, rep := range replicas {
				if rep.GetNodeId() == leader.GetNodeId() {
					leaderRep = rep
					break
				}
			}
			if leaderRep == nil {
				log.Warn("invalid route[%d]", route.GetRangeId())
				return route.GetLeader().GetNodeAddr()
			}
			// 随机选择
			var minOffset uint64
			var index int
			minOffset = math.MaxUint64
			for i := 0; i < len(replicas); i++ {
				var offset uint64
				if leaderRep.GetNodeId() == replicas[i].GetNodeId() {
					continue
				}
				if leaderRep.GetLogIndex() > replicas[i].GetLogIndex() {
					offset = leaderRep.GetLogIndex() - replicas[i].GetLogIndex()
				} else {
					offset = replicas[i].GetLogIndex() - leaderRep.GetLogIndex()
				}
				if offset < minOffset {
					minOffset = offset
					index = i
				}
			}
			return replicas[index].GetNodeAddr()
		}
	case WRITE:
		return route.GetLeader().GetNodeAddr()
	}
	// Must error
	return route.GetLeader().GetNodeAddr()
}

type SortRoutes []*metapb.Route

func (s SortRoutes) Len() int {
	return len(s)
}

func (s SortRoutes) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortRoutes) Less(i, j int) bool {
	if metapb.Compare(s[i].GetStartKey(), s[j].GetStartKey()) < 0 {
		return true
	}
	return false
}

// 需要返回有序的route列表
func (t *Table) AllRoutes() []*metapb.Route {
	//if !t.checkRoutes() {
	//	return nil
	//}
	_routes := t.routes
	if _routes.Len() == 0 {
		return nil
	}
	routes := make(SortRoutes, 0, _routes.Len())
	iter := func(i btree.Item) bool {
		if route, ok := i.(*metapb.Route); ok {
			routes = append(routes, route)
		} else {
			return false
		}
		return true
	}
	_routes.Ascend(iter)
	sort.Sort(routes)
	return routes
}

func (t *Table) FindColumn(columnName string) *metapb.Column {
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	if c, ok := t.columns[columnName]; ok {
		return c
	}
	return nil
}

func (t *Table) GetAllColumns() []*metapb.Column {
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	cols := make([]*metapb.Column, 0, len(t.Columns))
	for _, v := range t.Columns {
		cols = append(cols, v)
	}
	return cols
}

func (t *Table) FindColumnById(columnId uint64) *metapb.Column {
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	if c, ok := t.columnIds[columnId]; ok {
		return c
	}
	return nil
}

func (t *Table) AllIndexs() []uint64 {
	indexs := make([]uint64, 0)
	t.cLock.RLock()
	defer t.cLock.RUnlock()
	for _, c := range t.columns {
		if c.Index {
			indexs = append(indexs, c.Id)
		}
	}
	return indexs
}

func (t *Table) AddColumn(c *metapb.Column) {
	if c == nil {
		return
	}

	t.cLock.Lock()
	defer t.cLock.Lock()
	t.columns[c.Name] = c
	t.columnIds[c.Id] = c
}

func (t *Table) DeleteColumn(columnName string) {
	t.cLock.Lock()
	defer t.cLock.Lock()
	if c, ok := t.columns[columnName]; ok {
		delete(t.columns, columnName)
		delete(t.columnIds, c.Id)
	}

	return
}

func (t *Table) PkDupCheck() bool {
	return t.GetPkDupCheck()
}
