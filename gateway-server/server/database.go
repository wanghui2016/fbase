package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/btree"
	"master-server/client"
	masterServer "master-server/server"
	"model/pkg/metapb"
	"util/log"
	"util/ttlcache"
)

type httpReply struct {
	Code       int                            `json:"code"`
	Message    string                         `json:"message`
	Data       *metapb.TopologyEpoch          `json:"data"`
}

type DataBase struct {
	*metapb.DataBase
	// more ......
	cli    client.Client
	client *http.Client
	lock   sync.RWMutex
	tables map[string]*Table

	msManageAddrs []string

	missTables *ttlcache.TTLCache
}

func NewDataBase(db *metapb.DataBase, cli client.Client, msAddrs []string) *DataBase {
	transport := http.Transport{
		ResponseHeaderTimeout: time.Second * 61,
	}
	// for table ping
	client := &http.Client{
		Transport: &transport,
	}
	return &DataBase{
		DataBase:      db,
		cli:           cli,
		client:        client,
		msManageAddrs: msAddrs,
		tables:        make(map[string]*Table),
		missTables:    ttlcache.NewTTLCache(time.Second * 60),
	}
}

func (d *DataBase) DbName() string {
	return d.GetName()
}

func (d *DataBase) DbId() uint64 {
	return d.GetId()
}

func (d *DataBase) findTable(tableName string) *Table {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if t, ok := d.tables[tableName]; ok {
		return t
	}
	return nil
}

func (d *DataBase) findTableFromRemote(tableName string) *Table {
	t, err := d.cli.GetTable(d.DbId(), tableName)
	if err != nil {
		log.Error("find table from remote failed, err[%v]", err)
		return nil
	}
	return NewTable(t, d.cli)
}

func (d *DataBase) findTableByIdFromRemote(tableId uint64) *Table {
	t, err := d.cli.GetTableById(d.DbId(), tableId)
	if err != nil {
		log.Error("find table from remote failed, err[%v]", err)
		return nil
	}
	return NewTable(t, d.cli)
}

func (d *DataBase) FindTable(tableName string) *Table {
	t := d.findTable(tableName)
	if t == nil {
		d.lock.Lock()
		defer d.lock.Unlock()
		if t, ok := d.tables[tableName]; ok {
			return t
		}
		// 查询miss cache
		_, find := d.missTables.Get(tableName)
		if find {
			return nil
		}
		// TODO table not exist and delete or other error
		t = d.findTableFromRemote(tableName)
		if t == nil {
			d.missTables.Put(tableName, tableName)
			return nil
		}
		topo, err := d.cli.GetTopologyEpoch(d.DbId(), t.GetId(), &metapb.TableEpoch{})
		if err != nil {
			log.Error("get table topology failed, err[%v]", err)
			return nil
		}
		_routes := btree.New(10)
		for _, r := range topo.GetRoutes() {
			it := _routes.ReplaceOrInsert(r)
			if it != nil {
				// TODO alarm
				log.Error("table[%s:%s] conflict route[%v][%v]", d.DbName(), tableName, r, it)
			}
		}
		t.routes = _routes
		t.Epoch = topo.GetEpoch()
		t.RwPolicy = topo.GetRwPolicy()
		d.addTable(t)
	}
	return t
}

func (d *DataBase) addTable(t *Table) {
	if t == nil {
		return
	}
	// 已经存在了
	if _, find := d.tables[t.Name()]; find {
		return
	}
	d.tables[t.Name()] = t
	d.missTables.Delete(t.Name())
	go d.routesUpdateLoop(t)
}

func (d *DataBase) AddTable(t *Table) {
	if t == nil {
		return
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	// 已经存在了
	if _, find := d.tables[t.Name()]; find {
		return
	}
	d.tables[t.Name()] = t
	d.missTables.Delete(t.Name())
	go d.routesUpdateLoop(t)
}

func (d *DataBase) DeleteTable(name string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.tables, name)
}

func (d *DataBase) getMsHost() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	msAddrsLen := len(d.msManageAddrs)
	if msAddrsLen == 0 {
		return ""
	}
	return d.msManageAddrs[r.Intn(msAddrsLen)]
}

func (d *DataBase) tableTopologyUpdate(t *Table, version, confVersion uint64) error {
	var err error
	var rep *httpReply
	//var ok bool
	var routes []*metapb.Route
	url := d.getMsHost()
	fullUrl := fmt.Sprintf("http://%s/route/get?dbId=%d&tableId=%d&version=%d&confVersion=%d",
		url, t.GetDbId(), t.GetId(), version, confVersion)
	log.Debug("fullUrl: %s", fullUrl)
	rep, err = routesUpdate(d.client, fullUrl)
	if err != nil {
		time.Sleep(time.Millisecond * 100)
		log.Error("http route get error: %v", err)
		return nil
	}
	if rep.Code != masterServer.HTTP_OK {
		log.Error("table [%v:%v] route get error: %v", t.DbName(), t.Name(), rep.Message)
		switch rep.Code {
		case masterServer.HTTP_ERROR_DATABASE_FIND:
			fallthrough
		case masterServer.HTTP_ERROR_TABLE_FIND:
			d.DeleteTable(t.Name())
			log.Info("route update: delete table[%v: %v]", t.DbName(), t.Name())
			return errors.New("table not exist")
		}
		time.Sleep(time.Millisecond * 100)
		return nil
	}
	// 重试
	if rep.Data == nil {
		log.Warn("invalid get table topology response!!!!")
		time.Sleep(time.Millisecond * 100)
		return nil
	}
	// check table version
	// just judge !=
	if rep.Data.GetEpoch().GetConfVer() > confVersion ||
		rep.Data.GetEpoch().GetVersion() >  version {
		t.Epoch = rep.Data.GetEpoch()
		t.RwPolicy = rep.Data.GetRwPolicy()
		log.Info("route update: table[%v: %v] epoch changed, rw policy[%v]", t.DbName(), t.Name(), t.RwPolicy)
	}
	// check route version 路由没有变化
	if len(rep.Data.GetRoutes()) == 0 {
		log.Debug("len(rep.Data) == 0")
		return nil
	} else {
		routes = rep.Data.GetRoutes()
		version = rep.Data.Epoch.GetVersion()
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("table[%s:%s] route update: version [%v->%v] route: %v",
				t.DbName(), t.Name(), t.GetEpoch().GetVersion(), version, routes)
		} else {
			log.Info("table[%s:%s] route update: version [%v->%v] route size: %v",
				t.DbName(), t.Name(), t.GetEpoch().GetVersion(), version, len(routes))
		}
		_routes := btree.New(10)
		for _, r := range routes {
			it := _routes.ReplaceOrInsert(r)
			if it != nil {
				// TODO alarm
				log.Error("table[%s:%s] conflict route[%v][%v]", r, it)
			}
		}
		t.routes = _routes
	}
	return nil
}

func (d *DataBase) routesUpdateLoop(t *Table) {
	log.Info("table[%s:%s] start routes update loop!!!!", t.DbName(), t.Name())
	err := d.tableTopologyUpdate(t, 1, 1)
	if err != nil {
		return
	}
	for {
		version := t.GetEpoch().GetVersion()
		confVersion := t.GetEpoch().GetConfVer()
		err = d.tableTopologyUpdate(t, version, confVersion)
		if err != nil {
			// table is deleted
			return
		}
	}
}

func routesUpdate(cli *http.Client, fullUrl string) (*httpReply, error) {
	resp, err := cli.Get(fullUrl)
	if err != nil {
		log.Error("http route get error: %v", err)
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("empty http response")
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	reply, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("read http body error: %v", err)
		return nil, err
	}
	rep := &httpReply{}
	err = json.Unmarshal(reply, rep)
	if err != nil {
		log.Error("unmarshal http reply error: %v", err)
		return nil, err
	}
	return rep, nil
}
