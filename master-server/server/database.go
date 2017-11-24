package server

import (
	"sync"

	"model/pkg/metapb"
)

type Database struct {
	*metapb.DataBase
    lock    sync.Mutex
	// 存放当前的table
	// 按照name
	tableNs *TableNCache
	// 按照ID
	tableCIs *TableICache
	// 存放当前的table和正在删除的table
	tableIs *TableICache
}

func NewDatabase(db *metapb.DataBase) *Database {
	return &Database{
		DataBase: db,
		tableNs: NewTableNCache(),
		tableCIs: NewTableICache(),
		tableIs: NewTableICache(),
	}
}

func (db *Database) Name() string {
	return db.GetName()
}

// 查找当前的table
func (db *Database) FindCurTable(name string) (*Table, bool) {
	return db.tableNs.FindTable(name)
}

func (db *Database) FindCurTableById(id uint64) (*Table, bool) {
	return db.tableCIs.FindTable(id)
}

// 查找存在的table
func (db *Database) FindTableById(id uint64) (*Table, bool) {
	return db.tableIs.FindTable(id)
}

func (db *Database) CreateTable(t *Table) (error) {
    db.lock.Lock()
	defer db.lock.Unlock()
	if _, find := db.FindCurTable(t.GetName()); find {
		return ErrDupTable
	}
	db.tableNs.Add(t)
	db.tableCIs.Add(t)
	db.tableIs.Add(t)
	return nil
}

func (db *Database) AddTable(t *Table) {
	db.tableNs.Add(t)
	db.tableCIs.Add(t)
	db.tableIs.Add(t)
}

func (db *Database) AddDeleteTable(t *Table) {
	db.tableIs.Add(t)
}

// real delete
func (db *Database) DeleteTableById(id uint64) error {
	table, find := db.tableIs.FindTable(id)
	if !find {
		return ErrNotExistTable
	}
	db.tableIs.Delete(id)
	db.tableCIs.Delete(id)
	db.tableNs.Delete(table.GetName())
	return nil
}

func (db *Database) DeleteDelTable(id uint64) error {
	_, find := db.tableIs.FindTable(id)
	if !find {
		return ErrNotExistTable
	}
	db.tableIs.Delete(id)
	return nil
}

// 仅仅从当前table列表中删除
func (db *Database) DeleteCurTable(name string) error {
	t, find := db.tableNs.FindTable(name)
	if !find {
		return ErrNotExistTable
	}
	db.tableNs.Delete(t.GetName())
	db.tableCIs.Delete(t.GetId())
	return nil
}

func (db *Database) GetAllCurTable() []*Table {
	return db.tableNs.GetAllTable()
}

func (db *Database) GetAllTable() []*Table {
	return db.tableIs.GetAllTable()
}

type DbCache struct {
	lock    sync.RWMutex
	dbNs     map[string]*Database
	dbIs     map[uint64]*Database
}

func NewDbCache() *DbCache {
	return &DbCache{
		dbNs: make(map[string]*Database),
		dbIs: make(map[uint64]*Database)}
}

func (dc *DbCache) Add(d *Database) {
	dc.lock.Lock()
	defer dc.lock.Unlock()
	dc.dbNs[d.Name()] = d
	dc.dbIs[d.GetId()] = d
}

func (dc *DbCache) Delete(name string) {
	dc.lock.Lock()
	defer dc.lock.Unlock()
	if d, find := dc.dbNs[name]; find {
		delete(dc.dbNs, name)
		delete(dc.dbIs, d.GetId())
		return
	}
}

func (dc *DbCache) FindDb(name string) (*Database, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	if d, find := dc.dbNs[name]; find {
		return d, true
	}
	return nil, false
}

func (dc *DbCache) FindDbById(id uint64) (*Database, bool) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	if d, find := dc.dbIs[id]; find {
		return d, true
	}
	return nil, false
}

func (dc *DbCache) GetAllDatabase() []*Database {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	var dbs []*Database
	for _, d := range dc.dbNs {
		dbs = append(dbs, d)
	}
	return dbs
}

func (dc *DbCache) Size() int {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	return len(dc.dbNs)
}
