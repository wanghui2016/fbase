package server

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"time"

	"data-server/client"
	"gateway-server/mysql"
	"gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util"
	"util/hack"
	"util/log"
	"runtime"
	"util/deepcopy"
)

func (p *Proxy) HandleInsert(db string, stmt *sqlparser.Insert, args []interface{}) (*mysql.Result, error) {
	var parseTime time.Time
	start := time.Now()
	defer func() {
		delay := time.Since(start)
		trace := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(trace)
		p.sqlStats(trace.String(), time.Since(start), time.Since(parseTime))
		p.metric.AddApiWithDelay("insert", true, delay)
		if delay > time.Duration(p.config.InsertSlowLog)*time.Millisecond {
			log.Info("[insert slow log] %v %v", delay.String(), trace.String())
		}
	}()

	parser := &StmtParser{}

	// 解析表名
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[insert] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	// 解析插入列名
	cols, err := parser.parseInsertCols(stmt)
	if err != nil {
		log.Error("[insert] parse columns error(%v)", err)
		return nil, fmt.Errorf("handle insert parseColumn err %s", err.Error())
	}
	// 没有指定列名，添加表的所有列
	if len(cols) == 0 {
		columns := t.GetAllColumns()
		if len(columns) == 0 {
			log.Error("[insert] get table(%s.%s) all columns from router failed", db, tableName)
			return nil, fmt.Errorf("could not get colums info table(%s.%s)", db, tableName)
		}
		for _, c := range columns {
			cols = append(cols, c.Name)
		}
	}

	// 解析插入行值（可能有多行）
	rows, err := parser.parseInsertValues(stmt)
	if err != nil {
		log.Error("[insert] table %s.%s parse row values error(%v)", db, tableName, err)
		return nil, fmt.Errorf("handle insert parseRow err %s", err.Error())
	}
	// 检查每行值的个数跟列名个数是否相等
	for i, r := range rows {
		if len(r) != len(cols) {
			log.Error("[insert] table %s.%s Column count doesn't match value count at row %d(%d != %d)", db, tableName, i, len(r), len(cols))
			return nil, fmt.Errorf("Column count doesn't match value count at row %d", i)
		}
	}

	// 按照表的每个列查找对应列值位置
	colMap, t, err := p.matchInsertValues(t, cols, "")
	if err != nil {
		log.Error("[insert] table %s.%s match column values error(%v)", db, tableName, err)
		return nil, err
	}
	// 检查是否缺少主键列
	if err := p.checkPKMissing(t, colMap); err != nil {
		log.Error("[insert] table %s.%s missing column(%v)", db, tableName, err)
		return nil, err
	}

	parseTime = time.Now()
	// 编码、执行插入
	res := new(mysql.Result)
	affected, duplicateKey, err := p.insertRows(t, colMap, rows)
	if err != nil {
		log.Error("insert error table[%s:%s], err %s", db, tableName, err.Error())
		return nil, err
	}
	if len(duplicateKey) != 0 {
		resErr := new(mysql.SqlError)
		resErr.Code = mysql.ER_DUP_ENTRY
		resErr.State = "23000"
		message := ` Duplicate entry `
		message += `for key 'PRIMARY'`
		resErr.Message = message
		return nil, resErr
	}
	res.AffectedRows = affected
	res.Status = 0
	return res, nil
}

// 查找每列对应的列值的偏移，处理自动添加列逻辑
func (p *Proxy) matchInsertValues(t *Table, cols []string, version string) (colMap map[string]int, newTable *Table, err error) {
	newTable = t
	colMap = make(map[string]int)
	unrecognized := make(map[string]int) // 未识别的列，不在原表定义中的列
	for i, c := range cols {
		col := t.FindColumn(c)
		if col != nil { // 存在该列
			if _, ok := colMap[col.Name]; ok { // 重复了
				err = fmt.Errorf("duplicate column(%v) for insert", c)
				return
			}
			colMap[col.Name] = i
		} else { // 表中没有该列
			if _, ok := unrecognized[c]; ok {
				err = fmt.Errorf("duplicate unrecognized column(%v) for insert", c)
				return
			}
			unrecognized[c] = i
		}
	}

	db := t.DbName()
	dbId := t.GetDbId()
	table := t.Name()
	tableId := t.GetId()

	// 自动添加列
	if len(unrecognized) > 0 {
		addcols := make([]string, 0, len(unrecognized))
		for k := range unrecognized {
			addcols = append(addcols, k)
			log.Info("%s-%s col[%v] is null, prepare add", db, table, k)
		}
		log.Debug("autoAddColumn %v", unrecognized)
		err = p.autoAddColumn(dbId, tableId, addcols)
		if err != nil {
			log.Error("auto add column[%s:%s:%v] failed, err[%v]", db, table, addcols, err)
			return
		}
		newTable = p.router.FindTable(db, table)
		if newTable == nil {
			err = fmt.Errorf("update table %s.%s info failed", db, table)
			return
		}
		if version == "1.0.0" {
			t = newTable
			t_ := deepcopy.Iface(t.Table).(*metapb.Table)
			for _, col := range t_.Columns {
				col.DataType = metapb.DataType_Varchar
			}
			routes_ := t.routes
			policy_ := t.RwPolicy
			t = NewTable(t_, t.cli)
			t.routes = routes_
			t.RwPolicy = policy_
			log.Debug("command 1.0.0: %v", t.Columns)
			newTable = t
		}

		for c, index := range unrecognized {
			col := newTable.FindColumn(c)
			if col == nil {
				log.Info("%s-%s col[%s] is null, may be add failure ", db, table, c)
				err = fmt.Errorf("invalid column %s-%s col[%s] ", db, table, c)
				return
			}
			colMap[col.Name] = index
		}
	}
	return
}

// 检查插入时是否少了某列
func (p *Proxy) checkPKMissing(t *Table, colMap map[string]int) error {
	// 是否缺少主键
	for _, pk := range t.PKS() {
		if _, ok := colMap[pk]; !ok {
			return fmt.Errorf("pk(%s) is required for insert", pk)
		}
	}
	return nil
}

// 编码
func (p *Proxy) encodeRow(t *Table, colMap map[string]int, rowValue InsertRowValue) (*kvrpcpb.KeyValue, error) {
	var key, value []byte
	var err error

	// 编码主键
	for _, pk := range t.PKS() {
		i, ok := colMap[pk]
		if !ok {
			return nil, fmt.Errorf("pk(%s) is missing", pk)
		}
		col := t.FindColumn(pk)
		if col == nil {
			return nil, fmt.Errorf("invalid pk column(%s)", pk)
		}
		if i >= len(rowValue) {
			return nil, fmt.Errorf("invalid pk(%s) value", pk)
		}
		if rowValue[i] == nil {
			return nil, fmt.Errorf("pk(%s) could not be NULL", pk)
		}
		key, err = util.EncodePrimaryKey(key, col, rowValue[i])
		if err != nil {
			return nil, err
		}
	}

	// 编码列值
	/**
	for _, col := range t.table.Columns {
		// TODO: 不需要编码主键列
		// if col.PrimaryKey == 1 { // 跳过主键列
		// 	continue
		// }
		i, ok := colMap[col.Name]
		if !ok {
			return nil, fmt.Errorf("column(%s) is missing", col.Name)
		}
		if i >= len(rowValue) {
			return nil, fmt.Errorf("invalid column(%s)", col.Name)
		}
		value, err = util.EncodeColumnValue(value, col, rowValue[i])
		if err != nil {
			return nil, err
		}
	}
	*/
	for colName, colIndex := range colMap {
		col := t.FindColumn(colName)
		if col == nil {
			return nil, fmt.Errorf("invalid table(%s) column(%s)", t.GetName(), colName)
		}
		if colIndex >= len(rowValue) {
			return nil, fmt.Errorf("invalid column(%s)", col.Name)
		}
		value, err = util.EncodeColumnValue(value, col, rowValue[colIndex])
		if err != nil {
			return nil, err
		}
	}

	expireAt, err := findRowExpire(colMap, rowValue)
	if err != nil {
		return nil, fmt.Errorf("find row ttl error(%s)", err)
	}

	return &kvrpcpb.KeyValue{
		Key:      key,
		Value:    value,
		ExpireAt: expireAt,
	}, nil
}

func (p *Proxy) insertWork(t *Table, groups [][]*kvrpcpb.KeyValue) (affected uint64, duplicateKey []byte, err error) {
	for _, rows := range groups {
		_affected, _duplicateKey, _err := p.insert(t, rows)
		if _err != nil {
			err = _err
			return
		}
		if _duplicateKey != nil {
			duplicateKey = _duplicateKey
			return
		}
		affected += _affected
	}
	return
}

func (p *Proxy) insertRows(t *Table, colMap map[string]int, rows []InsertRowValue) (affected uint64, duplicateKey []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			b := make([]byte, 1024)
			n := runtime.Stack(b, false)
			log.Error("recover: %v, stack: %v", r, string(b[:n]))
		}
	}()
	var kvPairs []*kvrpcpb.KeyValue
	var kvGroup [][]*kvrpcpb.KeyValue

	var kv *kvrpcpb.KeyValue
	for i, r := range rows {
		kv, err = p.encodeRow(t, colMap, r)
		if err != nil {
			log.Error("[inert] table %s.%s encode row at %d failed: %v", t.DbName(), t.Name(), i, err)
			return
		}
		kvPairs = append(kvPairs, kv)
	}
	sort.Sort(KvParisSlice(kvPairs))
	// 按照route的范围划分kv group
	ggroup := make(map[uint64][]*kvrpcpb.KeyValue)
	key := &metapb.Key{}
	for _, kv := range kvPairs {
		//log.Debug("==========key[%v]", kv.GetKey())
		key.Key = kv.GetKey()
		key.Type = metapb.KeyType_KT_Ordinary
		route := t.FindRoute(key)
		if route == nil {
			err = ErrNoRoute
			return
		}
		var group []*kvrpcpb.KeyValue
		var ok bool
		if group, ok = ggroup[route.GetRangeId()]; !ok {
			group = make([]*kvrpcpb.KeyValue, 0)
			ggroup[route.GetRangeId()] = group
		}
		group = append(group, kv)
		// 每100个kv切割一下
		if len(group) == 100 {
			kvGroup = append(kvGroup, group)
			group = make([]*kvrpcpb.KeyValue, 0)
		}
		ggroup[route.GetRangeId()] = group
	}
	for _, group := range ggroup {
		if len(group) > 0 {
			kvGroup = append(kvGroup, group)
		}
	}
	var tasks []*InsertTask
	for _, rows := range kvGroup {
		task := GetInsertTask()
		task.init(p, t, rows)
		err = p.Submit(task)
		if err != nil {
			// release task
			PutInsertTask(task)
			log.Error("submit insert task failed, err[%v]", err)
			return
		}
		tasks = append(tasks, task)
	}
	// 存在部分task不能被回收的问题，但是不会造成内存泄漏
	for _, task := range tasks {
		err = task.Wait()
		if err != nil {
			log.Error("insert task do failed, err[%v]", err)
			PutInsertTask(task)
			return
		}
		if task.rest.GetDuplicateKey() != nil {
			duplicateKey = task.rest.GetDuplicateKey()
			PutInsertTask(task)
			return
		}
		affected += task.rest.GetAffected()
		PutInsertTask(task)
	}
	return
}

func (p *Proxy) insert(table *Table, rows []*kvrpcpb.KeyValue) (
	affected uint64, duplicateKey []byte, err error) {
	if len(rows) == 0 {
		err = ErrEmptyRow
		return
	}
	req := &kvrpcpb.KvInsertRequest{
		Rows:           rows,
		CheckDuplicate: table.PkDupCheck(),
		Timestamp:      p.clock.Now(),
	}
	var resps []*kvrpcpb.KvInsertResponse
	proxy := KvProxy{
		cli:          p.nodeCli,
		msCli:        p.msCli,
		clock:        p.clock,
		table:        table,
		findRoute:    table.FindRoute,
		writeTimeout: client.WriteTimeout,
		readTimeout:  client.ReadTimeoutShort,
	}
	rng := util.BytesPrefix(rows[len(rows)-1].GetKey())
	scope := &kvrpcpb.Scope{Start: rows[0].GetKey(), Limit: rng.Limit}
	resps, err = proxy.kvsInsert(req, scope)
	if err != nil {
		return
	}
	for _, resp := range resps {
		if resp.GetCode() == 0 {
			affected += resp.GetAffectedKeys()
		} else if resp.GetDuplicateKey() != nil {
			duplicateKey = resp.GetDuplicateKey()
			return
		}
	}
	return
}

func (p *Proxy) autoAddColumn(dbId, tableId uint64, cols []string) error {
	if err := p.router.addColumnToRemote(dbId, tableId, cols); err != nil {
		return err
	}
	database := p.router.findDBById(dbId)
	if database != nil {
		table := database.findTableByIdFromRemote(tableId)
		if table != nil {
			database.AddTable(table)
		}
	}
	return nil
}

type KvParisSlice []*kvrpcpb.KeyValue

func (p KvParisSlice) Len() int {
	return len(p)
}

func (p KvParisSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p KvParisSlice) Less(i int, j int) bool {
	return bytes.Compare(p[i].GetKey(), p[j].GetKey()) < 0
}

func findRowExpire(colMap map[string]int, rowValue InsertRowValue) (int64, error) {
	idx, ok := colMap[util.TTL_COL_NAME]
	if !ok {
		return 0, nil
	}
	if idx >= len(rowValue) {
		return 0, fmt.Errorf("invalid column(%s) pos", util.TTL_COL_NAME)
	}
	ttl, err := strconv.ParseInt(hack.String(rowValue[idx]), 10, 64)
	if err != nil {
		return 0, err
	}
	// ms to nano seconds
	return ttl * 1000000, nil
}
