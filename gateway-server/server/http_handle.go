package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"
	"strings"

	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/bufalloc"
	"util/log"
	"encoding/base64"
	"encoding/binary"
	"math"
	"runtime"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func httpReadQuery(r *http.Request) (*Query, error) {
	var err error

	bufferLen := int(r.ContentLength)
	if bufferLen <= 0 || bufferLen > 1024*1024*10 {
		bufferLen = 512
	}
	buffer := bufalloc.AllocBuffer(bufferLen)
	defer bufalloc.FreeBuffer(buffer)
	if _, err = buffer.ReadFrom(r.Body); err != nil {
		return nil, err
	}
	// defer r.Body.Close()

	var query *Query
	switch r.Header.Get("fbase-protocol-type") {
	case "protobuf":
		return nil, errors.New("protobuf is unsupported")
	case "json":
		fallthrough
	default:
		query = new(Query)
		log.Debug("query: %v", string(buffer.Bytes()))
		if err = json.Unmarshal(buffer.Bytes(), query); err != nil {
			return nil, err
		}
	}
	return query, nil
}

func httpSendReply(w http.ResponseWriter, reply interface{}) error {
	data, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	w.Header().Set("content-type", "application/json;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

func (q *Query) commandFieldNameToLower() {
	if q.Command == nil {
		return
	}
	cmd := q.Command

	if len(cmd.Field) != 0 {
		field := cmd.Field
		for i, c := range field {
			field[i] = strings.ToLower(c)
		}
	}

	andLower := func (and *And) {
		if and.Field != nil {
			and.Field.Column = strings.ToLower(and.Field.Column)
		}
	}

	if cmd.Filter != nil {
		filter := cmd.Filter
		if len(filter.Order) != 0 {
			order := filter.Order
			for i, o := range order {
				order[i].By = strings.ToLower(o.By)
			}
		}
		if len(filter.And) != 0 {
			and := filter.And
			for _, a := range and {
				andLower(a)
			}
		}
	}

	if len(cmd.PKs) != 0 {
		pks := cmd.PKs
		for _, pk := range pks {
			for _, and := range pk {
				andLower(and)
			}
		}
	}
}

func (s *Server) handleKVCommand(w http.ResponseWriter, r *http.Request) {
	var (
		query *Query
		err   error
		reply *Reply
	)

	defer func() {
		if reply == nil {
			reply = new(Reply)
		}
		if err := httpSendReply(w, reply); err != nil {
			log.Error("send http reply error(%v)", err)
			if r.Body != nil {
				r.Body.Close()
			}
		}
	}()
	if query, err = httpReadQuery(r); err != nil {
		log.Error("read query: %v", err)
		if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrClosedPipe {
			if r.Body != nil {
				r.Body.Close()
			}
		}
		reply = &Reply{Code: errCommandParse, Message: ErrHttpCmdParse.Error()}
		return
	}

	dbname := query.DatabaseName
	tname := query.TableName
	if len(dbname) == 0 {
		reply = &Reply{Code: errCommandNoDb, Message: ErrNotExistDatabase.Error()}
		return
	}
	if len(tname) == 0 {
		reply = &Reply{Code: errCommandNoTable, Message: ErrNotExistTable.Error()}
		return
	}
	if query.Command == nil {
		reply = &Reply{Code: errCommandEmpty, Message: ErrHttpCmdEmpty.Error()}
		return
	}
	t := s.proxy.router.FindTable(dbname, tname)
	if t == nil {
		log.Error("table %s.%s doesn.t exist", dbname, tname)
		reply = &Reply{Code: errCommandNoTable, Message: ErrNoRoute.Error()}
		return
	}

	query.commandFieldNameToLower()
	if query.Command.Version == "1.0.0" {
		if err := query.commandTranslateValue(t.columns); err != nil {
			log.Error("command transfer value: %v", err)
			// TODO return
		}
	}

	start := time.Now()
	switch query.Command.Type {
	case "get":
		reply, err = query.getCommand(s.proxy, t)
		if err != nil {
			log.Error("getcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: ErrHttpCmdRun.Error() }
		}
	case "set":
		reply, err = query.setCommand(s.proxy, t)
		if err != nil {
			log.Error("setcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: ErrHttpCmdRun.Error() }
		}
	case "del":
		reply, err = query.delCommand(s.proxy, t)
		if err != nil {
			log.Error("delcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: ErrHttpCmdRun.Error() }
		}
	default:
		log.Error("unknown command")
		reply = &Reply{Code: errCommandUnknown, Message: ErrHttpCmdUnknown.Error()}
	}

	delay := time.Since(start)
	cmd, _ := json.Marshal(query)
	s.proxy.sqlStats(string(cmd), delay, delay)
	if delay > time.Duration(s.proxy.config.SelectSlowLog)*time.Millisecond {
		log.Info("[kvcommand slow log %v %v ", delay.String(), string(cmd))
	}
}

func translateValue(columnMap map[string]*metapb.Column, f string, v interface{}) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			buf = buf[:n]
			log.Error("translate recover: %v, stack: %v", r, string(buf))
		}
	}()
	log.Debug("translate: field: %v, value(string): %v", f, v.(string))
	vByte, err := base64.StdEncoding.DecodeString(v.(string))
	if err != nil {
		return nil, fmt.Errorf("base64 decode error: %v", err)
	}
	switch columnMap[f].GetDataType() {
	case metapb.DataType_Tinyint:
		fallthrough
	case metapb.DataType_Smallint:
		fallthrough
	case metapb.DataType_Int:
		fallthrough
	case metapb.DataType_BigInt:
		switch len(vByte) {
		case 2:
			vInt := binary.BigEndian.Uint16(vByte)
			if columnMap[f].GetUnsigned() {
				return vInt, nil
			} else {
				return int16(vInt), nil
			}
		case 4:
			vInt := binary.BigEndian.Uint32(vByte)
			if columnMap[f].GetUnsigned() {
				return vInt, nil
			} else {
				return int32(vInt), nil
			}
		case 8:
			vInt := binary.BigEndian.Uint64(vByte)
			if columnMap[f].GetUnsigned() {
				return vInt, nil
			} else {
				return int64(vInt), nil
			}
		default:
			return nil, fmt.Errorf("wrong int len: %v", len(vByte))
		}
	case metapb.DataType_Float:
		fallthrough
	case metapb.DataType_Double:
		switch len(vByte) {
		case 4:
			vFloat := binary.BigEndian.Uint32(vByte)
			return math.Float32frombits(vFloat), nil
		case 8:
			vDouble := binary.BigEndian.Uint64(vByte)
			return math.Float64frombits(vDouble), nil
		default:
			return nil, fmt.Errorf("wrong float len: %v", len(vByte))
		}
	case metapb.DataType_Date:
		fallthrough
	case metapb.DataType_TimeStamp:
		fallthrough
	case metapb.DataType_Varchar:
		return string(vByte), nil
	case metapb.DataType_Binary:
		return vByte, nil
	default:
		return nil, errors.New("invalid data type")
	}
}

func (query *Query) commandTranslateValue(columnMap map[string]*metapb.Column) error {
	if query.Command == nil {
		return errors.New("query command is nil")
	}
	command := query.Command
	fieldNum := len(command.Field)
	if command.Type == "set" && len(query.Command.Values) != 0 {
		rows := query.Command.Values
		for _, row := range rows {
			if len(row) != fieldNum {
				return fmt.Errorf("len(row) %v != fieldNum %v", len(row), fieldNum)
			}

			for i := 0; i < fieldNum; i++ {
				var err error
				row[i], err = translateValue(columnMap, command.Field[i], row[i])
				if err != nil {
					return fmt.Errorf("translate value error: %v", err)
				}
				if row[i] == nil {
					return fmt.Errorf("translate value is nil")
				}
			}
		}
	}
	if query.Command.Filter != nil {
		filter := query.Command.Filter
		if filter.And != nil {
			ands := filter.And
			for _, and := range ands {
				if and == nil {
					return fmt.Errorf("filter and is nil")
				}
				if and.Field == nil {
					return fmt.Errorf("filter and field is nil")
				}
				if len(and.Field.Column) == 0 {
					return fmt.Errorf("filter and field column is nil")
				}
				if and.Field.Value == nil {
					return fmt.Errorf("filter and field value is nil")
				}
				var err error
				and.Field.Value, err = translateValue(columnMap, and.Field.Column, and.Field.Value)
				if err != nil {
					return fmt.Errorf("translate value error: %v", err)
				}
			}
		}
	}
	return nil
}

func (query *Query) getCommand(proxy *Proxy, t *Table) (*Reply, error) {
	log.Debug("get command: %v", query.Command)
	// 解析选择列
	columns := query.parseColumnNames()
	fieldList := make([]*kvrpcpb.SelectField, 0, len(columns))
	for _, c := range columns {
		col := t.FindColumn(c)
		if col == nil {
			return nil, fmt.Errorf("invalid column(%s)", c)
		}
		fieldList = append(fieldList, &kvrpcpb.SelectField{
			Typ:    kvrpcpb.SelectField_Column,
			Column: col,
		})
	}

	if len(query.Command.PKs) == 0 {
		order := query.parseOrder()
		log.Debug("getcommand order: %v", order)
		var matchs []Match = nil
		var err error
		if query.Command.Filter != nil {
			// 解析where条件
			matchs, err = query.parseMatchs(query.Command.Filter.And)
			if err != nil {
				log.Error("[get] handle parse where error: %v", err)
				return nil, err
			}
		}
		// 向dataserver查询
		//filter := &Filter{columns: columns, matchs: matchs}

		limit := query.parseLimit()
		log.Debug("getcommand limit: %v", limit)

		scope := query.parseScope()
		rowss, err := proxy.doSelect(t, fieldList, matchs, limit, scope)

		if err != nil {
			log.Error("getcommand doselect error: %v", err)
			return nil, err
		}
		return formatReply(t.columns, rowss, order, columns), nil
	} else {
		var allRows [][]*Row
		var tasks []*SelectTask
		for _, pk := range query.Command.PKs {
			matchs, err := query.parseMatchs(pk)
			//filter := &Filter{columns: columns, matchs: matchs}
			if err != nil {
				log.Error("[get] handle parse where error: %v", err)
				return nil, err
			}
			//rowss, err := proxy.doRangeSelect(t, filter, nil, nil)
			//rowss, err := proxy.doSelect(t, fieldList, matchs, nil, nil)
			//if err != nil {
			//	log.Error("getcommand doselect error: %v", err)
			//	return nil, err
			//}
			//allRows = append(allRows,rowss...)
			task := GetSelectTask()
			task.init(proxy, t, fieldList, matchs)
			//task := &SelectTask{
			//	p: proxy,
			//	table: t,
			//	fieldList: fieldList,
			//	matches: matchs,
			//	done: make(chan error, 1),
			//}
			err = proxy.Submit(task)
			if err != nil {
				log.Error("submit insert task failed, err[%v]", err)
				return nil, err
			}
			tasks = append(tasks, task)
		}
		for _, task := range tasks {
			err := task.Wait()
			if err != nil {
				log.Error("select task do failed, err[%v]", err)
				PutSelectTask(task)
				return nil, err
			}
			rowss := task.rest.rows
			if rowss != nil {
				allRows = append(allRows, rowss...)
			}
			PutSelectTask(task)
		}

		return formatReply(t.columns, allRows, nil, columns), nil
	}
}

func formatReply(columnMap map[string]*metapb.Column, rowss [][]*Row, order []*Order, columns []string) *Reply {
	rowset := make([][]interface{}, 0)
	for _, rows := range rowss {
		for _, row := range rows {
			row_ := make([]interface{}, 0)
			for _, f := range row.fields {
				if f.value == nil {
					row_ = append(row_, nil)
					continue
				}
				switch columnMap[f.col].GetDataType() {
				case metapb.DataType_Tinyint:
					fallthrough
				case metapb.DataType_Smallint:
					fallthrough
				case metapb.DataType_Int:
					fallthrough
				case metapb.DataType_BigInt:
					if columnMap[f.col].GetUnsigned() {
						if i, ok := f.value.(uint64); ok {
							row_ = append(row_, i)
						} else {
							log.Error("column %v is not uint64", f.col)
							return nil
						}
					} else {
						if i, ok := f.value.(int64); ok {
							row_ = append(row_, i)
						} else {
							log.Error("column %v is not int64", f.col)
							return nil
						}
					}
				case metapb.DataType_Float:
					fallthrough
				case metapb.DataType_Double:
					if ff, ok := f.value.(float64); ok {
						row_ = append(row_, ff)
					} else {
						log.Error("column %v is not float64", f.col)
						return nil
					}
				case metapb.DataType_Date:
					fallthrough
				case metapb.DataType_TimeStamp:
					fallthrough
				case metapb.DataType_Varchar:
					if str, ok := f.value.([]byte); ok {
						row_ = append(row_, string(str))
					} else {
						log.Error("column %v is not []byte", f.col)
						return nil
					}
				case metapb.DataType_Binary:
					row_ = append(row_, f.value)
				}
			}
			log.Debug("row: %v", row_)
			rowset = append(rowset, row_)
		}
	}

	if order != nil {
		for _, o := range order {
			for n, c := range columns {
				if c == o.By {
					// sort
					sorter := &rowsetSorter{
						rowset:          rowset,
						orderByFieldNum: n,
						column:          columnMap[c],
					}
					sort.Sort(sorter)
				}
			}
			break // TODO just loop once now
		}
	}

	return &Reply{
		Code:   0,
		Values: rowset,
	}
}

type rowsetSorter struct {
	rowset          [][]interface{}
	orderByFieldNum int
	column          *metapb.Column
}

func (s *rowsetSorter) Len() int {
	return len(s.rowset)
}

func (s *rowsetSorter) Less(i, j int) bool {
	switch s.column.GetDataType() {
	case metapb.DataType_Tinyint:
		fallthrough
	case metapb.DataType_Smallint:
		fallthrough
	case metapb.DataType_Int:
		fallthrough
	case metapb.DataType_BigInt:
		if s.column.GetUnsigned() {
			return uint64(s.rowset[i][s.orderByFieldNum].(uint64)) < uint64(s.rowset[j][s.orderByFieldNum].(uint64))
		} else {
			return int64(s.rowset[i][s.orderByFieldNum].(int64)) < int64(s.rowset[j][s.orderByFieldNum].(int64))
		}
	case metapb.DataType_Float:
		fallthrough
	case metapb.DataType_Double:
		return float64(s.rowset[i][s.orderByFieldNum].(float64)) < float64(s.rowset[j][s.orderByFieldNum].(float64))
	case metapb.DataType_Date:
		fallthrough
	case metapb.DataType_TimeStamp:
		fallthrough
	case metapb.DataType_Varchar:
		//return string(s.rowset[i][s.orderByFieldNum].([]byte)) < string(s.rowset[j][s.orderByFieldNum].([]byte))
		return bytes.Compare([]byte(s.rowset[i][s.orderByFieldNum].(string)), []byte(s.rowset[j][s.orderByFieldNum].(string))) == -1
	case metapb.DataType_Binary:
		return bytes.Compare([]byte(s.rowset[i][s.orderByFieldNum].([]byte)), []byte(s.rowset[j][s.orderByFieldNum].([]byte))) == -1
	default:
		log.Error("rowset sorter: invalid datatype")
		return false
	}
}

func (s *rowsetSorter) Swap(i, j int) {
	s.rowset[i], s.rowset[j] = s.rowset[j], s.rowset[i]
}

func (query *Query) setCommand(proxy *Proxy, t *Table) (*Reply, error) {
	log.Debug("set command: %v", query.Command)
	db := t.DbName()
	tableName := t.Name()
	// 解析选择列
	cols := query.parseColumnNames()

	// 按照表的每个列查找对应列值位置
	colMap, t, err := proxy.matchInsertValues(t, cols)
	if err != nil {
		log.Error("[insert] table %s.%s match column values error(%v)", db, tableName, err)
		return nil, err
	}

	// 检查是否缺少某列
	// TODO：支持默认值
	/*if err := proxy.checkMissingColumn(t, colMap); err != nil {
		log.Error("[insert] table %s.%s missing column(%v)", db, tableName, err)
		return nil, err
	}*/

	buffer := bufalloc.AllocBuffer(512)
	defer bufalloc.FreeBuffer(buffer)
	rows, err := query.parseRowValues(buffer)
	// rows, err := query.parseRowValues(t)
	if err != nil {
		log.Error("parse row values error: %v", err)
		return nil, err
	}

	affected, duplicateKey, err := proxy.insertRows(t, colMap, rows)
	if err != nil {
		log.Error("insert error %s- %s:%s", db, tableName, err.Error())
		return nil, err
	}
	if len(duplicateKey) > 0 {
		return nil, fmt.Errorf("duplicate key: %v", duplicateKey)
	}
	return &Reply{
		Code:         0,
		RowsAffected: affected,
	}, nil
}

func (query *Query) delCommand(proxy *Proxy, t *Table) (*Reply, error) {
	// 解析选择列
	//columns := query.parseColumnNames()
	var matchs []Match = nil
	var err error
	if query.Command.Filter != nil {
		// 解析where条件
		matchs, err = query.parseMatchs(query.Command.Filter.And)
		if err != nil {
			log.Error("[get] handle parse where error: %v", err)
			return nil, err
		}
	}

	// 向dataserver查询
	affectedRows, err := proxy.doDelete(t, matchs)
	if err != nil {
		return nil, err
	}
	return &Reply{
		Code:         0,
		RowsAffected: affectedRows,
	}, nil
}

func (s *Server) handleTableInfo(w http.ResponseWriter, r *http.Request) {
	dbname := r.FormValue("dbname")
	tname := r.FormValue("tablename")

	resp := new(Response)
	defer httpSendReply(w, resp)

	t := s.proxy.router.FindTable(dbname, tname)
	if t == nil {
		resp.Code = 1
		resp.Message = ErrNotExistTable.Error()
		return
	}

	type ColumnInfo struct {
		ColumnName string `json:"column_name"`
		DataType   string `json:"data_type"`
	}
	type RangeInfo struct {
		RangeId  uint64 `json:"range_id"`
		StartKey []byte `json:"start_key"`
		EndKey   []byte `json:"end_key"`
	}
	type tableInfo struct {
		Primarys []string      `json:"primarys"`
		Columns  []*ColumnInfo `json:"columns"`
		Ranges   []*RangeInfo  `json:"routes"`
	}
	tInfo := new(tableInfo)
	tInfo.Primarys = t.PKS()
	tInfo.Columns = func() []*ColumnInfo {
		var colInfos []*ColumnInfo
		for _, col := range t.GetAllColumns() {
			colInfos = append(colInfos, &ColumnInfo{
				ColumnName: col.GetName(),
				DataType:   metapb.DataType_name[int32(col.GetDataType())],
			})
		}
		return colInfos
	}()
	tInfo.Ranges = func() []*RangeInfo {
		var rngInfos []*RangeInfo
		for _, route := range t.AllRoutes() {
			rngInfos = append(rngInfos, &RangeInfo{
				RangeId:  route.GetId(),
				StartKey: route.GetStartKey().GetKey(),
				EndKey:   route.GetEndKey().GetKey(),
			})
		}
		log.Debug("table range info: %v", rngInfos)
		return rngInfos
	}()
	resp.Data = tInfo
}
