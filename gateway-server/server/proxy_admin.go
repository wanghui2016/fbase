package server

import (
	"errors"
	"fmt"

	"encoding/base64"

	"gateway-server/mysql"
	"gateway-server/sqlparser"
	"model/pkg/metapb"
	"util"
	"util/hack"
	"util/log"
)

// HandleTruncate truncate table
func (p *Proxy) HandleTruncate(db string, stmt *sqlparser.Truncate) (*mysql.Result, error) {
	var tableName string
	if stmt != nil && stmt.Table != nil {
		tableName = string(stmt.Table.Name)
	}

	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[truncate] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	// TODO:
	return nil, errors.New("not implement")
}

// HandleDescribe decribe table
func (p *Proxy) HandleDescribe(db string, stmt *sqlparser.Describe) (*mysql.Result, error) {
	tableName := string(stmt.TableName)

	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[describe] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	cols := t.GetAllColumns()
	if len(cols) == 0 {
		log.Error("[describe] table %s.%s invalid columns(null)", db, tableName)
		return nil, fmt.Errorf("table %s.%s invalid columns", db, tableName)
	}

	fieldNames := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	values := make([][]interface{}, len(cols))
	for i, col := range cols {
		nullable := "NO"
		if col.Nullable {
			nullable = "YES"
		}
		key := ""
		if col.PrimaryKey == 1 {
			key = "PRI"
		}
		values[i] = []interface{}{col.Name, col.DataType.String(), nullable, key, col.DefaultValue, col.Id}
	}

	r, err := buildResultset(nil, fieldNames, values)
	if err != nil {
		log.Error("build describe result set failed(%v), columns: %v, values: %v", err, fieldNames, values)
		return nil, err
	}

	result := &mysql.Result{
		Status:       0,
		AffectedRows: 0,
		Resultset:    r,
	}
	return result, nil
}

func (p *Proxy) HandleAdmin(db string, cmd string, args []string) (*mysql.Result, error) {
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("handle admin. db=%s, args=%s", db, args)
	}
	if len(args) == 0 || args[0] == "help" {
		return p.handleAdminHelp()
	}

	switch cmd {
	case "route":
		return p.handleAdminRoute(db, args)
	}

	return nil, fmt.Errorf("not implement")
}

func (p *Proxy) handleAdminHelp() (*mysql.Result, error) {
	return nil, fmt.Errorf("not implement")
}

// Usage
// 列举某个表的路由信息:
// 	   admin route('show', 'mytable')
// 查询某个表某个主键的路由信息:
// 	  admin route('show', 'mytable', 'pk1 value', 'pk2value', ... )
func (p *Proxy) handleAdminRoute(db string, args []string) (*mysql.Result, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("admin route: subcommand is required")
	}
	subCmd := args[0]

	if len(args) < 2 {
		return nil, fmt.Errorf("admin route: table name is required")
	}
	tableName := args[1]
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[admin] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	switch subCmd {
	case "show":
		return handleAdminRouteShow(t, args[2:])
	default:
		return nil, fmt.Errorf("admin rounte: unknown subcommand(%v)", subCmd)
	}
}

func handleAdminRouteShow(t *Table, keys []string) (*mysql.Result, error) {
	var routes []*metapb.Route
	if len(keys) == 0 {
		routes = t.AllRoutes()
	} else {
		pks := t.PKS()
		if len(keys) > len(pks) {
			return nil, fmt.Errorf("too mush pk values(%d > %d)", len(keys), len(pks))
		}
		var buf []byte
		var err error
		for i, key := range keys {
			col := t.FindColumn(pks[i])
			if col == nil {
				return nil, fmt.Errorf("could not find column(%v) in Table %s.%s", pks[i], t.DbName(), t.Name())
			}
			if buf, err = util.EncodePrimaryKey(buf, col, hack.Slice(key)); err != nil {
				return nil, err
			}
		}
		r := t.FindRoute(&metapb.Key{Type: metapb.KeyType_KT_Ordinary, Key: buf})
		if r != nil {
			routes = append(routes, r)
		}
	}

	fieldNames := []string{"ID", "StartKey", "EndKey", "LeaderID", "LeaderAddr", "Version"}

	if len(routes) == 0 {
		return &mysql.Result{
			Status:       0,
			AffectedRows: 0,
			Resultset:    newEmptyResultSet(fieldNames),
		}, nil
	}

	values := make([][]interface{}, len(routes))
	var leaderID uint64
	var leaderAddr string
	var version uint64
	for i, r := range routes {
		if r.Leader != nil {
			leaderID = r.Leader.NodeId
			leaderAddr = r.Leader.NodeAddr
		}
		if r.GetRangeEpoch() != nil {
			version = r.GetRangeEpoch().GetVersion()
		}
		values[i] = []interface{}{r.GetRangeId(), formatRouteKey(r.GetStartKey()), formatRouteKey(r.GetEndKey()), leaderID, leaderAddr, version}
	}
	r, err := buildResultset(nil, fieldNames, values)
	if err != nil {
		log.Error("build admin route show result failed(%v), columns: %v, values: %v", err, fieldNames, values)
		return nil, err
	}
	result := &mysql.Result{
		Status:       0,
		AffectedRows: 0,
		Resultset:    r,
	}
	return result, nil
}

func formatRouteKey(key *metapb.Key) []byte {
	switch key.Type {
	case metapb.KeyType_KT_NegativeInfinity:
		return []byte("[-∞]")
	case metapb.KeyType_KT_PositiveInfinity:
		return []byte("[+∞]")
	case metapb.KeyType_KT_Ordinary:
		k := key.GetKey()
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(k)))
		base64.StdEncoding.Encode(dst, k)
		return dst
	}
	return []byte("[Invalid]")
}
