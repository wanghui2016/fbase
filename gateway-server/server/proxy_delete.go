package server

import (
	"fmt"

	"data-server/client"
	"gateway-server/mysql"
	"gateway-server/sqlparser"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/log"
	"time"
)

// HandleDelete handle delete
func (p *Proxy) HandleDelete(db string, stmt *sqlparser.Delete, args []interface{}) (*mysql.Result, error) {
	var parseTime time.Time
	start := time.Now()
	defer func() {
		delay := time.Since(start)
		trace := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(trace)
		p.sqlStats(trace.String(), time.Since(start), time.Since(parseTime))
		p.metric.AddApiWithDelay("delete", true, delay)
		if delay > time.Duration(p.config.InsertSlowLog)*time.Millisecond {
			log.Info("[delete slow log] %v %v", delay.String(), trace.String())
		}
	}()

	parser := &StmtParser{}

	// 解析表明
	tableName := parser.parseTable(stmt)
	t := p.router.FindTable(db, tableName)
	if t == nil {
		log.Error("[delete] table %s.%s doesn.t exist", db, tableName)
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", db, tableName)
	}

	var matchs []Match
	if stmt.Where != nil {
		var err error
		matchs, err = parser.parseWhere(stmt.Where)
		if err != nil {
			log.Error("handle delete parse where error(%v)", err)
			return nil, err
		}
		log.Debug("matchs %v", matchs)
	}

	parseTime = time.Now()
	affectedRows, err := p.doDelete(t, matchs)
	if err != nil {
		return nil, err
	}
	ret := new(mysql.Result)
	ret.AffectedRows = affectedRows
	ret.Status = 0
	return ret, nil
}

func (p *Proxy) doDelete(t *Table, matches []Match) (affected uint64, err error) {
	pbMatches, err := makePBMatches(t, matches)
	if err != nil {
		log.Error("[delete]covert where matches failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return 0, err
	}
	key, scope, err := findPKScope(t, pbMatches)
	if err != nil {
		log.Error("[delete]get pk scope failed(%v), Table: %s.%s", err, t.DbName(), t.Name())
		return 0, err
	}
	// TODO: sync pool
	dreq := &kvrpcpb.KvDeleteRequest{
		Key:          key,
		Scope:        scope,
		WhereFilters: pbMatches,
		Timestamp:    p.clock.Now(),
	}
	affected, err = p.deleteRemote(t.DbName(), t.Name(), dreq)
	if err != nil {
		log.Error("[delete]delete failed. err: %v, key: %v, scope: %v", err, key, scope)
	} else {
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[delete]delete success. affected: %v, key: %v, scope: %v", affected, key, scope)
		}
	}
	return
}

func (p *Proxy) deleteRemote(db, table string, req *kvrpcpb.KvDeleteRequest) (uint64, error) {
	t := p.router.FindTable(db, table)
	if t == nil {
		return 0, ErrNotExistTable
	}
	proxy := KvProxy{
		cli:          p.nodeCli,
		msCli:        p.msCli,
		clock:        p.clock,
		table:        t,
		findRoute:    t.FindRoute,
		writeTimeout: client.WriteTimeout,
		readTimeout:  client.ReadTimeoutShort,
	}

	// single delete
	if len(req.Key) > 0 {
		_key := &metapb.Key{Key: req.Key, Type: metapb.KeyType_KT_Ordinary}
		resp, _, err := proxy.kvDelete(req, _key)
		if err != nil {
			return 0, err
		}
		if resp.GetCode() == 0 {
			return resp.GetAffectedKeys(), nil
		} else {
			return 0, fmt.Errorf("remote server return error. Code: %d", resp.Code)
		}
	}

	// batch delete
	resps, err := proxy.kvsDelete(req, req.Scope)
	if err != nil {
		return 0, err
	}
	var affected uint64
	for _, resp := range resps {
		if resp.GetCode() == 0 {
			affected += resp.AffectedKeys
		} else {
			return 0, fmt.Errorf("remote server return error. Code: %d", resp.Code)
		}
	}
	return affected, nil
}
