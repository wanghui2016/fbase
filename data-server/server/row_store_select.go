package server

import (
	"errors"

	"util/errcode"
	"model/pkg/kvrpcpb"
	"util/log"
)

// no aggregate function; no group by
func (s *RowStore) simpleSelect(req *kvrpcpb.KvSelectRequest) (*kvrpcpb.KvSelectResponse, error) {
	resp := new(kvrpcpb.KvSelectResponse)

	limit := req.GetLimit()
	if limit == nil {
		limit = &kvrpcpb.Limit{Offset: 0, Count: DefaultMaxKvCountLimit}
	} else {
		if limit.Count > DefaultMaxKvCountLimit {
			limit.Count = DefaultMaxKvCountLimit
		}
	}

	fetcher := newSelectRowFetcher(s, req)
	defer fetcher.Close()
	var all, count uint64
	for fetcher.Next() {
		if limit.Offset > all {
			all++
			continue
		}
		all++
		count++
		row := buildSimpleRow(fetcher.Key(), fetcher.Fields(), req.FieldList)
		resp.Rows = append(resp.Rows, row)
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("[range %d] select add row: %v-%v", s.id, fetcher.Key(), row.Fields)
		}
		if count >= limit.Count {
			break
		}
	}
	if fetcher.Error() != nil {
		return nil, fetcher.Error()
	}
	if len(resp.Rows) > 0 && log.GetFileLogger().IsEnableDebug() {
		log.Debug("[range %d] select offset %d rows: %d", s.id, all, len(resp.Rows))
	}
	resp.Offset = all
	resp.Code = errcode.SUCCESS
	return resp, nil
}

func (s *RowStore) selectGroupByAggre(req *kvrpcpb.KvSelectRequest) (*kvrpcpb.KvSelectResponse, error) {
	return nil, errors.New("not implement")
	// //TODO: offset, limit
	// rb := newAggreRowBuilder(req.FieldList, req.GroupBys)

	// fetcher := newSelectRowFetcher(s, req)
	// defer fetcher.Close()
	// for fetcher.Next() {
	// 	if err := rb.AddRow(fetcher.Fields()); err != nil {
	// 		return nil, err
	// 	}
	// }
	// if fetcher.Error() != nil {
	// 	return nil, fetcher.Error()
	// }
	// rows, err := rb.Build()
	// if err != nil {
	// 	return nil, err
	// }
	// resp := new(kvrpcpb.KvSelectResponse)
	// resp.Code = errcode.SUCCESS
	// resp.Rows = rows
	// return resp, nil
}

// Select select
func (s *RowStore) Select(req *kvrpcpb.KvSelectRequest) (*kvrpcpb.KvSelectResponse, error) {
	resp := new(kvrpcpb.KvSelectResponse)

	defer func() {
		if x := recover(); x != nil {
			log.Error("[range %d] select panic: %v", s.id, x)
			resp.Code = -1
		}
	}()

	// 是否有聚合函数
	var hasAggreFunc bool
	for _, f := range req.FieldList {
		if f.Typ == kvrpcpb.SelectField_AggreFunction {
			hasAggreFunc = true
			break
		}
	}

	// no group-by, no aggregate function
	if len(req.GroupBys) == 0 && !hasAggreFunc {
		return s.simpleSelect(req)
	}

	// only aggregate function
	if len(req.GroupBys) == 0 {
		return s.selectSimpleAggre(req)
	}

	return s.selectGroupByAggre(req)
}

func (s *RowStore) selectSimpleAggre(req *kvrpcpb.KvSelectRequest) (*kvrpcpb.KvSelectResponse, error) {
	resp := new(kvrpcpb.KvSelectResponse)

	aggreCals := make([]aggreCalculator, 0, len(req.FieldList))
	for _, f := range req.FieldList {
		if f.Typ != kvrpcpb.SelectField_AggreFunction {
			return nil, errors.New("mixture of aggregate and column select field")
		}
		cal, err := newAggreCalculator(f.AggreFunc, f.Column)
		if err != nil {
			return nil, err
		}
		aggreCals = append(aggreCals, cal)
	}

	fetcher := newSelectRowFetcher(s, req)
	defer fetcher.Close()
	var offset uint64
	for fetcher.Next() {
		offset++
		for i, cal := range aggreCals {
			col := req.FieldList[i].Column
			if col != nil {
				cal.Update(fetcher.Fields()[col.Id])
			} else {
				cal.Update(nil)
			}
		}
	}
	if fetcher.Error() != nil {
		return nil, fetcher.Error()
	}
	resp.Offset = offset
	resp.Rows = []*kvrpcpb.Row{buildAggreRow(aggreCals)}
	resp.Code = errcode.SUCCESS
	return resp, nil
}

func buildAggreRow(cals []aggreCalculator) *kvrpcpb.Row {
	var buf []byte
	counts := make([]int64, 0, len(cals))
	for _, cal := range cals {
		if log.GetFileLogger().IsEnableDebug() {
			log.Debug("aggre result: %v, count: %v", cal.GetResult(), cal.GetCount())
		}
		buf = EncodeFieldValue(buf, cal.GetResult())
		counts = append(counts, cal.GetCount())
	}
	return &kvrpcpb.Row{
		Fields:       buf,
		AggredCounts: counts,
	}
}

func buildSimpleRow(key []byte, fields map[uint64]FieldValue, fieldList []*kvrpcpb.SelectField) *kvrpcpb.Row {
	var buf []byte
	for _, f := range fieldList {
		buf = EncodeFieldValue(buf, fields[f.Column.Id])
	}
	return &kvrpcpb.Row{
		Key:    key,
		Fields: buf,
	}
}
