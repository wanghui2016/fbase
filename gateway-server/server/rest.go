package server

import (
	"errors"
	"fmt"

	"util/bufalloc"
	"util/log"
)

type Field_ struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
}

type And struct {
	Field  *Field_ `json:"field"`
	Relate string  `json:"relate"`
	Or     []*Or   `json:"or"`
}

type Or struct {
	Field  *Field_   `json:"field"`
	Relate MatchType `json:"relate"`
	And    []*And    `json:"and"`
}

type Limit_ struct {
	Offset   uint64 `json:"offset"`
	RowCount uint64 `json:"rowcount"`
}

type Order struct {
	By   string `json:"by"`
	Desc bool   `json:"desc"`
}

type Scope struct {
	Start []byte `json:"start"`
	End   []byte `json:"end"`
}

type Filter_ struct {
	And   []*And   `json:"and"`
	Scope *Scope   `json:"scope"`
	Limit *Limit_  `json:"limit"`
	Order []*Order `json:"order"`
}

type Command struct {
	Version string          `json:"version"`
	Type    string          `json:"type"`
	Field   []string        `json:"field"`
	Values  [][]interface{} `json:"values"`
	Filter  *Filter_        `json:"filter"`
	PKs     [][]*And        `json:"pks"`
}

type Query struct {
	Sign         string   `json:"sign"`
	DatabaseName string   `json:"databasename"`
	TableName    string   `json:"tablename"`
	Command      *Command `json:"command"`
}

type Reply struct {
	Code         int             `json:"code"`
	Message string 			`json:"message"`
	RowsAffected uint64          `json:"rowsaffected"`
	Values       [][]interface{} `json:"values"`
}

func (q *Query) parseColumnNames() []string {
	return q.Command.Field
}

func (q *Query) parseRowValues(buffer bufalloc.Buffer) ([]InsertRowValue, error) {
	if len(q.Command.Field) == 0 {
		return nil, errors.New("len(command.field) == 0")
	}

	var err error
	indexes := make([]int, 0, len(q.Command.Values)*len(q.Command.Field))
	for _, vs := range q.Command.Values {
		if len(vs) != len(q.Command.Field) {
			return nil, errors.New(fmt.Sprintf("len(values) != len(field) %v,%v",vs,q.Command.Field))
		}
		for _, v := range vs {
			if v == nil {
				indexes = append(indexes, buffer.Len())
				continue
			}
			_, err = fmt.Fprintf(buffer, "%v", v)
			if err != nil {
				return nil, err
			}
			indexes = append(indexes, buffer.Len())
		}
	}

	var values []InsertRowValue
	data := buffer.Bytes()
	var idx, prevIdx, pos int
	for i := 0; i < len(q.Command.Values); i++ {
		value := make([]SQLValue, 0, len(q.Command.Values[i]))
		for j := 0; j < len(q.Command.Values[i]); j++ {
			idx = indexes[pos]
			log.Debug("prev: %v, idx: %v", prevIdx, idx)
			value = append(value, data[prevIdx:idx])
			prevIdx = idx
			pos++
		}
		values = append(values, value)
	}

	return values, nil
}

func (q *Query) parseMatchs(ands []*And) ([]Match, error) {
	// TODO just AND now
	matchs := make([]Match, 0)

	var (
		column_    string
		sqlValue_  []byte
		matchType_ MatchType
	)

	for _, and := range ands {
		log.Debug("and: %v", and)
		if and.Field == nil || len(and.Field.Column) == 0 {
			return nil, errors.New("and field is nil")
		}
		column_ = and.Field.Column
		//col := t.FindColumn(column_)
		switch and.Relate {
		case "=":
			matchType_ = Equal
		case "!=":
			matchType_ = NotEqual
		case "<":
			matchType_ = Less
		case "<=":
			matchType_ = LessOrEqual
		case ">":
			matchType_ = Larger
		case ">=":
			matchType_ = LargerOrEqual
		default:
			return nil, errors.New("invalid type")
		}

		sqlValue_ = []byte(fmt.Sprintf("%v", and.Field.Value))
		matchs = append(matchs, Match{
			column:    column_,
			sqlValue:  sqlValue_,
			matchType: matchType_,
		})
	}

	return matchs, nil
}

func (q *Query) parseLimit() *Limit {
	if q.Command.Filter == nil || q.Command.Filter.Limit == nil {
		return nil
	}
	l := q.Command.Filter.Limit
	if l.Offset != 0 || l.RowCount != 0 {
		return &Limit{
			offset:   l.Offset,
			rowCount: l.RowCount,
		}
	} else {
		return nil
	}
}

func (q *Query) parseOrder() []*Order {
	if q.Command.Filter == nil || len(q.Command.Filter.Order) == 0 {
		return nil
	}
	o := q.Command.Filter.Order
	if len(o) != 0 {
		return o
	} else {
		return nil
	}
}

func (q *Query) parseScope() *Scope {
	if q.Command.Filter == nil {
		return nil
	}
	return q.Command.Filter.Scope
}
