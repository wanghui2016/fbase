package server

import (
	"fmt"

	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/encoding"
)

// RowDecoder  row decoder
type RowDecoder struct {
	fieldList []*kvrpcpb.SelectField // for select
	filters   []*kvrpcpb.Match       // where clause, for select and delete
	groupBys  []*metapb.Column       // for select

	decodeCols map[uint64]*metapb.Column
}

func newRowDecoder(fieldList []*kvrpcpb.SelectField, filters []*kvrpcpb.Match, groupBys []*metapb.Column) *RowDecoder {
	rd := &RowDecoder{
		fieldList: fieldList,
		filters:   filters,
		groupBys:  groupBys,
	}
	rd.init()
	return rd
}

func (d *RowDecoder) init() {
	// 选择需要从value中抽取哪些列
	d.decodeCols = make(map[uint64]*metapb.Column)
	// field lists
	for _, f := range d.fieldList {
		if f.Column != nil {
			d.decodeCols[f.Column.Id] = f.Column
		}
	}
	// where filter
	for _, m := range d.filters {
		d.decodeCols[m.Column.Id] = m.Column
	}
	// group bys
	for _, g := range d.groupBys {
		d.decodeCols[g.Id] = g
	}
}

// Decode decode
func (d *RowDecoder) Decode(buf []byte) (fields map[uint64]FieldValue, err error) {
	fields = make(map[uint64]FieldValue)
	// decode
	for len(buf) > 0 {
		// 解析列ID
		_, _, id, _, err := encoding.DecodeValueTag(buf)
		if err != nil {
			return nil, fmt.Errorf("decode row value tag failed(%v)", err)
		}
		colID := uint64(id)
		// 是否选择该列
		if col, ok := d.decodeCols[colID]; ok {
			remain, fval, err := d.decodeField(buf, col)
			if err != nil {
				return nil, fmt.Errorf("decode field(%v:%v) failed(%v), buf: %v", col.Id, col.Name, err, buf)
			}
			fields[colID] = fval
			buf = remain
		} else {
			_, l, err := encoding.PeekValueLength(buf)
			if err != nil {
				return nil, fmt.Errorf("decode row value length failed(%v)", err)
			}
			buf = buf[l:]
		}
	}
	return fields, nil
}

// DecodeAndFilter decode and filter
func (d *RowDecoder) DecodeAndFilter(buf []byte) (fields map[uint64]FieldValue, matched bool, err error) {
	fields, err = d.Decode(buf)
	if err != nil {
		return
	}
	matched = true
	if len(d.filters) > 0 {
		matched, err = Filter(fields, d.filters)
		return
	}
	return
}

func (d *RowDecoder) decodeField(buf []byte, col *metapb.Column) ([]byte, FieldValue, error) {
	switch col.DataType {
	case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
		remain, ival, err := encoding.DecodeIntValue(buf)
		if err != nil {
			return nil, nil, err
		}
		if col.Unsigned { // 无符号
			return remain, UIntValue(ival), nil
		}
		return remain, IntValue(ival), nil
	case metapb.DataType_Float, metapb.DataType_Double:
		remain, fval, err := encoding.DecodeFloatValue(buf)
		if err != nil {
			return nil, nil, err
		}
		return remain, FloatValue(fval), nil
	case metapb.DataType_Varchar, metapb.DataType_Binary, metapb.DataType_Date, metapb.DataType_TimeStamp:
		remain, bval, err := encoding.DecodeBytesValue(buf)
		if err != nil {
			return nil, nil, err
		}
		return remain, BytesValue(bval), nil
	default:
		return nil, nil, fmt.Errorf("RowDecoder::decodeField(): unsupported type(%s), column(%s)", col.DataType.String(), col.Name)
	}
}
