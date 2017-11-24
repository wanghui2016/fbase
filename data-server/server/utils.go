package server

import (
	"model/pkg/kvrpcpb"
	"util/encoding"
)

const (
	KB   uint64 = 1024
	MB          = 1024 * KB
	GB          = 1024 * MB
	PB          = 1024 * GB
)

func EncodeRow(fields []*kvrpcpb.Field) []byte {
	buff := make([]byte, 0)
	for _, f := range fields {
		// encode column ID
		buff = encoding.EncodeIntValue(buff, encoding.NoColumnID, int64(f.ColumnId))
		// encode value
		if f.Value == nil {
			buff = encoding.EncodeNullValue(buff, encoding.NoColumnID)
		} else {
			buff = encoding.EncodeBytesValue(buff, encoding.NoColumnID, f.Value)
		}
	}
	return buff
}

var (
	RowPrefix   = []byte("0")
	IndexPrefix = []byte("1")
)

func DecodeKey(key []byte) []byte {
	return key[1:]
}

// key in store is | flag(1 byte)| real key |
// flag: 0  nomal row.
// flag: 1  column index.
func EncodeKey(key []byte) []byte {
	buff := make([]byte, 0, len(key)+1)
	buff = append(buff, RowPrefix...)
	if len(key) > 0 {
		buff = append(buff, key...)
	}
	return buff
}

func DecodeIndex(buff []byte) *kvrpcpb.Field {
	var columnId int64
	var value []byte
	var err error
	buff, columnId, err = encoding.DecodeIntValue(buff)
	if err != nil {
		return nil
	}
	_, _, _, typ, err := encoding.DecodeValueTag(buff)
	if err != nil {
		return nil
	}
	switch typ {
	case encoding.Null:
	case encoding.Bytes:
		buff, value, err = encoding.DecodeBytesValue(buff)
		if err != nil {
			return nil
		}
	}
	return &kvrpcpb.Field{
		ColumnId: uint64(columnId),
		Value:    value,
	}
}

func EncodeIndex(field *kvrpcpb.Field) []byte {
	buff := make([]byte, 0, len(field.Value)+8+1)
	buff = append(buff, IndexPrefix...)
	buff = encoding.EncodeIntValue(buff, encoding.NoColumnID, int64(field.ColumnId))
	if field.Value == nil {
		buff = encoding.EncodeNullValue(buff, encoding.NoColumnID)
	} else {
		buff = encoding.EncodeBytesValue(buff, encoding.NoColumnID, field.Value)
	}
	return buff
}
