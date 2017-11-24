package server

import (
	"bytes"
	"fmt"
	"strconv"

	"model/pkg/metapb"
	"util/encoding"
)

// FieldValue field data
type FieldValue interface {
	Default() FieldValue

	Less(than FieldValue) bool
	Greater(than FieldValue) bool
	Equal(than FieldValue) bool

	Summable() bool
	Add(with FieldValue) FieldValue
	Divide(count int64) FieldValue

	// keep order
	EncodeAscending([]byte) []byte
	// with colID, don't keep order
	Encode([]byte, uint32) []byte
}

// UIntValue  unsigned integer
type UIntValue uint64

// Default default
func (u UIntValue) Default() FieldValue {
	return UIntValue(uint64(0))
}

// Less <
func (u UIntValue) Less(than FieldValue) bool {
	return u < than.(UIntValue)
}

// Greater >
func (u UIntValue) Greater(than FieldValue) bool {
	return u > than.(UIntValue)
}

// Equal ==
func (u UIntValue) Equal(than FieldValue) bool {
	return u == than.(UIntValue)
}

// Summable summable
func (u UIntValue) Summable() bool {
	return true
}

// Add sum
func (u UIntValue) Add(with FieldValue) FieldValue {
	return UIntValue(uint64(u) + uint64(with.(UIntValue)))
}

// Divide for avg
func (u UIntValue) Divide(count int64) FieldValue {
	return FloatValue(float64(u) / float64(count))
}

// EncodeAscending encode ascending
func (u UIntValue) EncodeAscending(b []byte) []byte {
	return encoding.EncodeUvarintAscending(b, uint64(u))
}

// Encode encode
func (u UIntValue) Encode(b []byte, colID uint32) []byte {
	return encoding.EncodeIntValue(b, colID, int64(u))
}

// IntValue  integer
type IntValue int64

// Default default
func (i IntValue) Default() FieldValue {
	return IntValue(int64(0))
}

// Less <
func (i IntValue) Less(than FieldValue) bool {
	return i < than.(IntValue)
}

// Greater >
func (i IntValue) Greater(than FieldValue) bool {
	return i > than.(IntValue)
}

// Equal ==
func (i IntValue) Equal(than FieldValue) bool {
	return i == than.(IntValue)
}

// Summable summable
func (i IntValue) Summable() bool {
	return true
}

// Add sum
func (i IntValue) Add(with FieldValue) FieldValue {
	return IntValue(int64(i) + int64(with.(IntValue)))
}

// Divide for avg
func (i IntValue) Divide(count int64) FieldValue {
	return FloatValue(float64(i) / float64(count))
}

// EncodeAscending encode ascending
func (i IntValue) EncodeAscending(b []byte) []byte {
	return encoding.EncodeVarintAscending(b, int64(i))
}

// Encode encode
func (i IntValue) Encode(b []byte, colID uint32) []byte {
	return encoding.EncodeIntValue(b, colID, int64(i))
}

// FloatValue  float field data
type FloatValue float64

// Default Default
func (f FloatValue) Default() FieldValue {
	return FloatValue(float64(0))
}

// Less <
func (f FloatValue) Less(than FieldValue) bool {
	return f < than.(FloatValue)
}

// Greater >
func (f FloatValue) Greater(than FieldValue) bool {
	return f > than.(FloatValue)
}

// Equal ==
func (f FloatValue) Equal(than FieldValue) bool {
	return f == than.(FloatValue)
}

// Summable summable
func (f FloatValue) Summable() bool {
	return true
}

// Add sum
func (f FloatValue) Add(with FieldValue) FieldValue {
	return FloatValue(float64(f) + float64(with.(FloatValue)))
}

// Divide for avg
func (f FloatValue) Divide(count int64) FieldValue {
	return FloatValue(float64(f) / float64(count))
}

// EncodeAscending encode ascending
func (f FloatValue) EncodeAscending(b []byte) []byte {
	return encoding.EncodeFloatAscending(b, float64(f))
}

// Encode encode
func (f FloatValue) Encode(b []byte, colID uint32) []byte {
	return encoding.EncodeFloatValue(b, colID, float64(f))
}

// BytesValue  bytes or string
type BytesValue []byte

// Default default
func (b BytesValue) Default() FieldValue {
	return BytesValue(nil)
}

// Less <
func (b BytesValue) Less(than FieldValue) bool {
	return bytes.Compare(b, than.(BytesValue)) < 0
}

// Greater >
func (b BytesValue) Greater(than FieldValue) bool {
	return bytes.Compare(b, than.(BytesValue)) > 0
}

// Equal ==
func (b BytesValue) Equal(than FieldValue) bool {
	return bytes.Equal(b, than.(BytesValue))
}

// Summable summable
func (b BytesValue) Summable() bool {
	return false
}

// Add sum
func (b BytesValue) Add(with FieldValue) FieldValue {
	return IntValue(0)
}

// Divide for avg
func (b BytesValue) Divide(count int64) FieldValue {
	return FloatValue(0)
}

// EncodeAscending encode ascending
func (b BytesValue) EncodeAscending(buf []byte) []byte {
	return encoding.EncodeBytesAscending(buf, b)
}

// Encode encode
func (b BytesValue) Encode(buf []byte, colID uint32) []byte {
	return encoding.EncodeBytesValue(buf, colID, b)
}

// ParseFieldFromString decode field value from sql string
func ParseFieldFromString(sval []byte, col *metapb.Column) (FieldValue, error) {
	switch col.DataType {
	case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
		if col.Unsigned { // 无符号
			ival, err := strconv.ParseUint(string(sval), 10, 64)
			if err != nil {
				return nil, err
			}
			return UIntValue(ival), nil
		} else { // 有符号
			ival, err := strconv.ParseInt(string(sval), 10, 64)
			if err != nil {
				return nil, err
			}
			return IntValue(ival), nil
		}
	case metapb.DataType_Float, metapb.DataType_Double:
		fval, err := strconv.ParseFloat(string(sval), 64)
		if err != nil {
			return nil, err
		}
		return FloatValue(fval), nil
	case metapb.DataType_Varchar, metapb.DataType_Binary, metapb.DataType_Date, metapb.DataType_TimeStamp:
		return BytesValue(sval), nil
	default:
		return nil, fmt.Errorf("unsupported type(%s), column(%s)", col.DataType.String(), col.Name)
	}
}

// EncodeFieldValue 编码FieldValue
func EncodeFieldValue(buf []byte, val FieldValue) []byte {
	if val == nil {
		return encoding.EncodeNullValue(buf, encoding.NoColumnID)
	}
	return val.Encode(buf, encoding.NoColumnID)
}

func EncodeFieldValueAscending(buf []byte, val FieldValue) []byte {
	if val == nil {
		return encoding.EncodeNullAscending(buf)
	}
	return val.EncodeAscending(buf)
}
