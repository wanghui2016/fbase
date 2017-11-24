package server

import (
	"fmt"

	"model/pkg/metapb"
)

type aggreCalculator interface {
	Update(value FieldValue)

	GetCount() int64
	GetResult() FieldValue
}

func newAggreCalculator(name string, col *metapb.Column) (aggreCalculator, error) {
	switch name {
	case "count":
		return &countCalculator{col: col}, nil
	case "min":
		return &minCalculator{col: col}, nil
	case "max":
		return &maxCalculator{col: col}, nil
	case "sum":
		return &sumCalculator{col: col}, nil
	case "avg":
		return &sumCalculator{col: col}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function(%v)", name)
	}
}

type countCalculator struct {
	count int64
	col   *metapb.Column
}

func (cc *countCalculator) Update(value FieldValue) {
	// count(*)
	if cc.col == nil {
		cc.count++
		return
	}

	if value != nil {
		cc.count++
	}
}

func (cc *countCalculator) GetCount() int64 {
	return 0
}

func (cc *countCalculator) GetResult() FieldValue {
	return IntValue(cc.count)
}

type minCalculator struct {
	min FieldValue
	col *metapb.Column
}

func (mic *minCalculator) Update(value FieldValue) {
	if value != nil {
		if mic.min == nil || value.Less(mic.min) {
			mic.min = value
		}
	}
}

func (mic *minCalculator) GetCount() int64 {
	return 0
}

func (mic *minCalculator) GetResult() FieldValue {
	return mic.min
}

type maxCalculator struct {
	max FieldValue
	col *metapb.Column
}

func (mac *maxCalculator) Update(value FieldValue) {
	if value != nil {
		if mac.max == nil || value.Greater(mac.max) {
			mac.max = value
		}
	}
}

func (mac *maxCalculator) GetCount() int64 {
	return 0
}

func (mac *maxCalculator) GetResult() FieldValue {
	return mac.max
}

type sumCalculator struct {
	sum   FieldValue
	count int64
	col   *metapb.Column
}

func (sc *sumCalculator) Update(value FieldValue) {
	if value == nil || !value.Summable() {
		return
	}
	if sc.sum == nil {
		sc.sum = value
	} else {
		sc.sum = sc.sum.Add(value)
	}
	sc.count++
}

func (sc *sumCalculator) GetCount() int64 {
	return sc.count
}

func (sc *sumCalculator) GetResult() FieldValue {
	return sc.sum
}
