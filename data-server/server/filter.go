package server

import (
	"fmt"

	"model/pkg/kvrpcpb"
	"util/log"
)

// 同一个列的where条件放到一起
func splitFilterByColID(filters []*kvrpcpb.Match) map[uint64][]*kvrpcpb.Match {
	if len(filters) == 0 {
		return nil
	}
	result := make(map[uint64][]*kvrpcpb.Match)
	for _, m := range filters {
		result[m.Column.Id] = append(result[m.Column.Id], m)
	}
	return result
}

// Filter 过滤
func Filter(fileds map[uint64]FieldValue, filters []*kvrpcpb.Match) (bool, error) {
	splitedMatches := splitFilterByColID(filters)
	for col, matches := range splitedMatches {
		f, ok := fileds[col]
		if !ok {
			// 没有该列
			log.Warn("columnId(%s) is missing when filter", col)
			return false, nil
		}
		b, err := matchField(f, matches)
		if err != nil {
			log.Warn("match filed failed(%v)", err)
			return false, err
		}
		// 被过滤掉该行
		if !b {
			return false, nil
		}
	}
	return true, nil
}

func matchField(value FieldValue, matches []*kvrpcpb.Match) (bool, error) {
	for _, m := range matches {
		threshold, err := ParseFieldFromString(m.Threshold, m.Column)
		if err != nil {
			return false, err
		}
		switch m.MatchType {
		case kvrpcpb.MatchType_Equal:
			if !value.Equal(threshold) {
				return false, nil
			}
		case kvrpcpb.MatchType_Larger:
			if !value.Greater(threshold) {
				return false, nil
			}
		case kvrpcpb.MatchType_LargerOrEqual:
			if value.Less(threshold) {
				return false, nil
			}
		case kvrpcpb.MatchType_Less:
			if !value.Less(threshold) {
				return false, nil
			}
		case kvrpcpb.MatchType_LessOrEqual:
			if value.Greater(threshold) {
				return false, nil
			}
		case kvrpcpb.MatchType_NotEqual:
			if value.Equal(threshold) {
				return false, nil
			}
		default:
			return false, fmt.Errorf("unsupported match type: %v", m.MatchType)
		}
	}
	return true, nil
}
