package server

import (
	"golang.org/x/net/context"
	"model/pkg/errorpb"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/log"
)

func (s *Server) KvRawGet(ctx context.Context, req *kvrpcpb.DsKvRawGetRequest) (*kvrpcpb.DsKvRawGetResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	rangeId := req.GetHeader().GetRangeId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	_resp, pbErr := s.handleRawGet(ctx, rangeId, req.GetReq())
	resp := &kvrpcpb.DsKvRawGetResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
			Error:     pbErr,
		},
		Resp: _resp,
	}
	return resp, nil
}

func (s *Server) KvRawPut(ctx context.Context, req *kvrpcpb.DsKvRawPutRequest) (*kvrpcpb.DsKvRawPutResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	rangeId := req.GetHeader().GetRangeId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	_resp, pbErr := s.handleRawPut(ctx, rangeId, req.GetReq())
	resp := &kvrpcpb.DsKvRawPutResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
			Error:     pbErr,
		},
		Resp: _resp,
	}
	return resp, nil
}

func (s *Server) KvRawDelete(ctx context.Context, req *kvrpcpb.DsKvRawDeleteRequest) (*kvrpcpb.DsKvRawDeleteResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	rangeId := req.GetHeader().GetRangeId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	_resp, pbErr := s.handleRawDelete(ctx, rangeId, req.GetReq())
	resp := &kvrpcpb.DsKvRawDeleteResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
			Error:     pbErr,
		},
		Resp: _resp,
	}
	return resp, nil
}

func (s *Server) KvRawExecute(ctx context.Context, req *kvrpcpb.DsKvRawExecuteRequest) (*kvrpcpb.DsKvRawExecuteResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	return &kvrpcpb.DsKvRawExecuteResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
		},
	}, nil
}

func (s *Server) KvSelect(ctx context.Context, req *kvrpcpb.DsKvSelectRequest) (*kvrpcpb.DsKvSelectResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	rangeId := req.GetHeader().GetRangeId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	_resp, pbErr := s.handleSelect(ctx, req.GetHeader().GetReplicaReadable(), rangeId, req.GetReq())
	resp := &kvrpcpb.DsKvSelectResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
			Error:     pbErr,
		},
		Resp: _resp,
	}
	return resp, nil
}

func (s *Server) KvInsert(ctx context.Context, req *kvrpcpb.DsKvInsertRequest) (*kvrpcpb.DsKvInsertResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	rangeId := req.GetHeader().GetRangeId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	_resp, pbErr := s.handleInsert(ctx, rangeId, req.GetReq())
	resp := &kvrpcpb.DsKvInsertResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
			Error:     pbErr,
		},
		Resp: _resp,
	}
	return resp, nil
}

func (s *Server) KvDelete(ctx context.Context, req *kvrpcpb.DsKvDeleteRequest) (*kvrpcpb.DsKvDeleteResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	rangeId := req.GetHeader().GetRangeId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	_resp, pbErr := s.handleDelete(ctx, rangeId, req.GetReq())
	resp := &kvrpcpb.DsKvDeleteResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
			Error:     pbErr,
		},
		Resp: _resp,
	}
	return resp, nil
}

func (s *Server) KvUpdate(ctx context.Context, req *kvrpcpb.DsKvUpdateRequest) (*kvrpcpb.DsKvUpdateResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	return &kvrpcpb.DsKvUpdateResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
		},
	}, nil
}

func (s *Server) KvReplace(ctx context.Context, req *kvrpcpb.DsKvReplaceRequest) (*kvrpcpb.DsKvReplaceResponse, error) {
	clusterId := req.GetHeader().GetClusterId()
	traceId := req.GetHeader().GetTraceId()
	timestamp := req.GetHeader().GetTimestamp()
	timestamp = s.clock.Update(timestamp)

	return &kvrpcpb.DsKvReplaceResponse{
		Header: &kvrpcpb.ResponseHeader{
			ClusterId: clusterId,
			Timestamp: timestamp,
			TraceId:   traceId,
			Now:       s.clock.Now(),
		},
	}, nil
}

func (s *Server) handleRawGet(ctx context.Context, rangeId uint64, req *kvrpcpb.KvRawGetRequest) (*kvrpcpb.KvRawGetResponse, *errorpb.Error) {
	rngs, err := s.GetRanges(rangeId, READ)
	if err != nil {
		return nil, err
	}
	// 遇到range刚刚分裂
	if len(rngs) == 2 {
		err = &errorpb.Error{RangeOffline: &errorpb.RangeOffline{RangeId: rangeId}}
	}
	var rng *Range
	var _err *errorpb.Error
	for _, rng = range rngs {
		_err = rng.CheckKey(req.GetKey())
		if _err == nil {
			resp := rng.RawGet(ctx, req)
			// update range metric
			if resp != nil {
				rng.addMetric(req.Size(), resp.Size())
			} else {
				rng.addMetric(req.Size(), 0)
			}
			return resp, err
		}
	}

	return nil, _err
}

func (s *Server) handleRawPut(ctx context.Context, rangeId uint64, req *kvrpcpb.KvRawPutRequest) (*kvrpcpb.KvRawPutResponse, *errorpb.Error) {
	rngs, err := s.GetRanges(rangeId, WRITE)
	if err != nil {
		return nil, err
	}
	// 遇到range刚刚分裂
	if len(rngs) == 2 {
		err = &errorpb.Error{RangeOffline: &errorpb.RangeOffline{RangeId: rangeId}}
	}
	var rng *Range
	var _err *errorpb.Error
	for _, rng = range rngs {
		_err = rng.CheckKey(req.GetKey())
		if _err == nil {
			resp := rng.RawPut(ctx, req)
			// update range metric
			if resp != nil {
				rng.addMetric(req.Size(), resp.Size())
			} else {
				rng.addMetric(req.Size(), 0)
			}
			return resp, err
		}
	}

	return nil, _err
}

func (s *Server) handleRawDelete(ctx context.Context, rangeId uint64, req *kvrpcpb.KvRawDeleteRequest) (*kvrpcpb.KvRawDeleteResponse, *errorpb.Error) {
	rngs, err := s.GetRanges(rangeId, WRITE)
	if err != nil {
		return nil, err
	}
	// 遇到range刚刚分裂
	if len(rngs) == 2 {
		err = &errorpb.Error{RangeOffline: &errorpb.RangeOffline{RangeId: rangeId}}
	}
	var rng *Range
	var _err *errorpb.Error
	for _, rng = range rngs {
		_err = rng.CheckKey(req.GetKey())
		if _err == nil {
			resp := rng.RawDelete(ctx, req)
			// update range metric
			if resp != nil {
				rng.addMetric(req.Size(), resp.Size())
			} else {
				rng.addMetric(req.Size(), 0)
			}
			return resp, err
		}
	}

	return nil, _err
}

func (s *Server) handleSelect(ctx context.Context, repReadAble bool, rangeId uint64, req *kvrpcpb.KvSelectRequest) (*kvrpcpb.KvSelectResponse, *errorpb.Error) {
	rng, err := s.GetRange(rangeId, repReadAble, READ)
	if err != nil {
		return nil, err
	}
	req.Timestamp = s.clock.Now()
	resp := rng.Select(ctx, req)
	// update metric
	if resp != nil {
		rng.addMetric(req.Size(), resp.Size())
	} else {
		rng.addMetric(req.Size(), 0)
	}

	return resp, nil
}

func (s *Server) handleInsert(ctx context.Context, rangeId uint64, req *kvrpcpb.KvInsertRequest) (*kvrpcpb.KvInsertResponse, *errorpb.Error) {
	rngs, err := s.GetRanges(rangeId, WRITE)
	if err != nil {
		return nil, err
	}

	var rng *Range
	var affectedKeys uint64
	var _err *errorpb.Error
	var resp *kvrpcpb.KvInsertResponse
	req.Timestamp = s.clock.Now()
	// TODO rollback when failed ??
	for _, rng = range rngs {
		resp, _err = rng.Insert(ctx, req)
		// update range metric
		if resp != nil {
			rng.addMetric(req.Size(), resp.Size())
		} else {
			rng.addMetric(req.Size(), 0)
		}

		if _err != nil {
			// 分裂
			if len(rngs) == 2 {
				// 返回原range不存在
				return nil, &errorpb.Error{RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId}}
			} else {
				return nil, _err
			}
		}
		affectedKeys += resp.AffectedKeys
		if resp.Code > 0 {
			resp.AffectedKeys = affectedKeys
			return resp, err
		}
	}
	resp = &kvrpcpb.KvInsertResponse{
		Code:         0,
		AffectedKeys: affectedKeys,
	}
	// 遇到range刚刚分裂
	if len(rngs) == 2 {
		err = &errorpb.Error{RangeOffline: &errorpb.RangeOffline{RangeId: rangeId}}
	}
	return resp, err
}

func (s *Server) handleDelete(ctx context.Context, rangeId uint64, req *kvrpcpb.KvDeleteRequest) (*kvrpcpb.KvDeleteResponse, *errorpb.Error) {
	rngs, err := s.GetRanges(rangeId, WRITE)
	if err != nil {
		return nil, err
	}
	var rng *Range
	var affectedKeys uint64
	req.Timestamp = s.clock.Now()
	// TODO rollback when failed ??
	for _, rng = range rngs {
		resp, _err := rng.Delete(ctx, req)
		// update range metric
		if resp != nil {
			rng.addMetric(req.Size(), resp.Size())
		} else {
			rng.addMetric(req.Size(), 0)
		}
		if _err != nil {
			// 分裂
			if len(rngs) == 2 {
				// 返回原range不存在
				return nil, &errorpb.Error{RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId}}
			} else {
				return nil, _err
			}
		}
		affectedKeys += resp.AffectedKeys
		if resp.Code > 0 {
			resp.AffectedKeys = affectedKeys
			return resp, nil
		}
	}
	resp := &kvrpcpb.KvDeleteResponse{
		Code:         0,
		AffectedKeys: affectedKeys,
	}
	return resp, nil
}

func (s *Server) GetRange(rangeId uint64, repReadAble bool, rw int) (*Range, *errorpb.Error) {
	rng, find := s.FindRange(rangeId)
	if !find {
		return nil, &errorpb.Error{
			RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId},
		}
	}
	var err *errorpb.Error
	if rw == READ {
		err = rng.CheckReadAble(repReadAble)
	} else {
		err = rng.CheckWriteAble()
	}
	if err != nil {
		return nil, err
	}
	return rng, nil
}

func (s *Server) GetRanges(rangeId uint64, rw int) ([]*Range, *errorpb.Error) {
	var rngs []*Range
	rng, find := s.FindRange(rangeId)
	if !find {
		return nil, &errorpb.Error{
			RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId},
		}
	}
	var err *errorpb.Error
	if rw == READ {
		err = rng.CheckReadAble(false)
	} else {
		err = rng.CheckWriteAble()
	}
	if err != nil {
		if err.RangeOffline == nil {
			return nil, err
		} else {
			if log.GetFileLogger().IsEnableDebug() {
				log.Debug("range split, and we proxy to request to new range")
			}
			left, find := s.FindRange(rng.left.GetId())
			if !find {
				log.Error("range[%d] not found", rng.left.GetId())
				return nil, &errorpb.Error{
					RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId},
				}
			}
			leader := left.GetLeader()
			if leader == nil || leader.GetNodeId() != s.node.GetId() || left.region.State != metapb.RangeState_R_Normal {
				return nil, &errorpb.Error{
					RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId},
				}
			}
			right, find := s.FindRange(rng.right.GetId())
			if !find {
				log.Error("range[%d] not found", rng.right.GetId())
				return nil, &errorpb.Error{
					RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId},
				}
			}
			leader = right.GetLeader()
			if leader == nil || leader.GetNodeId() != s.node.GetId() || right.region.State != metapb.RangeState_R_Normal {
				return nil, &errorpb.Error{
					RangeNotFound: &errorpb.RangeNotFound{RangeId: rangeId},
				}
			}
			rngs = append(rngs, left)
			rngs = append(rngs, right)
			return rngs, nil
		}
	}
	rngs = append(rngs, rng)
	return rngs, nil
}
