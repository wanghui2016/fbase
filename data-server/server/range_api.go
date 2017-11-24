package server

import (
	"fmt"
	"runtime"
	"time"

	"golang.org/x/net/context"
	"data-server/client"
	"model/pkg/errorpb"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"model/pkg/raft_cmdpb"
	"model/pkg/util"
	"raft"
	timerCtx "util/context"
	"util/deepcopy"
	"util/log"
)

const (
	SUCCESS            int32 = 0
	ER_NOT_LEADER            = 1
	ER_SERVER_BUSY           = 2
	ER_SERVER_STOP           = 3
	ER_READ_ONLY             = 4
	ER_ENTITY_NOT_EXIT       = 5
	ER_UNKNOWN               = 6
	ER_WRITE_TIMEOUT         = 7
	ER_READ_TIMEOUT          = 8

	// SQL ERROR CODE from 1000
)

func (r *Range) addMetric(reqSize int, respSize int) {
	r.metric.addRequest()
	r.metric.addInBytes(uint64(reqSize))
	r.metric.addOutBytes(uint64(respSize))
}

// RawGet kv get
func (r *Range) RawGet(ctx context.Context, req *kvrpcpb.KvRawGetRequest) *kvrpcpb.KvRawGetResponse {
	var resp *kvrpcpb.KvRawGetResponse
	if !r.raftGroup.IsLeader() {
		resp.Code = ER_NOT_LEADER
		return resp
	}
	start := time.Now()
	resp, err := r.rowstore.RawGet(req)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_raw_get", true, end.Sub(start))
	if err != nil {
		resp = new(kvrpcpb.KvRawGetResponse)
		resp.Code = mapCode(err)
	}

	return resp
}

// RawPut kv put
func (r *Range) RawPut(ctx context.Context, req *kvrpcpb.KvRawPutRequest) *kvrpcpb.KvRawPutResponse {
	var resp *kvrpcpb.KvRawPutResponse
	// read only
	if r.region.State == metapb.RangeState_R_Split {
		resp = new(kvrpcpb.KvRawPutResponse)
		resp.Code = ER_READ_ONLY
		return resp
	}
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()
	request.Request.RangeEpoch = r.region.RangeEpoch
	request.Request.CmdType = raft_cmdpb.CmdType_RawPut
	request.Request.KvRawPutReq = req
	start := time.Now()
	response, err := r.raftGroup.SubmitCommand(ctx, request)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_raw_put", err == nil, end.Sub(start))
	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)
	if err != nil {
		resp = new(kvrpcpb.KvRawPutResponse)
		resp.Code = mapCode(err)
	} else {
		resp = response.GetResponse().GetKvRawPutResp()
	}
	return resp
}

// RawDelete kv delete
func (r *Range) RawDelete(ctx context.Context, req *kvrpcpb.KvRawDeleteRequest) *kvrpcpb.KvRawDeleteResponse {
	var resp *kvrpcpb.KvRawDeleteResponse
	// read only
	if r.region.State == metapb.RangeState_R_Split {
		resp = new(kvrpcpb.KvRawDeleteResponse)
		resp.Code = ER_READ_ONLY
		return resp
	}
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()
	request.Request.RangeEpoch = r.region.RangeEpoch
	request.Request.CmdType = raft_cmdpb.CmdType_RawDelete
	request.Request.KvRawDeleteReq = req

	start := time.Now()
	response, err := r.raftGroup.SubmitCommand(ctx, request)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_raw_delete", true, end.Sub(start))
	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)
	if err != nil {
		resp = new(kvrpcpb.KvRawDeleteResponse)
		resp.Code = mapCode(err)
	} else {
		resp = response.GetResponse().GetKvRawDeleteResp()
	}
	return resp
}

// Select sql select
func (r *Range) Select(ctx context.Context, req *kvrpcpb.KvSelectRequest) *kvrpcpb.KvSelectResponse {
	var resp *kvrpcpb.KvSelectResponse
	// request := new(raft_cmdpb.RaftCmdRequest)
	// request.Type = raft_cmdpb.MessageType_Kv
	// request.Request = &raft_cmdpb.Request{
	// 	CmdType:   raft_cmdpb.CmdType_Select,
	// 	KvSelectReq: req,
	// }
	start := time.Now()
	resp, err := r.rowstore.Select(req)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_select", true, end.Sub(start))
	if err != nil {
		log.Error("[range %d] raft commit failed, err[%v]", r.GetRangeId(), err)
		resp = new(kvrpcpb.KvSelectResponse)
		resp.Code = mapCode(err)
	}
	return resp
}

// InsertBatch insert batch
type InsertBatch struct {
	req  *kvrpcpb.KvInsertRequest
	resp *kvrpcpb.KvInsertResponse
	done chan error
}

func (r *Range) BatchInsertWorker() {
	var batch *InsertBatch
	var ctx *timerCtx.TimerCtx
	var runCount uint64
	for {
		select {
		case <-r.quitCtx.Done():
			return
		case batch = <-r.insertQueue:
			runCount++
			var batchs []*InsertBatch
			var reqs []*kvrpcpb.KvInsertRequest
			var size, count int
			for _, row := range batch.req.Rows {
				size += len(row.GetKey())
				size += len(row.GetValue())
			}
			count++
			batchs = append(batchs, batch)
			reqs = append(reqs, batch.req)
			if size < int(1*MB) {
			LOOP:
				for {
					select {
					case <-r.quitCtx.Done():
						return
					case _batch := <-r.insertQueue:
						// range is closed
						if _batch == nil {
							return
						}
						for _, row := range _batch.req.Rows {
							size += len(row.GetKey())
							size += len(row.GetValue())
						}
						count++
						batchs = append(batchs, _batch)
						reqs = append(reqs, _batch.req)
						if size >= int(1*MB) || count >= 100 {
							break LOOP
						}
					default:
						break LOOP
					}
				}
			}

			if ctx == nil {
				ctx = timerCtx.NewTimerCtx(client.ReadTimeoutMedium)
			} else {
				ctx.Reset(client.ReadTimeoutMedium)
			}
			batchInsertReq := &kvrpcpb.KvBatchInsertRequest{
				Reqs: reqs,
			}
			resp, err := r.batchInsert(ctx, batchInsertReq)
			// check response err
			if err == nil {
				if resp.GetResponse().GetError() != nil {
					err = fmt.Errorf(resp.GetResponse().GetError().String())
				}
			}
			// get insert resps
			var batchInsertResps *kvrpcpb.KvBatchInsertResponse
			if err == nil {
				batchInsertResps = resp.GetResponse().GetKvBatchInsertResp()
				if len(batchInsertResps.Resps) != len(batchInsertReq.Reqs) {
					err = fmt.Errorf("inconsistent length of batch insert requests and responses")
				}
			}
			for i, b := range batchs {
				if err == nil {
					b.resp = batchInsertResps.Resps[i]
				}
				select {
				case b.done <- err:
				default:
				}
			}
			// 如果流量比较大，主动释放CPU
			if runCount%100 == 0 {
				runtime.Gosched()
			}
		}
	}
}

func (r *Range) insert(ctx context.Context, req *kvrpcpb.KvInsertRequest) (response *raft_cmdpb.RaftCmdResponse, err error) {
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()

	request.Request.RangeEpoch = r.region.RangeEpoch
	request.Request.CmdType = raft_cmdpb.CmdType_Insert
	request.Request.KvInsertReq = req

	start := time.Now()
	response, err = r.raftGroup.SubmitCommand(ctx, request)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_insert", true, end.Sub(start))
	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)
	return
}

func (r *Range) batchInsert(ctx context.Context, req *kvrpcpb.KvBatchInsertRequest) (response *raft_cmdpb.RaftCmdResponse, err error) {
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()

	request.Request.RangeEpoch = r.region.RangeEpoch
	request.Request.CmdType = raft_cmdpb.CmdType_BatchInsert
	request.Request.KvBatchInsertReq = req

	start := time.Now()
	response, err = r.raftGroup.SubmitCommand(ctx, request)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_insert", true, end.Sub(start))
	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)
	return
}

// Insert sql insert
func (r *Range) Insert(ctx context.Context, req *kvrpcpb.KvInsertRequest) (*kvrpcpb.KvInsertResponse, *errorpb.Error) {
	// TODO check key in range
	var rows []*kvrpcpb.KeyValue
	var _req *kvrpcpb.KvInsertRequest
	var resp *kvrpcpb.KvInsertResponse
	for _, row := range req.GetRows() {
		if r.CheckKey(row.GetKey()) == nil {
			rows = append(rows, row)
		}
	}
	if len(rows) == len(req.GetRows()) {
		_req = req
	} else if len(rows) == 0 {
		resp = new(kvrpcpb.KvInsertResponse)
		resp.Code = 0
		resp.AffectedKeys = 0
		return resp, nil
	} else {
		_req = &kvrpcpb.KvInsertRequest{
			Rows:           rows,
			CheckDuplicate: req.GetCheckDuplicate(),
			Timestamp:      req.GetTimestamp(),
		}
	}
	var response *raft_cmdpb.RaftCmdResponse
	var err error
	// 需要Key重复检查，暂时不合并提交
	if req.GetCheckDuplicate() {
		response, err = r.insert(ctx, _req)
	} else if r.server.conf.InsertBatchSwitch {
		batch := &InsertBatch{
			req:  _req,
			done: make(chan error, 1),
		}
		select {
		case <-r.quitCtx.Done():
			resp = new(kvrpcpb.KvInsertResponse)
			resp.Code = ER_SERVER_STOP
			return resp, nil
		case <-ctx.Done():
			resp = new(kvrpcpb.KvInsertResponse)
			resp.Code = ER_WRITE_TIMEOUT
			return resp, nil
		case r.insertQueue <- batch:
			select {
			case <-r.quitCtx.Done():
				resp = new(kvrpcpb.KvInsertResponse)
				resp.Code = ER_SERVER_STOP
				return resp, nil
			case <-ctx.Done():
				resp = new(kvrpcpb.KvInsertResponse)
				resp.Code = ER_WRITE_TIMEOUT
				return resp, nil
			case err = <-batch.done:
				if err == nil {
					if batch.resp == nil {
						err = fmt.Errorf("unexpected batch insert null response")
					} else {
						// 写成功了
						response = new(raft_cmdpb.RaftCmdResponse)
						response.Response = &raft_cmdpb.Response{
							KvInsertResp: &kvrpcpb.KvInsertResponse{
								Code:         SUCCESS,
								AffectedKeys: batch.resp.AffectedKeys,
							},
						}
					}
				}
			}
		// 批量队列已经满了，告警并单独提交
		default:
			log.Warn("range[%s] insert batch queueu is full!!, maybe we need expand queue", r.String())
			response, err = r.insert(ctx, _req)
		}
	} else {
		response, err = r.insert(ctx, _req)
	}

	if err != nil {
		log.Error("range[%s] raft commit failed, err[%v]", r.String(), err)
		if err == ErrStaleCmd {
			return nil, &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}
		}
		if err == raft.ErrNotLeader {
			leader := r.GetLeader()
			if leader == nil {
				return nil, &errorpb.Error{NoLeader: &errorpb.NoLeader{RangeId: r.GetRangeId()}}
			} else {
				epoch := deepcopy.Iface(r.region.GetRangeEpoch()).(*metapb.RangeEpoch)
				return nil, &errorpb.Error{NotLeader: &errorpb.NotLeader{RangeId: r.GetRangeId(), Epoch: epoch, Leader: leader}}
			}
		}
		resp = new(kvrpcpb.KvInsertResponse)
		resp.Code = mapCode(err)
	} else if response.GetResponse().GetError() != nil {
		return nil, response.GetResponse().GetError()
	} else {
		resp = response.GetResponse().GetKvInsertResp()
	}
	log.Debug("raft commit success")
	return resp, nil
}

// Delete sql delete
func (r *Range) Delete(ctx context.Context, req *kvrpcpb.KvDeleteRequest) (*kvrpcpb.KvDeleteResponse, *errorpb.Error) {
	var resp *kvrpcpb.KvDeleteResponse
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()

	request.Request.RangeEpoch = r.region.RangeEpoch
	request.Request.CmdType = raft_cmdpb.CmdType_Delete
	request.Request.KvDeleteReq = req

	start := time.Now()
	response, err := r.raftGroup.SubmitCommand(ctx, request)
	end := time.Now()
	r.server.metricMeter.AddApiWithDelay("raft_delete", true, end.Sub(start))
	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)

	if err != nil {
		log.Error("[range %d] raft commit failed, err[%v]", r.GetRangeId(), err)
		if err == ErrStaleCmd {
			return nil, &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}
		}
		if err == raft.ErrNotLeader {
			leader := r.GetLeader()
			if leader == nil {
				return nil, &errorpb.Error{NoLeader: &errorpb.NoLeader{RangeId: r.GetRangeId()}}
			} else {
				epoch := deepcopy.Iface(r.region.GetRangeEpoch()).(*metapb.RangeEpoch)
				return nil, &errorpb.Error{NotLeader: &errorpb.NotLeader{RangeId: r.GetRangeId(), Epoch: epoch, Leader: leader}}
			}
		}
		resp = new(kvrpcpb.KvDeleteResponse)
		resp.Code = mapCode(err)
	} else if response.GetResponse().GetError() != nil {
		return nil, response.GetResponse().GetError()
	} else {
		resp = response.GetResponse().GetKvDeleteResp()
	}
	return resp, nil
}

// Replace sql replace
func (r *Range) Replace(ctx context.Context, req *kvrpcpb.KvReplaceRequest) (*kvrpcpb.KvReplaceResponse, *errorpb.Error) {
	var resp *kvrpcpb.KvReplaceResponse
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()
	request.Request.CmdType = raft_cmdpb.CmdType_Replace
	request.Request.KvReplaceReq = req
	response, err := r.raftGroup.SubmitCommand(ctx, request)

	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)
	if err != nil {
		resp = new(kvrpcpb.KvReplaceResponse)
		resp.Code = mapCode(err)
	} else if response.GetResponse().GetError() != nil {
		return nil, response.GetResponse().GetError()
	} else {
		resp = response.GetResponse().GetKvReplaceResp()
	}
	return resp, nil
}

// Update sql update
func (r *Range) Update(ctx context.Context, req *kvrpcpb.KvUpdateRequest) (*kvrpcpb.KvUpdateResponse, *errorpb.Error) {
	var resp *kvrpcpb.KvUpdateResponse
	request := util.GetRaftCmdReq()
	request.Type = raft_cmdpb.MessageType_Data
	request.Request = util.GetRaftReq()
	request.Request.CmdType = raft_cmdpb.CmdType_Update
	request.Request.KvUpdateReq = req
	response, err := r.raftGroup.SubmitCommand(ctx, request)

	//request is be Marshaled in SubmitCommand,can release
	util.PutRaftReq(request.Request)
	util.PutRaftCmdReq(request)

	if err != nil {
		resp = new(kvrpcpb.KvUpdateResponse)
		resp.Code = mapCode(err)
	} else if response.GetResponse().GetError() != nil {
		return nil, response.GetResponse().GetError()
	} else {
		resp = response.GetResponse().GetKvUpdateResp()
	}
	return resp, nil
}
