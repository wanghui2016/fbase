package server

import (
	"time"
	"net/http"
	"strconv"

	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/statspb"
	"util/log"
	"model/pkg/taskpb"
)

type NodeDebug struct {
	*metapb.Node
	Ranges []*Range 	`json:"ranges"`
	Stats  *statspb.NodeStats 	`json:"stats"`
	LastHbTime   time.Time 		`json:"last_hb_time"`
	LastSchTime  time.Time 		`json:"last_sch_time"`
	LastOpt      *taskpb.Task 	`json:"last_opt"`
}

type RangeDebug struct {
	*metapb.Range
	Options *metapb.Option 		`json:"options"`
	Leader       *metapb.Peer 	`json:"leader"`
	Stats        *statspb.RangeStats `json:"stats"`
	DownPeers    []*mspb.PeerStats 	`json:"down_peers"`
	PendingPeers []*metapb.Peer 	`json:"pending_peers"`
	LastHbTime   time.Time 		`json:"last_hb_time"`
	LastSchTime  time.Time 		`json:"last_sch_time"`
	LastSch      *taskpb.Task	`json:last_sch`
}

func (service *Server) handleDebugNodeInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	nodeid, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong nodeid"
		return
	}
	data, err := service.cluster.instances.MarshalNode(nodeid)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	reply.Data = data
}

func (service *Server) handleDebugRangeInfo(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	nodeid, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong nodeid"
		return
	}
	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong rangeid"
		return
	}
	node, ok := service.cluster.instances.FindInstance(nodeid)
	if !ok {
		reply.Code = HTTP_ERROR
		reply.Message = "node is not existed"
		return
	}
	data, err := node.ranges.MarshalRange(rangeid)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	reply.Data = data
}

// 把miss的分片归到正式路由中
func (service *Server) handleDebugRangeHome(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong rangeid"
		return
	}
	cluster := service.cluster
	if _, find := cluster.FindRange(rangeid); find {
		log.Info("range[%d] already in home", rangeid)
		return
	}
	if r, find := cluster.FindRangeInMissCache(rangeid); find {
		table, ok := cluster.FindCurTableById(r.GetDbId(), r.GetTableId())
		if !ok {
			log.Warn("table[%s:%s] not found", r.GetDbName(), r.GetTableName())
			reply.Code = HTTP_ERROR_TABLE_FIND
			reply.Message = http_error_table_find
			return
		}
		err = table.RoutesUpdate(cluster, r)
		if err != nil {
			log.Warn("table[%s:%s] update routes failed, err[%v]", r.GetDbName(), r.GetTableName(), err)
			reply.Code = HTTP_ERROR
			reply.Message = http_error
			return
		}
	} else {
		log.Warn("range[%d] not found", rangeid)
		reply.Code = HTTP_ERROR_RANGE_FIND
		reply.Message = http_error_range_find
		return
	}
	log.Info("range[%d] to home success!!", rangeid)
}

func (service *Server) handleDebugRangeRaftStatus(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeId, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}

	region, find := service.cluster.FindRange(rangeId)
	if !find {
		reply.Code = HTTP_ERROR
		reply.Message = "range is not found"
		return
	}
	reply.Data = region.RaftStatus
}

func (service *Server) handleDebugLogSetLevel(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	level := r.FormValue("level")
	log.SetLevel(level)
	w.Write([]byte("OK"))
}

func (service *Server) handleDebugRangeTrace(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	rangeid, err := strconv.ParseUint(r.FormValue(HTTP_RANGE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong rangeid"
		return
	}
	trace := r.FormValue("trace")
	cluster := service.cluster
	if r, find := cluster.FindRange(rangeid); find {
		if trace == "true" {
			r.Trace = true
		} else {
			r.Trace = false
		}
		return
	}

	log.Info("range[%d] trace success!!", rangeid)
}

func (service *Server) handleDebugNodeTrace(w http.ResponseWriter, r *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)

	nodeid, err := strconv.ParseUint(r.FormValue(HTTP_NODE_ID), 10, 64)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = "wrong rangeid"
		return
	}
	trace := r.FormValue("trace")
	cluster := service.cluster
	if node, find := cluster.FindInstance(nodeid); find {
		if trace == "true" {
			node.Trace = true
		} else {
			node.Trace = false
		}
	}
	log.Info("node[%d] trace success!!", nodeid)
}