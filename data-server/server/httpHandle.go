package server

import (
	"fmt"
	"net/http"
	"strconv"
	"encoding/json"

	"util/log"
	"model/pkg/metapb"
)

func (s *Server) handleDebugRangeState(w http.ResponseWriter, r *http.Request) {
	rangeIdStr := r.FormValue("rangeid")
	if len(rangeIdStr) == 0 {
		log.Warn("invalid request param")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rangeId, err := strconv.ParseUint(rangeIdStr, 10, 64)
	if err != nil {
		log.Error("invalid rangeId[%s]", rangeIdStr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rng, find := s.FindRange(rangeId)
	if !find {
		w.Write([]byte(fmt.Sprintf("{\"error\": \"not found range[%d]\"}", rangeId)))
		return
	}
	status := "{\"store\": " + rng.rowstore.Status() + ", \"raft\":" + s.raftServer.Status(rangeId).String() + "}"
	w.Write([]byte(status))
	return
}

func (s *Server) handleDebugRangeInfo(w http.ResponseWriter, r *http.Request) {
	rangeIdStr := r.FormValue("rangeid")
	if len(rangeIdStr) == 0 {
		log.Warn("invalid request param")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rangeId, err := strconv.ParseUint(rangeIdStr, 10, 64)
	if err != nil {
		log.Error("invalid rangeId[%s]", rangeIdStr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rng, find := s.FindRange(rangeId)
	if !find {
		log.Warn("{\"error\": \"not found range[%d]\"}", rangeId)
		w.Write([]byte(fmt.Sprintf("no such range[%d]", rangeId)))
		return
	}
	data, err := json.Marshal(rng.region)
	if err != nil {
		log.Error("range marshal failed, err[%v]", err)
		return
	}
	w.Write(data)
	return
}

func (s *Server) handleTraceRange(w http.ResponseWriter, r *http.Request) {
	rangeIdStr := r.FormValue("rangeid")
	if len(rangeIdStr) == 0 {
		log.Warn("invalid request param")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	trace := r.FormValue("trace")
	rangeId, err := strconv.ParseUint(rangeIdStr, 10, 64)
	if err != nil {
		log.Error("invalid rangeId[%s]", rangeIdStr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rng, find := s.FindRange(rangeId)
	if !find {
		log.Warn("{\"error\": \"not found range[%d]\"}", rangeId)
		w.Write([]byte(fmt.Sprintf("no such range[%d]", rangeId)))
		return
	}
	if trace == "true" {
		rng.debug = true
	} else {
		rng.debug = false
	}

	w.Write([]byte("OK"))
	return
}

func (s *Server) handleDebugLogSetLevel(w http.ResponseWriter, r *http.Request) {
	log.SetLevel(r.FormValue("level"))
	w.Write([]byte("OK"))
}

type serverDebugInfo struct {
	RangeCount int `json:"ranges"`

	FailHB     int    `json:"fail_hb"`
	LastHB     string `json:"last_hb"`
	LastHBSucc string `json:"last_hb_succ"`
}

func (s *Server) handleDebugServerInfo(w http.ResponseWriter, r *http.Request) {
	info := &serverDebugInfo{
		RangeCount: len(s.ranges),
		FailHB:     s.heartbeatFailCount,
		LastHB:     s.lastHeartbeat.String(),
		LastHBSucc: s.lastHeartbeatSuccess.String(),
	}
	data, _ := json.Marshal(info)
	w.Write(data)
	return
}

func (s *Server) handleStopRange (w http.ResponseWriter, r *http.Request) {
	rangeIdStr := r.FormValue("rangeid")
	if len(rangeIdStr) == 0 {
		log.Warn("invalid request param")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rangeId, err := strconv.ParseUint(rangeIdStr, 10, 64)
	if err != nil {
		log.Error("invalid rangeId[%s]", rangeIdStr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rng, find := s.FindRange(rangeId)
	if !find {
		log.Warn("{\"error\": \"not found range[%d]\"}", rangeId)
		w.Write([]byte(fmt.Sprintf("no such range[%d]", rangeId)))
		return
	}
	rng.region.State = metapb.RangeState_R_Abnormal
	w.Write([]byte("OK"))
	log.Info("range[%s] ban rw success", rng.String())
	return
}

func (s *Server) handleCloseRange(w http.ResponseWriter, r *http.Request) {
	rangeIdStr := r.FormValue("rangeid")
	if len(rangeIdStr) == 0 {
		log.Warn("invalid request param")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rangeId, err := strconv.ParseUint(rangeIdStr, 10, 64)
	if err != nil {
		log.Error("invalid rangeId[%s]", rangeIdStr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rng := s.DelRange(rangeId)
	if rng == nil {
		log.Warn("{\"error\": \"not found range[%d]\"}", rangeId)
		w.Write([]byte(fmt.Sprintf("no such range[%d]", rangeId)))
		return
	}
	rng.Close()
	w.Write([]byte("OK"))
	log.Info("range[%s] close success", rng.String())
	return
}

func (s *Server) handleConfigSet(w http.ResponseWriter, r *http.Request) {
	diskQuotaStr := r.FormValue("diskQuota")
	if len(diskQuotaStr) != 0 {
		diskQuota, err := strconv.ParseUint(diskQuotaStr, 10, 64)
		if err != nil {
			log.Error("diskQuota parse uint64 error: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.conf.diskQuota = diskQuota
	}
	w.Write([]byte("OK"))
}

func (s *Server) handleConfigGet(w http.ResponseWriter, r *http.Request) {
	var resp string
	diskStr := r.FormValue("disk")
	if len(diskStr) != 0 {
		resp = fmt.Sprintf(`# disk
quota: %v`, s.conf.diskQuota)
	}
	w.Write([]byte(resp))
}
