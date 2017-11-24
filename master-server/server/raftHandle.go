package server

import (
	"github.com/juju/errors"
	"raft"
	"util/log"
)

var (
	ErrUnknownCommandType        = errors.New("unknown command type")
	Timestamp             uint64 = 1
)

func (service *Server) HandleLeaderChange(leader uint64) {
	service.leader <- leader
}

func (service *Server) HandleFatalEvent(err *raft.FatalError) {
    // TODO event
	log.Error("raft fatal: id[%d], err[%v]", err.ID, err.Err)
}
