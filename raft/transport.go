package raft

import (
	"raft/proto"
)

type Transport interface {
	Send(m *proto.Message)
	SendSnapshot(m *proto.Message, rs *snapshotStatus)
	Stop()
}
