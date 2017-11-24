package server

import (
	"time"
	"math/rand"
	"util/log"
)

type Filter interface {
	FilterInstance(ins *Instance) bool
	FilterRange(r *Range) bool
}

type DebugInstanceFilter struct {
	size        int
	num         int

	offset      int
	index       int
	count       int
}

func NewDebugInstanceFilter(size, num int) Filter {
	if size < num {
		return nil
	}
	rand.Seed(time.Now().UnixNano())
	return &DebugInstanceFilter{
		size:         size,
		num:          num,
		offset:       rand.Intn(size-num+1),
		index:        0,
		count:        0,
	}
}

func (r *DebugInstanceFilter) FilterInstance(ins *Instance) bool {
	if r == nil {
		return false
	}
	if r.index != r.offset {
		r.index++
		return false
	} else {
		r.count++
		if r.count <= r.num {
			return true
		}
	}
	return false
}

func (r *DebugInstanceFilter) FilterRange(region *Range) bool {
	return false
}

type DebugTransferFilter struct {
	except     []*Instance
}

func NewDebugTransferFilter(except []*Instance) Filter {
	return &DebugTransferFilter{except: except}
}

func (t *DebugTransferFilter) FilterInstance(ins *Instance) bool {
	if !ins.AllocSch() {
		return false
	}

	if ins.GetDiskFree() < MinDiskFree {
		return false
	}

	for _, n := range t.except {
		if n.Id == ins.Id {
			return false
		}
	}

	return true
}

func (t *DebugTransferFilter) FilterRange(r *Range) bool {
	return false
}

type TransferFilter struct {
	cluster    *Cluster
	// 指定机房
	expectRoom *Room
	switchsMap map[string]struct{}
	macsMap    map[string]struct{}
}

func NewTransferFilter(cluster *Cluster, expectRoom *Room, switchsMap, macsMap map[string]struct{}) Filter {
	return &TransferFilter{
		cluster: cluster,
		expectRoom: expectRoom,
		switchsMap: switchsMap,
		macsMap: macsMap}
}

func (t *TransferFilter) FilterInstance(ins *Instance) bool {
	if !ins.AllocSch() {
		return false
	}

	if ins.GetDiskFree() < MinDiskFree {
		return false
	}

	mac, find := t.cluster.FindMachine(ins.MacIp)
	if !find {
		log.Warn("machine[%s] not exist", ins.MacIp)
		return false
	}
	if mac.GetRoom().GetRoomName() != t.expectRoom.GetRoomName() {
		log.Warn("diff room: %v != %v", mac.GetRoom().GetRoomName(), t.expectRoom.GetRoomName())
		return false
	}

	if _, ok := t.macsMap[ins.MacIp]; ok {
		log.Warn("dup machine: %v", ins.MacIp)
		return false
	}
	if _, ok := t.switchsMap[mac.GetSwitchIp()]; ok {
		log.Warn("dup switch: %v", mac.GetSwitchIp())
		return false
	}

	return true
}

func (t *TransferFilter) FilterRange(r *Range) bool {
	return false
}

type DeployInstanceFilter struct {

}

func NewDeployInstanceFilter() Filter {
	return &DeployInstanceFilter{

	}
}

func (r *DeployInstanceFilter) FilterInstance(ins *Instance) bool {
	if r == nil {
		return false
	}
	if !ins.AllocSch() {
		return false
	}

	if ins.GetDiskFree() < MinDiskFree {
		return false
	}

	return true
}

func (r *DeployInstanceFilter) FilterRange(region *Range) bool {
	return false
}