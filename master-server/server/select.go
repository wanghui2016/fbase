package server

import (
	"sort"
	"time"
	"math/rand"

	"model/pkg/metapb"
	"util/log"
)

type Selecter interface {
	SelectInstance(n int, fs ...Filter) ([]*Instance)
	SelectRange(n int, fs ...Filter) ([]*Range)
}

type TransferSelect struct {
	zones []*Zone
	policy *metapb.DeployV1Policy
}

func NewTransferSelect(zones []*Zone, policy *metapb.DeployV1Policy) Selecter {
	return &TransferSelect{zones: zones, policy: policy}
}

func (t *TransferSelect) findZone(name string) *Zone {
	for _, z := range t.zones {
		if z.GetZoneName() == name {
			return z
		}
	}
	return nil
}

func (t *TransferSelect) SelectInstance(num int, fs ...Filter) ([]*Instance) {
	var group []*Instance

	// 目标机房
	var rooms []*Room
	//master zone
	mZoneP := t.policy.GetMaster()
	zone := t.findZone(mZoneP.GetZoneName())
	_rooms := selectRooms(mZoneP, zone)
	if len(_rooms) > 0 {
		rooms = append(rooms, _rooms...)
	}

	// slaves Zone
	for _, sZoneP := range t.policy.GetSlaves() {
		zone := t.findZone(sZoneP.GetZoneName())
		_rooms := selectRooms(sZoneP, zone)
		if len(_rooms) > 0 {
			rooms = append(rooms, _rooms...)
		}
	}

	for _, room := range rooms {
		macs := room.GetAllMachines()
		if len(macs) == 0 {
			continue
		}
		_macs := MacsByRangesSlice(macs)
		sort.Sort(_macs)
		func(filters []Filter) {
			for _, mac := range _macs {
				inss := InstanceByLoadSlice(mac.GetAllInstances())
				sort.Sort(inss)
				for _, _ins := range inss {
					ok := true
					for _, f := range filters {
						if !f.FilterInstance(_ins) {
							ok = false
							break
						}
					}
					if !ok {
						continue
					}
					group = append(group, _ins)
					return
				}
			}
		} (fs)
		if len(group) >= num {
			return group[:num]
		}
	}
	return nil
}

func (t *TransferSelect) SelectRange(n int, fs ...Filter) ([]*Range) {
	return nil
}

// 本机搭建测试环境使用
type DebugTransferSelect struct {
	inss []*Instance
}

func NewDebugTransferSelect(inss []*Instance) Selecter {
	return &DebugTransferSelect{inss: inss}
}

func (t *DebugTransferSelect) SelectInstance(num int, fs ...Filter) ([]*Instance) {
	var group []*Instance
	inss := InstanceByLoadSlice(t.inss)
	sort.Sort(inss)
	for _, _ins := range inss {
		ok := true
		for _, f := range fs {
			if !f.FilterInstance(_ins) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		group = append(group, _ins)
	}

	// TODO 按照负载排序
	// 选择负载最小的nodes
	if len(group) < num {
		// ErrNotEnoughResources
		return nil
	}

	return group[:num]
}

func (t *DebugTransferSelect) SelectRange(n int, fs ...Filter) ([]*Range) {
	return nil
}

// 本机搭建测试环境使用
type DebugDeploySelect struct {
	inss []*Instance
}

func NewDebugDeploySelect(inss []*Instance) Selecter {
	return &DebugDeploySelect{inss: inss}
}

func RandSlic(inss []*Instance) []*Instance{
	if len(inss) == 0 {
		return nil
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	offset := r.Intn(len(inss))
	randomNodes := make([]*Instance,0,len(inss))
	randomNodes = append(randomNodes,inss[offset:]...)
	randomNodes = append(randomNodes,inss[0:offset]...)

	return randomNodes
}

func (t *DebugDeploySelect) SelectInstance(num int, fs ...Filter) ([]*Instance) {
	randomInss := RandSlic(t.inss)

	if len(randomInss) < num {
		return nil
	}

	var group []*Instance
	for _, ins := range randomInss {
		find := true
		for _, f := range fs {
			if !f.FilterInstance(ins) {
				find = false
				break
			}
		}
		if !find {
			continue
		}
		group = append(group, ins)
	}

	// TODO 按照负载排序
	// 选择负载最小的nodes
	if len(group) < num {
		// ErrNotEnoughResources
		return nil
	}

	//开发环境不需要node的ip必须不同,可以设置NeedNodeIpDiff为false
	return group[:num]
}

func (t *DebugDeploySelect) SelectRange(n int, fs ...Filter) ([]*Range) {
	return nil
}

type DeployV1Select struct {
	zones []*Zone
	//默认random策略
	policy *metapb.DeployV1Policy
}

func NewDeployV1Select(zones []*Zone, policy *metapb.DeployV1Policy) Selecter {
	return &DeployV1Select{zones: zones, policy: policy}
}

func (d *DeployV1Select) findZone(name string) *Zone {
	for _, z := range d.zones {
		if z.GetZoneName() == name {
			return z
		}
	}
	return nil
}

// 只支持三副本
func (d *DeployV1Select) SelectInstance(num int, fs ...Filter) ([]*Instance) {
	var instances []*Instance
	var inssMap map[string]*Instance
	var switchMap map[string]*Instance
	inssMap = make(map[string]*Instance)
	switchMap = make(map[string]*Instance)

	// 目标机房
	var rooms []*Room
	//master zone
	mZoneP := d.policy.GetMaster()
	zone := d.findZone(mZoneP.GetZoneName())
	_rooms := selectRooms(mZoneP, zone)
	if len(_rooms) > 0 {
		rooms = append(rooms, _rooms...)
	}

	// slaves Zone
	for _, sZoneP := range d.policy.GetSlaves() {
		zone := d.findZone(sZoneP.GetZoneName())
		_rooms := selectRooms(sZoneP, zone)
		if len(_rooms) > 0 {
			rooms = append(rooms, _rooms...)
		}
	}
	for _, room := range rooms {
		macs := room.GetAllMachines()
		if len(macs) == 0 {
			continue
		}
		_macs := MacsByRangesSlice(macs)
		sort.Sort(_macs)
		func (filters []Filter) {
			for _, mac := range _macs {
				// 一个交换机只选一个节点
				if _, find := switchMap[mac.GetSwitchIp()]; find {
					continue
				}
				// 一个物理节点只选择一个节点
				if _, find := inssMap[mac.GetIp()]; find {
					continue
				}
				// TODO 默认每个物理机都会启动一定数量的DS
				inss := mac.GetAllInstances()
				log.Debug("getall instances: %v", inss)
				if len(inss) == 0 {
					continue
				}
				_inss := InstanceByRangesSlice(inss)
				sort.Sort(_inss)
				for _, ins := range _inss {
					find := true
					for _, f := range filters {
						if !f.FilterInstance(ins) {
							find = false
							break
						}
					}
					if !find {
						continue
					}
					instances = append(instances, ins)
					switchMap[mac.GetSwitchIp()] = ins
					inssMap[mac.GetIp()] = ins
					return
				}
			}
		}(fs)
	}
    if len(instances) >= num {
	    return instances[:num]
    }
	return nil
}

func (d *DeployV1Select) SelectRange(n int, fs ...Filter) ([]*Range) {
	return nil
}

type MacsByRangesSlice []*Machine

func (p MacsByRangesSlice) Len() int {
	return len(p)
}

func (p MacsByRangesSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p MacsByRangesSlice) Less(i int, j int) bool {
	return len(p[i].GetAllRanges()) > len(p[j].GetAllRanges())
}

func selectRooms(zoneP *metapb.ZoneV1Policy, zone *Zone) ([]*Room) {
	var rooms []*Room
	if zoneP == nil {
		log.Error("invalid policy")
		return nil
	}
	if zone == nil {
		log.Error("invalid zone[%s]", zoneP.GetZoneName())
		return nil
	}
	mRoomP := zoneP.GetMaster()
	mRoom, find := zone.FindRoom(mRoomP.GetRoomName())
	if !find {
		log.Error("invalid room[%s:%s]", zoneP.GetZoneName(), mRoomP.GetRoomName())
		return nil
	}
	for i := int32(0); i < mRoomP.GetDsNum(); i++ {
		rooms = append(rooms, mRoom)
	}
	for _, sRoomP := range zoneP.GetSlaves() {
		sRoom, find := zone.FindRoom(sRoomP.GetRoomName())
		if !find {
			log.Error("invalid room[%s:%s]", zoneP.GetZoneName(), sRoomP.GetRoomName())
			return nil
		}
		for i := int32(0); i < sRoomP.GetDsNum(); i++ {
			rooms = append(rooms, sRoom)
		}
	}
	return rooms
}
