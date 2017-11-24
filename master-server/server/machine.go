package server

import (
	"sync"

	"model/pkg/metapb"
)

type Machine struct {
	*metapb.Machine

	instances   *InstanceCache
}

func NewMachine(m *metapb.Machine) *Machine {
	return &Machine{Machine: m, instances: NewInstanceCache()}
}

func (m *Machine) AddInstance(i *Instance) {
    m.instances.Add(i)
}

func (m *Machine) DelInstance(id uint64) {
	m.instances.Delete(id)
}

func (m *Machine) GetInstance(id uint64) (*Instance, bool) {
	return m.instances.FindInstance(id)
}

func (m *Machine) GetAllInstances() []*Instance {
	return m.instances.GetAllInstances()
}

func (m *Machine) GetAllRanges() []*Range {
	var ranges []*Range
	for _, ins := range m.GetAllInstances() {
		rngs := ins.GetAllRanges()
		if len(rngs) > 0 {
			ranges = append(ranges, rngs...)
		}
	}
	return ranges
}

type MachineCache struct {
	lock      sync.RWMutex
	macs     map[string]*Machine
}

func NewMachineCache() *MachineCache {
	return &MachineCache{macs: make(map[string]*Machine)}
}

func (mc *MachineCache) Reset() {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	mc.macs = make(map[string]*Machine)
}

func (mc *MachineCache) Add(m *Machine) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	mc.macs[m.GetIp()] = m
}

func (mc *MachineCache) Delete(ip string) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	delete(mc.macs, ip)
}

func (mc *MachineCache) FindMachine(ip string) (*Machine, bool) {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	if n, find := mc.macs[ip]; find {
		return n, true
	}
	return nil, false
}

func (mc *MachineCache) GetAllMachine() []*Machine {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	var macs []*Machine
	for _, n := range mc.macs {
		macs = append(macs, n)
	}
	return macs
}

func (mc *MachineCache) Size() int {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	return len(mc.macs)
}