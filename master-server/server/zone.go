package server

import (
	"sync"

	"model/pkg/metapb"
)

type Zone struct {
	*metapb.Zone

	rooms *RoomCache
}

func NewZone(zone *metapb.Zone) *Zone {
	return &Zone{Zone: zone, rooms: NewRoomCache()}
}

func (z *Zone) AddRoom(r *Room) {
	z.rooms.Add(r)
}

func (z *Zone) DelRoom(room string) {
	z.rooms.Delete(room)
}

func (z *Zone) FindRoom(room string) (*Room, bool) {
	return z.rooms.FindRoom(room)
}

func (z *Zone) GetAllRooms() ([]*Room) {
	return z.rooms.GetAllRooms()
}

type ZoneCache struct {
	lock      sync.RWMutex
	zones     map[string]*Zone
}

func NewZoneCache() *ZoneCache {
	return &ZoneCache{zones: make(map[string]*Zone)}
}

func (mc *ZoneCache) Reset() {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	mc.zones = make(map[string]*Zone)
}

func (mc *ZoneCache) Add(z *Zone) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	mc.zones[z.GetZoneName()] = z
}

func (mc *ZoneCache) Delete(name string) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	delete(mc.zones, name)
}

func (mc *ZoneCache) FindZone(name string) (*Zone, bool) {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	if n, find := mc.zones[name]; find {
		return n, true
	}
	return nil, false
}

func (mc *ZoneCache) GetAllZones() []*Zone {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	var zones []*Zone
	for _, n := range mc.zones {
		zones = append(zones, n)
	}
	return zones
}

func (mc *ZoneCache) Size() int {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	return len(mc.zones)
}