package server

import (
	"sync"

	"model/pkg/metapb"
)

type Room struct {
	*metapb.Room
	
	macs   *MachineCache
}

func NewRoom(r *metapb.Room) *Room {
	return &Room{Room: r, macs: NewMachineCache()}
}

func (r *Room) AddMachine(m *Machine) {
	r.macs.Add(m)
}

func (r *Room) DelMachine(ip string) {
	r.macs.Delete(ip)
}

func (r *Room) GetMachine(ip string) (*Machine, bool) {
	return r.macs.FindMachine(ip)
}

func (r *Room) GetAllMachines() []*Machine {
	return r.macs.GetAllMachine()
}

type RoomCache struct {
	lock      sync.RWMutex
	rooms     map[string]*Room
}

func NewRoomCache() *RoomCache {
	return &RoomCache{rooms: make(map[string]*Room)}
}

func (rc *RoomCache) Reset() {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	rc.rooms = make(map[string]*Room)
}

func (rc *RoomCache) Add(r *Room) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	rc.rooms[r.GetRoomName()] = r
}

func (rc *RoomCache) Delete(room string) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	delete(rc.rooms, room)
}

func (rc *RoomCache) FindRoom(room string) (*Room, bool) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if n, find := rc.rooms[room]; find {
		return n, true
	}
	return nil, false
}

func (rc *RoomCache) GetAllRooms() []*Room {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	var rooms []*Room
	for _, r := range rc.rooms {
		rooms = append(rooms, r)
	}
	return rooms
}

func (rc *RoomCache) Size() int {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	return len(rc.rooms)
}