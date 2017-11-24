package server

import (
	"sync"
	"fmt"

	"util/log"
	"model/pkg/metapb"
)

type Session struct {
	id uint64
	wanted int
	notify chan *metapb.TopologyEpoch
	//notifyRoute chan *RouteData
	quit chan struct{}
}

func NewSession(id uint64) *Session {
	return &Session{
		id: id,
		notify: make(chan *metapb.TopologyEpoch, 1), // TODO make(chan Data, 1000)
		//notifyRoute: make(chan *RouteData),
		quit: make(chan struct{}),
	}
}

type TableSession struct {
	sLock   sync.RWMutex
	sessions map[uint64]*Session
}

func NewTableSession() *TableSession {
	return &TableSession{sessions: make(map[uint64]*Session)}
}

func (ts *TableSession) AddSession(s *Session) {
	ts.sLock.Lock()
	defer ts.sLock.Unlock()
	ts.sessions[s.id] = s
}

func (ts *TableSession) DelSession(id uint64) {
	ts.sLock.Lock()
	defer ts.sLock.Unlock()
	delete(ts.sessions, id)
}

func (ts *TableSession) NotifyAllSession(d *metapb.TopologyEpoch) {
	ts.sLock.RLock()
	defer ts.sLock.RUnlock()
	log.Debug("route notify all session ...")

	for _, s := range ts.sessions {
		select {
		case <-s.quit:
			log.Debug("delete session: %d", s.id)
		case s.notify <-d:
			log.Debug("notify route")
		default:
			log.Warn("session channel closed")
		}
	}
}

type SessionManager struct {
	lock   sync.Mutex
	maxSessionId    uint64

	rLock  sync.RWMutex
	routeSessions map[string]*TableSession
	//routeNotify  chan *RouteData
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		routeSessions: make(map[string]*TableSession), // tableId
		//routeNotify: make(chan *RouteData, 100)
		}
}

func (sm *SessionManager) GenSessionId() uint64 {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.maxSessionId++
	return sm.maxSessionId
}

func (sm *SessionManager) AddSession(table string) (*Session){
	id := sm.GenSessionId()
	s := NewSession(id)

	sm.rLock.Lock()
	defer sm.rLock.Unlock()
	var ts *TableSession
	var ok bool
	if ts, ok = sm.routeSessions[table]; !ok {
		sm.routeSessions[table] = NewTableSession()
		ts = sm.routeSessions[table]
	}
	ts.AddSession(s)
	log.Debug("add rout session :%v", table)
	return s
}

func (sm *SessionManager) DelSession(table string, id uint64) {
	sm.rLock.Lock()
	defer sm.rLock.Unlock()
	var ts *TableSession
	var ok bool
	if ts, ok = sm.routeSessions[table]; !ok {
		return
	}
	ts.DelSession(id)
	log.Debug("del route session :%v", table)
}

func (sm *SessionManager) Notify(data interface{}) {
	switch d := data.(type) {
	case *metapb.TopologyEpoch:
		log.Debug("route notify")
		//sm.routeNotify <- d
		table := fmt.Sprintf("$%d#%d", d.DbId, d.TableId)
	    ts, find := sm.findSession(table)
		if !find {
			//log.Warn("not found table[%s] session", table)
			return
		}
	    ts.NotifyAllSession(d)
	}
}

func (sm *SessionManager) findSession(table string) (*TableSession, bool) {
	sm.rLock.RLock()
	defer sm.rLock.RUnlock()
	if ts, ok := sm.routeSessions[table]; ok {
		return ts, true
	}
	return nil, false
}

