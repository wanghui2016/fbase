package ttlcache

import (
	"sync"
	"time"
)

type lruNode struct {
	obj     interface{}
	createtime time.Time
	dietime time.Time
}

type TTLCache struct {
	timeout time.Duration
	cache   map[string]*lruNode
	lock    sync.RWMutex
}

func NewTTLCache(timeout time.Duration) *TTLCache {
	return &TTLCache{
		timeout: timeout,
		cache:   make(map[string]*lruNode),
	}
}

func (self *TTLCache) Put(key string, obj interface{}) {
	self.lock.Lock()
	defer self.lock.Unlock()
	var node *lruNode
	var find bool
	now := time.Now()
	if node, find = self.cache[key]; find {
		//未超时
		if node.dietime.After(time.Now()) {
			node.dietime = now.Add(self.timeout)
			node.obj = obj
		}
	}
	self.cache[key] = &lruNode{
		obj:     obj,
		dietime: now.Add(self.timeout),
		createtime: now,
	}
}

func (self *TTLCache) get(key string) (*lruNode, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	node, ok := self.cache[key]
	if ok {
		return node, true
	}
	return nil, false
}

func (self *TTLCache) Get(key string) (interface{}, bool) {
	node, ok := self.get(key)
	if ok {
		if node.dietime.After(time.Now()) {
			return node.obj, true
		}
		return func () (interface{}, bool){
			self.lock.Lock()
			defer self.lock.Unlock()
			node, ok := self.cache[key]
			if ok {
				if node.dietime.After(time.Now()) {
					return node.obj, true
				}
			}
			delete(self.cache, key)
			return nil, false
		}()
	}
	return nil, false
}

func (self *TTLCache) Delete(key string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	var find bool
	if _, find = self.cache[key]; find {
		delete(self.cache, key)
	}
}
