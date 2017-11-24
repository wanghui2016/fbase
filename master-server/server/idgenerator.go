package server

import (
	"sync"

	"util/log"
	sErr "engine/errors"
)

var genStep uint64 = 10

type IDGenerator interface {
	GenID() (uint64, error)
}

type TaskIDGenerator struct {
	*idGenerator
}

func NewTaskIDGenerator(store Store) IDGenerator {
	return &TaskIDGenerator{idGenerator: NewIDGenerator([]byte(TASK_ID), genStep, store)}
}

type RangeIDGenerator struct {
	*idGenerator
}

func NewRangeIDGenerator(store Store) IDGenerator {
	return &RangeIDGenerator{idGenerator: NewIDGenerator([]byte(RANGE_ID), genStep, store)}
}

type TableIDGenerator struct {
	*idGenerator
}

func NewTableIDGenerator(store Store) IDGenerator {
	return &TableIDGenerator{idGenerator: NewIDGenerator([]byte(TABLE_ID), genStep, store)}
}

type NodeIDGenerator struct {
	*idGenerator
}

func NewNodeIDGenerator(store Store) IDGenerator {
	return &NodeIDGenerator{idGenerator: NewIDGenerator([]byte(NODE_ID), genStep, store)}
}

type DatabaseIDGenerator struct {
	*idGenerator
}

func NewDatabaseIDGenerator(store Store) IDGenerator {
	return &DatabaseIDGenerator{idGenerator: NewIDGenerator([]byte(DATABASE_ID), genStep, store)}
}

type idGenerator struct {
	lock   sync.Mutex
	base uint64
	end  uint64

	key []byte
	step uint64

	store Store
}

func NewIDGenerator(key []byte, step uint64, store Store) *idGenerator {
	return &idGenerator{key: key, step: step, store: store}
}

func (id *idGenerator) GenID() (uint64, error) {
	id.lock.Lock()
	defer id.lock.Unlock()

	if id.base == id.end {
		log.Debug("[GENID] before generate!!!!!! (base %d, end %d)", id.base, id.end)
		end, err := id.generate()
		if err != nil {
			return 0, err
		}

		id.end = end
		id.base = id.end - id.step
		log.Debug("[GENID] after generate!!!!!! (base %d, end %d)", id.base, id.end)
	}

	id.base++

	return id.base, nil
}

func (id *idGenerator) get(key []byte) ([]byte, error) {
	value, err := id.store.Get(key)
	if err != nil {
		if err == sErr.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return value, nil
}

func (id *idGenerator) put(key, value []byte) error {
	return id.store.Put(key, value)
}

func (id *idGenerator) generate() (uint64, error) {
	value, err := id.get(id.key)
	if err != nil {
		return 0, err
	}

	var end uint64

	if value != nil {
		end, err = bytesToUint64(value)
		if err != nil {
			return 0, err
		}
	}
	end += id.step
	value = uint64ToBytes(end)
	err = id.put(id.key, value)
	if err != nil {
		return 0, err
	}

	return end, nil
}
