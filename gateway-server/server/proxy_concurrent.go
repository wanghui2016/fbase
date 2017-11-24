package server

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/log"
)

type Task interface {
	Do()
	Wait() error
	Reset()
}

func (p *Proxy) Submit(t Task) error {
	index := time.Now().UnixNano() % int64(len(p.taskQueues))
	select {
	case <-p.ctx.Done():
		return errors.New("server closed")
	case p.taskQueues[index] <- t:
		return nil
	}
}

func (p *Proxy) workMonitor() {
	p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case index := <-p.workRecover:
			// chan closed
			if index <= 0 {
				return
			}
			p.wg.Add(1)
			go p.work(index, p.taskQueues[index])
		}
	}
}

func (p *Proxy) work(index int, queue chan Task) {
	defer func() {
		p.wg.Done()
		if r := recover(); r != nil {
			fn := func() string {
				n := 10000
				var trace []byte
				for i := 0; i < 5; i++ {
					trace = make([]byte, n)
					nbytes := runtime.Stack(trace, false)
					if nbytes < len(trace) {
						return string(trace[:nbytes])
					}
					n *= 2
				}
				return string(trace)
			}
			select {
			case <-p.ctx.Done():
				return
			case p.workRecover <- index:
			}

			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
	for {
		select {
		case <-p.ctx.Done():
			return
		case t := <-queue:
			// chan closed
			if t == nil {
				return
			}
			t.Do()
		}
	}
}

type InsertResult struct {
	affected     uint64
	duplicateKey []byte
}

func (r *InsertResult) GetAffected() uint64 {
	if r == nil {
		return 0
	}
	return r.affected
}

func (r *InsertResult) GetDuplicateKey() []byte {
	if r == nil {
		return nil
	}
	return r.duplicateKey
}

type InsertTask struct {
	p     *Proxy
	table *Table
	rows  []*kvrpcpb.KeyValue
	done  chan error
	rest  *InsertResult
}

func (it *InsertTask) init(proxy *Proxy, table *Table, rows []*kvrpcpb.KeyValue) *InsertTask {
	if it == nil {
		return it
	}
	it.p = proxy
	it.table = table
	it.rows = rows
	return it
}

func (it *InsertTask) Do() {
	affected, duplicateKey, err := it.p.insert(it.table, it.rows)
	if err != nil {
		it.done <- err
		return
	}

	it.rest = &InsertResult{affected, duplicateKey}
	it.done <- nil
	return
}

func (it *InsertTask) Wait() error {
	select {
	case <-it.p.ctx.Done():
		return errors.New("proxy already closed")
	case err := <-it.done:
		return err
	}
	return nil
}

func (it *InsertTask) Reset() {
	if it == nil {
		return
	}
	*it = InsertTask{done: make(chan error, 1)}
}

var insertTaskPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &InsertTask{done: make(chan error, 1)}
	},
}

func GetInsertTask() *InsertTask {
	return insertTaskPool.Get().(*InsertTask)
}

func PutInsertTask(task *InsertTask) {
	if task == nil {
		return
	}
	task.Reset()
	insertTaskPool.Put(task)
}

type SelectResult struct {
	rows [][]*Row
}

type SelectTask struct {
	p         *Proxy
	table     *Table
	fieldList []*kvrpcpb.SelectField
	matches   []Match
	done      chan error
	rest      *SelectResult
}

func (it *SelectTask) init(proxy *Proxy, table *Table, fieldList []*kvrpcpb.SelectField, matches []Match) *SelectTask {
	if it == nil {
		return it
	}
	it.p = proxy
	it.table = table
	it.fieldList = fieldList
	it.matches = matches
	return it
}

func (it *SelectTask) Do() {
	rows, err := it.p.doSelect(it.table, it.fieldList, it.matches, nil, nil)
	if err != nil {
		log.Error("getcommand doselect error: %v", err)
		it.done <- err
		return
	}
	it.rest = &SelectResult{rows}
	it.done <- nil
	return
}

func (it *SelectTask) Wait() error {
	select {
	case <-it.p.ctx.Done():
		return errors.New("proxy already closed")
	case err := <-it.done:
		return err
	}
	return nil
}

func (it *SelectTask) Reset() {
	if it == nil {
		return
	}
	*it = SelectTask{done: make(chan error, 1)}
}

var selectTaskPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &SelectTask{done: make(chan error, 1)}
	},
}

func GetSelectTask() *SelectTask {
	return selectTaskPool.Get().(*SelectTask)
}

func PutSelectTask(task *SelectTask) {
	if task == nil {
		return
	}
	task.Reset()
	selectTaskPool.Put(task)
}

func newAggreTask(p *Proxy, kvproxy *KvProxy, key *metapb.Key, req *kvrpcpb.KvSelectRequest) *aggreTask {
	return &aggreTask{
		p:       p,
		kvproxy: kvproxy,
		key:     key,
		req:     req,
		done:    make(chan error, 1),
	}
}

type aggreTask struct {
	p       *Proxy
	kvproxy *KvProxy
	key     *metapb.Key
	req     *kvrpcpb.KvSelectRequest
	done    chan error
	result  []*kvrpcpb.Row
}

func (t *aggreTask) Do() {
	resp, _, err := t.kvproxy.kvQuery(t.req, t.key)
	if err == nil && resp.GetCode() != 0 {
		log.Error("select aggre: remote server return code: %v", resp.GetCode())
		err = fmt.Errorf("remote server return code: %v", resp.GetCode())
	}
	if err != nil {
		t.done <- err
		return
	}
	t.result = resp.GetRows()
	t.done <- nil
}

func (t *aggreTask) Wait() error {
	select {
	case <-t.p.ctx.Done():
		return errors.New("proxy already closed")
	case err := <-t.done:
		return err
	}
}

func (t *aggreTask) Reset() {
}
