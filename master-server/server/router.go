package server

import (
	"time"
	"sync"
	"net/http"
	"encoding/json"
	"bytes"
	"io/ioutil"
	"errors"
	"runtime"

	"model/pkg/metapb"
	"util/log"
	"master-server/client"
	"util/server"
	"golang.org/x/net/context"
	"github.com/golang/protobuf/proto"
)

const (
	ROUTE = "/route/update"
)

type Router interface {
	GetTopologyEpoch(dbId, tableId uint64, epoch *metapb.TableEpoch)  (*metapb.TopologyEpoch, error)
	Publish(channel string, message interface{}) error
	Subscribe(channel string) (error)
	UnSubscribe(channel string) (error)
	Receive() (interface{}, error)
	AddSession(table string) *Session
	DeleteSession(table string, sessionId uint64)
	Notify(data interface{})
	Close()
}

type RemoteRouter struct {
	svr     *server.Server
	cli     client.Client
	self    string
	cluster map[uint64]*Point

	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	lock       sync.RWMutex
	subscribe  map[string]server.ServiceHttpHandler
	pubChannel chan *metapb.TopologyEpoch
	subChannel chan *metapb.TopologyEpoch
	sessionManager *SessionManager
}

func NewRemoteRouter(svr *server.Server, self string, cluster map[uint64]*Point) *RemoteRouter {
	ctx, cancel := context.WithCancel(context.Background())
	msAddrs := make([]string, 0)
	for _, node := range cluster {
		msAddrs = append(msAddrs, node.RpcAddress)
	}
	cli, _ := client.NewClient(msAddrs)
	r := &RemoteRouter{
		svr: svr,
		self: self,
		ctx: ctx,
		cancel: cancel,
		cluster: cluster,
		cli: cli,
		subscribe: make(map[string]server.ServiceHttpHandler),
		pubChannel: make(chan *metapb.TopologyEpoch, 1000),
		subChannel: make(chan *metapb.TopologyEpoch, 1000),
		sessionManager: NewSessionManager(),
	}
	r.wg.Add(1)
	go r.Broadcast()
	return r
}

func (r *RemoteRouter) GetTopologyEpoch(dbId, tableId uint64, _epoch *metapb.TableEpoch)  (topo *metapb.TopologyEpoch, err error) {
	// TODO if leader get from local
	retryCount := 3
    for i := 0; i < retryCount; i++ {
	    topo, err = r.cli.GetTopologyEpoch(dbId, tableId, _epoch)
	    if err != nil {
		    if i < retryCount  - 1 {
			    time.Sleep(time.Millisecond * time.Duration((i + 1) * 10))
		    }
		    continue
	    }
	    break
    }
	return
}

// 发布路由变更消息
func (r *RemoteRouter) Publish(channel string, message interface{}) error {
	switch channel {
	case ROUTE:
		r.pubChannel <- message.(*metapb.TopologyEpoch)
	default:
		return ErrBrokenChannel
	}
	return nil
}

//　订阅路由变更消息
func (r *RemoteRouter) Subscribe(channel string) (error) {
	switch channel {
	case ROUTE:
		// TODO http handle
		r.lock.Lock()
		defer r.lock.Unlock()
		if _, find := r.subscribe[channel]; find {
			return nil
		}
		r.subscribe[channel] = server.ServiceHttpHandler{Handler: r.handleRouteSub}
		r.svr.Handle(channel, server.ServiceHttpHandler{Handler: r.handleRouteSub})
	default:
		return ErrInvalidChannel
	}
	return nil
}

// 取消订阅路由变更消息
func (r *RemoteRouter) UnSubscribe(channel string) (error) {
    return nil
}

// 接收订阅的消息
func (r *RemoteRouter) Receive() (interface{}, error) {
	defer func() {
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
			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
    select {
	case <-r.ctx.Done():
		return nil, ErrClosedChannel
	case routes := <-r.subChannel:
	    if routes == nil {
		    return nil, ErrBrokenChannel
	    }
		return routes, nil
	}
}

func (r *RemoteRouter) Close() {
	r.cancel()
	r.wg.Wait()
    return
}

func (r *RemoteRouter) Notify(data interface{}) {
	r.sessionManager.Notify(data)
}

func (r *RemoteRouter) AddSession(table string) *Session {
	return r.sessionManager.AddSession(table)
}

func (r *RemoteRouter) DeleteSession(table string, sessionId uint64) {
	r.sessionManager.DelSession(table, sessionId)
}

func (r *RemoteRouter) Broadcast() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case topo := <-r.pubChannel:
			log.Debug("route broadcast: %v", topo)
		    // 重新获取一次路由信息
		    //_topo, err := r.GetTopologyEpoch(topo.DbId, topo.TableId, &metapb.TableEpoch{})
		    //if err != nil {
			 //   log.Warn("get routes failed, err[%v]", err)
			 //   continue
		    //}
			for _, node := range r.cluster {
				err := r.broadcast(node.ManageAddress, topo)
				if err != nil {
					log.Warn("broadcast route info to [%s] failed, err[%v]", node.ManageAddress, err)
				}
			}
		}
	}

}

func (r *RemoteRouter) broadcast(addr string, message *metapb.TopologyEpoch) error {
	retry := 3
	// 本地更新，直接处理
	if r.self == addr {
		log.Debug("local notify!!!!!!")
		r.sessionManager.Notify(message)
	} else {
		url := "http://" + addr + ROUTE
		data, err := proto.Marshal(message)
		if err != nil {
			return err
		}
		var resp *http.Response
		for i := 0; i < retry; i++ {
			req, err := http.NewRequest("POST", url, bytes.NewReader(data))
			if err != nil {
				return err
			}
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				if i < retry - 1 {
					time.Sleep(time.Millisecond * time.Duration((i + 1) * 10))
				}
				continue
			}
			break
		}
		if err != nil {
			log.Warn("route update failed, err[%v]", err)
			return err
		}
		// maybe nil for response
		if resp == nil {
			log.Error("invalid response!!!")
			return errors.New("invalid response")
		}
		if resp.Body != nil {
			defer resp.Body.Close()
		}
		if resp.StatusCode != http.StatusOK {
			errBody, err := ioutil.ReadAll(resp.Body)
			if err == nil {
				log.Warn("http errbody: [%s]", string(errBody))
			}

			return ErrBadResponse
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Error("read http response body failed, err[%v]", err)
			return err
		}
		reply := new(httpReply)
		err = json.Unmarshal(body, reply)
		if err != nil {
			log.Error("json unmarshal failed, data[%s], err[%v]", string(body), err)
			return err
		}
		if reply.Code != 0 {
			return errors.New(reply.Message)
		}
	}
	return nil
}

func (r *RemoteRouter) handleRouteSub(w http.ResponseWriter, req *http.Request) {
	reply := &httpReply{}
	defer sendReply(w, reply)
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		return
	}
	message := new(metapb.TopologyEpoch)
	err = proto.Unmarshal(body, message)
	if err != nil {
		reply.Code = HTTP_ERROR
		reply.Message = err.Error()
		log.Error("proto unmarshal failed, err[%v]", err)
		return
	}
	select {
	case <-r.ctx.Done():
		reply.Code = HTTP_ERROR
		reply.Message = ErrInvalidChannel.Error()
		return
	case r.subChannel <- message:
		// TODO timeout ???
	default:
		log.Warn("sub channel is full!!!!!!")
	}

	log.Debug("subscribe success!!!!")
	return
}
