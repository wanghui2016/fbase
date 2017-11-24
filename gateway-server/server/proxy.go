package server

import (
	"fmt"
	"time"
	"sync"

	"github.com/juju/errors"
	msClient"master-server/client"
	dsClient "data-server/client"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util/hlc"
	"util/log"
	"util/metrics"
	"golang.org/x/net/context"
)

type Report struct {
}

func (r *Report) ReportInterval() time.Duration {
	return time.Minute
}

func (r *Report) Report(data []byte) error {
	fmt.Printf("%s\n\n", string(data))
	log.Info("metrics: %s", string(data))
	return nil
}

type Proxy struct {
	msCli   msClient.Client
	nodeCli dsClient.KvClient
	config  *Config
	router  *Router

	metric *metrics.MetricMeter

	clock *hlc.Clock

	taskQueues []chan Task
	workRecover chan int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	stats *Stats
}

func NewProxy(msAddrs, msMangeAddrs []string, config *Config) *Proxy {
	msCli, err := msClient.NewClient(msAddrs)
	if err != nil {
		return nil
	}
	router := NewRouter(msCli, msMangeAddrs)
	if router == nil {
		return nil
	}
	var taskQueues []chan Task
	for i := 0; i < int(config.MaxWorkNum); i++ {
		queue := make(chan Task, config.MaxTaskQueueLen)
		taskQueues = append(taskQueues, queue)
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &Proxy{
		router:  router,
		msCli:   msCli,
		nodeCli: dsClient.NewRPCClientWithOpts(config.GrpcPoolSize, int32(config.GrpcInitWinSize)),
		//metric:  metrics.NewMetricMeter("gateway", new(Report)),
		clock:   hlc.NewClock(hlc.UnixNano, 0),
		config:  config,
		ctx:     ctx,
		cancel:  cancel,
		taskQueues: taskQueues,
		workRecover: make(chan int, config.MaxWorkNum),
		stats: NewStats(config),
	}
	if config.OpenMetric {
		proxy.metric = metrics.NewMetricMeter("gateway", new(Report))
	}
	for i, queue := range taskQueues {
		proxy.wg.Add(1)
		go proxy.work(i, queue)
	}
	proxy.wg.Add(1)
	go proxy.workMonitor()
	return proxy
}

func (p *Proxy) Close() {
	p.cancel()
	p.wg.Wait()
}

type KvProxy struct {
	cli          dsClient.KvClient
	msCli        msClient.Client
	clock        *hlc.Clock
	table        *Table
	findRoute    func(key *metapb.Key) *metapb.Route
	writeTimeout time.Duration
	readTimeout  time.Duration
}

func (p *KvProxy) rawPut(req *kvrpcpb.KvRawPutRequest) (*kvrpcpb.KvRawPutResponse, error) {
	in := &kvrpcpb.DsKvRawPutRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req: req,
	}

	var route *metapb.Route
	retryCount := 5
	key := &metapb.Key{Key: req.GetKey(), Type: metapb.KeyType_KT_Ordinary}
	for i := 0; i < retryCount; i++ {
		route = p.findRoute(key)
		if route == nil {
			return nil, ErrNoRoute
		}
		in.Header.RangeId = route.GetRangeId()
		in.Header.Timestamp = p.clock.Now()
		addr := GetRouteAddr(route, nil, WRITE)
		out, err := p.cli.RawPut(addr, in, p.writeTimeout, p.readTimeout)
		if err != nil {
			if err == dsClient.ErrConnUnavailable {
			//	for _, peer := range route.GetRange().GetPeers() {
			//		if peer.GetNodeId() == leader.GetNodeId() {
			//			continue
			//		}
			//		// TODO nodes cache in gateway
			//		node, _err := p.msCli.GetNode(peer.GetNodeId())
			//		if _err != nil {
			//			log.Warn("get node[%d] failed, err[%v]", peer.GetNodeId(), _err)
			//			return nil, ErrInternalError
			//		}
			//		out, err = p.cli.RawPut(node.GetAddress(), in, p.writeTimeout, p.readTimeout)
			//		if err != nil {
			//			log.Warn("raw get failed, err[%v]", err)
			//			continue
			//		}
			//		leader = &metapb.Leader{
			//			RangeId: route.GetRangeId(),
			//			NodeId: node.GetId(),
			//			NodeAddr: node.GetAddress(),
			//		}
			//		route.UpdateLeader(leader)
			//		break
			//	}
			//	if err != nil {
			//		return nil, err
			//	}
			//} else {
				return nil, err
			}
		}
		retry, err := p.dealResponseHeader(out.GetHeader(), route)
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		// 请求成功
		return out.GetResp(), nil
	}
	return nil, errors.New("range busy")
}

func (p *KvProxy) rawGet(req *kvrpcpb.KvRawGetRequest) (*kvrpcpb.KvRawGetResponse, error) {
	in := &kvrpcpb.DsKvRawGetRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req: req,
	}
	var route *metapb.Route
	retryCount := 5
	key := &metapb.Key{Key: req.GetKey(), Type: metapb.KeyType_KT_Ordinary}
	for i := 0; i < retryCount; i++ {
		route = p.findRoute(key)
		if route == nil {
			return nil, ErrNoRoute
		}
		in.Header.RangeId = route.GetRangeId()
		in.Header.Timestamp = p.clock.Now()
		addr := GetRouteAddr(route, nil, READ)
		out, err := p.cli.RawGet(addr, in, p.writeTimeout, p.readTimeout)
		if err != nil {
			// DS宕机，需要重试其他的节点获取leader
			//if err == dsClient.ErrConnUnavailable {
			//	for _, peer := range route.GetRange().GetPeers() {
			//		if peer.GetNodeId() == leader.GetNodeId() {
			//			continue
			//		}
			//		// TODO nodes cache in gateway
			//		node, _err := p.msCli.GetNode(peer.GetNodeId())
			//		if _err != nil {
			//			log.Warn("get node[%d] failed, err[%v]", peer.GetNodeId(), _err)
			//			return nil, ErrInternalError
			//		}
			//		out, err = p.cli.RawGet(node.GetAddress(), in, p.writeTimeout, p.readTimeout)
			//		if err != nil {
			//			log.Warn("raw get failed, err[%v]", err)
			//			continue
			//		}
			//		leader = &metapb.Leader{
			//			RangeId: route.GetRangeId(),
			//			NodeId: node.GetId(),
			//			NodeAddr: node.GetAddress(),
			//		}
			//		route.UpdateLeader(leader)
			//		break
			//	}
			//	if err != nil {
			//		return nil, err
			//	}
			//} else {
				return nil, err
			//}
		}
		retry, err := p.dealResponseHeader(out.GetHeader(), route)
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		// 请求成功
		return out.GetResp(), nil
	}
	return nil, errors.New("range busy")
}

func (p *KvProxy) rawDelete(req *kvrpcpb.KvRawDeleteRequest) (*kvrpcpb.KvRawDeleteResponse, error) {
	in := &kvrpcpb.DsKvRawDeleteRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req: req,
	}

	var route *metapb.Route
	retryCount := 5
	key := &metapb.Key{Key: req.GetKey(), Type: metapb.KeyType_KT_Ordinary}
	for i := 0; i < retryCount; i++ {
		route = p.findRoute(key)
		if route == nil {
			return nil, ErrNoRoute
		}
		in.Header.RangeId = route.GetRangeId()
		in.Header.Timestamp = p.clock.Now()
		addr := GetRouteAddr(route, nil, WRITE)
		out, err := p.cli.RawDelete(addr, in, p.writeTimeout, p.readTimeout)
		if err != nil {
			//if err == dsClient.ErrConnUnavailable {
			//	for _, peer := range route.GetRange().GetPeers() {
			//		if peer.GetNodeId() == leader.GetNodeId() {
			//			continue
			//		}
			//		// TODO nodes cache in gateway
			//		node, _err := p.msCli.GetNode(peer.GetNodeId())
			//		if _err != nil {
			//			log.Warn("get node[%d] failed, err[%v]", peer.GetNodeId(), _err)
			//			return nil, ErrInternalError
			//		}
			//		out, err = p.cli.RawDelete(node.GetAddress(), in, p.writeTimeout, p.readTimeout)
			//		if err != nil {
			//			log.Warn("raw get failed, err[%v]", err)
			//			continue
			//		}
			//		leader = &metapb.Leader{
			//			RangeId: route.GetRangeId(),
			//			NodeId: node.GetId(),
			//			NodeAddr: node.GetAddress(),
			//		}
			//		route.UpdateLeader(leader)
			//		break
			//	}
			//	if err != nil {
			//		return nil, err
			//	}
			//} else {
				return nil, err
			//}
		}
		retry, err := p.dealResponseHeader(out.GetHeader(), route)
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		// 请求成功
		return out.GetResp(), nil
	}
	return nil, errors.New("range busy")
}

func (p *KvProxy) kvsInsert(req *kvrpcpb.KvInsertRequest, scope *kvrpcpb.Scope) ([]*kvrpcpb.KvInsertResponse, error) {
	var key, start, limit *metapb.Key
	var resp *kvrpcpb.KvInsertResponse
	var resps []*kvrpcpb.KvInsertResponse
	var route *metapb.Route
	var err error
	if scope.Start == nil {
		start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
	} else {
		start = &metapb.Key{Key: scope.Start, Type: metapb.KeyType_KT_Ordinary}
	}
	if scope.Limit == nil {
		limit = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
	} else {
		limit = &metapb.Key{Key: scope.Limit, Type: metapb.KeyType_KT_Ordinary}
	}
	for {
		if key == nil {
			key = start
		} else if route != nil {
			key = route.GetEndKey()
			// check key in range
			if metapb.Compare(key, start) < 0 || metapb.Compare(key, limit) >= 0 {
				// 遍历完成，直接退出循环
				break
			}
		} else {
			return nil, ErrInternalError
		}
		resp, route, err = p.kvInsert(req, key)
		if err != nil {
			return nil, err
		}
		resps = append(resps, resp)
	}
	if len(resps) == 0 {
		resp = &kvrpcpb.KvInsertResponse{Code: 0, AffectedKeys: 0}
		resps = append(resps, resp)
	}
	return resps, nil
}

func (p *KvProxy) kvInsert(req *kvrpcpb.KvInsertRequest, key *metapb.Key) (*kvrpcpb.KvInsertResponse, *metapb.Route, error) {
	in := &kvrpcpb.DsKvInsertRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req: req,
	}

	var route *metapb.Route
	retryCount := 5
	if len(req.GetRows()) == 0 {
		return nil, nil, errors.New("zero rows to insert")
	}
	for i := 0; i < retryCount; i++ {
		route = p.findRoute(key)
		if route == nil {
			return nil, nil, ErrNoRoute
		}
		in.Header.RangeId = route.GetRangeId()
		in.Header.Timestamp = p.clock.Now()
		addr := GetRouteAddr(route, p.table.RwPolicy, WRITE)
		if log.IsEnableDebug() {
			log.Debug("insert %s in range %d", addr, route.GetRangeId())
		}
		out, err := p.cli.Insert(addr, in, dsClient.WriteTimeout, dsClient.ReadTimeoutShort)
		if err != nil {
			if err == dsClient.ErrConnUnavailable {
			//	for _, peer := range route.GetRange().GetPeers() {
			//		if peer.GetNodeId() == leader.GetNodeId() {
			//			continue
			//		}
			//		// TODO nodes cache in gateway
			//		node, _err := p.msCli.GetNode(peer.GetNodeId())
			//		if _err != nil {
			//			log.Warn("get node[%d] failed, err[%v]", peer.GetNodeId(), _err)
			//			return nil, nil, ErrInternalError
			//		}
			//		out, err =  p.cli.Insert(node.GetAddress(), in, dsClient.WriteTimeout, dsClient.ReadTimeoutShort)
			//		if err != nil {
			//			log.Warn("raw get failed, err[%v]", err)
			//			continue
			//		}
			//		leader = &metapb.Leader{
			//			RangeId: route.GetRangeId(),
			//			NodeId: node.GetId(),
			//			NodeAddr: node.GetAddress(),
			//		}
			//		route.UpdateLeader(leader)
			//		break
			//	}
			//	if err != nil {
			//		return nil, nil, err
			//	}
			//} else {
				return nil, nil, err
			}
		}
		retry, err := p.dealResponseHeader(out.GetHeader(), route)
		if err != nil {
			return nil, route, err
		}
		if retry {
			continue
		}
		// 请求成功
		return out.GetResp(), route, nil
	}
	return nil, route, errors.New("range busy")
}

func (p *KvProxy) kvQuery(req *kvrpcpb.KvSelectRequest, key *metapb.Key) (*kvrpcpb.KvSelectResponse, *metapb.Route, error) {
	var in *kvrpcpb.DsKvSelectRequest
	in = &kvrpcpb.DsKvSelectRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req: req,
	}
	// 副本可读
	if p.table.RwPolicy.Policy != metapb.RwPolicy_RW_OnlyMaster {
		in.Header.ReplicaReadable = true
	}

	var route *metapb.Route
	retryCount := 5
	for i := 0; i < retryCount; i++ {
		route = p.findRoute(key)
		if route == nil {
			return nil, nil, ErrNoRoute
		}
		in.Header.RangeId = route.GetRangeId()
		in.Header.Timestamp = p.clock.Now()
		addr := GetRouteAddr(route, p.table.RwPolicy, READ)
		if log.IsEnableDebug() {
			log.Debug("query %s in range %d", addr, route.GetRangeId())
		}
		out, err := p.cli.Select(addr, in, p.writeTimeout, p.readTimeout)
		if err != nil {
			if err == dsClient.ErrConnUnavailable {
			//	for _, peer := range route.GetRange().GetPeers() {
			//		if peer.GetNodeId() == leader.GetNodeId() {
			//			continue
			//		}
			//		// TODO nodes cache in gateway
			//		node, _err := p.msCli.GetNode(peer.GetNodeId())
			//		if _err != nil {
			//			log.Warn("get node[%d] failed, err[%v]", peer.GetNodeId(), _err)
			//			return nil, nil, ErrInternalError
			//		}
			//		out, err = p.cli.Select(node.GetAddress(), in, p.writeTimeout, p.readTimeout)
			//		if err != nil {
			//			log.Warn("raw get failed, err[%v]", err)
			//			continue
			//		}
			//		leader = &metapb.Leader{
			//			RangeId: route.GetRangeId(),
			//			NodeId: node.GetId(),
			//			NodeAddr: node.GetAddress(),
			//		}
			//		route.UpdateLeader(leader)
			//		break
			//	}
			//	if err != nil {
			//		return nil, nil, err
			//	}
			//} else {
				return nil, nil, err
			}
		}
		retry, err := p.dealResponseHeader(out.GetHeader(), route)
		if err != nil {
			return nil, route, err
		}
		if retry {
			continue
		}
		// 请求成功
		return out.GetResp(), route, nil
	}
	return nil, route, errors.New("range busy")
}

func (p *KvProxy) kvsDelete(req *kvrpcpb.KvDeleteRequest, scope *kvrpcpb.Scope) ([]*kvrpcpb.KvDeleteResponse, error) {
	var key, start, limit *metapb.Key
	var resp *kvrpcpb.KvDeleteResponse
	var resps []*kvrpcpb.KvDeleteResponse
	var route *metapb.Route
	var err error
	if scope.Start == nil {
		start = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_NegativeInfinity}
	} else {
		start = &metapb.Key{Key: scope.Start, Type: metapb.KeyType_KT_Ordinary}
	}
	if scope.Limit == nil {
		limit = &metapb.Key{Key: nil, Type: metapb.KeyType_KT_PositiveInfinity}
	} else {
		limit = &metapb.Key{Key: scope.Limit, Type: metapb.KeyType_KT_Ordinary}
	}
	for {
		if key == nil {
			key = start
		} else if route != nil {
			key = route.GetEndKey()
			// check key in range
			if metapb.Compare(key, start) < 0 || metapb.Compare(key, limit) >= 0 {
				// 遍历完成，直接退出循环
				break
			}
		} else {
			return nil, ErrInternalError
		}
		resp, route, err = p.kvDelete(req, key)
		if err != nil {
			return nil, err
		}
		resps = append(resps, resp)
	}
	if len(resps) == 0 {
		resp = &kvrpcpb.KvDeleteResponse{Code: 0, AffectedKeys: 0}
		resps = append(resps, resp)
	}
	return resps, nil
}

func (p *KvProxy) kvDelete(req *kvrpcpb.KvDeleteRequest, key *metapb.Key) (*kvrpcpb.KvDeleteResponse, *metapb.Route, error) {
	in := &kvrpcpb.DsKvDeleteRequest{
		Header: &kvrpcpb.RequestHeader{},
		Req: req,
	}

	var route *metapb.Route
	retryCount := 5
	for i := 0; i < retryCount; i++ {
		route = p.findRoute(key)
		if route == nil {
			return nil, nil, ErrNoRoute
		}
		in.Header.RangeId = route.GetRangeId()
		in.Header.Timestamp = p.clock.Now()
		addr := GetRouteAddr(route, p.table.RwPolicy, WRITE)
		out, err := p.cli.Delete(addr, in, p.writeTimeout, p.readTimeout)
		if err != nil {
			if err == dsClient.ErrConnUnavailable {
			//	for _, peer := range route.GetRange().GetPeers() {
			//		if peer.GetNodeId() == leader.GetNodeId() {
			//			continue
			//		}
			//		// TODO nodes cache in gateway
			//		node, _err := p.msCli.GetNode(peer.GetNodeId())
			//		if _err != nil {
			//			log.Warn("get node[%d] failed, err[%v]", peer.GetNodeId(), _err)
			//			return nil, nil, ErrInternalError
			//		}
			//		out, err = p.cli.Delete(node.GetAddress(), in, p.writeTimeout, p.readTimeout)
			//		if err != nil {
			//			log.Warn("raw get failed, err[%v]", err)
			//			continue
			//		}
			//		leader = &metapb.Leader{
			//			RangeId: route.GetRangeId(),
			//			NodeId: node.GetId(),
			//			NodeAddr: node.GetAddress(),
			//		}
			//		route.UpdateLeader(leader)
			//		break
			//	}
			//	if err != nil {
			//		return nil, nil, err
			//	}
			//} else {
				return nil, nil, err
			}
		}
		retry, err := p.dealResponseHeader(out.GetHeader(), route)
		if err != nil {
			return nil, route, err
		}
		if retry {
			continue
		}
		// 请求成功
		return out.GetResp(), route, nil
	}
	return nil, route, errors.New("range busy")
}

func (p *KvProxy) dealResponseHeader(header *kvrpcpb.ResponseHeader, route *metapb.Route) (retry bool, err error) {
	p.clock.Update(header.GetNow())
	pbErr := header.GetError()
	if pbErr == nil {
		return false, nil
	}
	if pbErr.GetNotLeader() != nil {
		log.Warn("range[%s] leader changed, new leader[%v]",
			route.SString(), pbErr.GetNotLeader().GetLeader().GetNodeAddr())
		route.UpdateLeader(pbErr.NotLeader.Leader)
		retry = true
		return
	}
	if pbErr.GetNoLeader() != nil {
		log.Warn("range[%s] no leader, we need retry", route.SString())
		// TODO sleep ????
		time.Sleep(time.Millisecond * time.Duration(10))
		retry = true
		return
	}
	// This is not the case, there must be some is wrong!!!,just return error
	if pbErr.GetKeyNotInRange() != nil {
		log.Warn("key[%v] not in range(%s)[%v, %v]",
			pbErr.KeyNotInRange.Key, route.SString(), pbErr.KeyNotInRange.StartKey, pbErr.KeyNotInRange.EndKey)
		err = ErrNoRoute
		return
	}
	// range must is delete, we need update route
	if pbErr.GetRangeNotFound() != nil {
		log.Warn("range[%s] not found", route.SString())
		retry = true
		return
	}
	// range split finish, data server proxy for this request, we need update route
	if pbErr.GetRangeOffline() != nil {
		log.Warn("range[%s] offline", route.SString())
		return
	}
	// stale epoch
	if pbErr.GetRangeSplit() != nil {
		log.Warn("range[%s] splitting!!!!!!", route.SString())
		time.Sleep(time.Millisecond * time.Duration(100))
		retry = true
		return
	}
	if pbErr.GetStaleCommand() != nil {
		log.Warn("range[%s] stale command", route.SString())
		time.Sleep(time.Millisecond * time.Duration(100))
		retry = true
		return
	}
	return false, errors.New("server error")
}
