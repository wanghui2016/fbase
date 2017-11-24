package server

import (
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"engine"
	"engine/rowstore/cache"
	"engine/rowstore/opt"
	esch "engine/scheduler"
	"model/pkg/mspb"
	"raft"
	raftproto "raft/proto"
	"raft/storage/wal"
	"raftgroup"
	"util"
	"util/log"
	"util/server"
	"model/pkg/metapb"
)

var DefaultRaftLogCount uint64 = 10000

type Point struct {
	Id            uint64
	ManageAddress string
	RpcAddress    string
	HeartbeatAddr string
	ReplicateAddr string
}

type Server struct {
	NodeId       uint64
	ClusterNodes map[uint64]*Point

	conf *Config
	// raft
	raftConfig *raft.RaftConfig
	raftServer *raft.RaftServer
	raftGroup  *raftgroup.RaftGroup

	cluster   *Cluster
	pusher    Pusher
	FileStore *FileStore
	alarm     Alarm
	store     Store

	router Router

	server      *server.Server
	rpcServer   *grpc.Server
	coordinator *Coordinator

	leader chan uint64
	quit   chan struct{}
	wg     sync.WaitGroup
}

func (service *Server) ParseClusterInfo() {
	service.ClusterNodes = make(map[uint64]*Point, 3)
	nodeInfos := strings.Split(service.conf.ClusterInfos, ";")
	var match bool = false
	for _, nodeInfo := range nodeInfos {
		elems := strings.Split(nodeInfo, "-")
		if len(elems) != 6 {
			log.Panic("clusterInfos format{nodeId-ip-port-raftHeartbeatPort-raft.replicaPort[;nodeId-ip-port-raftHeartbeatPort-raft.replicaPort] }is error %s", nodeInfo)
		}
		node := new(Point)
		node.Id, _ = strconv.ParseUint(elems[0], 10, 64)
		ip := elems[1]
		node.ManageAddress = fmt.Sprintf("%s:%s", ip, elems[2])
		node.RpcAddress = fmt.Sprintf("%s:%s", ip, elems[3])
		if !match {
			if service.conf.NodeID != 0 {
				if service.conf.NodeID == node.Id {
					match = true
					service.conf.AppServerIp = ip
					service.conf.AppManagePort = elems[2]
					service.conf.AppServerPort = elems[3]
				}
			} else {
				for _, ip_ := range util.GetLocalIps() {
					if ip_ == ip {
						match = true
						service.conf.NodeID = node.Id
						service.conf.AppServerIp = ip_
						service.conf.AppManagePort = elems[2]
						service.conf.AppServerPort = elems[3]
						log.Debug("match cluster node[%d], mode 2", node.Id)
						break
					}
				}
			}
		}
		node.HeartbeatAddr = fmt.Sprintf("%s:%s", ip, elems[4])
		node.ReplicateAddr = fmt.Sprintf("%s:%s", ip, elems[5])
		service.ClusterNodes[node.Id] = node
	}

	if !match {
		log.Fatal("no ip matches in clusterinfo")
	}
}

func (service *Server) NewStore() Store {
	esc := &esch.Config{
		Concurrency:    1,
		WriteRateLimit: 1024 * 1024 * 50,
	}
	esch.StartScheduler(esc)

	rc := raft.DefaultConfig()
	rc.RetainLogs = uint64(service.conf.RaftRetainLogs)
	rc.TickInterval = time.Millisecond * time.Duration(service.conf.RaftHeartbeatInterval)
	rc.HeartbeatAddr = service.ClusterNodes[service.NodeId].HeartbeatAddr
	rc.ReplicateAddr = service.ClusterNodes[service.NodeId].ReplicateAddr
	// master server cluster
	rc.Resolver = NewResolver(service.ClusterNodes)
	rc.NodeID = uint64(service.conf.NodeID)
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		log.Error("new raft server failed, err[%v]", err)
		return nil
	}

	raftGroup := raftgroup.NewRaftGroup(1, rs, nil, nil)
	path := filepath.Join(service.conf.DataPath, "data")
	store, applyId, err := engine.NewRowStore(1, path, nil, nil, &opt.Options{BlockCache: cache.NewCache(cache.NewLRU(512 * 1024 * 1024))})
	if err != nil {
		log.Error("open store failed, err[%v]", err)
		return nil
	}
	saveStore := NewSaveStore(raftGroup, store)
	// raft group create at end !!!!!!!
	path = filepath.Join(service.conf.DataPath, "raft")
	raftStorage, err := wal.NewStorage(1, path, nil)
	if err != nil {
		log.Error("service new raft store error: %s", err)
		return nil
	}
	var raftPeers []raftproto.Peer
	raftPeers = make([]raftproto.Peer, 0, len(service.ClusterNodes))
	for _, n := range service.ClusterNodes {
		peer := raftproto.Peer{Type: raftproto.PeerNormal, ID: n.Id}
		raftPeers = append(raftPeers, peer)
	}
	raftGroup.RegisterApplyHandle(saveStore.HandleCmd)
	raftGroup.RegisterPeerChangeHandle(saveStore.HandlePeerChange)
	raftGroup.RegisterGetSnapshotHandle(saveStore.HandleGetSnapshot)
	raftGroup.RegisterApplySnapshotHandle(saveStore.HandleApplySnapshot)
	raftGroup.RegisterLeaderChangeHandle(service.HandleLeaderChange)
	raftGroup.RegisterFatalEventHandle(service.HandleFatalEvent)

	var raftConfig *raft.RaftConfig
	raftConfig = &raft.RaftConfig{
		ID:           1,
		Applied:      applyId,
		Peers:        raftPeers,
		Storage:      raftStorage,
		StateMachine: raftGroup,
	}
	//err = raftGroup.Create(raftConfig)
	//if err != nil {
	//	log.Error("create raft group failed， err[%v]", err)
	//	return nil
	//}
	service.raftGroup = raftGroup
	service.raftServer = rs
	service.raftConfig = raftConfig
	return saveStore
}

func (service *Server) InitServer(conf *Config) {
	MasterConf = conf
	service.conf = conf
	service.leader = make(chan uint64, 5)
	service.alarm = NewAlarm(uint64(conf.ClusterID), conf.AlarmUrl, conf.AlarmMail, conf.AlarmSms)
	service.quit = make(chan struct{})
	service.ParseClusterInfo()
	service.NodeId = conf.NodeID

	saveStore := service.NewStore()
	if saveStore == nil {
		log.Fatal("create save store failed")
		return
	}
	service.store = saveStore
	service.cluster = NewCluster(uint64(conf.ClusterID), uint64(conf.NodeID), saveStore, service.alarm, service.conf)
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s?readTimeout=5s&writeTimeout=5s&timeout=10s", conf.MonitorDbUser, conf.MonitorDbPasswd, conf.MonitorDbAddr, DATABASE_FBASE)
	service.pusher = NewLocalPusher(dns, service.cluster)
	service.coordinator = NewCoordinator(service.cluster, service.pusher, service.alarm)

	service.FileStore = NewFileStore(service.conf.ServAddrFtp)
	// 最后加载raft
	err := service.raftGroup.Create(service.raftConfig)
	if err != nil {
		log.Fatal("raft load failed, err[%v]", err)
	}
	s := server.NewServer()
	s.Init("masterserver", &server.ServerConfig{
		Ip:      service.conf.AppServerIp,
		Port:    service.conf.AppManagePort,
		Version: "v1",
	}, &Report{})
	//s.RpcHandle(service.rpcWork)
	s.Handle("/database/create", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler:service.handleDatabaseCreate})
	s.Handle("/table/create", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleTableCreate})
	s.Handle("/sql/table/create", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleSqlTableCreate})
	s.Handle("/range/delete", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRangeDelete})
	//s.Handle("/database/delete", service.handleDatabaseDelete)
	s.Handle("/table/edit", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleTableEdit})
	s.Handle("/table/delete", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleTableDelete})
	s.Handle("/table/get/rwPolicy", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleTableGetRwPolicy})
	s.Handle("/table/set/rwPolicy", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleTableSetRwPolicy})
	//s.Handle("/node/delete", service.handleNodeDelete)
	//s.Handle("/range/delete", service.handleRangeDelete)
	s.Handle("/node/activate", server.ServiceHttpHandler{Verifier: nil, Proxy: service.donotProxy, Handler: service.handleNodeActivate})
	//s.Handle("/node/login", service.handleNodeLogin)
	s.Handle("/node/logout", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleNodeLogout})
	//s.Handle("/gateway/activate", service.handleGatewayActivate) // TODO

	s.Handle("/database/getall", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleDBGetAll})
	s.Handle("/table/getall", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleTableGetAll})
	s.Handle("/node/getall", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleNodeGetAll})
	s.Handle("/range/getall", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRangeGetAll})
	s.Handle("/node/stats", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleNodeStats})
	s.Handle("/range/stats", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRangeStats})
	s.Handle("/master/getleader", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: nil, Handler: service.handleMasterGetLeader})
	s.Handle("/range/getleader", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRangeGetLeader})
	s.Handle("/range/getpeerinfo", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRangeGetPeerInfo})

	s.Handle("/task/getAllTask", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleGetAllTask})
	s.Handle("/task/getAllEmptyTask", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleGetAllEmptyTask})
	s.Handle("/task/getAllFailTask", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleGetAllFailTask})
	s.Handle("/task/getAllTimeoutTask", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleGetTimeoutTask})
	s.Handle("/task/retry", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRetryTask})
	s.Handle("/task/delete", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleDeleteTask})
	s.Handle("/task/pause", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handlePauseTask})
	s.Handle("/task/modifyCfg", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: nil, Handler: service.handleModifyCfg})

	s.Handle("/manage/range/failover", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeFailOver})
	s.Handle("/manage/range/split", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeSplit})
	s.Handle("/manage/range/transfer", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeTransfer})
	s.Handle("/manage/range/add/peer", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeAddPeer})
	s.Handle("/manage/range/del/peer", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeDelPeer})
	s.Handle("/manage/machine/add", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageAddMachine})
	s.Handle("/manage/machine/list", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageListMachine})
	s.Handle("/manage/machine/get", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageGetMachine})
	s.Handle("/manage/range/leader/transfer", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleRangeLeaderTransfer})
	s.Handle("/manage/getAutoScheduleInfo", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageGetAutoScheduleInfo})
	s.Handle("/manage/setAutoScheduleInfo", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageSetAutoScheduleInfo})
	s.Handle("/manage/getTableAutoScheduleInfo", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageGetTableAutoScheduleInfo})
	s.Handle("/manage/setTableAutoScheduleInfo", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageSetTableAutoScheduleInfo})
	s.Handle("/manage/range/stop", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeStop})
	s.Handle("/manage/range/close", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeClose})
	s.Handle("/manage/range/genId", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeGenId})
	s.Handle("/manage/range/replace", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageRangeReplace})

	s.Handle("/manage/getClusterDeployPolicy", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageGetClusterDeployPolicy})
	s.Handle("/manage/setClusterDeployPolicy", server.ServiceHttpHandler{Verifier: service.verifier, Proxy: service.proxy, Handler: service.handleManageSetClusterDeployPolicy})

	s.Handle("/debug/node/info", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleDebugNodeInfo})
	s.Handle("/debug/range/info", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleDebugRangeInfo})
	s.Handle("/debug/range/home", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleDebugRangeHome})
	s.Handle("/debug/range/trace", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleDebugRangeTrace})
	s.Handle("/debug/node/trace", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleDebugNodeTrace})
	s.Handle("/debug/log/setlevel", server.ServiceHttpHandler{Verifier: nil, Proxy: nil, Handler: service.handleDebugLogSetLevel})
	s.Handle("/debug/range/raftstatus", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleDebugRangeRaftStatus})

	s.Handle("/route/get", server.ServiceHttpHandler{Handler: service.handleRouteGet})
	s.Handle("/route/check", server.ServiceHttpHandler{Verifier: nil, Proxy: service.proxy, Handler: service.handleRouteCheck})
	service.server = s
	service.router = NewRemoteRouter(s, service.conf.AppServerIp, service.ClusterNodes)
}

func (service *Server) Start() error {
	defer func() {
		if x := recover(); x != nil {
			buf := make([]byte, 1<<20)
			runtime.Stack(buf, true)
			log.Error("\n--------------------\n%s", buf)
			panic(nil)
		}
	}()

	// rpc
	lis, err := net.Listen("tcp", ":"+service.conf.AppServerPort)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	mspb.RegisterMsServerServer(s, service)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	go func() {
		if err = s.Serve(lis); err != nil {
			log.Fatal("failed to serve: %v", err)
		}
	}()
	service.rpcServer = s
	service.background()
	service.server.Run()
	return nil
}

func (service *Server) GetLeader() *Point {
	leader := service.cluster.GetLeader()
	if leader == raft.NoLeader {
		return nil
	}
	return service.ClusterNodes[leader]
}

func (service *Server) IsLeader() bool {
	return service.raftGroup.IsLeader()
}

func (service *Server) watchLeader() {
	defer service.wg.Done()
	for {
		select {
		case <-service.quit:
			return
		case leader := <-service.leader:
			log.Info("leader change[%d]", leader)
			// leader 没有变更
			if service.cluster.GetLeader() == leader {
				log.Info("leader no change")
				return
			}
			service.coordinator.Stop()
			service.cluster.Close()
			service.pusher.Close()
			// 本节点当选为leader
			if service.NodeId == leader {
				log.Info("be elected leader")
				cluster := NewCluster(uint64(service.conf.ClusterID), uint64(service.NodeId), service.store, service.alarm, service.conf)
				err := cluster.LoadCache()
				if err != nil {
					message := fmt.Sprintf("节点[%s] 异常信息[%v]", service.ClusterNodes[service.NodeId].RpcAddress, err)
					service.alarm.Alarm("系统异常", message)
					log.Fatal("master server load disk to mem failed, err[%v]", err)
					return
				}
				dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", service.conf.MonitorDbUser, service.conf.MonitorDbPasswd, service.conf.MonitorDbAddr, DATABASE_FBASE)
				pusher := NewLocalPusher(dns, cluster)
				pusher.Run()
				coordinator := NewCoordinator(cluster, pusher, service.alarm)
				coordinator.Run()
				service.coordinator = coordinator
				service.cluster = cluster
				service.pusher = pusher
			}
			service.cluster.UpdateLeader(leader)
		}
	}
}

func (service *Server) ReceiveRoutesLoop() {
	defer service.wg.Done()
	err := service.router.Subscribe(ROUTE)
	if err != nil {
		log.Fatal("subscribe failed, err[%v]", err)
		return
	}
	for {
		select {
		case <-service.quit:
			return
		default:
		}
		data, err := service.router.Receive()
		if err != nil {
			// 进程退出
			log.Error("router recv failed, err[%v]", err)
			continue
		}
		log.Debug("recv routes[%v] update!!!!!!", data.(*metapb.TopologyEpoch))
		service.router.Notify(data)
	}

}

func (service *Server) getClusterRpcAddress() []string {
	addrs := make([]string, 0)
	for _, node := range service.ClusterNodes {
		addrs = append(addrs, node.RpcAddress)
	}
	return addrs
}

func (service *Server) getClusterManageAddress() []string {
	addrs := make([]string, 0)
	for _, node := range service.ClusterNodes {
		addrs = append(addrs, node.ManageAddress)
	}
	return addrs
}

func (service *Server) background() {
	service.wg.Add(1)
	go service.watchLeader()
	service.wg.Add(1)
	go service.ReceiveRoutesLoop()
}

// Quit 保存退出
func (service *Server) Quit() {
	// TODO: safe quit
}

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
