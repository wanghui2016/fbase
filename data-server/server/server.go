package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	utilnet "github.com/shirou/gopsutil/net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	dsClient "data-server/client"
	"engine/rowstore/cache"
	engineSch "engine/scheduler"
	msClient "master-server/client"
	"model/pkg/eventpb"
	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/schpb"
	"raft"
	"util/hlc"
	"util/log"
	"util/metrics"
	netSvr "util/server"
	"github.com/shirou/gopsutil/process"
	"os"
)

const (
	RANGE_CMD_NUMBER = iota
	RANGE_NET_INPUT_BYTES
	RANGE_NET_OUTPUT_BYTES
)

const (
	MaxHeartbeatFailTimes = 10
	MaxHeartbeatFailTime  = time.Duration(2) * time.Minute
)

type Server struct {
	conf *Config

	server *netSvr.Server

	raftConfig *raft.Config
	raftServer *raft.RaftServer

	node   *metapb.Node
	ioStat *utilnet.IOCountersStat

	lock   sync.RWMutex
	ranges map[uint64]*Range

	mscli msClient.Client
	dsCli dsClient.SchClient

	cluster *Cluster

	clock *hlc.Clock

	eventChan chan *eventpb.Event
	quit      chan struct{}

	metricMeter  *metrics.MetricMeter
	machineStats *MachineStats
	procStats *process.Process

	heartbeatFailCount   int
	lastHeartbeat        time.Time
	lastHeartbeatSuccess time.Time
	blockCache           *cache.Cache
}

func (s *Server) Heartbeat() {
	s.lastHeartbeat = time.Now()
	s.lastHeartbeatSuccess = time.Now()
	stats, err := utilnet.IOCounters(false)
	if err != nil {
		log.Fatal("get net io count failed, err[%v]", err)
		return
	}
	if len(stats) == 0 {
		log.Fatal("invalid io stats")
		return
	}
	s.ioStat = &stats[0]
	ticker := time.NewTimer(time.Duration(0))
	//check heartbeat
	//go s.HeartbeatMonitor()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			ticker.Reset(time.Second * time.Duration(s.conf.HeartbeatInterval))
			s.heartbeat()
		}
	}
}

func (s *Server) EventCollect(event *eventpb.Event) {
	select {
	case s.eventChan <- event:
	default:
		log.Warn("event chan is full!!!!")
	}
}

func (s *Server) EventReport() {
	req := new(mspb.ReportEventRequest)
	for event := range s.eventChan {
		var err error
		for i := 0; i < 3; i++ {
			req.Event = event
			_, err = s.mscli.ReportEvent(req)
			if err != nil {
				log.Error("report event failed, err[%v]", err)
				time.Sleep(time.Millisecond * time.Duration(10*(i+1)))
				continue
			}
			break
		}
	}
}

func (s *Server) Start() {
	// rpc
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.conf.AppRpcPort))
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	svr := grpc.NewServer(grpc.InitialWindowSize(int32(s.conf.GrpcInitWinSize)))
	kvrpcpb.RegisterKvServerServer(svr, s)
	schpb.RegisterSchServerServer(svr, s)
	// Register reflection service on gRPC server.
	reflection.Register(svr)
	go func() {
		if err = svr.Serve(lis); err != nil {
			log.Fatal("failed to serve: %v", err)
		}
	}()
	go s.Heartbeat()
	go s.EventReport()
	s.server.Run()
}

func (s *Server) Stop() {
	defer s.blockCache.Close()
	close(s.quit)
	if s.server != nil {
		s.server.Close()
	}
	if s.metricMeter != nil {
		s.metricMeter.Stop()
	}
	s.closeAllRange()
}

func (s *Server) closeAllRange() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, rng := range s.ranges {
		rng.Close()
	}
}

func InitServer(conf *Config) *Server {
	// engine compact scheduler
	esc := &engineSch.Config{
		Concurrency:    conf.CompactionConcurrency,
		WriteRateLimit: conf.CompactionWriteRateLimit,
	}
	engineSch.StartScheduler(esc)

	// cli
	msCli, err := msClient.NewClient(conf.MasterServerAddrs)
	if err != nil {
		log.Fatal("create client for master server failed, err[%v]", err)
		return nil
	}
	dsCli := dsClient.NewSchClient()

	// cluster
	cluster := NewCluster(msCli)

	// conf raft
	rc := raft.DefaultConfig()
	// TODO 200ms??
	rc.RetainLogs = uint64(conf.RaftRetainLogs)
	rc.TickInterval = time.Millisecond * time.Duration(conf.RaftHeartbeatInterval)
	rc.HeartbeatAddr = conf.RaftHeartbeatAddr
	rc.ReplicateAddr = conf.RaftReplicaAddr
	rc.Resolver = NewResolver(cluster)
	rc.MaxReplConcurrency = conf.RaftReplicaConcurrency
	rc.MaxSnapConcurrency = conf.RaftSnapshotConcurrency
	rc.NodeID = uint64(conf.NodeID)
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		log.Fatal("boot raft server failed, err[%v]", err)
		return nil
	}
	service := new(Server)
	service.blockCache = cache.NewCache(cache.NewLRU(conf.BlockCacheSize))
	// http server
	s := netSvr.NewServer()
	config := &netSvr.ServerConfig{
		Port:    fmt.Sprintf("%d", conf.AppManagePort),
		Version: conf.AppVersion,
	}
	s.Init(conf.AppName, config, nil)
	s.Handle("/debug/range/state", netSvr.ServiceHttpHandler{Handler: service.handleDebugRangeState})
	s.Handle("/debug/range/info", netSvr.ServiceHttpHandler{Handler: service.handleDebugRangeInfo})
	s.Handle("/debug/server/info", netSvr.ServiceHttpHandler{Handler: service.handleDebugServerInfo})
	s.Handle("/debug/log/setlevel", netSvr.ServiceHttpHandler{Handler: service.handleDebugLogSetLevel})
	s.Handle("/debug/range/trace", netSvr.ServiceHttpHandler{Handler: service.handleTraceRange})
	s.Handle("/manage/stop/range", netSvr.ServiceHttpHandler{Handler: service.handleStopRange})
	s.Handle("/manage/close/range", netSvr.ServiceHttpHandler{Handler: service.handleCloseRange})
	s.Handle("/config/set", netSvr.ServiceHttpHandler{Handler: service.handleConfigSet})
	s.Handle("/config/get", netSvr.ServiceHttpHandler{Handler: service.handleConfigGet})

	//s.RpcHandle(service.rpcWork)

	// self info
	node := &metapb.Node{
		Id:      uint64(conf.NodeID),
		Address: fmt.Sprintf(":%d", conf.AppRpcPort),
		State:   metapb.NodeState_N_Initial,
		RaftAddrs: &metapb.RaftAddrs{
			HeartbeatAddr: conf.RaftHeartbeatAddr,
			ReplicateAddr: conf.RaftReplicaAddr,
		},
		Version: "",
	}

	service.conf = conf
	service.server = s
	service.mscli = msCli
	service.dsCli = dsCli
	service.cluster = cluster
	service.clock = hlc.NewClock(hlc.UnixNano, 0)
	service.raftConfig = rc
	service.raftServer = rs
	service.node = node
	service.eventChan = make(chan *eventpb.Event, 10000)
	service.quit = make(chan struct{})
	service.ranges = make(map[uint64]*Range, 1000)
	if conf.OpenMetric {
		service.metricMeter = metrics.NewMetricMeter("data-server", &Report{})
	}
	service.machineStats = &MachineStats{
		diskPath: conf.DataPath,
	}
	service.procStats = func() *process.Process {
		p, err := process.NewProcess(int32(os.Getpid()))
		if err != nil {
			log.Fatal("new process error: %v", err)
		}
		return p
	}()
	return service
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
