package server

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"util/config"
	"util/log"
)

type Config struct {
	AppName       string
	AppVersion    string
	AppManagePort int
	AppRpcPort    int
	DataPath      string
	NodeID        uint64

	LogDir    string
	LogModule string
	LogLevel  string

	MasterServerAddrs       []string
	MasterManageAddrs       []string
	HeartbeatInterval       int
	RaftHeartbeatAddr       string
	RaftHeartbeatInterval   int
	RaftRetainLogs          int
	RaftReplicaAddr         string
	RaftReplicaConcurrency  int
	RaftSnapshotConcurrency int
	// 副本的日志落后超过这个值就不允许分裂
	RaftMaxAllocLogBackward int

	// 存储
	BlockCacheSize           int64
	CompactionConcurrency    int   // compaction并发（协程）
	CompactionWriteRateLimit int64 // compaction写入限速（单位：每协程每秒多少字节）

	OpenMetric bool

	InsertBatchSwitch bool
	GrpcInitWinSize int

	diskQuota uint64
}

func (c *Config) LoadConfig() {
	var found bool
	config.InitConfig()
	if config.Config == nil {
		log.Fatal("No Config.")
		return
	}
	if c.AppManagePort, found = config.Config.Int("app.manage.port"); !found {
		log.Panic("app.manage.port not specified")
	}
	if c.AppRpcPort, found = config.Config.Int("app.rpc.port"); !found {
		log.Panic("app.rpc.port not specified")
	}
	if c.AppVersion, found = config.Config.String("app.version"); !found {
		log.Panic("app.version not specified")
	}
	if c.AppName, found = config.Config.String("app.name"); !found {
		log.Panic("app.name not specified")
	}

	if c.LogDir, found = config.Config.String("log.dir"); !found {
		log.Panic("log.dir not specified")
	}

	if c.LogModule, found = config.Config.String("log.module"); !found {
		log.Panic("log.module not specified")
	}

	if c.LogLevel, found = config.Config.String("log.level"); !found {
		log.Panic("log.level not specified")
	}

	if c.DataPath, found = config.Config.String("db.path"); !found {
		log.Panic("Db url not specified")
	}

	c.OpenMetric = config.Config.BoolDefault("metrics.flag", false)
	c.InsertBatchSwitch = config.Config.BoolDefault("range.insert.batch", true)

	var addrs string
	if addrs, found = config.Config.String("master.addrs"); !found {
		log.Panic("master.addrs not specified")
	}
	list := strings.Split(addrs, ";")
	if len(list) == 0 {
		log.Panic("master.addrs is empty")
	}
	var msServerAddrs, msManageAddrs []string
	for _, item := range list {
		ipports := strings.Split(item, ":")
		if len(ipports) != 2 {
			log.Panic("master addrs is invalid")
		}
		ip := ipports[0]
		ports := strings.Split(ipports[1], "-")
		if len(ipports) != 2 {
			log.Panic("master addrs is invalid")
		}
		sPort := ports[0]
		mPort := ports[1]
		sAddrs := fmt.Sprintf("%s:%s", ip, sPort)
		mAddrs := fmt.Sprintf("%s:%s", ip, mPort)
		msServerAddrs = append(msServerAddrs, sAddrs)
		msManageAddrs = append(msManageAddrs, mAddrs)
	}
	c.MasterServerAddrs = msServerAddrs
	c.MasterManageAddrs = msManageAddrs

	var raftHeartbeatPort int
	if raftHeartbeatPort, found = config.Config.Int("raft.heartbeatPort"); !found {
		log.Panic("raft.heartbeatPort not specified")
	}
	c.RaftHeartbeatAddr = fmt.Sprintf(":%d", raftHeartbeatPort)
	var raftReplicaPort int
	if raftReplicaPort, found = config.Config.Int("raft.replicaPort"); !found {
		log.Panic("raft.replicaPort not specified")
	}
	c.RaftReplicaAddr = fmt.Sprintf(":%d", raftReplicaPort)
	c.RaftHeartbeatInterval = config.Config.IntDefault("raft.heartbeatInterval", 500)
	c.RaftRetainLogs = config.Config.IntDefault("raft.retainLogs", 100000)
	c.RaftMaxAllocLogBackward = config.Config.IntDefault("raft.maxAllocLogBackward", 2000)
	c.RaftReplicaConcurrency = config.Config.IntDefault("raft.replConcurrency", 20)
	c.RaftSnapshotConcurrency = config.Config.IntDefault("raft.snapConcurrency", 15)
	log.Info("[config] raft replica concurrency: %v", c.RaftReplicaConcurrency)
	log.Info("[config] raft snapshot concurrency: %v", c.RaftSnapshotConcurrency)

	c.HeartbeatInterval = config.Config.IntDefault("ping.frequency", 10)

	// egnine
	c.BlockCacheSize = int64(config.Config.IntDefault("block.size", 1073741824))
	c.CompactionConcurrency = config.Config.IntDefault("compaction.concurrency", 15)
	c.CompactionWriteRateLimit = int64(config.Config.IntDefault("compaction.writeRatelimit", 30*1024*1024))
	log.Info("[config] engine block cache: %v", c.BlockCacheSize)
	log.Info("[config] compaction concurrency: %v", c.CompactionConcurrency)
	log.Info("[config] compaction write ratelimit: %v", c.CompactionWriteRateLimit)
	c.GrpcInitWinSize = config.Config.IntDefault("grpc.win.size", 64 * 1024)
	// active
	for _, master := range c.MasterManageAddrs {
		log.Debug("node try to actice to %s", master)
		nodeId, err := active(master, c.AppRpcPort, raftHeartbeatPort, raftReplicaPort)
		if err != nil {
			log.Error("node active: %v", err)
			continue
		}
		c.NodeID = nodeId
		break
	}
	if c.NodeID == 0 {
		log.Panic("node id is empty")
	}

	if diskQuota, found := config.Config.String("disk.quota"); !found {
		log.Panic("disk.quota is not found")
	} else {
		var err error
		c.diskQuota, err = strconv.ParseUint(diskQuota, 10, 64)
		if err != nil {
			log.Panic("disk.quota is not uint64")
		}
	}
}

func active(host string, serverPort, raftHeartbeatPort, raftReplicaPort int) (uint64, error) {
	var (
		err  error
		resp *http.Response
		data []byte
	)
	type httpReply struct {
		Code    int         `json:"code"`
		Message string      `json:"message`
		Data    interface{} `json:"data"`
	}
	reply := new(httpReply)

	d := fmt.Sprint(time.Now().Unix())
	h := md5.New()
	h.Write([]byte("sharkstore-masterserver"))
	h.Write([]byte(d))
	s := fmt.Sprintf("%x", h.Sum(nil))

	var i int
	for i = 0; i < 3; i++ {
		if resp, err = http.Get(fmt.Sprintf("http://%s/node/activate?serverPort=%v&raftHeartbeatPort=%v&raftReplicaPort=%v&d=%s&s=%s",
			host, serverPort, raftHeartbeatPort, raftReplicaPort, d, s)); err != nil {
			fmt.Errorf("node activate: %s cannot be visited: %v\n", host, err)
			continue
		}
		break
	}
	if err != nil {
		return 0, err
	}
	if resp.Body == nil {
		return 0, errors.New("bad response")
	}
	defer resp.Body.Close()
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return 0, fmt.Errorf("node activate: http read master %s response body error: %v\n", host, err)
	}
	if err = json.Unmarshal(data, reply); err != nil {
		return 0, fmt.Errorf("node activate: decode master %s http reply error: %v\n", host, err)
	}
	if reply.Code != 0 {
		return 0, fmt.Errorf("node activate: master %s reply code %d message %s\n", host, reply.Code, reply.Message)
	}
	nodeIdStr, ok := reply.Data.(string)
	if !ok {
		log.Panic("json node id is not string")
	}
	return strconv.ParseUint(nodeIdStr, 10, 64)
}
