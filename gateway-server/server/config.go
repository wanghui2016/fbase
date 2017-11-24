package server

import (
	"strings"
	"fmt"

	"util/config"
	"util/log"
)

var DefaultMaxRawCount uint64 = 10000
var DefaultMaxWorkNum  uint64 = 100
var DefaultMaxTaskQueueLen  uint64 = 10000
var DefaultHttpPort int = 8080
var DefaultInsertSlowLog int = 50
var DefaultSelectSlowlog int = 200

type Config struct {
	Addr               string

	LogDir             string
	LogModule          string
	LogLevel           string

	MasterServerAddrs  []string
	MasterManageAddrs  []string
	SignKey            string

	MaxClients         int

	User               string
	Password           string

	Charset            string

	HttpPort           int

	MaxLimit	uint64
	MaxWorkNum  uint64
	MaxTaskQueueLen uint64
	InsertSlowLog int
	SelectSlowLog int
	OpenMetric bool

	GrpcPoolSize     int
	GrpcInitWinSize  int
	SlowlogSlowerThanUsec int
	SlowlogMaxLen int
	HeartbeatIntervalSec int
}

func (c *Config)LoadConfig() {
	var found bool
	config.InitConfig()
	if config.Config == nil {
		log.Fatal("No Config.")
		return
	}
	var port int
	if port, found = config.Config.Int("mysql.port"); !found {
		log.Panic("mysql.port not specified")
	}
	c.Addr = fmt.Sprintf(":%d", port)

	if c.MaxClients, found = config.Config.Int("mysql.maxclients"); !found {
		log.Panic("mysql.maxclients not specified")
	}

	if c.User, found = config.Config.String("mysql.user"); !found {
		log.Panic("mysql.user not specified")
	}
	if c.Password, found = config.Config.String("mysql.password"); !found {
		log.Panic("mysql.password not specified")
	}
	c.Charset, _ = config.Config.String("mysql.charset")


	if c.LogDir,found = config.Config.String("log.dir");!found {
		log.Panic("log.dir not specified")
	}

	if c.LogModule,found = config.Config.String("log.module");!found {
		log.Panic("log.module not specified")
	}

	if c.LogLevel,found = config.Config.String("log.level");!found {
		log.Panic("log.level not specified")
	}

	if c.SignKey, found = config.Config.String("master.sign"); !found {
		log.Panic("master.sign not specified")
	}
	var addrs string
	if addrs, found = config.Config.String("master.addrs"); !found {
		log.Panic("master.addrs not specified")
	}
	list := strings.Split(addrs, ";")
	if len(list) == 0 {
		log.Panic("master.addrs invalid")
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

	if c.HttpPort, found = config.Config.Int("http.port"); !found {
		log.Warn("http.port not specified")
		c.HttpPort = DefaultHttpPort
	}

	if maxLimit, found := config.Config.Int("max.record.limit"); !found{
		c.MaxLimit = DefaultMaxRawCount
	}else{
		c.MaxLimit = uint64(maxLimit)
	}
	if maxNum, found := config.Config.Int("max.work.num"); !found{
		c.MaxWorkNum = DefaultMaxWorkNum
	}else{
		c.MaxWorkNum = uint64(maxNum)
	}
	if maxLen, found := config.Config.Int("max.taskqueue.len"); !found{
		c.MaxTaskQueueLen = DefaultMaxTaskQueueLen
	}else{
		c.MaxTaskQueueLen = uint64(maxLen)
	}


	if c.InsertSlowLog, found = config.Config.Int("insert.slowlog"); !found {
		log.Warn("http.port not specified")
		c.InsertSlowLog = DefaultInsertSlowLog
	}
	if c.SelectSlowLog, found = config.Config.Int("select.slowlog"); !found {
		log.Warn("http.port not specified")
		c.SelectSlowLog = DefaultSelectSlowlog
	}

	c.OpenMetric = config.Config.BoolDefault("metrics.flag", false)
	c.GrpcPoolSize = config.Config.IntDefault("grpc.pool.size", 3)
	c.GrpcInitWinSize = config.Config.IntDefault("grpc.win.size", 64 * 1024)

	if c.SlowlogSlowerThanUsec, found = config.Config.Int("slowlog.slowerthanusec"); !found {
		log.Warn("slowlog.SlowlogSlowerThanUsec not specified, default 10000")
		c.SlowlogSlowerThanUsec = 10000
	}
	if c.SlowlogMaxLen, found = config.Config.Int("slowlog.maxlen"); !found {
		log.Warn("slowlog.maxlen not specified, default 10")
		c.SlowlogMaxLen = 10
	}
	if c.HeartbeatIntervalSec, found = config.Config.Int("heartbeat.intervalsec"); !found {
		log.Warn("heartbeat.intervalsec not specified, default 10")
		c.HeartbeatIntervalSec = 10
	}
}
