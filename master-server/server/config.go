package server

import (
	"strconv"

	"util/config"
	"util/log"
	"time"
)

type Config struct {
	AppName       string
	AppVersion    string
	AppServerIp   string
	AppServerPort string
	AppManagePort string
	DataPath      string
	NodeID        uint64
	Token string
	ClusterID     int
	ClusterInfos  string
	Mode          string
	NeedNodeIpDiff bool

	LogDir string
	LogModule string
	LogLevel string

	ThresholdRangeSplitSize uint64
	MemdbSize uint64

	//TsoSaveInterval time.Duration

	RaftHeartbeatInterval  int
	RaftRetainLogs     int

	MonitorDbUser string
	MonitorDbPasswd string
	MonitorDbAddr string
	MonitorTTL int64

	ServAddrFtp string

	AlarmUrl        string
	AlarmMail       string
	AlarmSms        string

	SchFailoverInterval int
	SchDiskBalanceInterval int
	SchRangeGCInterval int
	SchSplitInterval int
	SchLeaderBalanceInterval int
	SchLeaderScoreBalanceInterval int
	SchRangeBalanceInterval int
	SchRangeBalanceByNodeScoreInterval int
	SchLeaderBalancePercent float64
	SchRangesBalancePercent float64

	//node score config
	NodeTotalDiskRead       float64
	NodeTotalDiskWrite      float64
	NodeTotalNetIn          float64
	NodeTotalNetOut         float64

	NodeCpuProcRateThreshold        float64
	NodeMemoryUsedPercentThreshold  float64
	NodeDiskProcRateThreshold       float64
	NodeSwapUsedPercentThreshold    float64
	NodeDiskReadPercThreshold       float64
	NodeDiskWritePercThreshold      float64
	NodeNetInPercThreshold          float64
	NodeNetOutPercThreshold         float64
	NodeRangeCountPercThreshold     float64
	NodeLeaderCountPercThreshold    float64

	NodeCpuWeight           float64
	NodeMemoryWeight        float64
	NodeDiskWeight          float64
	NodeSwapWeight          float64
	NodeDiskReadWeight      float64
	NodeDiskWriteWeight     float64
	NodeNetInWeight         float64
	NodeNetOutWeight        float64
	NodeRangeCountWeight    float64
	NodeLeaderCountWeight   float64

	//range score config
	RangeTotalSize        float64
	RangeTotalOps         float64
	RangeTotalNetInBytes  float64
	RangeTotalNetOutBytes float64

	RangeSizePercThreshold      float64
	RangeOpsPercThreshold       float64
	RangeNetInPercThreshold     float64
	RangeNetOutPercThreshold    float64

	RangeSizeWeight      float64
	RangeOpsWeight       float64
	RangeNetInWeight     float64
	RangeNetOutWeight    float64

	RangeScoreMulti      float64
	NodeScoreMulti       float64
	NodeDiskMustFreeSize uint64
}
var MasterConf *Config
func (c *Config) LoadConfig() {
	var found bool
	config.InitConfig()
	if config.Config == nil {
		log.Fatal("No Config.")
		return
	}
	var nodeId int
	if nodeId, found = config.Config.Int("app.id"); !found {
		nodeId = 0
	}
	c.NodeID = uint64(nodeId)
	if c.AppVersion, found = config.Config.String("app.version"); !found {
		log.Panic("app.port not specified")
	}
	if c.AppName, found = config.Config.String("app.name"); !found {
		log.Panic("app.port not specified")
	}
	if c.Token, found = config.Config.String("app.token"); !found {
		log.Panic("app.token not specified")
	}
	if c.Mode, found = config.Config.String("app.mode"); !found {
		c.Mode = "release"
	}
	if c.Mode != "debug" && c.Mode != "release" {
		log.Panic("app.mode invalid")
	}
	if c.NeedNodeIpDiff, found = config.Config.Bool("app.NeedNodeIpDiff"); !found {
		c.NeedNodeIpDiff = false
	}
	if c.LogDir,found = config.Config.String("log.dir");!found {
		log.Panic("log.dir not specified")
	}
	if c.LogModule,found = config.Config.String("log.module");!found {
		log.Panic("log.module not specified")
	}
	if c.LogLevel,found = config.Config.String("log.level");!found {
		log.Panic("log.level not specified")
	}

	if c.DataPath, found = config.Config.String("db.path"); !found {
		log.Panic("Db url not specified")
	}
	//	var clusterInfos string
	if c.ClusterID, found = config.Config.Int("cluster.id"); !found {
		log.Panic("cluster.id not specified")
	}
	if c.ClusterInfos, found = config.Config.String("cluster.infos"); !found {
		log.Panic("cluster.infos not specified format:nodeId-ip-port-raftHeartbeatPort-raft.replicaPort[;nodeId-ip-port-raftHeartbeatPort-raft.replicaPort]")
	}

	var ThresholdRangeSplitSize, MemdbSize string
	var err error
	if ThresholdRangeSplitSize, found = config.Config.String("range.SplitThresholdSize"); found {
		c.ThresholdRangeSplitSize, err = strconv.ParseUint(ThresholdRangeSplitSize, 10, 64)
		if err != nil {
			log.Panic("range.ThresholdRangeSplitSize is not uint64")
		}
	} else {
		c.ThresholdRangeSplitSize = 64*1024*1024 *2
	}

	if MemdbSize, found = config.Config.String("option.MemdbSize"); found {
		c.MemdbSize, err = strconv.ParseUint(MemdbSize, 10, 64)
		if err != nil {
			log.Panic("range.MemdbSize is not uint64")
		}
	} else {
		c.MemdbSize = 64*1024*1024
	}

	//var intervalStr string
	//var saveInterval time.Duration
	//if intervalStr, found = config.Config.String("tso.SaveInterval"); !found {
	//	log.Panic("tso.SaveInterval not specified")
	//}
	//saveInterval, err = time.ParseDuration(intervalStr)
	//if err != nil {
	//	log.Panic("tso.SaveInterval invalid")
	//}
	//c.TsoSaveInterval = saveInterval

	c.RaftHeartbeatInterval =  config.Config.IntDefault("raft.heartbeatInterval", 500)
	c.RaftRetainLogs =  config.Config.IntDefault("raft.retainLogs", 100000)

	if c.MonitorDbUser, found = config.Config.String("monitor.dbuser"); !found {
		log.Panic("monitor user not specified")
	}
	if c.MonitorDbPasswd, found = config.Config.String("monitor.dbpasswd"); !found {
		log.Panic("monitor user not specified")
	}
	if c.MonitorDbAddr, found = config.Config.String("monitor.dbaddr"); !found {
		log.Panic("monitor user not specified")
	}
	if MonitorTTL, found := config.Config.Int("monitor.ttl"); !found {
		c.MonitorTTL = int64(10*24*time.Hour)
	} else {
		c.MonitorTTL = int64(MonitorTTL)
	}

	if c.ServAddrFtp,found = config.Config.String("servaddr.ftp");!found {
		log.Panic("servaddr.ftp not specified")
	}

	c.AlarmUrl = config.Config.StringDefault("alarm.url", "")
	c.AlarmMail = config.Config.StringDefault("alarm.mail", "")
	c.AlarmSms = config.Config.StringDefault("alarm.sms", "")

	var SchFailoverInterval string
	if SchFailoverInterval, found = config.Config.String("sch.SchFailoverInterval"); found {
		c.SchFailoverInterval, err = strconv.Atoi(SchFailoverInterval)
		if err != nil {
			log.Panic("sch.SchFailoverInterval is not int64")
		}
	} else {
		c.SchFailoverInterval = 10
	}

	var SchDiskBalanceInterval string
	if SchDiskBalanceInterval, found = config.Config.String("sch.SchDiskBalanceInterval"); found {
		c.SchDiskBalanceInterval, err = strconv.Atoi(SchDiskBalanceInterval)
		if err != nil {
			log.Panic("sch.SchDiskBalanceInterval is not int64")
		}
	} else {
		c.SchDiskBalanceInterval = 60
	}

	var SchRangeGCInterval string
	if SchRangeGCInterval, found = config.Config.String("sch.SchRangeGCInterval"); found {
		c.SchRangeGCInterval, err = strconv.Atoi(SchRangeGCInterval)
		if err != nil {
			log.Panic("sch.SchRangeGCInterval is not int64")
		}
	} else {
		c.SchRangeGCInterval = 120
	}

	var SchSplitInterval string
	if SchSplitInterval, found = config.Config.String("sch.SchSplitInterval"); found {
		c.SchSplitInterval, err = strconv.Atoi(SchSplitInterval)
		if err != nil {
			log.Panic("sch.SchSplitInterval is not int64")
		}
	} else {
		c.SchSplitInterval = 30
	}

	var SchLeaderBalanceInterval string
	if SchLeaderBalanceInterval, found = config.Config.String("sch.SchLeaderBalanceInterval"); found {
		c.SchLeaderBalanceInterval, err = strconv.Atoi(SchLeaderBalanceInterval)
		if err != nil {
			log.Panic("sch.SchLeaderBalanceInterval is not int64")
		}
	} else {
		c.SchLeaderBalanceInterval = 60*60
	}

	var SchLeaderScoreBalanceInterval string
	if SchLeaderScoreBalanceInterval, found = config.Config.String("sch.SchLeaderScoreBalanceInterval"); found {
		c.SchLeaderScoreBalanceInterval, err = strconv.Atoi(SchLeaderScoreBalanceInterval)
		if err != nil {
			log.Panic("sch.SchLeaderScoreBalanceInterval is not int64")
		}
	} else {
		c.SchLeaderScoreBalanceInterval = 60*60
	}

	var SchRangeBalanceInterval string
	if SchRangeBalanceInterval, found = config.Config.String("sch.SchRangeBalanceInterval"); found {
		c.SchRangeBalanceInterval, err = strconv.Atoi(SchRangeBalanceInterval)
		if err != nil {
			log.Panic("sch.SchRangeBalanceInterval is not int64")
		}
	} else {
		c.SchRangeBalanceInterval = 60*60
	}
	var SchRangeBalanceByNodeScoreInterval string
	if SchRangeBalanceByNodeScoreInterval, found = config.Config.String("sch.SchRangeBalanceByNodeScoreInterval"); found {
		c.SchRangeBalanceByNodeScoreInterval, err = strconv.Atoi(SchRangeBalanceByNodeScoreInterval)
		if err != nil {
			log.Panic("sch.SchRangeBalanceByNodeScoreInterval is not int64")
		}
	} else {
		c.SchRangeBalanceByNodeScoreInterval = 60*60
	}

	var SchLeaderBalancePercent string
	if SchLeaderBalancePercent, found = config.Config.String("sch.SchLeaderBalancePercent"); found {
		c.SchLeaderBalancePercent, err = strconv.ParseFloat(SchLeaderBalancePercent, 64)
		if err != nil {
			log.Panic("sch.SchLeaderBalancePercent is not float64")
		}
	} else {
		c.SchLeaderBalancePercent = 0.3
	}

	var SchRangesBalancePercent string
	if SchRangesBalancePercent, found = config.Config.String("sch.SchRangesBalancePercent"); found {
		c.SchRangesBalancePercent, err = strconv.ParseFloat(SchRangesBalancePercent, 64)
		if err != nil {
			log.Panic("sch.SchRangesBalancePercent is not float64")
		}
	} else {
		c.SchRangesBalancePercent = 0.3
	}

	//node score config
	if NodeTotalDiskRead, found := config.Config.String("score.NodeTotalDiskRead"); found {
		c.NodeTotalDiskRead, err = strconv.ParseFloat(NodeTotalDiskRead, 64)
		if err != nil {
			log.Panic("score.NodeTotalDiskRead is not float64")
		}
	} else {
		c.NodeTotalDiskRead = float64(GB)
	}

	if NodeTotalDiskWrite, found := config.Config.String("score.NodeTotalDiskWrite"); found {
		c.NodeTotalDiskWrite, err = strconv.ParseFloat(NodeTotalDiskWrite, 64)
		if err != nil {
			log.Panic("score.NodeTotalDiskWrite is not float64")
		}
	} else {
		c.NodeTotalDiskWrite = float64(GB)
	}

	if NodeTotalNetIn, found := config.Config.String("score.NodeTotalNetIn"); found {
		c.NodeTotalNetIn, err = strconv.ParseFloat(NodeTotalNetIn, 64)
		if err != nil {
			log.Panic("score.NodeTotalNetIn is not float64")
		}
	} else {
		c.NodeTotalNetIn = 1000*float64(GB)
	}

	if NodeTotalNetOut, found := config.Config.String("score.NodeTotalNetOut"); found {
		c.NodeTotalNetOut, err = strconv.ParseFloat(NodeTotalNetOut, 64)
		if err != nil {
			log.Panic("score.NodeTotalNetOut is not float64")
		}
	} else {
		c.NodeTotalNetOut = 1000*float64(GB)
	}

	if NodeCpuProcRateThreshold, found := config.Config.String("score.NodeCpuProcRateThreshold"); found {
		c.NodeCpuProcRateThreshold, err = strconv.ParseFloat(NodeCpuProcRateThreshold, 64)
		if err != nil {
			log.Panic("score.NodeCpuProcRateThreshold is not float64")
		}
	} else {
		c.NodeCpuProcRateThreshold = 80
	}

	if NodeMemoryUsedPercentThreshold, found := config.Config.String("score.NodeMemoryUsedPercentThreshold"); found {
		c.NodeMemoryUsedPercentThreshold, err = strconv.ParseFloat(NodeMemoryUsedPercentThreshold, 64)
		if err != nil {
			log.Panic("score.NodeMemoryUsedPercentThreshold is not float64")
		}
	} else {
		c.NodeMemoryUsedPercentThreshold = 70
	}

	if NodeDiskProcRateThreshold, found := config.Config.String("score.NodeDiskProcRateThreshold"); found {
		c.NodeDiskProcRateThreshold, err = strconv.ParseFloat(NodeDiskProcRateThreshold, 64)
		if err != nil {
			log.Panic("score.NodeDiskProcRateThreshold is not float64")
		}
	} else {
		c.NodeDiskProcRateThreshold = 60
	}

	if NodeSwapUsedPercentThreshold, found := config.Config.String("score.NodeSwapUsedPercentThreshold"); found {
		c.NodeSwapUsedPercentThreshold, err = strconv.ParseFloat(NodeSwapUsedPercentThreshold, 64)
		if err != nil {
			log.Panic("score.NodeSwapUsedPercentThreshold is not float64")
		}
	} else {
		c.NodeSwapUsedPercentThreshold = 10
	}

	if NodeDiskReadPercThreshold, found := config.Config.String("score.NodeDiskReadPercThreshold"); found {
		c.NodeDiskReadPercThreshold, err = strconv.ParseFloat(NodeDiskReadPercThreshold, 64)
		if err != nil {
			log.Panic("score.NodeDiskReadPercThreshold is not float64")
		}
	} else {
		c.NodeDiskReadPercThreshold = 70
	}

	if NodeDiskWritePercThreshold, found := config.Config.String("score.NodeDiskWritePercThreshold"); found {
		c.NodeDiskWritePercThreshold, err = strconv.ParseFloat(NodeDiskWritePercThreshold, 64)
		if err != nil {
			log.Panic("score.NodeDiskWritePercThreshold is not float64")
		}
	} else {
		c.NodeDiskWritePercThreshold = 70
	}

	if NodeNetInPercThreshold, found := config.Config.String("score.NodeNetInPercThreshold"); found {
		c.NodeNetInPercThreshold, err = strconv.ParseFloat(NodeNetInPercThreshold, 64)
		if err != nil {
			log.Panic("score.NodeNetInPercThreshold is not float64")
		}
	} else {
		c.NodeNetInPercThreshold = 70
	}

	if NodeNetOutPercThreshold, found := config.Config.String("score.NodeNetOutPercThreshold"); found {
		c.NodeNetOutPercThreshold, err = strconv.ParseFloat(NodeNetOutPercThreshold, 64)
		if err != nil {
			log.Panic("score.NodeNetOutPercThreshold is not float64")
		}
	} else {
		c.NodeNetOutPercThreshold = 70
	}

	if NodeRangeCountPercThreshold, found := config.Config.String("score.NodeRangeCountPercThreshold"); found {
		c.NodeRangeCountPercThreshold, err = strconv.ParseFloat(NodeRangeCountPercThreshold, 64)
		if err != nil {
			log.Panic("score.NodeRangeCountPercThreshold is not float64")
		}
	} else {
		c.NodeRangeCountPercThreshold = 50
	}

	if NodeLeaderCountPercThreshold, found := config.Config.String("score.NodeLeaderCountPercThreshold"); found {
		c.NodeLeaderCountPercThreshold, err = strconv.ParseFloat(NodeLeaderCountPercThreshold, 64)
		if err != nil {
			log.Panic("score.NodeLeaderCountPercThreshold is not float64")
		}
	} else {
		c.NodeLeaderCountPercThreshold = 50
	}

	if NodeCpuWeight, found := config.Config.String("score.NodeCpuWeight"); found {
		c.NodeCpuWeight, err = strconv.ParseFloat(NodeCpuWeight, 64)
		if err != nil {
			log.Panic("score.NodeCpuWeight is not float64")
		}
	} else {
		c.NodeCpuWeight = 16
	}
	if NodeMemoryWeight, found := config.Config.String("score.NodeMemoryWeight"); found {
		c.NodeMemoryWeight, err = strconv.ParseFloat(NodeMemoryWeight, 64)
		if err != nil {
			log.Panic("score.NodeMemoryWeight is not float64")
		}
	} else {
		c.NodeMemoryWeight = 64
	}
	if NodeDiskWeight, found := config.Config.String("score.NodeDiskWeight"); found {
		c.NodeDiskWeight, err = strconv.ParseFloat(NodeDiskWeight, 64)
		if err != nil {
			log.Panic("score.NodeDiskWeight is not float64")
		}
	} else {
		c.NodeDiskWeight = 64
	}
	if NodeSwapWeight, found := config.Config.String("score.NodeSwapWeight"); found {
		c.NodeSwapWeight, err = strconv.ParseFloat(NodeSwapWeight, 64)
		if err != nil {
			log.Panic("score.NodeSwapWeight is not float64")
		}
	} else {
		c.NodeSwapWeight = 16
	}
	if NodeDiskReadWeight, found := config.Config.String("score.NodeDiskReadWeight"); found {
		c.NodeDiskReadWeight, err = strconv.ParseFloat(NodeDiskReadWeight, 64)
		if err != nil {
			log.Panic("score.NodeDiskReadWeight is not float64")
		}
	} else {
		c.NodeDiskReadWeight = 16
	}
	if NodeDiskWriteWeight, found := config.Config.String("score.NodeDiskWriteWeight"); found {
		c.NodeDiskWriteWeight, err = strconv.ParseFloat(NodeDiskWriteWeight, 64)
		if err != nil {
			log.Panic("score.NodeDiskWriteWeight is not float64")
		}
	} else {
		c.NodeDiskWriteWeight = 16
	}
	if NodeNetInWeight, found := config.Config.String("score.NodeNetInWeight"); found {
		c.NodeNetInWeight, err = strconv.ParseFloat(NodeNetInWeight, 64)
		if err != nil {
			log.Panic("score.NodeNetInWeight is not float64")
		}
	} else {
		c.NodeNetInWeight = 16
	}
	if NodeNetOutWeight, found := config.Config.String("score.NodeNetOutWeight"); found {
		c.NodeNetOutWeight, err = strconv.ParseFloat(NodeNetOutWeight, 64)
		if err != nil {
			log.Panic("score.NodeNetOutWeight is not float64")
		}
	} else {
		c.NodeNetOutWeight = 16
	}
	if NodeRangeCountWeight, found := config.Config.String("score.NodeRangeCountWeight"); found {
		c.NodeRangeCountWeight, err = strconv.ParseFloat(NodeRangeCountWeight, 64)
		if err != nil {
			log.Panic("score.NodeRangeCountWeight is not float64")
		}
	} else {
		c.NodeRangeCountWeight = 16
	}
	if NodeLeaderCountWeight, found := config.Config.String("score.NodeLeaderCountWeight"); found {
		c.NodeLeaderCountWeight, err = strconv.ParseFloat(NodeLeaderCountWeight, 64)
		if err != nil {
			log.Panic("score.NodeLeaderCountWeight is not float64")
		}
	} else {
		c.NodeLeaderCountWeight = 16
	}

	//range score config
	if RangeTotalSize, found := config.Config.String("score.RangeTotalSize"); found {
		c.RangeTotalSize, err = strconv.ParseFloat(RangeTotalSize, 64)
		if err != nil {
			log.Panic("score.RangeTotalSize is not float64")
		}
	} else {
		c.RangeTotalSize = float64(GB)
	}
	if RangeTotalOps, found := config.Config.String("score.RangeTotalOps"); found {
		c.RangeTotalOps, err = strconv.ParseFloat(RangeTotalOps, 64)
		if err != nil {
			log.Panic("score.RangeTotalOps is not float64")
		}
	} else {
		c.RangeTotalOps = 200000
	}
	if RangeTotalNetInBytes, found := config.Config.String("score.RangeTotalNetInBytes"); found {
		c.RangeTotalNetInBytes, err = strconv.ParseFloat(RangeTotalNetInBytes, 64)
		if err != nil {
			log.Panic("score.RangeTotalNetInBytes is not float64")
		}
	} else {
		c.RangeTotalNetInBytes = 1000*float64(GB)
	}
	if RangeTotalNetOutBytes, found := config.Config.String("score.RangeTotalNetOutBytes"); found {
		c.RangeTotalNetOutBytes, err = strconv.ParseFloat(RangeTotalNetOutBytes, 64)
		if err != nil {
			log.Panic("score.RangeTotalNetOutBytes is not float64")
		}
	} else {
		c.RangeTotalNetOutBytes = 1000*float64(GB)
	}
	if RangeSizePercThreshold, found := config.Config.String("score.RangeSizePercThreshold"); found {
		c.RangeSizePercThreshold, err = strconv.ParseFloat(RangeSizePercThreshold, 64)
		if err != nil {
			log.Panic("score.RangeSizePercThreshold is not float64")
		}
	} else {
		c.RangeSizePercThreshold = 70
	}
	if RangeOpsPercThreshold, found := config.Config.String("score.RangeOpsPercThreshold"); found {
		c.RangeOpsPercThreshold, err = strconv.ParseFloat(RangeOpsPercThreshold, 64)
		if err != nil {
			log.Panic("score.RangeOpsPercThreshold is not float64")
		}
	} else {
		c.RangeOpsPercThreshold = 40
	}
	if RangeNetInPercThreshold, found := config.Config.String("score.RangeNetInPercThreshold"); found {
		c.RangeNetInPercThreshold, err = strconv.ParseFloat(RangeNetInPercThreshold, 64)
		if err != nil {
			log.Panic("score.RangeNetInPercThreshold is not float64")
		}
	} else {
		c.RangeNetInPercThreshold = 50
	}
	if RangeNetOutPercThreshold, found := config.Config.String("score.RangeNetOutPercThreshold"); found {
		c.RangeNetOutPercThreshold, err = strconv.ParseFloat(RangeNetOutPercThreshold, 64)
		if err != nil {
			log.Panic("score.RangeNetOutPercThreshold is not float64")
		}
	} else {
		c.RangeNetOutPercThreshold = 50
	}
	if RangeSizeWeight, found := config.Config.String("score.RangeSizeWeight"); found {
		c.RangeSizeWeight, err = strconv.ParseFloat(RangeSizeWeight, 64)
		if err != nil {
			log.Panic("score.RangeSizeWeight is not float64")
		}
	} else {
		c.RangeSizeWeight = 32
	}
	if RangeOpsWeight, found := config.Config.String("score.RangeOpsWeight"); found {
		c.RangeOpsWeight, err = strconv.ParseFloat(RangeOpsWeight, 64)
		if err != nil {
			log.Panic("score.RangeOpsWeight is not float64")
		}
	} else {
		c.RangeOpsWeight = 32
	}
	if RangeNetInWeight, found := config.Config.String("score.RangeNetInWeight"); found {
		c.RangeNetInWeight, err = strconv.ParseFloat(RangeNetInWeight, 64)
		if err != nil {
			log.Panic("score.RangeNetInWeight is not float64")
		}
	} else {
		c.RangeNetInWeight = 16
	}
	if RangeNetOutWeight, found := config.Config.String("score.RangeNetOutWeight"); found {
		c.RangeNetOutWeight, err = strconv.ParseFloat(RangeNetOutWeight, 64)
		if err != nil {
			log.Panic("score.RangeNetOutWeight is not float64")
		}
	} else {
		c.RangeNetOutWeight = 16
	}

	if NodeScoreMulti, found := config.Config.String("score.NodeScoreMulti"); found {
		c.NodeScoreMulti, err = strconv.ParseFloat(NodeScoreMulti, 64)
		if err != nil {
			log.Panic("score.NodeScoreMulti is not float64")
		}
	} else {
		c.NodeScoreMulti = 8
	}
	if RangeScoreMulti, found := config.Config.String("score.RangeScoreMulti"); found {
		c.RangeScoreMulti, err = strconv.ParseFloat(RangeScoreMulti, 64)
		if err != nil {
			log.Panic("score.RangeScoreMulti is not int")
		}
	} else {
		c.RangeScoreMulti = 4
	}
	if NodeDiskMustFreeSize, found := config.Config.String("score.NodeDiskMustFreeSize"); found {
		c.NodeDiskMustFreeSize, err = strconv.ParseUint(NodeDiskMustFreeSize, 10, 64)
		if err != nil {
			log.Panic("score.NodeDiskMustFreeSize is not int")
		}
	} else {
		c.NodeDiskMustFreeSize = 10*GB
	}



}
