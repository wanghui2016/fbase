package server

import (
	"time"
	"sync"
	"fmt"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"util/log"
	"golang.org/x/net/context"
)

type Pusher interface {
	Run()
	Close()
	Push(format string, v ...interface{}) error
}

const (
	DATABASE_FBASE = "fbase"
)

// 监控表
//var (
//	GatewayMonitor = []*Column{
//		&Column{Name: "time", DataType: "bigint", PrimaryKey: true, },
//		&Column{Name: "address", DataType: "string", PrimaryKey: true, },
//		&Column{Name: "command", DataType: "string", PrimaryKey: true, },
//		&Column{Name: "calls", DataType: "bigint", },
//		&Column{Name: "total_usec", DataType: "bigint", },
//		&Column{Name: "parse_usec", DataType: "bigint", },
//		&Column{Name: "call_usec", DataType: "bigint", },
//		&Column{Name: "hits", DataType: "bigint", },
//		&Column{Name: "misses", DataType: "bigint", },
//		&Column{Name: "slowlogs", DataType: "string", },
//		&Column{Name: "trips", DataType: "string", },
//	}
//	RangeMonitor = []*metapb.Column{
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "range_size", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//
//	NodeProcessMonitor = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Unsigned: true},
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "host", DataType: metapb.DataType_Varchar, },
//
//		&metapb.Column{Name: "cpu_percent", DataType: metapb.DataType_Double, },
//		&metapb.Column{Name: "io_read_count", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "io_write_count", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "io_read_bytes", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "io_write_bytes", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "memory_rss", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "memory_vms", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "memory_swap", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "memory_percent", DataType: metapb.DataType_Double, },
//		&metapb.Column{Name: "net_connection_count", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_byte_sent", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_byte_recv", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_packet_sent", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_packet_recv", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_err_in", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_err_out", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_drop_in", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "net_drop_out", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "leader_count", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//
//	MachineMonitor = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Unsigned: true},
//
//		//&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//		&metapb.Column{Name: "host", DataType: metapb.DataType_Varchar, },
//		&metapb.Column{Name: "cpu_proc_rate", DataType: metapb.DataType_Double, },
//		&metapb.Column{Name: "total_memory", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "used_memory", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "total_swap_memory", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "used_swap_memory", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "total_disk", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "used_disk", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "load", DataType: metapb.DataType_Double, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//	DbMonitor = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "dbid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//		//&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		//&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "db_size", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//	TableMonitor = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "tableid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		//&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		//&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "table_size", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//	ClusterMonitor = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//		//&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//		&metapb.Column{Name: "total_capacity", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "used_capacity", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//
//	ClusterTask = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "taskid", DataType: metapb.DataType_BigInt,PrimaryKey: 1, },
//
//		//&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "finish_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//		&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "used_time", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "state", DataType: metapb.DataType_Varchar, },
//		&metapb.Column{Name: "detail", DataType: metapb.DataType_Varchar, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//
//	EventStatistics = []*metapb.Column {
//		&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1,  },
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "db_name", DataType: metapb.DataType_Varchar },
//		&metapb.Column{Name: "table_name", DataType: metapb.DataType_Varchar, },
//		&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, Unsigned: true},
//		&metapb.Column{Name: "start_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "end_time", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "statistics_type", DataType: metapb.DataType_Varchar, },
//
//		&metapb.Column{Name: "ttl", DataType: metapb.DataType_BigInt, },
//	}
//)

var (
	EventStatisticsSQL = `insert into event_statistics (clusterid, db_name, table_name, rangeid, nodeid, start_time, end_time, statistics_type, ttl) values (%d,"%s","%s",%d,%d,%d,%d,"%s",%d)`
	TaskSQL = `insert into cluster_task (clusterid, finish_time, taskid, create_time, used_time, state, detail, ttl) values (%d,%d,%d,%d,%d,"%s","%s",%d)`
	ClusterMonitorSQL = `insert into cluster_monitor (clusterid, create_time, total_capacity, used_capacity, ops, bytes_in_per_sec, bytes_out_per_sec,
		range_count, connected_clients, ttl) values (%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)`
	NodeProcessMonitorSQL = `insert into node_process_monitor (clusterid, nodeid, create_time, host,
	cpu_percent,
	io_read_count, io_write_count, io_read_bytes, io_write_bytes,
	memory_rss, memory_vms, memory_swap, memory_percent,
	net_connection_count, net_byte_sent, net_byte_recv, net_packet_sent, net_packet_recv, net_err_in, net_err_out, net_drop_in, net_drop_out,
	range_count, leader_count, ttl)
	values (%d,%d,%d,"%s",
	%f,
	%d,%d,%d,%d,
	%d,%d,%d,%f,
	%d,%d,%d,%d,%d,%d,%d,%d,%d,
	%d,%d,%d)`
	NodeMonitorSQL = `insert into node_monitor (clusterid, nodeid, create_time, host, cpu_proc_rate, total_memory, used_memory,
			total_swap_memory, used_swap_memory, total_disk, used_disk, connected_clients, range_count, bytes_in_per_sec,
			bytes_out_per_sec, load, ttl) values (%d,%d,%d,"%s",%f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%d)`
	DatabaseMonitorSQL = `insert into db_monitor (clusterid, dbid, create_time, db_size, ops, bytes_in_per_sec, bytes_out_per_sec, range_count, ttl) values (%d,%d,%d,%d,%d,%d,%d,%d,%d)`
	TableMonitorSQL = `insert into table_monitor (clusterid, tableid, create_time, table_size, ops, bytes_in_per_sec, bytes_out_per_sec, range_count, ttl) values (%d,%d,%d,%d,%d,%d,%d,%d,%d)`
	RangeMonitorSQL = `insert into range_monitor (clusterid, rangeid, create_time, range_size, ops, bytes_in_per_sec, bytes_out_per_sec, ttl) values (%d,%d,%d,%d,%d,%d,%d,%d)`
	GatewayMonitorSQL = `insert into gateway_monitor (create_time, address, command, calls, total_usec, parse_usec, call_usec, hits, misses, slowlogs, trips, ttl) values (%d,%s,%s,%d,%d,%d,%d,%d,%d,%s,%s,%d)`
)

type PushMessage struct {
	Format    string
	Args      []interface{}
}

type LocalPusher struct {
	dns   string
	db    *sql.DB

	cluster *Cluster

	message chan *PushMessage

	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewLocalPusher(dns string, cluster *Cluster) Pusher {
	ctx, cancel := context.WithCancel(context.Background())
	p := &LocalPusher{
		dns:   dns,
		cluster: cluster,
		message: make(chan *PushMessage, 10000),
		ctx: ctx,
		cancel: cancel,
	}
	return p
}

func (p *LocalPusher) work() {
	log.Info("start pusher worker")
	defer p.wg.Done()
	for {
		select {
		case msg := <-p.message:
			p.push(msg.Format, msg.Args...)
		case <-p.ctx.Done():
			log.Info("pusher stopped: %v", p.ctx.Err())
			return
		}
	}
}

func (p *LocalPusher) collection() {
	log.Info("start collection")
	defer p.wg.Done()
	timer := time.NewTicker(time.Minute)
	for {
		select {
		case <-timer.C:
		    if p.cluster.IsLeader() {
			    p.scan()
		    }
		case <-p.ctx.Done():
			log.Info("pusher stopped: %v", p.ctx.Err())
			return
		}
	}
}

func (p *LocalPusher) Run() {
	p.wg.Add(1)
	go p.work()
	p.wg.Add(1)
	go p.collection()
}

func (p *LocalPusher) Close() {
	p.cancel()
	p.wg.Wait()
	if p.db != nil {
		p.db.Close()
	}
	return
}

type ClusterStats struct {
	Ops                    uint64
	BytesInPerSec          uint64
	BytesOutPerSec         uint64
	TotalCommandsProcessed uint64
	TotalCapacity          uint64
	UsedCapacity           uint64
	KeyspaceMisses         uint64
	RangeCount             uint64
	ConnectedClients       uint64
}

type DbStats struct {
	Size                   uint64
	Ops                    uint64
	BytesInPerSec          uint64
	BytesOutPerSec         uint64
	TotalCommandsProcessed uint64
	KeyspaceMisses         uint64
	RangeCount             uint64
}

type TableStats struct {
	Size                   uint64
	Ops                    uint64
	BytesInPerSec          uint64
	BytesOutPerSec         uint64
	TotalCommandsProcessed uint64
	KeyspaceMisses         uint64
	RangeCount             uint64
}

func (m *LocalPusher) scan() {
	dbs := m.cluster.GetAllDatabase()
	var clusterStats ClusterStats
	var clusterStatsCount int
	var rangesCount int
	for _, db := range dbs {
		dbStats := DbStats{}
		dbStatsCount := 0
		tables := db.GetAllTable()
		for _, table := range tables {
			tableStats := TableStats{}
			ranges := table.GetAllRanges()
			rangesCount += len(ranges)
			tableStats.RangeCount = uint64(len(ranges))
			for _, r := range ranges {
				tableStats.BytesInPerSec += r.Stats.BytesInPerSec
				tableStats.BytesOutPerSec += r.Stats.BytesOutPerSec
				tableStats.Ops += r.Stats.Ops
				tableStats.Size += r.Stats.Size_
				tableStats.TotalCommandsProcessed += r.Stats.TotalCommandsProcessed
				tableStats.KeyspaceMisses += r.Stats.KeyspaceMisses
				if err := m.Push(RangeMonitorSQL,
					m.cluster.GetClusterId(), r.GetId(), time.Now().Unix(), r.Stats.Size_, r.Stats.Ops,
					r.Stats.BytesInPerSec, r.Stats.BytesOutPerSec, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
					log.Error("pusher range info error: %s", err.Error())
				}
			}
			if err := m.Push(TableMonitorSQL,
				m.cluster.GetClusterId(), table.GetId(), time.Now().Unix(), tableStats.Size, tableStats.Ops,
				tableStats.BytesInPerSec, tableStats.BytesOutPerSec, tableStats.RangeCount, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
				log.Error("pusher table info error: %s", err.Error())
			}
			dbStats.BytesInPerSec += tableStats.BytesInPerSec
			dbStats.BytesOutPerSec += tableStats.BytesOutPerSec
			dbStats.Ops += tableStats.Ops
			dbStats.Size += tableStats.Size
			dbStats.TotalCommandsProcessed += tableStats.TotalCommandsProcessed
			dbStats.KeyspaceMisses += tableStats.KeyspaceMisses
			dbStats.RangeCount += tableStats.RangeCount
			dbStatsCount++
		}
		if dbStatsCount == 0 {
			continue
		}
		if err := m.Push(DatabaseMonitorSQL,
			m.cluster.GetClusterId(), db.GetId(), time.Now().Unix(), dbStats.Size, dbStats.Ops,
			dbStats.BytesInPerSec, dbStats.BytesOutPerSec, dbStats.RangeCount, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("pusher database info error: %s", err.Error())
		}
		clusterStats.RangeCount += uint64(rangesCount)
		clusterStats.BytesInPerSec += dbStats.BytesInPerSec
		clusterStats.BytesOutPerSec += dbStats.BytesOutPerSec
		clusterStats.Ops += dbStats.Ops
		clusterStats.TotalCommandsProcessed += dbStats.TotalCommandsProcessed
		clusterStats.KeyspaceMisses += dbStats.KeyspaceMisses
		clusterStatsCount++
	}
	if clusterStatsCount == 0 {
		return
	}
	nodes := m.cluster.GetAllInstances()
	for _, n := range nodes {
		clusterStats.TotalCapacity += n.Stats.DiskTotal
		clusterStats.UsedCapacity += n.Stats.DiskUsed

		if err := m.Push(NodeMonitorSQL,
			m.cluster.GetClusterId(), n.ID(), time.Now().Unix(), n.GetAddress(),
			n.Stats.CpuProcRate, n.Stats.MemoryTotal, n.Stats.MemoryUsed, n.Stats.SwapMemoryTotal,
			n.Stats.SwapMemoryUsed, n.Stats.DiskTotal, n.Stats.DiskUsed, n.Stats.NetTcpConnections,
			n.RangeCount, n.Stats.NetIoInBytePerSec, n.Stats.NetIoOutBytePerSec, n.Stats.Load5, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("push node info failed, err[%V]", err)
		}

		processStats := n.ProcessStats
		if err := m.Push(NodeProcessMonitorSQL,
			m.cluster.GetClusterId(), n.ID(), time.Now().Unix(), n.GetAddress(),
			processStats.GetCpuPercent(),
			processStats.GetIoReadCount(), processStats.GetIoWriteCount(), processStats.GetIoReadBytes(), processStats.GetIoWriteBytes(),
			processStats.GetMemoryRss(), processStats.GetMemoryVms(), processStats.GetMemorySwap(), processStats.GetMemoryPercent(),
			processStats.GetNetConnectionCount(), processStats.GetNetByteSent(), processStats.GetNetByteRecv(), processStats.GetNetPacketSent(), processStats.GetNetPacketRecv(),
			processStats.GetNetErrIn(), processStats.GetNetErrOut(), processStats.GetNetDropIn(), processStats.GetNetErrOut(),
			n.RangeCount, n.RangeLeaderCount, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
			log.Error("push node process info failed, err[%V]", err)
		}
		log.Debug("---- process monitor sql ---- ")
		log.Debug(NodeProcessMonitorSQL,
			m.cluster.GetClusterId(), n.ID(), time.Now().Unix(), n.GetAddress(),
			processStats.GetCpuPercent(),
			processStats.GetIoReadCount(), processStats.GetIoWriteCount(), processStats.GetIoReadBytes(), processStats.GetIoWriteBytes(),
			processStats.GetMemoryRss(), processStats.GetMemoryVms(), processStats.GetMemorySwap(), processStats.GetMemoryPercent(),
			processStats.GetNetConnectionCount(), processStats.GetNetByteSent(), processStats.GetNetByteRecv(), processStats.GetNetPacketSent(), processStats.GetNetPacketRecv(),
			processStats.GetNetErrIn(), processStats.GetNetErrOut(), processStats.GetNetDropIn(), processStats.GetNetErrOut(),
			n.RangeCount, n.RangeLeaderCount, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000)
	}
	if err := m.Push(ClusterMonitorSQL, m.cluster.GetClusterId(), time.Now().Unix(), clusterStats.TotalCapacity, clusterStats.UsedCapacity,
		clusterStats.Ops, clusterStats.BytesInPerSec, clusterStats.BytesOutPerSec, clusterStats.RangeCount, clusterStats.ConnectedClients, (m.cluster.conf.MonitorTTL+time.Now().UnixNano())/1000000); err != nil {
		log.Error("pusher cluster info error: %s", err.Error())
	}
}

// 串行执行，因此不需要锁
func (m *LocalPusher) push(format string, args ...interface{}) error {
	if m.db == nil {
		db, err := sql.Open("mysql", m.dns)
		if err != nil {
			log.Error("mysql server[%s] open failed, err[%v]", m.dns, err)
			return err
		}
		db.SetMaxOpenConns(50)
		db.SetMaxIdleConns(10)
		m.db = db
	}
	SQL := fmt.Sprintf(format, args...)
	log.Debug("sql %s", SQL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := m.db.ExecContext(ctx, SQL)
	if err != nil {
		log.Warn("push failed, err[%v]", err)
		// 检查服务端是否健康
		ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
		defer cancel1()
		err = m.db.PingContext(ctx1)
		if err != nil {
			log.Warn("mysql server unhealthy, err [%v]!!!!", err)
		}
	}

	return err
}

func (m *LocalPusher) Push(format string, args ...interface{}) error {
	msg := &PushMessage{
		Format:  format,
		Args:    args,
	}
	select {
	case m.message <- msg:
	default:
		log.Error("pusher message queue is full!!!")
		return ErrPusherBusy
	}
	return nil
}
