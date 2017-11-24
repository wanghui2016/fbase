package server

import (
	"time"
	"sort"
	"sync"
	"errors"
	"fmt"
	"runtime"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"model/pkg/metapb"
	"model/pkg/mspb"
	"model/pkg/schpb"
	"model/pkg/statspb"
	"util/log"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
	"os"
)

func (s *Server) CreateRange(ctx context.Context, req *schpb.CreateRangeRequest) (*schpb.CreateRangeResponse, error) {
	resp := &schpb.CreateRangeResponse{}
	rng, find := s.FindRange(req.GetRange().GetId())
	if find {
		log.Warn("range[%d] already exist", req.GetRange().GetId())
		return resp, nil
	}

	rng, err := NewRange(req.GetRange(), s, nil, false)
	if err != nil {
		log.Error("create new range[%v] failed, err[%v]", req.GetRange().String(), err)
		return nil, err
	}
	s.AddRange(rng)
	// add node to cluster
	log.Info("create new range[%s] success", rng.String())
	return resp, nil
}

func (s *Server) DeleteRange(ctx context.Context, req *schpb.DeleteRangeRequest) (*schpb.DeleteRangeResponse, error) {
	rng := s.DelRange(req.GetRangeId())
	if rng == nil {
		log.Warn("range[%d] not found", req.GetRangeId())
		resp := &schpb.DeleteRangeResponse{}
		return resp, nil
	}
	rng.Clean()
	resp := &schpb.DeleteRangeResponse{}
	log.Info("delete range[%s] success", rng.String())
	return resp, nil
}

func (s *Server) TransferRangeLeader(ctx context.Context, req *schpb.TransferRangeLeaderRequest) (*schpb.TransferRangeLeaderResponse, error) {
	rng, find := s.FindRange(req.GetRangeId())
	if !find {
		log.Error("range[%d] not found", req.GetRangeId())
		return nil, errors.New("range not found")
	}
	resp := &schpb.TransferRangeLeaderResponse{}
	err := rng.RaftLeaderTransfer()
	if err != nil {
		log.Error("range change leader failed, err[%v]", err)
		return nil, err
	}
	log.Info("range[%s] transfer leader success", rng.String())
	return resp, nil
}

func (s *Server) OfflineRange(ctx context.Context, req *schpb.OfflineRangeRequest) (*schpb.OfflineRangeResponse, error) {
	rng, find := s.FindRange(req.GetRangeId())
	if !find {
		log.Error("range[%d] not found", req.GetRangeId())
		return nil, errors.New("range not found")
	}
	rng.region.State = metapb.RangeState_R_Offline
	resp := &schpb.OfflineRangeResponse{}
	return resp, nil
}

func (s *Server) StopRange(ctx context.Context, req *schpb.StopRangeRequest) (*schpb.StopRangeResponse, error) {
	rng, find := s.FindRange(req.GetRangeId())
	if !find {
		log.Error("range[%d] not found", req.GetRangeId())
		return nil, errors.New("range not found")
	}
	rng.region.State = metapb.RangeState_R_Abnormal
	resp := &schpb.StopRangeResponse{}
	return resp, nil
}

func (s *Server) CloseRange(ctx context.Context, req *schpb.CloseRangeRequest) (*schpb.CloseRangeResponse, error) {
	rng := s.DelRange(req.GetRangeId())
	if rng == nil {
		log.Error("range[%d] not found", req.GetRangeId())
		return nil, errors.New("range not found")
	}
	rng.Close()
	resp := &schpb.CloseRangeResponse{}
	return resp, nil
}

func (s *Server) SplitRangePrepare(ctx context.Context, req *schpb.SplitRangePrepareRequest) (*schpb.SplitRangePrepareResponse, error) {
	rng, find := s.FindRange(req.GetRangeId())
	if !find {
		log.Error("range[%d] not found", req.GetRangeId())
		return nil, errors.New("range not found")
	}

	ready := rng.RangeSplitPrepare()
	resp := &schpb.SplitRangePrepareResponse{Ready: ready}
	log.Info("range[%s] split prepare[%v]", rng.String(), ready)
	return resp, nil
}

func (s *Server) AddRange(rng *Range) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ranges[rng.GetRangeId()] = rng
}

func (s *Server) delRange(rangeId uint64) *Range {
	rng, find := s.FindRange(rangeId)
	if !find {
		return nil
	}
	// delete range
	s.lock.Lock()
	delete(s.ranges, rangeId)
	s.lock.Unlock()
	return rng
}

func (s *Server) DelRange(rangeId uint64) *Range {
	rng := s.delRange(rangeId)
	if rng == nil {
		return nil
	}
	return rng
}

func (s *Server) FindRange(rangeId uint64) (*Range, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if rng, ok := s.ranges[rangeId]; ok {
		return rng, true
	}
	return nil, false
}

func (s *Server) GetAllRanges() []*Range {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var rngs []*Range
	for _, rng := range s.ranges {
		rngs = append(rngs, rng)
	}
	return rngs
}

func (s *Server) processStats() *statspb.ProcessStats {
	cpuPercent := func() float64 {
		if cpu, err := s.procStats.CPUPercent(); err != nil {
			log.Debug("cpu stats error: %v", err)
			return 0.0
		} else {
			return cpu
		}
	}()
	ioReadCount, ioWriteCount, ioReadBytes, ioWriteBytes := func() (rc uint64, wc uint64, rb uint64, wb uint64 ) {
		if io, err := s.procStats.IOCounters(); err != nil {
			log.Debug("io stats error: %v", err)
		} else {
			rc = io.ReadCount
			wc = io.WriteCount
			rb = io.ReadBytes
			wb = io.WriteBytes
		}
		return
	}()
	memoryRss, memoryVms, memorySwap, memoryPercent := func() (rss uint64, vms uint64, swap uint64, percent float64) {
		if mem, err := s.procStats.MemoryInfo(); err != nil {
			log.Debug("memory info stats error: %v", err)
		} else {
			rss = mem.RSS
			vms = mem.VMS
			swap = mem.Swap
		}
		if per, err := s.procStats.MemoryPercent(); err != nil {
			log.Debug("memory percent stats error: %v", err)
		} else {
			percent = float64(per)
		}
		return
	}()
	netConnectionCount, netByteSent, netByteRecv, netPacketSent, netPacketRecv, netErrIn, netErrOut, netDropIn, netDropOut :=
		func() (connectCount uint64, byteSent uint64, byteRecv uint64, packetSent uint64, packetRecv uint64, errIn uint64, errOut uint64, dropIn uint64, dropOut uint64) {
			if counters, err := s.procStats.NetIOCounters(false); err != nil {
				log.Debug("net stats error: %v", err)
			} else {
				connectCount = uint64(len(counters))
				for _, counter := range counters {
					byteSent += counter.BytesSent
					byteRecv += counter.BytesRecv
					packetSent += counter.PacketsSent
					packetRecv += counter.PacketsRecv
					errIn += counter.Errin
					errOut += counter.Errout
					dropIn += counter.Dropin
					dropOut += counter.Dropout
				}
			}
			return
		}()
	stats := &statspb.ProcessStats{
		CpuPercent: cpuPercent,
		IoReadCount: ioReadCount,
		IoWriteCount: ioWriteCount,
		IoReadBytes: ioReadBytes,
		IoWriteBytes: ioWriteBytes,
		MemoryRss: memoryRss,
		MemoryVms: memoryVms,
		MemorySwap: memorySwap,
		MemoryPercent: memoryPercent,
		NetConnectionCount: netConnectionCount,
		NetByteSent: netByteSent,
		NetByteRecv: netByteRecv,
		NetPacketSent: netPacketSent,
		NetPacketRecv: netPacketRecv,
		NetErrIn: netErrIn,
		NetErrOut: netErrOut,
		NetDropIn: netDropIn,
		NetDropOut: netDropOut,
	}
	return stats
}

func (s *Server) machineStat() (stats *statspb.NodeStats, err error) {
	stats = new(statspb.NodeStats)
	cpuInfo(s.machineStats)
	memInfo(s.machineStats)
	diskInfo(s.machineStats, s.conf.diskQuota)
	netInfo(s.machineStats)
	machineStats := s.machineStats
	stats.MemoryTotal = machineStats.MemoryTotal
	stats.MemoryUsedRss = machineStats.MemoryUsedRss
	stats.MemoryUsed = machineStats.MemoryUsed
	stats.MemoryFree = machineStats.MemoryFree
	stats.MemoryUsedPercent = machineStats.MemoryUsedPercent
	stats.SwapMemoryTotal = machineStats.SwapMemoryTotal
	stats.SwapMemoryUsed = machineStats.SwapMemoryUsed
	stats.SwapMemoryFree = machineStats.SwapMemoryFree
	stats.SwapMemoryUsedPercent = machineStats.SwapMemoryUsedPercent
	// CPU
	stats.CpuProcRate = machineStats.CpuProcRate
	stats.CpuCount = machineStats.CpuCount
	stats.Load1 = machineStats.Load1
	stats.Load5 = machineStats.Load5
	stats.Load15 = machineStats.Load15
	// Disk
	stats.DiskTotal = machineStats.DiskTotal
	stats.DiskUsed = machineStats.DiskUsed
	stats.DiskFree = machineStats.DiskFree
	stats.DiskProcRate = machineStats.DiskProcRate
	// Net
	stats.NetIoInBytePerSec = machineStats.NetIoInBytePerSec
	stats.NetIoOutBytePerSec = machineStats.NetIoOutBytePerSec
	stats.NetTcpConnections = machineStats.NetTcpConnections
	stats.NetTcpActiveOpensPerSec = machineStats.NetTcpActiveOpensPerSec
	return stats, nil
}

func (s *Server) heartbeat() {
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
	nhb := &mspb.NodeHeartbeatRequest{
		NodeId:    s.node.GetId(),
		NodeState: s.node.GetState(),
	}
	stats, err := s.machineStat()
	if err != nil {
		log.Error("get machine stat failed, err[%v]", err)
		return
	}
	interval := s.conf.HeartbeatInterval
	if !s.lastHeartbeat.IsZero() {
		delay := time.Now().Sub(s.lastHeartbeat)
		if delay > time.Second*time.Duration(interval+1) {
			log.Error("heartbeat is delay too long %v", delay.String())
		}
	}
	if !s.lastHeartbeatSuccess.IsZero() && !s.lastHeartbeat.IsZero() {
		delay := s.lastHeartbeat.Sub(s.lastHeartbeatSuccess)
		if delay > time.Second*time.Duration(interval*3) {
			log.Error("Heartbeat failure is delay too long %v", delay.String())
		}
	}

	//nhb.NodeStats = stats
	var allOfflineRanges []uint64
	ranges := s.GetAllRanges()
	nhb.RangeCount = uint32(len(ranges))
	var splitCount, applySnapCount, leaderCount uint32
	var allRanges []uint64
	for _, r := range ranges {
		allRanges = append(allRanges, r.GetRangeId())
		if r.region.State == metapb.RangeState_R_Offline {
			allOfflineRanges = append(allOfflineRanges, r.GetRangeId())
		}
		if r.region.State == metapb.RangeState_R_Split {
			splitCount++
		}
		if r.region.State == metapb.RangeState_R_LoadSnap {
			applySnapCount++
		}
		stats.Ops += r.Stats().GetOps()
		if !r.IsLeader() {
			continue
		}
		leaderCount++
	}
	nhb.RangesOffline = allOfflineRanges
	nhb.RangeLeaderCount = leaderCount
	nhb.RangeSplitCount = splitCount
	nhb.ApplyingSnapCount = applySnapCount
	nhb.Stats = stats
	nhb.Ranges = allRanges
	nhb.ProcessStats = s.processStats()
	//log.Debug("///////////heartbeat///////////")
	//log.Debug("HB: %s", hb.String())
	s.lastHeartbeat = time.Now()
	resp, err := s.mscli.NodeHeartbeat(nhb)
	if err != nil {
		s.heartbeatFailCount++
		log.Warn("heartbeat failed, err[%v]", err)
		return
	}
	s.lastHeartbeatSuccess = time.Now()
	s.heartbeatFailCount = 0
	s.dealNodeHeartbeatResp(resp)
}

func (s *Server) rangesRecover(wg *sync.WaitGroup, ranges []*metapb.Range) {
	defer wg.Done()
	for _, r := range ranges {
		_, find := s.FindRange(r.GetId())
		if find {
			continue
		}
		rng, err := NewRange(r, s, nil, false)
		if err != nil {
			log.Error("create new range[%v] failed, err[%v]", r.String(), err)
			// TODO error
			continue
		}
		s.AddRange(rng)
	}
}

func (s *Server) RangesRecover(ranges []*metapb.Range) {
	// 首先排序
	sort.Sort(metapb.RangeByIdSlice(ranges))
	// 将具有分裂关系的range放在一个组内
	var wg sync.WaitGroup
	var conflictRanges, irrelatedRanges []*metapb.Range
	var conflictRangesM map[uint64]*metapb.Range
	conflictRangesM = make(map[uint64]*metapb.Range)
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[i].GetDbId() != ranges[j].GetDbId() || ranges[i].GetTableId() != ranges[j].GetTableId() {
				continue
			}
			if metapb.Compare(ranges[i].StartKey, ranges[j].StartKey) >= 0 && metapb.Compare(ranges[i].StartKey, ranges[j].EndKey) < 0 {
				conflictRangesM[ranges[i].GetId()] = ranges[i]
				conflictRangesM[ranges[j].GetId()] = ranges[j]
			} else if metapb.Compare(ranges[i].EndKey, ranges[j].StartKey) > 0 && metapb.Compare(ranges[i].EndKey, ranges[j].EndKey) <= 0 {
				conflictRangesM[ranges[i].GetId()] = ranges[i]
				conflictRangesM[ranges[j].GetId()] = ranges[j]
			} else if metapb.Compare(ranges[i].StartKey, ranges[j].StartKey) <= 0 && metapb.Compare(ranges[i].EndKey, ranges[j].EndKey) >= 0 {
				conflictRangesM[ranges[i].GetId()] = ranges[i]
				conflictRangesM[ranges[j].GetId()] = ranges[j]
			}

		}
	}
	for i := 0; i < len(ranges); i++ {
		if _, ok := conflictRangesM[ranges[i].GetId()]; ok {
			conflictRanges = append(conflictRanges, ranges[i])
		} else {
			irrelatedRanges = append(irrelatedRanges, ranges[i])
		}
	}
	sort.Sort(metapb.RangeByIdSlice(conflictRanges))
	wg.Add(1)
	go s.rangesRecover(&wg, conflictRanges)
	num := len(irrelatedRanges) / 10
	start := 0
	end := 0
	for i := 0; i <= num && start < len(irrelatedRanges); i++ {
		start = i * 10
		end = (i + 1) * 10
		if end > len(irrelatedRanges) {
			end = len(irrelatedRanges)
		}
		wg.Add(1)
		go s.rangesRecover(&wg, irrelatedRanges[start:end])
	}
	wg.Wait()
}

func (s *Server) dealNodeHeartbeatResp(resp *mspb.NodeHeartbeatResponse) {
	task := resp.GetTask()
	if task == nil {
		return
	}
	switch task.Type {
	case taskpb.TaskType_NodeLogout:
		log.Info("node logout")
		ranges := s.GetAllRanges()
		for _, rng := range ranges {
			s.DelRange(rng.GetRangeId())
			rng.Clean()
		}
		s.node.State = metapb.NodeState_N_Logout
	case taskpb.TaskType_NodeLogin:
		log.Info("node login")
		login := task.GetNodeLogin()
		// ranges按照ID由小到大排序
		ranges := login.GetRanges()
		if len(ranges) == 0 {
			s.node.State = metapb.NodeState_N_Login
			return
		}
		s.RangesRecover(ranges)
		s.node.State = metapb.NodeState_N_Login
	case taskpb.TaskType_NodeDeleteRanges:
		deleteRanges := task.GetNodeDeleteRanges()
		log.Info("delete ranges[%v]", deleteRanges.GetRangeIds())
		for _, rangeId := range deleteRanges.GetRangeIds() {
			rng := s.DelRange(rangeId)
			if rng == nil {
				log.Warn("range[%d] not found", rangeId)
				continue
			}
			rng.Clean()
		}
	case taskpb.TaskType_NodeCreateRanges:
		createRanges := task.GetNodeCreateRanges()
		log.Info("create ranges[%v]", createRanges.GetRanges())
		for _, r := range createRanges.GetRanges() {
			rng, find := s.FindRange(r.GetId())
			if find {
				log.Warn("range[%s] already exist", rng.String())
				continue
			}

			rng, err := NewRange(r, s, nil, false)
			if err != nil {
				log.Error("create new range[%v] failed, err[%v]", r.String(), err)
				continue
			}
			s.AddRange(rng)
			// add node to cluster
			log.Info("create new range[%s] success", rng.String())
		}
	}
}

type MachineStats struct {
	// Memory
	MemoryTotal           uint64  `json:"memory_total,omitempty"`
	MemoryUsedRss         uint64  `json:"memory_used_rss,omitempty"`
	MemoryUsed            uint64  `json:"memory_used,omitempty"`
	MemoryFree            uint64  `json:"memory_free,omitempty"`
	MemoryUsedPercent     float64 `json:"memory_used_percent,omitempty"`
	SwapMemoryTotal       uint64  `json:"swap_memory_total,omitempty"`
	SwapMemoryUsed        uint64  `json:"swap_memory_used,omitempty"`
	SwapMemoryFree        uint64  `json:"swap_memory_free,omitempty"`
	SwapMemoryUsedPercent float64 `json:"swap_memory_used_percent,omitempty"`
	// CPU
	CpuProcRate float64 `json:"cpu_proc_rate,omitempty"`
	CpuCount    uint32  `json:"cpu_count,omitempty"`
	Load1       float64 `json:"load1,omitempty"`
	Load5       float64 `json:"load5,omitempty"`
	Load15      float64 `json:"load15,omitempty"`
	// Disk
	diskPath     string
	DiskTotal    uint64  `json:"disk_total,omitempty"`
	DiskUsed     uint64  `json:"disk_used,omitempty"`
	DiskFree     uint64  `json:"disk_free,omitempty"`
	DiskProcRate float64 `json:"disk_proc_rate,omitempty"`
	// Net
	netLastTime        time.Time
	netIoInBytes       uint64
	netIoOutBytes      uint64
	netIoInPackage     uint64
	netIoOutPackage    uint64
	NetIoInBytePerSec  uint64 `json:"net_io_in_flow_per_sec,omitempty"`
	NetIoOutBytePerSec uint64 `json:"net_io_out_flow_per_sec,omitempty"`

	netTcpActiveOpens       uint64
	NetTcpConnections       uint32 `json:"net_tcp_connections,omitempty"`
	NetTcpActiveOpensPerSec uint64 `json:"net_tcp_active_opens_per_sec,omitempty"`
}

var (
	zeroTime = time.Time{}
)

func cpuInfo(stats *MachineStats) (err error) {
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
			err = fmt.Errorf("%v", r)
			return
		}
	}()
	// process cpu
	var cpus int
	if stats.CpuCount == 0 {
		cpus, err = cpu.Counts(false)
		if err != nil {
			log.Error("get CPU counts failed, err[%v]", err)
			return
		}
		if cpus == 0 {
			log.Error("invalid cpus count")
			err = errors.New("invalid cpus count")
			return
		}
		stats.CpuCount = uint32(cpus)
	}

	var cpu_ []float64
	if cpu_, err = cpu.Percent(time.Second, false); err == nil {
		stats.CpuProcRate = 0
		var cpuPercent float64
		for i := 0; i < len(cpu_); i++ {
			cpuPercent += cpu_[i]
		}
		if len(cpu_) > 0 {
			stats.CpuProcRate = cpuPercent / float64(len(cpu_))
		}
	}

	// load
	var avg *load.AvgStat
	if avg, err = load.Avg(); err == nil {
		stats.Load1 = avg.Load1
		stats.Load5 = avg.Load5
		stats.Load15 = avg.Load15
	}
	return
}

func memInfo(stats *MachineStats) (err error) {
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
			err = fmt.Errorf("%v", r)
			return
		}
	}()
	var memory_ *mem.VirtualMemoryStat
	if memory_, err = mem.VirtualMemory(); err == nil {
		if memory_.Total > 0 && memory_.Used > 0 {
			stats.MemoryUsed = memory_.Used
			stats.MemoryFree = memory_.Free
			stats.MemoryTotal = memory_.Total
			stats.MemoryUsedPercent = memory_.UsedPercent
		}
	}
	var swap *mem.SwapMemoryStat
	if swap, err = mem.SwapMemory(); err == nil {
		stats.SwapMemoryTotal = swap.Total
		stats.SwapMemoryUsed = swap.Used
		stats.SwapMemoryFree = swap.Free
		stats.SwapMemoryUsedPercent = swap.UsedPercent
	}
	return
}

func diskInfo(stats *MachineStats, quota uint64) (err error) {
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
			err = fmt.Errorf("%v", r)
			return
		}
	}()
	//disk
	if _, err := os.Stat(stats.diskPath); err != nil {
		os.MkdirAll(stats.diskPath, 0755)
	}
	du := dirUsage(stats.diskPath)
	stats.DiskTotal = quota
	stats.DiskUsed = du
	stats.DiskFree = quota - du
	stats.DiskProcRate = float64(du)/float64(quota)
	return
}

func netInfo(stats *MachineStats) (err error) {
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
			err = fmt.Errorf("%v", r)
			return
		}
	}()
	var ioStat []net.IOCountersStat
	ioStat, err = net.IOCounters(false)
	if err != nil {
		log.Error("get net IO counter failed, err[%v]", err)
		return err
	}
	if len(ioStat) == 0 {
		return errors.New("invalid net IO stat")
	}
	tcpProto, err := net.ProtoCounters([]string{"tcp"})
	if err != nil {
		log.Error("get net proto cpunter failed, err[%v]", err)
		return err
	}
	var netIoInBytes, netIoOutBytes, netIoInPackage, netIoOutPackage uint64
	netIoInBytes = ioStat[0].BytesRecv
	netIoOutBytes = ioStat[0].BytesSent
	netIoInPackage = ioStat[0].PacketsRecv
	netIoOutPackage = ioStat[0].PacketsSent
	var tcpConnections, tcpActiveOpens uint64
	tcpConnections = uint64(tcpProto[0].Stats["CurrEstab"])
	tcpActiveOpens = uint64(tcpProto[0].Stats["ActiveOpens"])
	if stats.netLastTime != zeroTime {
		stats.NetIoInBytePerSec = uint64(float64(netIoInBytes-stats.netIoInBytes) / time.Since(stats.netLastTime).Seconds())
		stats.NetIoOutBytePerSec = uint64(float64(netIoOutBytes-stats.netIoOutBytes) / time.Since(stats.netLastTime).Seconds())
		stats.NetTcpActiveOpensPerSec = uint64(float64(tcpActiveOpens-stats.netTcpActiveOpens) / time.Since(stats.netLastTime).Seconds())
	}
	stats.NetTcpConnections = uint32(tcpConnections)
	stats.netIoInBytes = netIoInBytes
	stats.netIoOutBytes = netIoOutBytes
	stats.netIoInPackage = netIoInPackage
	stats.netIoOutPackage = netIoOutPackage
	stats.netTcpActiveOpens = tcpActiveOpens
	stats.netLastTime = time.Now()
	return
}
