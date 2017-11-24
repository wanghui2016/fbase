package server

import (
	"sort"
	"time"

	"model/pkg/metapb"
	"util/log"
	"util/deepcopy"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
	"fmt"
)

const (
	MinDiskFree uint64 = 10 * 1000 * 1000 * 1000    // 10G
)

type DiskBalanceScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	alarm            Alarm
}

func NewDiskBalanceScheduler(interval time.Duration, cluster *Cluster, alarm Alarm) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &DiskBalanceScheduler{
		name:     "disk_balance_sheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster: cluster,
		alarm: alarm,
	}
}

func (db *DiskBalanceScheduler) GetName() string {
	return db.name
}

func (db *DiskBalanceScheduler) schedule(srcInss []*Instance, dstInssGroup [][]*Instance) *Task {
	for _, ins := range srcInss {
		if !ins.AllocSch() {
			continue
		}
		srcMac, find := db.cluster.FindMachine(ins.MacIp)
		if !find {
			log.Error("invalid machine %s", ins.MacIp)
			continue
		}
		var targetIns *Instance
		var targetRange *Range
		for _, inss := range dstInssGroup {
			for _, tIns := range inss {
				if !tIns.AllocSch() {
					continue
				}
				if tIns.GetDiskUsedPercent() > float64(90.0) {
					continue
				}
				dstMac, find := db.cluster.FindMachine(tIns.MacIp)
				if !find {
					log.Error("invalid machine %s", tIns.MacIp)
					continue
				}
				// 同地域
				if srcMac.GetZone().GetZoneName() != dstMac.GetZone().GetZoneName() {
					continue
				}
				// 同机房
				if srcMac.GetRoom().GetRoomName() != dstMac.GetRoom().GetRoomName() {
					continue
				}
				selecter := NewRangeBalanceSeleter(ins)
				filter := NewRangeBalanceFilter(tIns.GetAllRanges(), srcMac, dstMac, db.cluster)
				ranges := selecter.SelectRange(1, filter)
				// 未找到合适的分片，继续
				if len(ranges) == 0 {
					continue
				}
				targetIns = tIns
				targetRange = ranges[0]
				taskId, err := db.cluster.GenTaskId()
				if err != nil {
					log.Error("gen task ID failed, err[%v]", err)
					return nil
				}
				upPeer := &metapb.Peer{
					Id: targetRange.ID(),
					NodeId: targetIns.GetId(),
				}
				downPeer := &metapb.Peer{
					Id: targetRange.ID(),
					NodeId: ins.GetId(),
				}
				rng := deepcopy.Iface(targetRange.Range).(*metapb.Range)
				t := &taskpb.Task{
					Type:   taskpb.TaskType_RangeTransfer,
					Meta:   &taskpb.TaskMeta{
						TaskId:   taskId,
						CreateTime: time.Now().Unix(),
						State: taskpb.TaskState_TaskWaiting,
						Timeout: DefaultTransferTaskTimeout,
					},
					RangeTransfer: &taskpb.TaskRangeTransfer{
						Range:      rng,
						UpPeer:     upPeer,
						DownPeer:   downPeer,
					},
				}
				task := NewTask(t)
				err = task.GenSubTasks(db.cluster)
				if err != nil {
					log.Error("gen sub tasks failed, err[%v]", err)
					return nil
				}
				log.Info("disk blance range[%s] from [%s] to [%s]",
					targetRange.SString(), ins.GetAddress(), targetIns.GetAddress())
				return task
			}
		}
	}
	return nil
}

// 磁盘占用分为三档
// 一档: 磁盘占用0% ~ 50%
// 二挡: 磁盘占用50% ~ 70%
// 三档: 磁盘占用70% ~ 100%
// 当一档和三档均有节点分布触发迁移
// 当有节点磁盘占用超过90%触发迁移并告警
func (db *DiskBalanceScheduler) Schedule() *Task {
	nodes := db.cluster.GetAllActiveInstances()
	num := len(nodes)
	if num == 0 {
		return nil
	}
	// 按照磁盘占用率，有小到大排序
	orderNodes := InstanceByDiskSlice(nodes)
	sort.Sort(orderNodes)
	var d90pInss, dThreeInss, dTwoInss, dOneInss []*Instance
	for _, ins := range orderNodes {
		diskPercent := ins.GetDiskUsedPercent()
		if diskPercent < float64(50.0) {
			dOneInss = append(dOneInss, ins)
		} else if diskPercent >= float64(50.0) && diskPercent < float64(70.0) {
			dTwoInss = append(dTwoInss, ins)
		} else {
			dThreeInss = append(dThreeInss, ins)
			if diskPercent > float64(90.0) {
				d90pInss = append(d90pInss, ins)
			}
		}
	}
	// 有节点磁盘占用超过90%触发迁移并告警
	if len(d90pInss) > 0 {
		message := "节点 "
		for _, ins := range d90pInss {
			message += fmt.Sprintf("%s ", ins.GetAddress())
		}
		message += "磁盘占用超过警戒值,触发分片迁移"
		log.Warn("%s", message)
		db.alarm.Alarm("磁盘空间不足", message)
		var targetInssGroup [][]*Instance
		targetInssGroup = append(targetInssGroup, dOneInss)
		targetInssGroup = append(targetInssGroup, dTwoInss)
		targetInssGroup = append(targetInssGroup, dThreeInss)
		task := db.schedule(d90pInss, targetInssGroup)
		if task != nil {
			return task
		}
	}

	if len(dThreeInss) > 0 && len(dOneInss) > 0 {
		var targetInssGroup [][]*Instance
		targetInssGroup = append(targetInssGroup, dOneInss)
		targetInssGroup = append(targetInssGroup, dTwoInss)
		task := db.schedule(dThreeInss, targetInssGroup)
		if task != nil {
			return task
		}
	}

	return nil
}

func (db *DiskBalanceScheduler) AllowSchedule() bool {
	if db.cluster.autoTransferUnable {
		return false
	}
	return true
}

func (db *DiskBalanceScheduler) GetInterval() time.Duration {
	return db.interval
}

func (db *DiskBalanceScheduler) Ctx() context.Context {
	return db.ctx
}

func (db *DiskBalanceScheduler) Stop() {
	db.cancel()
}

type RangeBalanceSeleter struct {
	// 磁盘占用率高的节点
	ins      *Instance
}

func NewRangeBalanceSeleter(n *Instance) Selecter {
	return &RangeBalanceSeleter{ins: n}
}

func (rb *RangeBalanceSeleter) SelectInstance(n int, fs ...Filter) ([]*Instance) {
	return nil
}

func (rb *RangeBalanceSeleter) SelectRange(n int, fs ...Filter) ([]*Range) {
	var ranges []*Range
	for _, r := range rb.ins.GetAllRanges() {
		find := true
		for _, f := range fs {
			if !f.FilterRange(r) {
				find = false
				break
			}
		}
		if find {
			ranges = append(ranges, r)
			if len(ranges) == n {
				return ranges
			}
		}
	}
	return nil
}

type RangeBalanceFilter struct {
	unExpectRanges     []*Range
	srcMac             *Machine
	dstMac             *Machine
	cluster            *Cluster
}

func NewRangeBalanceFilter(ranges []*Range, srcMac, dstMac *Machine, cluster *Cluster) Filter {
	return &RangeBalanceFilter{
		unExpectRanges: ranges,
		srcMac: srcMac,
		dstMac: dstMac,
		cluster: cluster}
}

func (rf *RangeBalanceFilter) FilterInstance(node *Instance) bool {
	return false
}

func (rf *RangeBalanceFilter)FilterRange(r *Range) bool {
	if !r.AllowSch(rf.cluster) {
		return false
	}
	// 因此要均衡磁盘空间，因此尽量选择空间占用大的分片
	if r.Stats.GetSize_() < DefaultMaxRangeSize / 2 {
		return false
	}
	// 检查目标分片是否在目标节点上有副本
	for _, unExp := range rf.unExpectRanges {
		if unExp.ID() == r.ID() {
			return false
		}
	}
	for _, peer := range r.GetPeers() {
		if ins, find := rf.cluster.FindInstance(peer.GetNodeId()); find {
			if mac, find := rf.cluster.FindMachine(ins.MacIp); find {
				// 允许副本在本交换机内网中迁移
				if rf.srcMac.GetSwitchIp() == mac.GetSwitchIp() {
					continue
				}
				// 不允许存在多个副本同在一个交换机内网中
				if rf.dstMac.GetSwitchIp() == mac.GetSwitchIp() {
					return false
				}
			}
		}
	}
	return true
}





