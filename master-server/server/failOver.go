package server

import (
	"time"
	"fmt"

	"util/log"
	"model/pkg/metapb"
	"util/deepcopy"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
)

// 根据上报的
type FailoverScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
	alarm            Alarm
}

func NewFailoverScheduler(interval time.Duration, cluster *Cluster, alarm Alarm) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &FailoverScheduler{
		name: "failover_scheduler",
		ctx: ctx,
		cancel: cancel,
		interval: interval,
		cluster: cluster,
	    alarm: alarm}
}

func (f *FailoverScheduler) GetName() string {
	return f.name
}

func (f *FailoverScheduler) Schedule() *Task {
	// check node failover
	for _, n := range f.cluster.GetAllInstances() {
		if n.IsFault() {
			continue
		}
		downCount := 0
		ranges := n.GetAllRanges()
		num := len(ranges)
		for _, r := range ranges {
			for _, d := range r.GetDownPeers() {
				if d.GetPeer().GetNodeId() == n.ID() && d.GetDownSeconds() > DefaultDownTimeLimit {
					downCount++
				}
			}
		}
		if (downCount * 2 > num || num == 0) && !n.LastHbTime.IsZero() && uint64(time.Since(n.LastHbTime).Seconds()) > DefaultDownTimeLimit {
			err := f.cluster.UpdateNodeState(n, metapb.NodeState_N_Tombstone)
			if err != nil {
				log.Error("update node[%s] state failed, err[%v]", n.GetAddress(), err)
				return nil
			}
			log.Warn("node[%s] tombstone, we need failover", n.Address)
		}
	}
	for _, r := range f.cluster.GetAllRanges() {
		select {
		case <-f.ctx.Done():
			return nil
		default:
		}
		for _, p := range r.GetPeers() {
			node, find := f.cluster.FindInstance(p.GetNodeId())
			if !find {
				log.Error("node[%d] not foud", p.GetNodeId())
				continue
			}
			if node.IsFault() {
				t := FailOverSchedule(p, r, f.cluster, f.alarm)
				if t != nil {
					return t
				}
				break
			}
		}
	}
	return nil
}

func (f *FailoverScheduler) AllowSchedule() bool {
	if f.cluster.autoFailoverUnable {
		return false
	}
	return true
}

func (f *FailoverScheduler) GetInterval() time.Duration {
	return f.interval
}

func (f *FailoverScheduler) Ctx() context.Context {
	return f.ctx
}

func (f *FailoverScheduler) Stop() {
	f.cancel()
}

// failOver任务，需要先删除down的节点，然后再添加新的节点，这样才能确保新的写入可以成功
func FailOverSchedule(down *metapb.Peer, r *Range, cluster *Cluster, alarm Alarm) *Task {
	if !r.AllowFailoverSch(cluster) {
		log.Debug("donot allowfailover: %v", *r)
		return nil
	}
	downNode, find := cluster.FindInstance(down.GetNodeId())
	if !find {
		log.Error("node[%d] not found", down.GetNodeId())
		return nil
	}
	var selecter Selecter
	var filter Filter
	if cluster.conf.Mode == "debug" || cluster.deploy == nil {
		selecter = NewDebugTransferSelect(cluster.GetAllActiveInstances())
		var except []*Instance
		for _, p := range r.Peers {
			n, find := cluster.FindInstance(p.NodeId)
			if !find {
				log.Error("invalid node[%d]", p.NodeId)
				return nil
			}
			except = append(except, n)
		}
		filter = NewDebugTransferFilter(except)
	} else {
		selecter = NewTransferSelect(cluster.GetAllZones(), cluster.deploy)
		// expect room
		ins, find := cluster.FindInstance(down.GetNodeId())
		if !find {
			log.Error("instance[%d] not exist", down.GetNodeId())
			return nil
		}
		mac, find := cluster.FindMachine(ins.MacIp)
		if !find {
			log.Error("machine[%s] not exist", ins.MacIp)
			return nil
		}
		zone, find := cluster.FindZone(mac.GetZone().GetZoneName())
		if !find {
			log.Error("zone[%s] not exist", mac.GetZone().GetZoneName())
			return nil
		}
		room, find := zone.FindRoom(mac.GetRoom().GetRoomName())
		if !find {
			log.Error("room[%s:%s] not exist", mac.GetZone().GetZoneName(), mac.GetRoom().GetRoomName())
			return nil
		}
		var switchsMap, macsMap map[string]struct{}
		switchsMap = make(map[string]struct{})
		macsMap = make(map[string]struct{})
		for _, p := range r.Peers {
			n, find := cluster.FindInstance(p.NodeId)
			if !find {
				log.Error("invalid node[%d]", p.NodeId)
				return nil
			}
			mac, find := cluster.FindMachine(n.MacIp)
			if !find {
				log.Error("machine[%s] not exist", ins.MacIp)
				return nil
			}
			macsMap[mac.GetIp()] = struct {}{}
			switchsMap[mac.GetSwitchIp()] = struct {}{}
		}
		filter = NewTransferFilter(cluster, room, switchsMap, macsMap)
	}

	nodes := selecter.SelectInstance(1, filter)
	if len(nodes) == 0 {
		var nodesInfo string
		for _, node := range cluster.GetAllActiveInstances() {
			nodesInfo += fmt.Sprintf(`{ node[%s] state[%s] tasks num[%d] free disk[%d] }`,
				node.GetAddress(), node.GetState().String(), len(node.AllTask()), node.GetDiskFree())
		}
		log.Warn("schedule select node resource failed, all nodes info[%s]", nodesInfo)
		//message := fmt.Sprintf("分片[%s] 故障副本[%s], 没有可用的节点资源", r.SString(), downNode.GetAddress())
		//alarm.Alarm("分片故障恢复异常", message)
		return nil
	}
	taskId, err := cluster.GenTaskId()
	if err != nil {
		log.Error("gen task ID failed, err[%v]", err)
		return nil
	}
	upPeer := &metapb.Peer{
		Id:     r.ID(),
		NodeId:   nodes[0].GetId(),
	}
	downPeer := &metapb.Peer{
		Id:     r.ID(),
		NodeId:   down.GetNodeId(),
	}
	rng := deepcopy.Iface(r.Range).(*metapb.Range)
	t := &taskpb.Task{
		Type:   taskpb.TaskType_RangeFailOver,
		Meta:   &taskpb.TaskMeta{
			TaskId:   taskId,
			CreateTime: time.Now().Unix(),
			State: taskpb.TaskState_TaskWaiting,
			Timeout: DefaultFailOverTaskTimeout,
		},
		RangeFailover: &taskpb.TaskRangeFailover{
			Range:      rng,
			UpPeer:     upPeer,
			DownPeer:   downPeer,
		},
	}
	task := NewTask(t)
	task.GenSubTasks(cluster)
	log.Info("range[%s] faileover, transfer from [%s] to [%s]",
		r.SString(), downNode.GetAddress(), nodes[0].GetAddress())

	return task
}
