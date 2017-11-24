package server

import (
	"sort"
	"time"
	"math"

	"model/pkg/metapb"
	"util/log"
	"util/deepcopy"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
)

type RangesBalanceScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
}

func NewRangesBalanceScheduler(interval time.Duration, cluster *Cluster) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &RangesBalanceScheduler{
		name:     "ranges_balance_scheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster: cluster,
	}
}

func (rb *RangesBalanceScheduler) GetName() string {
	return rb.name
}

/**
1.判断当前所有节点的ranges数是否需要调整
2.需要调整:
3.查找冒尖的节点(downPeer),即需要减少ranges数的节点,从ranges数最高的节点开始
4.查找ranges将要change到的目标节点(upPeer),从ranges数最少的节点找起
5.change ranges from downPeer -> upPeer
6.目标节点的交换机不能同range的其他副本所在的交换机相同
**/
func (rb *RangesBalanceScheduler) Schedule() *Task {
	nodes := rb.cluster.GetAllActiveInstances()
	nodesSize := len(nodes)
	if nodesSize <= 1 {
		return nil
	}

	// 判断是否需要均衡
	if !rb.needBalance(nodes){
		return nil
	}

	log.Debug("ranges... needBalance... start...")
	avg := rb.getAvg(nodes)
	avg = avg*(1+MasterConf.SchRangesBalancePercent)

	// 按照ranges数量，从大到小排序
	orderNodes := InstanceByRangesSlice(nodes)
	sort.Sort(orderNodes)
	for i := 0; i < len(orderNodes); i++ {
		if !orderNodes[i].AllocSch() {
			log.Debug("rangesBalance[i] orderNodes[%d] %s.AllocSch() false",i,orderNodes[i].Address)
			continue
		}
		//小于浮动值30%的,不调整,当前及以后的节点肯定小于avg,所以break
		if float64(orderNodes[i].GetRangesNum()) < avg {
			break
		}
        iMac, find := rb.cluster.FindMachine(orderNodes[i].MacIp)
		if !find {
			log.Warn("machine %s not found", orderNodes[i].MacIp)
			return nil
		}
		for j := nodesSize - 1; j > i; j-- {
			if !orderNodes[j].AllocSch() {
				log.Debug("rangesBalance[j] orderNodes[%d] %s.AllocSch() false",j,orderNodes[j].Address)
				continue
			}
			//目标节点ranges数加1后,如超过avg,不允许ranges往该节点change
			if float64(orderNodes[j].GetRangesNum()+1) > avg {
				continue
			}
			//目标节点磁盘剩余不够10G,不允许往该节点迁移
			if orderNodes[j].Stats.DiskFree < MasterConf.NodeDiskMustFreeSize {
				continue
			}
			jMac, find := rb.cluster.FindMachine(orderNodes[j].MacIp)
			if !find {
				log.Warn("machine %s not found", orderNodes[j].MacIp)
				return nil
			}
			// 只能选择同机房的实例
			if jMac.GetRoom().GetRoomName() != iMac.GetRoom().GetRoomName() {
				continue
			}

			selecter := NewRangeBalanceSeleter(orderNodes[i])
			filter := NewRangeBalanceFilter(orderNodes[j].GetAllRanges(), iMac, jMac, rb.cluster)
			ranges := selecter.SelectRange(1, filter)

			// 未找到合适的分片，继续
			if len(ranges) == 0 {
				log.Debug("----------未找到合适的分片------------")
				continue
			}

			// 我们只选择了一个分片
			r := ranges[0]
			taskId, err := rb.cluster.GenTaskId()
			if err != nil {
				log.Debug("gen task ID failed, err[%v]", err)
				return nil
			}
			upPeer := &metapb.Peer{
				Id: r.ID(),
				NodeId: orderNodes[j].Node.GetId(),
			}
			downPeer := &metapb.Peer{
				Id: r.ID(),
				NodeId: orderNodes[i].Node.GetId(),
			}
			rng := deepcopy.Iface(r.Range).(*metapb.Range)
			t := &taskpb.Task{
				Type:   taskpb.TaskType_RangeTransfer,
				Meta:   &taskpb.TaskMeta{
					TaskId:   taskId,
					CreateTime: time.Now().Unix(),
					State: taskpb.TaskState_TaskWaiting,
					Timeout: DefaultTransferTaskTimeout,
				},
				RangeTransfer: &taskpb.TaskRangeTransfer{
					Range:    rng,
					UpPeer:   upPeer,
					DownPeer: downPeer,
				},
			}
			task := NewTask(t)

			log.Info("transfer ranges from[address:%s->rangesNum:%d] to[address:%s->rangesNum:%d] avg->%f, i:%d, j:%d]",
				orderNodes[i].Address,
				orderNodes[i].GetRangesNum(),
				orderNodes[j].Address,
				orderNodes[j].GetRangesNum(),avg,i,j)
			log.Debug("ranges blance range[%s] from [%s] to [%s]", r.SString(), orderNodes[i].Address, orderNodes[j].Address)

			// 修改range的ranges，模拟源节点ranges数减1,目标节点ranges数加1
			orderNodes[i].RangeCount -= 1
			orderNodes[j].RangeCount += 1
			log.Debug("...ranges balance end...")
			return task
		}
	}
	return nil
}

/**
判断当前所有节点的ranges数是否需要调整,举例说明:
n1 n2 n3 n4 n5
50 0  0  30 20
一共5个节点,各个节点ranges数分别为50 0 0 30 20

1.求平均值,即最优部署,公式: (n1.rangesNum+...+最后节点.rangesNum)/总节点数
(50+0+0+30+20)/5=20
avg=20

2.求方差,(各个节点ranges数减去avg的平方和)/总节点数,公式: ( [(n1.rangesNum-avg)的平方] + ... + [(最后节点.rangesNum-avg)的平方] ) / 总节点数
((50-20)的平方 + (0-20)的平方 + (0-20)的平方 + (30-20)的平方 + (20-20)的平方) / 5
(900+400+400+100+0)/5=360

3.求容忍的(非最优解)的水平线方差,最优部署值上浮30%
avg = avg + avg*30%
avg = 20 + 20*0.3 = 26
能容忍的水平线的方差:
( (26-20)的平方 + (26-20)的平方 + (26-20)的平方 + (26-20)的平方 + (26-20)的平方 )/5
(36+36+36+36+36)/5=36
演算后即: (avg*0.3)的平方->(avg*0.3)*(avg*0.3)

4.(2.结果>3.结果),需要调整
360 > 36
 */
func (rb *RangesBalanceScheduler) needBalance(nodes []*Instance) bool{
	avg := rb.getAvg(nodes)

	diffSum := 0.0
	for _,node := range nodes {
		diff := float64(node.GetRangesNum()) - avg
		diffSum += (diff*diff)
	}

	fangcha := diffSum / float64(len(nodes))
	fangcha = math.Trunc(fangcha*100)/100.0
	allowFangCha := (avg * MasterConf.SchRangesBalancePercent) * (avg * MasterConf.SchRangesBalancePercent)
	//log.Debug("RangesBalance fangcha %f allowFangCha %f:",fangcha, allowFangCha)
	return fangcha > allowFangCha
}

func (rb *RangesBalanceScheduler) sumRangesNum(nodes []*Instance) (sumRangesNum int){
	for _,node := range nodes {
		sumRangesNum += node.GetRangesNum()
	}
	return
}

func (rb *RangesBalanceScheduler) getAvg(nodes []*Instance) (avg float64){
	floatSum := float64(rb.sumRangesNum(nodes))
	floatNum := float64(len(nodes))
	avg = floatSum/ floatNum
	avg = math.Trunc(avg*100)/100
	return
}

func (rb *RangesBalanceScheduler) AllowSchedule() bool {
	if rb.cluster.autoTransferUnable {
		return false
	}
	return true
}

func (rb *RangesBalanceScheduler) GetInterval() time.Duration {
	return rb.interval
}

func (rb *RangesBalanceScheduler) Ctx() context.Context {
	return rb.ctx
}

func (rb *RangesBalanceScheduler) Stop() {
	rb.cancel()
}


