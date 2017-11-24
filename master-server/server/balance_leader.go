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

type LeaderBalanceScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
}

func NewLeaderBalanceScheduler(interval time.Duration, cluster *Cluster) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &LeaderBalanceScheduler{
		name:     "leader_balance_scheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster: cluster,
	}
}

func (db *LeaderBalanceScheduler) GetName() string {
	return db.name
}

/**
1.判断当前所有节点的leader数是否需要调整
2.需要调整:
3.查找冒尖的节点(downPeer),即需要减少leader数的节点,从leader数最高的节点开始
4.查找leader将要change到的目标节点(upPeer),从leader数最少的节点找起
5.change leader from downPeer -> upPeer
 */
func (db *LeaderBalanceScheduler) Schedule() *Task {
	nodes := db.cluster.GetAllActiveInstances()
	nodesSize := len(nodes)
	if nodesSize <= 1 {
		return nil
	}

	// leader总数小于5,不调整
	if db.sumLeaderNum(nodes) <= 5{
		return nil
	}
	// 判断是否需要均衡
	if !db.needBalance(nodes){
		return nil
	}

	log.Debug("needBalance... start...")
	avg := db.getAvg(nodes)
	avg = avg*(1+MasterConf.SchLeaderBalancePercent)

	// 按照leader数量，从大到小排序
	orderNodes := InstanceByLeaderSlice(nodes)
	sort.Sort(orderNodes)
	for i := 0; i < len(orderNodes); i++ {
		if !orderNodes[i].AllocSch() {
			log.Debug("[i] orderNodes[%d] %s.AllocSch() false",i,orderNodes[i].Address)
			continue
		}
		//小于浮动值30%的,不调整,当前及以后的节点肯定小于avg,所以break
		if float64(orderNodes[i].GetLeaderNum()) < avg {
			break
		}

		for j := nodesSize - 1; j > i; j-- {
			if !orderNodes[j].AllocSch() {
				log.Debug("[j] orderNodes[%d] %s.AllocSch() false",j,orderNodes[j].Address)
				continue
			}
			//目标节点leader数加1后,如超过avg,不允许leader往该节点change
			if float64(orderNodes[j].GetLeaderNum()+1) > avg {
				continue
			}

			selecter := NewLeaderBalanceSeleter(orderNodes[j])
			filter := NewLeaderBalanceFilter(orderNodes[i].GetLeaderRanges(), db.cluster)
			ranges := selecter.SelectRange(1, filter)

			// 未找到合适的分片，继续
			if len(ranges) == 0 {
				log.Debug("----------未找到合适的分片------------")
				continue
			}

			// 我们只选择了一个分片
			r := ranges[0]
			taskId, err := db.cluster.GenTaskId()
			if err != nil {
				log.Debug("gen task ID failed, err[%v]", err)
				return nil
			}
			expLeader := &metapb.Peer{
				Id: r.ID(),
				NodeId: orderNodes[j].Node.GetId(),
			}
			preLeader := &metapb.Peer{
				Id: r.ID(),
				NodeId: orderNodes[i].Node.GetId(),
			}
			rng := deepcopy.Iface(r.Range).(*metapb.Range)
			t := &taskpb.Task{
				Type:   taskpb.TaskType_RangeLeaderTransfer,
				Meta:   &taskpb.TaskMeta{
					TaskId:   taskId,
					CreateTime: time.Now().Unix(),
					State: taskpb.TaskState_TaskWaiting,
					Timeout: DefaultChangeLeaderTaskTimeout,
				},
				RangeLeaderTransfer: &taskpb.TaskRangeLeaderTransfer{
					Range:      rng,
					ExpLeader:  expLeader,
					PreLeader:  preLeader,
				},
			}
			task := NewTask(t)

			log.Info("change leader from[address:%s->leaderNum:%d] to[address:%s->leaderNum:%d] avg->%f, i:%d, j:%d]",
				orderNodes[i].Address,
				orderNodes[i].GetLeaderNum(),
				orderNodes[j].Address,
				orderNodes[j].GetLeaderNum(),avg,i,j)
			log.Info("leader blance range[%s] from [%s] to [%s]",
				r.SString(), orderNodes[i].Node.GetAddress(), orderNodes[j].Node.GetAddress())

			// 修改range的leader，模拟源节点leader数减1,目标节点leader数加1
			orderNodes[i].RangeLeaderCount -= 1
			orderNodes[j].RangeLeaderCount += 1
			log.Debug("...balance end...")
			return task
		}
	}
	return nil
}

/**
判断当前所有节点的leader数是否需要调整,举例说明:
n1 n2 n3 n4 n5
50 0  0  30 20
一共5个节点,各个节点leader数分别为50 0 0 30 20

1.求平均值,即最优部署,公式: (n1.leaderNum+...+最后节点.leaderNum)/总节点数
(50+0+0+30+20)/5=20
avg=20

2.求方差,(各个节点leader数减去avg的平方和)/总节点数,公式: ( [(n1.leaderNum-avg)的平方] + ... + [(最后节点.leaderNum-avg)的平方] ) / 总节点数
((50-20)的平方 + (0-20)的平方 + (0-20)的平方 + (30-20)的平方 + (20-20)的平方) / 5
(900+400+400+100+0)/5=360

3.求容忍的(非最优解)的水平线方差,最优部署值上浮30%
avg = avg + avg*30%
avg = 20 + 20*0.3 = 26
能容忍的水平线的方差:
( (26-20)的平方 + (26-20)的平方 + (26-20)的平方 + (26-20)的平方 + (26-20)的平方 )/5
(36+36+36+36+36)/5=36
演算后即: (avg*0.3)的平方->(avg*0.3)*(avg*0.3)

4.(b结果>c结果),需要调整
360 > 36
 */
func (db *LeaderBalanceScheduler) needBalance(nodes []*Instance) bool{
	avg := db.getAvg(nodes)

	diffSum := 0.0
	for _,node := range nodes {
		diff := float64(node.GetLeaderNum()) - avg
		diffSum += (diff*diff)
	}

	fangcha := diffSum / float64(len(nodes))
	fangcha = math.Trunc(fangcha*100)/100.0
	allowFangCha := (avg * MasterConf.SchLeaderBalancePercent) * (avg * MasterConf.SchLeaderBalancePercent)
	log.Debug("fangcha %f allowFangCha %f:",fangcha, allowFangCha)
	return fangcha > allowFangCha
}

func (db *LeaderBalanceScheduler) sumLeaderNum(nodes []*Instance) (sumLeaderNum int){
	for _,node := range nodes {
		sumLeaderNum += node.GetLeaderNum()
	}
	return
}

func (db *LeaderBalanceScheduler) getAvg(nodes []*Instance) (avg float64){
	floatSum := float64(db.sumLeaderNum(nodes))
	floatNum := float64(len(nodes))
	avg = floatSum/ floatNum
	avg = math.Trunc(avg*100)/100
	return
}

func (db *LeaderBalanceScheduler) AllowSchedule() bool {
	if db.cluster.autoTransferUnable {
		return false
	}
	return true
}

func (db *LeaderBalanceScheduler) GetInterval() time.Duration {
	return db.interval
}

func (db *LeaderBalanceScheduler) Ctx() context.Context {
	return db.ctx
}

func (db *LeaderBalanceScheduler) Stop() {
	db.cancel()
}

type LeaderBalanceSeleter struct {
	// 磁盘占用率高的节点
	node      *Instance
}

func NewLeaderBalanceSeleter(n *Instance) Selecter {
	return &LeaderBalanceSeleter{node: n}
}

func (s *LeaderBalanceSeleter) SelectInstance(n int, fs ...Filter) ([]*Instance) {
	return nil
}

func (s *LeaderBalanceSeleter) SelectRange(n int, fs ...Filter) ([]*Range) {
	log.Debug("SelectRange start...")
	var ranges []*Range
	for _, r := range s.node.GetSlaveRanges() {
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

type LeaderBalanceFilter struct {
	expectRanges []*Range
	cluster      *Cluster
}

func NewLeaderBalanceFilter(ranges []*Range, cluster *Cluster) Filter {
	return &LeaderBalanceFilter{expectRanges: ranges, cluster: cluster}
}

func (f *LeaderBalanceFilter) FilterInstance(node *Instance) bool {
	return false
}

func (f *LeaderBalanceFilter)FilterRange(r *Range) bool {
	if !r.AllowSch(f.cluster) {
		log.Debug("range not allow sch %d",r.Id)
		return false
	}

	for _, rng := range f.expectRanges {
		if rng.ID() == r.ID() {
			//在期望列表中找到分片id,过滤成功
			log.Debug("find range success %d %d",rng.ID(), r.ID())
			return true
		}
	}
	return false
}


