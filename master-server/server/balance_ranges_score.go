package server

import (
	"sort"
	"math"
	"time"

	"model/pkg/metapb"
	"util/log"
	"util/deepcopy"
	"golang.org/x/net/context"
	"model/pkg/taskpb"
)

type RangeBalanceByNodeScoreScheduler struct {
	name             string
	ctx              context.Context
	cancel           context.CancelFunc
	interval         time.Duration
	cluster          *Cluster
}

func NewRangeBalanceByNodeScoreScheduler(interval time.Duration, cluster *Cluster) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &RangeBalanceByNodeScoreScheduler{
		name:     "ranges_balance_by_node_score_scheduler",
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
		cluster: cluster,
	}
}

func (db *RangeBalanceByNodeScoreScheduler) GetName() string {
	return db.name
}

/**
1.判断当前所有节点的副本是否需要调整
2.需要调整:
3.查找冒尖的节点(downPeer),即需要减少副本数的节点,从副本数最高的节点开始
4.查找将要迁移到的目标节点(upPeer),从副本数最少的节点找起
5.迁移副本 from downPeer -> upPeer
 */
func (db *RangeBalanceByNodeScoreScheduler) Schedule() *Task {
	nodes := db.cluster.GetAllActiveInstances()
	nodesSize := len(nodes)
	if nodesSize <= 1 {
		return nil
	}

	// 判断是否需要均衡
	if !db.needBalance(nodes){
		return nil
	}

	log.Debug("RangeBalanceByNodeScore... needBalance... start...")
	avg := db.getAvg(nodes)
	avg = avg*(1+MasterConf.SchRangesBalancePercent)

	// 按照range分数，从大到小排序
	orderNodes := InstanceByScoreSlice(nodes)
	sort.Sort(orderNodes)
	for i := 0; i < len(orderNodes); i++ {
		if !orderNodes[i].AllocSch() {
			log.Debug("[i] orderNodes[%d] %s.AllocSch() false (range)",i,orderNodes[i].Address)
			continue
		}
		//小于浮动值30%的,不调整,当前及以后的节点肯定小于avg,所以break
		if float64(orderNodes[i].GetScore()) < avg {
			break
		}

		for j := nodesSize - 1; j > i; j-- {
			if !orderNodes[j].AllocSch() {
				log.Debug("[j] orderNodes[%d] %s.AllocSch() false (range)",j,orderNodes[j].Address)
				continue
			}

			//目标节点磁盘剩余不够10G,不允许往该节点迁移
			if orderNodes[j].Stats.DiskFree < MasterConf.NodeDiskMustFreeSize {
				continue
			}

			selecter := NewRangeBalanceByNodeScoreSeleter(orderNodes[i])
			filter := NewRangeBalanceByNodeScoreFilter(orderNodes[j].GetAllRanges(), db.cluster)
			ranges := selecter.SelectRange(1, filter)

			// 未找到合适的分片，继续
			if len(ranges) == 0 {
				log.Debug("---1111-------未找到合适的分片------------")
				continue
			}

			// 我们只选择了一个分片
			r := ranges[0]

			//目标节点分数 + 副本分数,如超过avg,不允许往该节点迁移
			if float64(orderNodes[j].GetScore() + r.GetScore()) > avg {
				continue
			}

			taskId, err := db.cluster.GenTaskId()
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

			log.Info("range transfer from[address:%s->rangeNum:%d] to[address:%s->rangeNum:%d] avg->%f, i:%d, j:%d]",
				orderNodes[i].Address,
				orderNodes[i].GetRangesNum(),
				orderNodes[j].Address,
				orderNodes[j].GetRangesNum(),avg,i,j)
			log.Info("range transfer from[address:%s->nodeScore:%f] to[address:%s->nodeScore:%f] avg->%f, i:%d, j:%d]",
				orderNodes[i].Address,
				orderNodes[i].GetScore(),
				orderNodes[j].Address,
				orderNodes[j].GetScore(),avg,i,j)
			log.Debug("range blance range[%s:%s:%d] from [%v] to [%v]",
				r.GetDbName(), r.GetTableName(), r.GetId())

			// 修改range的range，模拟源节点range数减1,目标节点range数加1
			orderNodes[i].RangeCount -= 1
			orderNodes[j].RangeCount += 1
			orderNodes[i].Score -= r.GetScore()
			orderNodes[j].Score += r.GetScore()
			log.Debug("...range balance end...")
			return task
		}
	}
	return nil
}

/**
加入node分数后的方案：
判断当前所有节点的range数是否需要调整,举例说明:
n1 n2 n3 n4 n5
50 0  0  30 20
一共5个节点,各个节点node分数分别为50 0 0 30 20

1.求平均值,即最优部署,公式: (node1.score+...+最后node.score)/总节点数
(50+0+0+30+20)/5=100/5=20
avg=20

2.求方差,(各个节点node分数减去avg的平方和)/总节点数,公式: ( [(node1.score-avg)的平方] + ... + [(最后node.score-avg)的平方] ) / 总节点数
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
func (db *RangeBalanceByNodeScoreScheduler) needBalance(nodes []*Instance) bool{
	avg := db.getAvg(nodes)

	diffSum := 0.0
	for _,node := range nodes {
		diff := float64(node.GetScore()) - avg
		diffSum += (diff*diff)
	}

	fangcha := diffSum / float64(len(nodes))
	fangcha = math.Trunc(fangcha*100)/100.0
	allowFangCha := (avg * MasterConf.SchRangesBalancePercent) * (avg * MasterConf.SchRangesBalancePercent)
	log.Debug("range fangcha %f allowFangCha %f:",fangcha, allowFangCha)
	return fangcha > allowFangCha
}

func (db *RangeBalanceByNodeScoreScheduler) sumNodeScore(nodes []*Instance) (totalScore float64){
	for _,node := range nodes {
		totalScore += node.GetScore()
	}
	return
}

func (db *RangeBalanceByNodeScoreScheduler) getAvg(nodes []*Instance) (avg float64){
	totalScore := float64(db.sumNodeScore(nodes))
	floatNum := float64(len(nodes))
	avg = totalScore/ floatNum
	avg = math.Trunc(avg*100)/100
	return
}

func (db *RangeBalanceByNodeScoreScheduler) AllowSchedule() bool {
	if db.cluster.autoTransferUnable {
		return false
	}
	return true
}

func (db *RangeBalanceByNodeScoreScheduler) GetInterval() time.Duration {
	return db.interval
}

func (db *RangeBalanceByNodeScoreScheduler) Ctx() context.Context {
	return db.ctx
}

func (db *RangeBalanceByNodeScoreScheduler) Stop() {
	db.cancel()
}

type RangeBalanceByNodeScoreSeleter struct {
	node      *Instance
}

func NewRangeBalanceByNodeScoreSeleter(n *Instance) Selecter {
	return &RangeBalanceByNodeScoreSeleter{node: n}
}

func (s *RangeBalanceByNodeScoreSeleter) SelectInstance(n int, fs ...Filter) ([]*Instance) {
	return nil
}

func (s *RangeBalanceByNodeScoreSeleter) SelectRange(n int, fs ...Filter) ([]*Range) {
	log.Debug("range SelectRange start...")
	var ranges []*Range
	for _, r := range s.node.GetSlaveRanges() {
		find := false
		for _, f := range fs {
			//在非期望列表中找到分片id,过滤失败,返回false
			if !f.FilterRange(r) {
				find = true
				break
			}
		}
		//源端副本range在目标node上没有副本
		if !find {
			ranges = append(ranges, r)
			if len(ranges) == n {
				return ranges
			}
		}
	}
	return nil
}

type RangeBalanceByNodeScoreFilter struct {
	unExpectRanges []*Range
	cluster        *Cluster
}

func NewRangeBalanceByNodeScoreFilter(ranges []*Range, cluster *Cluster) Filter {
	return &RangeBalanceByNodeScoreFilter{unExpectRanges: ranges, cluster: cluster}
}

func (f *RangeBalanceByNodeScoreFilter) FilterInstance(node *Instance) bool {
	return false
}

func (f *RangeBalanceByNodeScoreFilter)FilterRange(r *Range) bool {
	if !r.AllowSch(f.cluster) {
		log.Debug("range range not allow sch %d",r.Id)
		return false
	}

	for _, rng := range f.unExpectRanges {
		if rng.ID() == r.ID() {
			//在非期望列表中找到分片id,过滤失败
			return false
		}
	}
	log.Debug("range FilterRange success %d", r.ID())
	return true
}


