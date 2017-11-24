package server
//
//import (
//	"sort"
//	"time"
//	"math"
//
//	"model/pkg/metapb"
//	"util/log"
//	"util/deepcopy"
//	"golang.org/x/net/context"
//	"model/pkg/taskpb"
//)
//
//type LeaderScoreBalanceScheduler struct {
//	name             string
//	ctx              context.Context
//	cancel           context.CancelFunc
//	interval         time.Duration
//	cluster          *Cluster
//}
//
//func NewLeaderScoreBalanceScheduler(interval time.Duration, cluster *Cluster) Scheduler {
//	ctx, cancel := context.WithCancel(context.Background())
//	return &LeaderScoreBalanceScheduler{
//		name:     "leader_score_balance_scheduler",
//		ctx:      ctx,
//		cancel:   cancel,
//		interval: interval,
//		cluster: cluster,
//	}
//}
//
//func (db *LeaderScoreBalanceScheduler) GetName() string {
//	return db.name
//}
//
///**
//1.判断当前所有节点的leader数是否需要调整
//2.需要调整:
//3.查找冒尖的节点(downPeer),即需要减少leader分数的节点,从leader分数最高的节点开始
//4.查找leader将要change到的目标节点(upPeer),从leader分数最少的节点找起
//5.change leader from downPeer -> upPeer
// */
//func (db *LeaderScoreBalanceScheduler) Schedule() *Task {
//	nodes := db.cluster.GetAllActiveInstances()
//	nodesSize := len(nodes)
//	if nodesSize <= 1 {
//		return nil
//	}
//
//	// leader总数小于5,不调整
//	if db.sumLeaderNum(nodes) <= 5{
//		return nil
//	}
//	// 判断是否需要均衡
//	if !db.needBalance(nodes){
//		return nil
//	}
//
//	log.Debug("LeaderBalanceByScore... needBalance... start...")
//	avg := db.getAvg(nodes)
//	avg = avg*(1+MasterConf.SchLeaderBalancePercent)
//
//	// 按照leader分数，从大到小排序
//	orderNodes := InstanceByLeaderTotalScoreOfNodeSlice(nodes)
//	sort.Sort(orderNodes)
//	for i := 0; i < len(orderNodes); i++ {
//		if !orderNodes[i].AllocSch() {
//			log.Debug("[i] orderNodes[%d] %s.AllocSch() false",i,orderNodes[i].Address)
//			continue
//		}
//		//小于浮动值30%的,不调整,当前及以后的节点肯定小于avg,所以break
//		if float64(orderNodes[i].GetTotalLeaderScore()) < avg {
//			break
//		}
//
//		for j := nodesSize - 1; j > i; j-- {
//			if !orderNodes[j].AllocSch() {
//				log.Debug("[j] orderNodes[%d] %s.AllocSch() false",j,orderNodes[j].Address)
//				continue
//			}
//
//			selecter := NewLeaderScoreBalanceSeleter(orderNodes[j])
//			filter := NewLeaderScoreBalanceFilter(orderNodes[i].GetLeaderRanges(), db.cluster)
//			ranges := selecter.SelectRange(1, filter)
//
//			// 未找到合适的分片，继续
//			if len(ranges) == 0 {
//				log.Debug("----------未找到合适的分片------------")
//				continue
//			}
//
//			// 我们只选择了一个分片
//			r := ranges[0]
//			//源端分片leader分数
//			leaderRng := orderNodes[i].ranges.ranges[r.ID()]
//			//目标节点分数 - 副本分数 + leader分数,如超过avg,不允许leader往该节点change
//			if float64(orderNodes[j].GetScore()- r.GetScore() + leaderRng.GetScore()) > avg {
//				continue
//			}
//			taskId, err := db.cluster.GenTaskId()
//			if err != nil {
//				log.Debug("gen task ID failed, err[%v]", err)
//				return nil
//			}
//			upPeer := &metapb.Peer{
//				Id: r.ID(),
//				NodeId: orderNodes[j].GetId(),
//			}
//			downPeer := &metapb.Peer{
//				Id: r.ID(),
//				NodeId: orderNodes[i].GetId(),
//			}
//			rng := deepcopy.Iface(r.Range).(*metapb.Range)
//			t := &taskpb.Task{
//				Type:   taskpb.TaskType_RangeLeaderTransfer,
//				Meta:   &taskpb.TaskMeta{
//					TaskId:   taskId,
//					CreateTime: time.Now().Unix(),
//					State: taskpb.TaskState_TaskWaiting,
//					Timeout: DefaultChangeLeaderTaskTimeout,
//				},
//				RangeLeaderTransfer: &taskpb.TaskRangeLeaderTransfer{
//					Range:      rng,
//					ExpLeader:   upPeer,
//					PreLeader: downPeer,
//				},
//			}
//			task := NewTask(t)
//
//			log.Info("change leader by score from[address:%s->leaderNum:%d] to[address:%s->leaderNum:%d] avg->%f, i:%d, j:%d]",
//				orderNodes[i].Address,
//				orderNodes[i].GetLeaderNum(),
//				orderNodes[j].Address,
//				orderNodes[j].GetLeaderNum(),avg,i,j)
//			log.Debug("leader blance range[%s:%s:%d] from [%v] to [%v]",
//				r.GetDbName(), r.GetTableName(), r.GetId())
//
//			// 源节点leader数减1,目标节点leader数加1
//			orderNodes[i].RangeLeaderCount -= 1
//			orderNodes[j].RangeLeaderCount += 1
//			orderNodes[i].Score -= (leaderRng.GetScore()-r.GetScore())
//			orderNodes[j].Score += (leaderRng.GetScore()-r.GetScore())
//			orderNodes[i].TotalLeaderScore -= leaderRng.GetScore()
//			orderNodes[j].TotalLeaderScore += leaderRng.GetScore()
//			log.Debug("...balance end...")
//			return task
//		}
//	}
//	return nil
//}
//
///**
//加入分数后的方案：
//判断当前所有节点的leader数是否需要调整,举例说明:
//n1 n2 n3 n4 n5
//50 0  0  30 20
//一共5个节点,各个节点leader数分别为50 0 0 30 20
//为方便说明,假设副本分数统一为10(线上正常情况下,副本分数都不一样)
//50*10 0*10 0*10 30*10 20*10
//500   0    0    300   200
//
//1.求平均值,即最优部署,公式: (leader1.score+...+最后leader.score)/总节点数
//(500+0+0+300+200)/5=1000/5=200
//avg=200
//
//2.求方差,(各个节点leader分数减去avg的平方和)/总节点数,公式: ( [(leader1.score-avg)的平方] + ... + [(最后leader.score-avg)的平方] ) / 总节点数
//((500-200)的平方 + (0-200)的平方 + (0-200)的平方 + (300-200)的平方 + (200-200)的平方) / 5
//(90000+40000+40000+10000+0)/5=36000
//
//3.求容忍的(非最优解)的水平线方差,最优部署值上浮30%
//avg = avg + avg*30%
//avg = 200 + 200*0.3 = 260
//能容忍的水平线的方差:
//( (260-200)的平方 + (260-200)的平方 + (260-200)的平方 + (260-200)的平方 + (260-200)的平方 )/5
//(3600+3600+3600+3600+3600)/5=3600
//演算后即: (avg*0.3)的平方->(avg*0.3)*(avg*0.3)
//
//4.(2.结果>3.结果),需要调整
//36000 > 3600
// */
//func (db *LeaderScoreBalanceScheduler) needBalance(nodes []*Instance) bool{
//	avg := db.getAvg(nodes)
//
//	diffSum := 0.0
//	for _,node := range nodes {
//		diff := node.GetTotalLeaderScore() - avg
//		diffSum += (diff*diff)
//	}
//
//	fangcha := diffSum / float64(len(nodes))
//	fangcha = math.Trunc(fangcha*100)/100.0
//	allowFangCha := (avg * MasterConf.SchLeaderBalancePercent) * (avg * MasterConf.SchLeaderBalancePercent)
//	log.Debug("fangcha %f allowFangCha %f:",fangcha, allowFangCha)
//	return fangcha > allowFangCha
//}
//
//func (db *LeaderScoreBalanceScheduler) sumLeaderNum(nodes []*Instance) (sumLeaderNum int){
//	for _,node := range nodes {
//		sumLeaderNum += node.GetLeaderNum()
//	}
//	return
//}
//
//func (db *LeaderScoreBalanceScheduler) sumAllRngsOfNodeScore(nodes []*Instance) (totalScore float64){
//	for _,node := range nodes {
//		totalScore += node.GetTotalLeaderScore()
//	}
//	return
//}
//
//func (db *LeaderScoreBalanceScheduler) getAvg(nodes []*Instance) (avg float64){
//	totalScore := db.sumAllRngsOfNodeScore(nodes)
//	floatNum := float64(len(nodes))
//	avg = totalScore/ floatNum
//	avg = math.Trunc(avg*100)/100
//	return
//}
//
//func (db *LeaderScoreBalanceScheduler) AllowSchedule() bool {
//	if db.cluster.autoTransferUnable {
//		return false
//	}
//	return true
//}
//
//func (db *LeaderScoreBalanceScheduler) GetInterval() time.Duration {
//	return db.interval
//}
//
//func (db *LeaderScoreBalanceScheduler) Ctx() context.Context {
//	return db.ctx
//}
//
//func (db *LeaderScoreBalanceScheduler) Stop() {
//	db.cancel()
//}
//
//type LeaderScoreBalanceSeleter struct {
//	node      *Instance
//}
//
//func NewLeaderScoreBalanceSeleter(n *Instance) Selecter {
//	return &LeaderScoreBalanceSeleter{node: n}
//}
//
//func (s *LeaderScoreBalanceSeleter) SelectInstance(n int, fs ...Filter) ([]*Instance) {
//	return nil
//}
//
//func (s *LeaderScoreBalanceSeleter) SelectRange(n int, fs ...Filter) ([]*Range) {
//	log.Debug("SelectRange start...")
//	var ranges []*Range
//	for _, r := range s.node.GetSlaveRanges() {
//		find := true
//		for _, f := range fs {
//			//在期望列表中找到分片id,过滤成功
//			if !f.FilterRange(r) {
//				find = false
//				break
//			}
//		}
//		//目标node上有副本,与源端leader同一分片
//		if find {
//			ranges = append(ranges, r)
//			if len(ranges) == n {
//				return ranges
//			}
//		}
//	}
//	return nil
//}
//
//type LeaderScoreBalanceFilter struct {
//	expectRanges []*Range
//	cluster      *Cluster
//}
//
//func NewLeaderScoreBalanceFilter(ranges []*Range, cluster *Cluster) Filter {
//	return &LeaderScoreBalanceFilter{expectRanges: ranges, cluster: cluster}
//}
//
//func (f *LeaderScoreBalanceFilter) FilterInstance(node *Instance) bool {
//	return false
//}
//
//func (f *LeaderScoreBalanceFilter)FilterRange(r *Range) bool {
//	if !r.AllowSch(f.cluster) {
//		log.Debug("range not allow sch %d",r.Id)
//		return false
//	}
//
//	for _, rng := range f.expectRanges {
//		if rng.ID() == r.ID() {
//			//在期望列表中找到分片id,过滤成功
//			log.Debug("find range success %d %d",rng.ID(), r.ID())
//			return true
//		}
//	}
//	return false
//}


