// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"container/heap"
	"fmt"
	"sort"

	"engine/rowstore/opt"
)

const (
	supportAdjust      = 1.01
	approximationRatio = 1.05
)

type compactPolicy interface {
	pick(tfiles []*tFile) (selected []*tFile, score float64)
}

// 给定IO预算，选择输入，使得compact前后的width之差最大化
type widthCompactPolicy struct {
	budgetMB int
	icmp     *iComparer
	logger   *dbLogger
}

func newWidthCompactPolicy(budgetInMB int, icmp *iComparer, logger *dbLogger) compactPolicy {
	return &widthCompactPolicy{
		budgetMB: budgetInMB,
		icmp:     icmp,
		logger:   logger,
	}
}

type tFileInfo struct {
	f         *tFile
	sizeMB    int // 0-1背包问题的weight以MB为单位
	minUkey   []byte
	maxUkey   []byte
	cdfMinKey float64 // 文件对应的区间左边的宽度坐标
	cdfMaxKey float64 // 文件对应的区间右边的宽度坐标
	density   float64 // width/size 性价比，用于贪婪计算
}

func (t *tFileInfo) Width() float64 {
	return t.cdfMaxKey - t.cdfMinKey
}

type boundType int

const (
	boundMin = iota
	boundMax
)

type tBound struct {
	ikey internalKey
	f    *tFile
	bt   boundType
}

type boundSorter struct {
	icmp   *iComparer
	bounds []tBound
}

func (bs boundSorter) Len() int {
	return len(bs.bounds)
}

func (bs boundSorter) Less(i, j int) bool {
	comp := bs.icmp.Compare(bs.bounds[i].ikey, bs.bounds[j].ikey)
	if comp != 0 {
		return comp < 0
	}
	// ikey相等， min在前
	return bs.bounds[i].bt == boundMin
}

func (bs boundSorter) Swap(i, j int) {
	bs.bounds[i], bs.bounds[j] = bs.bounds[j], bs.bounds[i]
}

// 计算每个tFile的width等信息，并且分别按tFile的min_key, max_key排序输出
func (w *widthCompactPolicy) collectWidthOrdered(tfiles []*tFile) (ascByMin, ascByMax []*tFileInfo) {
	ascByMin = make([]*tFileInfo, 0, len(tfiles))
	ascByMax = make([]*tFileInfo, 0, len(tfiles))

	// 所有tFile的边界，排序
	bounds := make([]tBound, 0, 2*len(tfiles))
	for _, f := range tfiles {
		bounds = append(bounds, tBound{ikey: f.imin, f: f, bt: boundMin})
		bounds = append(bounds, tBound{ikey: f.imax, f: f, bt: boundMax})
	}
	bs := boundSorter{icmp: w.icmp, bounds: bounds}
	sort.Sort(bs)

	var ukeyPrev []byte
	var totalWidth float64
	active := make(map[*tFile]*tFileInfo)
	// 排序后边界两两相邻的组成一个小区间，计算每个小区间的width
	// tFile的宽度 等于 它包含的所有小区间宽度之和
	for _, b := range bounds {
		intervalWidth := w.calIntervalWidth(ukeyPrev, b.ikey.ukey(), active)

		// 更新active中tFile的width
		for _, info := range active {
			info.cdfMaxKey += intervalWidth
		}

		totalWidth += intervalWidth
		ukeyPrev = b.ikey.ukey()

		switch b.bt {
		case boundMin: // 新加入tFile
			info := &tFileInfo{
				f:         b.f,
				sizeMB:    int(b.f.size) / opt.MiB,
				minUkey:   b.f.imin.ukey(),
				maxUkey:   b.f.imax.ukey(),
				cdfMinKey: totalWidth,
				cdfMaxKey: totalWidth,
			}
			ascByMin = append(ascByMin, info)
			active[b.f] = info
		case boundMax: // 剔除终止边界对应的tFile
			info, ok := active[b.f]
			if !ok {
				panic("widthCompactPolicy::collectOrdered(): expected tfile exist")
			}
			if info.cdfMaxKey != totalWidth { // 应该相等
				panic("widthCompactPolicy::collectOrdered(): cdfMaxKey check failed")
			}
			ascByMax = append(ascByMax, info)
			delete(active, b.f)
		default:
			panic("widthCompactPolicy::collectOrdered(): invalid bound type")
		}
	}

	if len(active) != 0 {
		panic("widthCompactPolicy::collectOrdered(): active should be empty")
	}

	// 检查正确性
	if err := w.checkWidthCorrectness(ascByMin, ascByMax, totalWidth); err != nil {
		panic("widthCompactPolicy::collectOrdered(): check correctness failed:" + err.Error())
	}

	// 转换成总宽度的比例, 计算密度
	for _, info := range ascByMin {
		info.cdfMinKey /= totalWidth
		info.cdfMaxKey /= totalWidth
		info.density = (info.cdfMaxKey - info.cdfMinKey) / (float64)(info.sizeMB)
	}
	return
}

func (w *widthCompactPolicy) checkWidthCorrectness(ascByMin, ascByMax []*tFileInfo, totalWidth float64) error {
	if len(ascByMin) != len(ascByMax) {
		return fmt.Errorf("the size should be the same")
	}

	if totalWidth < 0 {
		return fmt.Errorf("total width should equal or greater than zero")
	}

	if len(ascByMin) > 0 {
		if ascByMin[0].cdfMinKey != 0.0 {
			return fmt.Errorf("asc_by_min cdfMinkey should be zero")
		}
		if ascByMax[len(ascByMax)-1].cdfMaxKey != totalWidth {
			return fmt.Errorf("asc_by_max cdfMaxkey should equal to total width")
		}
	}

	// check ordered
	for i := 1; i < len(ascByMin); i++ {
		lh := ascByMin[i-1]
		rh := ascByMin[i]
		if lh.cdfMinKey > rh.cdfMinKey {
			return fmt.Errorf("asc_by_min wrong cdfMinkey order")
		}
		if w.icmp.uCompare(lh.minUkey, rh.minUkey) > 0 {
			return fmt.Errorf("asc_by_min wrong minUkey order")
		}
	}
	for i := 1; i < len(ascByMax); i++ {
		lh := ascByMax[i-1]
		rh := ascByMax[i]
		if lh.cdfMaxKey > rh.cdfMaxKey {
			return fmt.Errorf("asc_by_max wrong cdfMaxKey order")
		}
		if w.icmp.uCompare(lh.maxUkey, rh.maxUkey) > 0 {
			return fmt.Errorf("asc_by_max wrong maxUkey order")
		}
	}

	return nil
}

// 计算小区间宽度
func (w *widthCompactPolicy) calIntervalWidth(left, right []byte, active map[*tFile]*tFileInfo) (width float64) {
	for f, info := range active {
		tLeft := info.minUkey
		tRight := info.maxUkey
		// TODO: 检查left，right有相同前缀
		// TODO: 检查[left, right]在[tLeft, tRight]内
		commonPrefix := commonPrefixLen(tLeft, tRight)
		tLeftInt := keyTailToInt(tLeft, commonPrefix)
		tRightInt := keyTailToInt(tRight, commonPrefix)
		leftInt := keyTailToInt(left, commonPrefix)
		rightInt := keyTailToInt(right, commonPrefix)

		if tRightInt > tLeftInt {
			width += (float64)(rightInt-leftInt) / (float64)(tRightInt-tLeftInt) * (float64)(f.size)
		} else if tRightInt < tLeftInt {
			panic("widthCompactPolicy::calIntervalWidth(): invalid ukey integer")
		}
	}
	return
}

type infoHeap []*tFileInfo

func (h infoHeap) Len() int            { return len(h) }
func (h infoHeap) Less(i, j int) bool  { return h[i].density < h[j].density }
func (h infoHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *infoHeap) Push(x interface{}) { *h = append(*h, x.(*tFileInfo)) }
func (h *infoHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (h *infoHeap) Top() *tFileInfo {
	return (*h)[0]
}

// greedySolver 估算，贪婪算法，连续（分数）背包
type greedySolver struct {
	maxWeiget   int
	totalWeight int
	totalValue  float64
	solution    *infoHeap
}

func newGreedySolver(maxWeiget int) *greedySolver {
	if maxWeiget <= 0 {
		panic("engine::newGreedySolver(): invalid max weight")
	}
	gs := &greedySolver{
		maxWeiget: maxWeiget,
		solution:  &infoHeap{},
	}
	heap.Init(gs.solution)
	return gs
}

func (gs *greedySolver) Add(candidate *tFileInfo) {
	// 如果背包满了，不再添加性价比比背包里所有东西都低的
	if gs.totalWeight >= gs.maxWeiget && candidate.density <= gs.solution.Top().density {
		return
	}

	heap.Push(gs.solution, candidate)
	gs.totalWeight += candidate.sizeMB
	gs.totalValue += candidate.Width()

	// 加入背包后，可能需要扔掉一些性价比低的，最多允许有一件超出容量(取这一件的部分)
	for gs.totalWeight-gs.solution.Top().sizeMB > gs.maxWeiget {
		gs.totalWeight -= gs.solution.Top().sizeMB
		gs.totalValue -= gs.solution.Top().Width()
		heap.Pop(gs.solution)
	}
}

// 计算对应的0-1背包问题最优解的下界和上界
func (gs *greedySolver) computeLowerAndUpperBound() (lower, upper float64) {
	excess := gs.totalWeight - gs.maxWeiget
	if excess <= 0 {
		return gs.totalValue, gs.totalValue
	}

	top := gs.solution.Top()
	lower = gs.totalValue - top.Width()
	if top.Width() > lower {
		lower = top.Width()
	}

	excessFraction := (float64(excess)) / float64(top.sizeMB)
	upper = gs.totalValue - excessFraction*top.Width()

	return
}

// 找出最优解下界对应的solution
func (gs *greedySolver) getLowerBoundSolution() (s []*tFileInfo) {
	excess := gs.totalWeight - gs.maxWeiget
	if excess <= 0 {
		s = append(s, (*gs.solution)...)
		return
	}
	top := gs.solution.Top()
	if gs.totalValue-top.Width() > top.Width() {
		for i := 1; i < gs.solution.Len(); i++ {
			s = append(s, (*gs.solution)[i])
		}
	} else {
		s = append(s, gs.solution.Top())
	}
	return
}

// 贪婪算法计算连续背包问题，算出0-1背包问题的上界和下界（去除最后分数部分）
func (w *widthCompactPolicy) runApproximation(ascByMin, ascByMax []*tFileInfo) (bestUpperBound []float64, bestChosen []*tFile, bestValue float64) {
	for _, a := range ascByMin {
		gs := newGreedySolver(w.budgetMB)
		bestValue = 0
		bestUpper := 0.0
		// [abmin, abmax] a,b组成的区间
		abmin := a.cdfMinKey
		abmax := a.cdfMaxKey
		// 针对每对a,b组成区间[a,b]，计算compact该区间的收益
		for _, b := range ascByMax {
			// 只选择边界起点在a右边的，这样内循环里左边界是固定的, 右边界=max(b.cdfMaxkey, a.cdfMaxKey)
			// 并且内循环里按b.cdfMaxKey 递增，后面的区间就包含了前面的，可以利用前面计算的结果（整个内循环只需要一个greedySolver)
			if b.cdfMinKey < abmin {
				continue
			}
			if b.cdfMaxKey > abmax {
				abmax = b.cdfMaxKey
			}
			uionWidth := abmax - abmin // compact后的区间长度，compact后的文件彼此不重叠
			gs.Add(b)
			lower, upper := gs.computeLowerAndUpperBound()
			lower -= uionWidth * supportAdjust // 减去unionWidth代表收益，supportAdjust惩罚系数，同样的收益下选择宽度更小的（更少文件）
			upper -= uionWidth * supportAdjust
			if upper > bestUpper {
				bestUpper = upper
			}
			// 按下界计算bestValue
			if lower > bestValue {
				bestValue = lower
				s := gs.getLowerBoundSolution()
				bestChosen = nil
				for _, info := range s {
					bestChosen = append(bestChosen, info.f)
				}
			}
		}
		// 每个以a.min为起点的区间的最优解上界
		bestUpperBound = append(bestUpperBound, bestUpper)
	}

	return
}

type knapsackItem struct {
	weight int
	value  float64
}

type knapsackSolver struct {
	capacity int            // 背包容量
	items    []knapsackItem // 物品
	itemIdx  int            // 当前选择第几个物品
	dp       [][]float64    // dp二维数组
}

func newKnapsackSolver(capacity int, items []knapsackItem) *knapsackSolver {
	ks := &knapsackSolver{
		capacity: capacity,
		items:    items,
		dp:       make([][]float64, len(items)),
	}
	for i := 0; i < len(items); i++ {
		ks.dp[i] = make([]float64, capacity+1)
	}
	return ks
}

func (ks *knapsackSolver) advance() bool {
	if ks.itemIdx >= len(ks.items) {
		return false
	}

	i := ks.itemIdx
	weight := ks.items[ks.itemIdx].weight
	value := ks.items[ks.itemIdx].value
	for j := 0; j <= ks.capacity; j++ {
		if i > 0 {
			ks.dp[i][j] = ks.dp[i-1][j]
		}
		if j >= weight {
			prevValue := 0.0
			if i > 0 {
				prevValue = ks.dp[i-1][j-weight]
			}
			if prevValue+value > ks.dp[i][j] {
				ks.dp[i][j] = prevValue + value
			}
		}
	}

	ks.itemIdx++
	return true
}

func (ks *knapsackSolver) getSolution() (idx int, bestValue float64) {
	return ks.itemIdx - 1, ks.dp[ks.itemIdx-1][ks.capacity]
}

func (ks *knapsackSolver) getChosen() []bool {
	i := ks.itemIdx - 1
	j := ks.capacity
	chosen := make([]bool, ks.itemIdx)
	for i >= 0 && j > 0 {
		v := ks.dp[i][j]
		prev := 0.0
		if i > 0 {
			prev = ks.dp[i-1][j]
		}
		if v > prev { // 选了i
			chosen[i] = true
			j -= ks.items[i].weight
			i--
		} else if v == prev {
			i--
		} else {
			panic("knapsackSolver::getChoosen() invalid dp data")
		}
	}

	return chosen
}

// 计算0-1背包问题
func (w *widthCompactPolicy) runExact(ascByMin, ascByMax []*tFileInfo, approxUpperBound []float64,
	approxSolution []*tFile, approxBestValue float64) (bestSolution []*tFile, bestValue float64) {
	bestValue = approxBestValue
	bestSolution = approxSolution
	for i, a := range ascByMin {
		// 如果以a为起点的近似最优解上界 < 当前最优解 * 近似一个系数，则跳过它, 缩小问题规模
		// 也就是说如果它的最优解上界只比之前的大那么一丁点的话，那么没必要花费太大力气求解它，不如就使用之前的
		if approxUpperBound[i] < bestValue*approximationRatio {
			continue
		}

		var candidates []*tFileInfo
		var ksItems []knapsackItem
		for _, b := range ascByMax {
			if b.cdfMinKey < a.cdfMinKey {
				continue
			}
			candidates = append(candidates, b)
			ksItems = append(ksItems, knapsackItem{weight: b.sizeMB, value: b.Width()})
		}

		if len(candidates) == 0 {
			continue
		}

		var chosen []bool
		chosenEmpty := true
		ks := newKnapsackSolver(w.budgetMB, ksItems)
		for ks.advance() {
			j, value := ks.getSolution()
			abmin := a.cdfMinKey
			abmax := a.cdfMaxKey
			b := candidates[j]
			if b.cdfMaxKey > abmax {
				abmax = b.cdfMaxKey
			}
			value -= (abmax - abmin) * supportAdjust
			if value > bestValue {
				bestValue = value
				chosen = ks.getChosen()
				chosenEmpty = false
			}
		}
		if !chosenEmpty {
			bestSolution = nil
			for i, b := range chosen {
				if b {
					bestSolution = append(bestSolution, candidates[i].f)
				}
			}
		}
	}

	return
}

func (w *widthCompactPolicy) pick(tfiles []*tFile) (results []*tFile, score float64) {
	defer func() {
		if x := recover(); x != nil {
			w.logger.Panic("pick comapct files panic(%v)! version: %v", x, tFiles(tfiles).DebugString())
		}
	}()

	ascByMin, ascByMax := w.collectWidthOrdered(tfiles)
	if len(ascByMin) <= 2 {
		return nil, 0
	}

	// 估算
	bestUpperBound, bestChosen, score := w.runApproximation(ascByMin, ascByMax)

	// 精确计算
	results, score = w.runExact(ascByMin, ascByMax, bestUpperBound, bestChosen, score)

	if w.logger.IsEnableDebug() {
		w.logger.Debug("quality: %v, seleted: %d", score, len(results))
	}

	if score <= 0 {
		return nil, 0
	}

	return
}
