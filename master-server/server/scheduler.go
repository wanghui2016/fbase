package server

import (
	"time"

	"golang.org/x/net/context"
)

type Scheduler interface {
	GetName() string
	Schedule() *Task
	AllowSchedule() bool
	GetInterval() time.Duration
	Ctx() context.Context
	Stop()
}


type InstanceByRangesSlice []*Instance

func (p InstanceByRangesSlice) Len() int {
	return len(p)
}

func (p InstanceByRangesSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByRangesSlice) Less(i int, j int) bool {
	return p[i].GetRangesNum() > p[j].GetRangesNum()
}

type InstanceByLeaderSlice []*Instance

func (p InstanceByLeaderSlice) Len() int {
	return len(p)
}

func (p InstanceByLeaderSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByLeaderSlice) Less(i int, j int) bool {
	return p[i].GetLeaderNum() > p[j].GetLeaderNum()
}

type InstanceByDiskSlice []*Instance

func (p InstanceByDiskSlice) Len() int {
	return len(p)
}

func (p InstanceByDiskSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByDiskSlice) Less(i int, j int) bool {
	return p[i].GetDiskUsedPercent() < p[j].GetDiskUsedPercent()
}

type InstanceByLoadSlice []*Instance

func (p InstanceByLoadSlice) Len() int {
	return len(p)
}

func (p InstanceByLoadSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByLoadSlice) Less(i int, j int) bool {
	return p[i].GetLoad() < p[j].GetLoad()
}

type InstanceByNetSlice []*Instance

func (p InstanceByNetSlice) Len() int {
	return len(p)
}

func (p InstanceByNetSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByNetSlice) Less(i int, j int) bool {
	return p[i].GetNetIO() < p[j].GetNetIO()
}

type InstanceByScoreSlice []*Instance

func (p InstanceByScoreSlice) Len() int {
	return len(p)
}

func (p InstanceByScoreSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByScoreSlice) Less(i int, j int) bool {
	return p[i].GetScore() > p[j].GetScore()
}

type InstanceByLeaderTotalScoreOfNodeSlice []*Instance

func (p InstanceByLeaderTotalScoreOfNodeSlice) Len() int {
	return len(p)
}

func (p InstanceByLeaderTotalScoreOfNodeSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p InstanceByLeaderTotalScoreOfNodeSlice) Less(i int, j int) bool {
	return p[i].GetTotalLeaderScore() > p[j].GetTotalLeaderScore()
}