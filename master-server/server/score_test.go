package server

import (
	"testing"
	"encoding/json"

	"model/pkg/statspb"
)

func TestCaculateNodeScore(t *testing.T) {
	/**
	--- PASS: TestCaculateNodeScore (0.00s)
        score_test.go:24: cpuProcRate:79.948586
        score_test.go:30: MemoryUsedPercent:94.471740
        score_test.go:36: DiskProcRate:83.243051
        score_test.go:42: SwapMemoryUsedPercent:4.049704
        score_test.go:49: DiskReadBytePerSec3285366, diskReadPerc:0.032854
        score_test.go:56: DiskWriteBytePerSec 16655081, diskWritePerc:0.166551
        score_test.go:63: NetIoInBytePerSec3869810, netInPerc:0.038698
        score_test.go:70: NetIoOutBytePerSec 3870845, netOutPerc:0.038708
        score_test.go:77: rangeCountPerc:26.666667
        score_test.go:84: leaderCountPerc:40.000000
        score_test.go:87: cpuScore:1279.177378 + memScore:48369.531027 + diskScore:42620.442257 + swapScore:64.795263 + diskReadScore:0.525659 + diskWriteScore:2.664813 + netInScore:0.619170 + netOutScore:0.619335 + rangeCountScore:426.666667 + leaderCountScore:640.000000
        score_test.go:88: score:93405.041568
        score_test.go:89: caculateNodeScore end...
PASS
	 */
	//clsAllRangeSize := 30
	//clsAllLeaderSize := 10

	stats := &statspb.NodeStats{}
	nodeStat := "{\"memory_total\":8057360384,\"memory_used\":7929798656,\"memory_free\":127561728,\"memory_used_percent\":94.47174028749512,\"swap_memory_total\":19998437376,\"swap_memory_used\":809877504,\"swap_memory_free\":19188559872,\"swap_memory_used_percent\":4.049703928227557,\"cpu_proc_rate\":79.94858611779274,\"cpu_count\":4,\"load1\":3.77,\"load5\":1.71,\"load15\":1.22,\"disk_total\":55097540608,\"disk_used\":45864873984,\"disk_free\":6410268672,\"disk_proc_rate\":83.24305128301962,\"disk_read_byte_per_sec\":3285366,\"disk_write_byte_per_sec\":16655081,\"net_io_in_byte_per_sec\":3869810,\"net_io_out_byte_per_sec\":3870845,\"net_tcp_connections\":635,\"ops\":645,\"range_count\":8,\"range_leader_count\":4}"
	err := json.Unmarshal([]byte(nodeStat),stats)
	if err != nil{
		t.Error(err)
	}

	cpuScore := stats.CpuProcRate * 16
	if stats.CpuProcRate > 80 {
		cpuScore = cpuScore * 8
	}
	t.Logf("cpuProcRate:%f", stats.CpuProcRate)

	memScore := stats.MemoryUsedPercent * 64
	if stats.MemoryUsedPercent > 70 {
		memScore = memScore * 8
	}
	t.Logf("MemoryUsedPercent:%f", stats.MemoryUsedPercent)

	diskScore := stats.DiskProcRate * 64
	if stats.DiskProcRate > 60 {
		diskScore = diskScore * 8
	}
	t.Logf("DiskProcRate:%f", stats.DiskProcRate)

	swapScore := stats.SwapMemoryUsedPercent * 16
	if stats.SwapMemoryUsedPercent > 10 {
		swapScore = swapScore * 8
	}
	t.Logf("SwapMemoryUsedPercent:%f", stats.SwapMemoryUsedPercent)

	diskReadPerc := (float64(stats.DiskReadBytePerSec) / 10000000000.0)*100
	diskReadScore := diskReadPerc * 16
	if diskReadPerc > 70 {
		diskReadScore = diskReadScore * 8
	}
	t.Logf("DiskReadBytePerSec%d, diskReadPerc:%f", stats.DiskReadBytePerSec, diskReadPerc)

	diskWritePerc := (float64(stats.DiskWriteBytePerSec) / 10000000000.0)*100
	diskWriteScore := diskWritePerc * 16
	if diskWritePerc > 70 {
		diskWriteScore = diskWriteScore * 8
	}
	t.Logf("DiskWriteBytePerSec %d, diskWritePerc:%f", stats.DiskWriteBytePerSec, diskWritePerc)

	netInPerc := (float64(stats.NetIoInBytePerSec) / 10000000000.0)*100
	netInScore := netInPerc * 16
	if netInPerc > 70 {
		netInScore = netInScore * 8
	}
	t.Logf("NetIoInBytePerSec%d, netInPerc:%f", stats.NetIoInBytePerSec, netInPerc)

	netOutPerc := (float64(stats.NetIoOutBytePerSec)/ 10000000000.0)*100
	netOutScore := netOutPerc * 16
	if netOutPerc > 70 {
		netOutScore = netOutScore * 8
	}
	t.Logf("NetIoOutBytePerSec %d, netOutPerc:%f", stats.NetIoOutBytePerSec,netOutPerc)

	//rangeCountPerc := (float64(stats.RangeCount)/float64(clsAllRangeSize))*100
	//rangeCountScore := rangeCountPerc * 16
	//if rangeCountPerc > 50 {
	//	rangeCountScore = rangeCountScore * 8
	//}
	//t.Logf("rangeCountPerc:%f", rangeCountPerc)
	//
	//leaderCountPerc := (float64(stats.RangeLeaderCount)/float64(clsAllLeaderSize))*100
	//leaderCountScore := leaderCountPerc * 16
	//if leaderCountPerc > 50 {
	//	leaderCountScore = leaderCountScore * 8
	//}
	//t.Logf("leaderCountPerc:%f", leaderCountPerc)

	//Score := cpuScore + memScore + diskScore + swapScore + diskReadScore + diskWriteScore + netInScore + netOutScore + rangeCountScore + leaderCountScore
	//t.Logf("cpuScore:%f + memScore:%f + diskScore:%f + swapScore:%f + diskReadScore:%f + diskWriteScore:%f + netInScore:%f + netOutScore:%f + rangeCountScore:%f + leaderCountScore:%f",cpuScore, memScore, diskScore, swapScore, diskReadScore, diskWriteScore, netInScore, netOutScore, rangeCountScore, leaderCountScore)
	//t.Logf("score:%f",Score)
	//t.Logf("caculateNodeScore end...")
}

func TestCaculateRangeScore(t *testing.T) {
	/**
	--- PASS: TestCaculateRangeScore (0.00s)
        score_test.go:93: caculateRangeScore start...
        score_test.go:105: sizePerc:11.961145, opsPerc:0.160500, inPerc:0.037604, outPerc:0.001670
        score_test.go:123: 388.521029
	 */
	t.Logf("caculateRangeScore start...")
	stats := &statspb.RangeStats{}
	rangeData := "{\"size\":128431821,\"ops\":321,\"bytes_in_per_sec\":376037,\"bytes_out_per_sec\":16698}"
	err := json.Unmarshal([]byte(rangeData), stats)
	if err != nil{
		t.Error(err)
	}

	sizePerc := (float64(stats.Size_)/(1024*1024*1024.0))*100
	opsPerc := (float64(stats.Ops)/200000.0)*100
	inPerc := (float64(stats.BytesInPerSec)/1000000000.0)*100
	outPerc := (float64(stats.BytesOutPerSec)/1000000000.0)*100
	t.Logf("sizePerc:%f, opsPerc:%f, inPerc:%f, outPerc:%f", sizePerc, opsPerc, inPerc, outPerc)

	sizeScore := sizePerc*32
	if sizePerc > 70 {
		sizeScore = sizeScore  * 4
	}
	opsScore := opsPerc*32
	if opsPerc > 40 {
		opsScore = opsScore * 4
	}
	inScore := inPerc * 16
	if inPerc > 50 {
		inScore = inScore * 4
	}
	outScore := outPerc * 16
	if outPerc > 50 {
		outScore = outScore * 4
	}
	t.Logf("%f", sizeScore + opsScore + inScore + outScore)
}