package server
//
//import (
//	"time"
//	"testing"
//	"encoding/json"
//	"model/pkg/metapb"
//)
//
//func TestTableColumnRename(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	property, err := json.Marshal(TableProperty{Columns: table0})
//	if err != nil {
//		t.Errorf("init error: %s\n", err)
//		time.Sleep(time.Second)
//		return
//	}
//	table, err := cluster.CreateTable("db0", "table0", table0, nil, false, false, "")
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	table0 = []*metapb.Column{
//		&metapb.Column{Name: "rangeID", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	property, err = json.Marshal(TableProperty{Columns: table0})
//	if err != nil {
//		t.Errorf("init error: %s\n", err)
//		time.Sleep(time.Second)
//		return
//	}
//	err = cluster.EditTable("db0", "table0", string(property))
//	if err != nil {
//		t.Errorf("test failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	_, find := table.GetColumnByName("rangeID")
//	if !find {
//		t.Errorf("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Logf("test success!!!")
//	time.Sleep(time.Second)
//}
//
//func TestTableColIsSqlWord(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	_, err := createVirtualTable(cluster, "db0", "table0", table0, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//
//	table1 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//		&metapb.Column{Name: "select", DataType: metapb.DataType_BigInt },
//	}
//	_, err = createVirtualTable(cluster, "db0", "table1", table1, nil)
//	if err == nil {
//		t.Errorf("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("test success")
//}
//
//func TestTableColIsSqlWord1(t *testing.T) {
//	var (
//		RangeMonitor = []*metapb.Column{
//			&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			//&Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			//&Column{Name: "size", DataType: metapb.DataType_BigInt, },
//
//			&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "range_size", DataType: metapb.DataType_BigInt, },
//
//			&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//		}
//
//		NodeMonitor = []*metapb.Column {
//			&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "nodeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, Unsigned: true},
//
//			//&Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//			&metapb.Column{Name: "host", DataType: metapb.DataType_Varchar, },
//			&metapb.Column{Name: "cpu_proc_rate", DataType: metapb.DataType_Double, },
//			&metapb.Column{Name: "total_memory", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "used_memory", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "total_swap_memory", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "used_swap_memory", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "total_disk", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "used_disk", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "load", DataType: metapb.DataType_Double, },
//		}
//		DbMonitor = []*metapb.Column {
//			&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "dbid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//			//&Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			//&Column{Name: "size", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "db_size", DataType: metapb.DataType_BigInt, },
//
//			&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//		}
//		TableMonitor = []*metapb.Column {
//			&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "tableid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			//&Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			//&Column{Name: "size", DataType: metapb.DataType_BigInt, },
//
//			&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "table_size", DataType: metapb.DataType_BigInt, },
//
//			&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//		}
//		ClusterMonitor = []*metapb.Column {
//			&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//			//&Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//			&metapb.Column{Name: "total_capacity", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "used_capacity", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "ops", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_in_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "bytes_out_per_sec", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "range_count", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "connected_clients", DataType: metapb.DataType_BigInt, },
//		}
//
//		ClusterTask = []*metapb.Column {
//			&metapb.Column{Name: "clusterid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "taskid", DataType: metapb.DataType_BigInt,PrimaryKey: 1, },
//
//			//&Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//			&metapb.Column{Name: "finish_time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//
//			&metapb.Column{Name: "create_time", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "used_time", DataType: metapb.DataType_BigInt, },
//			&metapb.Column{Name: "state", DataType: metapb.DataType_Varchar, },
//			&metapb.Column{Name: "detail", DataType: metapb.DataType_Varchar, },
//		}
//	)
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	cluster.CreateDatabase("db0", "")
//
//	_, err := createVirtualTable(cluster, "db0", "r", RangeMonitor, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	_, err = createVirtualTable(cluster, "db0", "n", NodeMonitor, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	_, err = createVirtualTable(cluster, "db0", "d", DbMonitor, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	_, err = createVirtualTable(cluster, "db0", "t", TableMonitor, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	_, err = createVirtualTable(cluster, "db0", "c", ClusterMonitor, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	_, err = createVirtualTable(cluster, "db0", "task", ClusterTask, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//
//	t.Log("test success")
//}
//
//func TestTableColIsSqlWord2(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	regxs := []*metapb.Column{
//		&metapb.Column{Name: "selec[a-z]", DataType:  metapb.DataType_BigInt,},
//	}
//	table, err := createVirtualTable(cluster, "db0", "table0", table0, regxs)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	clos := []*metapb.Column{
//		&metapb.Column{Name: "selecc", DataType: metapb.DataType_BigInt,},
//	}
//    _, err = table.UpdateSchema(clos, store)
//	if err != nil {
//		t.Error("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	clos = []*metapb.Column{
//		&metapb.Column{Name: "select", DataType: metapb.DataType_BigInt,},
//	}
//	_, err = table.UpdateSchema(clos, store)
//	if err == nil {
//		t.Error("test failed")
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("test success")
//}
//
//func TestTableColIsSqlWord3(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	_, err := createVirtualTable(cluster, "db0", "table0", table0, nil)
//	if err == nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	t.Log("test success")
//}
//
//func TestTableColIsSqlWord4(t *testing.T) {
//	store := NewLocalStore()
//	alarm := NewTestAlarm()
//	cluster := NewCluster(uint64(1), uint64(1), store, alarm)
//	cluster.cli = NewLocalClient()
//	cluster.UpdateLeader(uint64(1))
//	cluster.LoadCache()
//
//	cluster.CreateDatabase("db0", "")
//	table0 := []*metapb.Column{
//		&metapb.Column{Name: "rangeid", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "time", DataType: metapb.DataType_BigInt, PrimaryKey: 1, },
//		&metapb.Column{Name: "size", DataType: metapb.DataType_BigInt, },
//	}
//	table, err := createVirtualTable(cluster, "db0", "table0", table0, nil)
//	if err != nil {
//		t.Errorf("create table failed, err[%v]", err)
//		time.Sleep(time.Second)
//		return
//	}
//	for i, col := range table.Columns {
//		if col.Name != table0[i].Name {
//			t.Errorf("test failed")
//			time.Sleep(time.Second)
//			return
//		}
//	}
//	t.Log("test success")
//}
//
//
//func createVirtualTable(cluster *Cluster, dbname, tablename string, cols []*metapb.Column, regxs []*metapb.Column) (*Table, error) {
//	t, err := cluster.CreateTable(dbname, tablename, cols, regxs, false, false, "")
//	if err != nil {
//		return nil, err
//	}
//	return t, nil
//}