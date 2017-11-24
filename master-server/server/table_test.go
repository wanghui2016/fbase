package server

import (
	"fmt"
	"raft/logger"
	"util/log"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"

	"model/pkg/mspb"
	"model/pkg/metapb"
	"net/http"
)

var dbName string = "test_db_1"
var tName string = "test_table_1"
var dbId uint64 = 1
var tId uint64 = 2

//var properties string = "{\"columns\":[{\"autoincrement\":\"false\",\"datatype\":\"int\",\"defaultvalue\":\"\",\"index\":true,\"name\":\"id\",\"nullable\":false,\"primarykey\":true,\"rmark\":\"\"},{\"autoincrement\":\"false\",\"datatype\":\"varchar\",\"defaultvalue\":\"\",\"index\":true,\"name\":\"name\",\"nullable\":false,\"primarykey\":false,\"rmark\":\"\"},{\"autoincrement\":\"false\",\"datatype\":\"varchar\",\"defaultvalue\":\"\",\"index\":false,\"name\":\"str-[a-z0-9]+\",\"nullable\":false,\"primarykey\":false,\"rmark\":\"\"}]}"
var properties string = "{ \"columns\": [ { \"autoincrement\": \"false\", \"datatype\": \"int\", \"defaultvalue\": \"\", \"index\": true, \"name\": \"id\", \"nullable\": false, \"primarykey\": true, \"rmark\": \"\" }, { \"autoincrement\": \"false\", \"datatype\": \"varchar\", \"defaultvalue\": \"\", \"index\": true, \"name\": \"name\", \"nullable\": false, \"primarykey\": false, \"rmark\": \"\" } ], \"regxs\": [ { \"autoincrement\": \"false\", \"datatype\": \"varchar\", \"defaultvalue\": \"\", \"index\": false, \"name\": \"str-[ a-z0-9 ]+\", \"nullable\": false, \"primarykey\": false, \"rmark\": \"\" } ] }"


func TestParseProperties(t *testing.T) {
	cols, regxs, err := ParseProperties(properties)
	if err != nil {
		fmt.Println(err.Error())
	}
	str, err := ToTableProperty(cols)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(" ^^^^^^^^^ test parse properties")
	fmt.Println(regxs)
	fmt.Println(str)
}

func createMaster() *Server {
	/**
		app.id = 1
	app.port= 8887
	app.version = v1
	app.name = master-node
	app.isprod = true

	log.dir = ./log
	log.module = master
	log.level=debug

	db.path = ./data

	cluster.infos = 1-192.168.211.149-8887-8877-8867

	ping.frequency = 10

	range.SplitThresholdSize = 10000000

	monitor.dbuser = test
	monitor.dbpasswd = 123456
	monitor.dbaddr = 192.168.192.36:5000

	servaddr.master = 192.168.211.149:8887
	servaddr.ftp = 192.168.183.66:21

	tso.SaveInterval = 3s
	*/
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	// load config file
	conf := &Config{
		AppName:       "master-node-1",
		AppVersion:    "v1.0.0",
		AppServerIp:   "127.0.0.1",
		AppServerPort: "8887",
		DataPath:      "/tmp/fbase/master/data",
		NodeID:        1,
		ClusterInfos:  "1-127.0.0.1-8887-8877-8867",
		Mode:          "debug",

		LogDir:    "/tmp/fbase/master/logs",
		LogModule: "master",
		LogLevel:  "debug",

		ThresholdRangeSplitSize: 2000000,
		MemdbSize:               10240,
		MonitorDbUser:           "test",
		MonitorDbPasswd:         "123456",
		MonitorDbAddr:           "127.0.0.1:5000",
		ServAddrFtp:             "127.0.0.1:21",
	}

	os.RemoveAll(conf.DataPath)
	os.RemoveAll(conf.LogDir)
	os.MkdirAll(conf.DataPath, 0777)
	os.MkdirAll(conf.LogDir, 0777)

	log.InitFileLog(conf.LogDir, conf.LogModule, conf.LogLevel)
	// config raft logger
	logger.SetLogger(log.GetFileLogger())
	master := new(Server)
	fmt.Println("init server")
	master.InitServer(conf)
	fmt.Println("server start")
	return master
}

func createTable(server *Server,t *testing.T) {

	defer func() {
		if x := recover(); x != nil {
			// 回滚

			buf := make([]byte, 10240)
			stackSize := runtime.Stack(buf, true)
			fmt.Printf("%s:\n%s\n", x,string(buf[0:stackSize]))
		}
	}()
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("http request start====")
	createDB(server)
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)

	req.Form = make(map[string][]string)
	req.Form.Set("dbname", dbName)

	req.Form.Set("tablename", tName)
	req.Form.Set("properties", properties)
	isNeedRange := "false"
	req.Form.Set("needrange", isNeedRange)

	w := httptest.NewRecorder()

	server.handleTableCreate(w, req)
	fmt.Println(w.Body.String())

	tt,ok:=server.cluster.FindCurTable(dbName,tName)

	if !ok {
		t.Error("create table error")
	}
	size := len(tt.Columns)

	autoCreateColumn(server,t)
	//autoCreateColumn2(server,t)
	if len(tt.Columns) <= size {
		t.Error("old size:%d , new size:%d",size,len(tt.Columns))
	}else{
		fmt.Printf("old size:%d , new size:%d\n",size,len(tt.Columns))
	}
	if len(tt.Columns) != size+2 {
		t.Error("old size:%d , new size:%d",size,len(tt.Columns))
	}
	fmt.Println(tt.String())
	server.quit <- struct {}{}
}

func autoCreateColumn(server *Server,t *testing.T){
	/*req := new(mspb.AddColumnRequest)
	req.DbName=dbName
	req.TableName=tName
	cols := make([]*metapb.Column,0)
	col := &metapb.Column{
		Name:"str-col1",
	}
	cols = append(cols,col)
	req.Columns= cols

	cli, _ := client.NewClient([]string{"127.0.0.1:8887"})
	if cols_, err := cli.AddColumns(dbName, tName, cols); err != nil {
		fmt.Println("add columns error: ", err)
	} else {
		fmt.Println("++++++++++++++", cols_)
		t, _ := server.cluster.FindTable(dbName, tName)
		fmt.Printf("%v\n", t.Columns)
		os.Exit(0)
	}*/

	//resp,err := server.handleAddColumns(req)
	//if err != nil {
	//	fmt.Println(err.Error())
	//}else{
	//	fmt.Println(resp.String())
	//}
	//flag := false
	//for _,addCol := range resp.Columns {
	//	if addCol.Name == col.Name {
	//		flag = true
	//	}
	//}
	//if !flag ||  len(resp.Columns)!=1 {
	//	t.Error("add column error\n%s\n",resp.Columns)
	//}

}

func autoCreateColumn2(server *Server,t *testing.T){
	req := new(mspb.AddColumnRequest)
	req.DbId=dbId
	req.TableId=tId
	cols := make([]*metapb.Column,0)
	col := &metapb.Column{
		Name:"str-col1",
	}
	col2 := &metapb.Column{
		Name:"str-col2",
	}
	cols = append(cols,col2)
	cols = append(cols,col)
	req.Columns= cols
	resp,err := server.handleAddColumns(nil, req)
	if err != nil {
		fmt.Println(err.Error())
	}else{
		fmt.Println(resp.String())
	}

	if len(resp.Columns)!=1 {
		t.Error("add column error\n%s\n",resp.Columns)
	}
	fmt.Printf("autoCreateColumn2:\n%s\n",resp.Columns)
}

func createDB(server *Server){
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)

	req.Form = make(map[string][]string)
	req.Form.Set("name", dbName)
	properties := "{}"
	req.Form.Set("properties", properties)

	w := httptest.NewRecorder()

	server.handleDatabaseCreate(w, req)
	fmt.Println(w.Body.String())
}

func TestCreateTable(t *testing.T) {
	server := createMaster()
	go createTable(server,t)
	server.Start()
	fmt.Println("server start====")
}

func TestCreateKey(t *testing.T){
	for i := 97; i < 123; i++ {
		for j := 97; j < 123; j++ {
			fmt.Printf("\"%c%c\",",i,j)

		}

	}
}
