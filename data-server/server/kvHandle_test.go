package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"model/pkg/kvrpcpb"
	"model/pkg/metapb"
	"util"
)

func TestRangeSetGet(t *testing.T) {
	conf := &Config{
		AppName:       "data-server",
		AppVersion:    "v1",
		AppManagePort: 6060,
		DataPath:      "/export/Data/data-server",
		NodeID:        1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   &metapb.Key{Key: []byte("a"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("z"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	defer rng1.Clean()
	svr.AddRange(rng1)
	r2 := &metapb.Range{
		Id:         2,
		StartKey:   &metapb.Key{Key: []byte("a"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("e"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng2, err := NewRange(r2, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng2)
	defer rng2.Clean()
	r3 := &metapb.Range{
		Id:         3,
		StartKey:   &metapb.Key{Key: []byte("e"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("z"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng3, err := NewRange(r3, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng3)
	defer rng3.Clean()
	// 等待选举产生leader
	time.Sleep(time.Second)
	putReq := &kvrpcpb.KvRawPutRequest{
		Key:   []byte("aaaa"),
		Value: []byte("bbbbbbb"),
	}
	hdr := &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	putResp, pbErr := svr.handleRawPut(context.Background(), hdr.RangeId, putReq)
	if pbErr != nil {
		t.Errorf("raw put failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if putResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("bbbbb"),
		Value: []byte("ccccccc"),
	}
	hdr = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	putResp, pbErr = svr.handleRawPut(context.Background(), hdr.RangeId, putReq)
	if pbErr != nil {
		t.Errorf("raw put failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if putResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	getReq := &kvrpcpb.KvRawGetRequest{
		Key: []byte("bbbbb"),
	}
	hdr = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr := svr.handleRawGet(context.Background(), hdr.RangeId, getReq)
	if pbErr != nil {
		t.Errorf("raw get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if bytes.Compare(getResp.GetValue(), putReq.GetValue()) != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("yyyyyy"),
		Value: []byte("wwwwwwwww"),
	}
	hdr = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	putResp, pbErr = svr.handleRawPut(context.Background(), hdr.RangeId, putReq)
	if pbErr != nil {
		t.Errorf("raw put failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if putResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	delReq := &kvrpcpb.KvRawDeleteRequest{
		Key: []byte("bbbbb"),
	}
	hdr = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	delResp, pbErr := svr.handleRawDelete(context.Background(), hdr.RangeId, delReq)
	if pbErr != nil {
		t.Errorf("raw delete failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if delResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	getReq = &kvrpcpb.KvRawGetRequest{
		Key: []byte("bbbbb"),
	}
	hdr = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr = svr.handleRawGet(context.Background(), hdr.RangeId, getReq)
	if pbErr != nil {
		t.Errorf("raw get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if getResp.Code == 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	t.Log("test success")
}

func TestRangeSplitSetGet(t *testing.T) {
	conf := &Config{
		AppName:       "data-server",
		AppVersion:    "v1",
		AppManagePort: 6060,
		DataPath:      "/export/Data/data-server",
		NodeID:        1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   &metapb.Key{Key: []byte("a"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("z"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng1)
	defer rng1.Clean()
	r2 := &metapb.Range{
		Id:         2,
		StartKey:   &metapb.Key{Key: []byte("a"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("e"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng2, err := NewRange(r2, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng2)
	defer rng2.Clean()
	r3 := &metapb.Range{
		Id:         3,
		StartKey:   &metapb.Key{Key: []byte("e"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Key: []byte("z"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng3, err := NewRange(r3, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng3)
	defer rng3.Clean()
	// 等待选举产生leader
	time.Sleep(time.Second)

	// range 1
	putReq := &kvrpcpb.KvRawPutRequest{
		Key:   []byte("aaaa"),
		Value: []byte("bbbbbbb"),
	}
	header := &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	ctx := context.Background()
	svr.handleRawPut(ctx, header.RangeId, putReq)
	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("bbbbb"),
		Value: []byte("ccccccc"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	svr.handleRawPut(ctx, header.RangeId, putReq)
	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("yyyyyy"),
		Value: []byte("wwwwwwwww"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	svr.handleRawPut(ctx, header.RangeId, putReq)

	// range 2
	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("aaaa"),
		Value: []byte("bbbbbbb"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	svr.handleRawPut(ctx, header.RangeId, putReq)
	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("bbbbb"),
		Value: []byte("ccccccc"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	svr.handleRawPut(ctx, header.RangeId, putReq)

	// range 3
	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("yyyyyy"),
		Value: []byte("wwwwwwwww"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   3,
		Timestamp: svr.clock.Now(),
	}
	svr.handleRawPut(ctx, header.RangeId, putReq)

	// 模拟range 1分裂
	rng1.region.State = metapb.RangeState_R_Offline
	rng1.left = rng2.region
	rng1.right = rng3.region
	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("ccccc"),
		Value: []byte("ddddddd"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	putResp, pbErr := svr.handleRawPut(ctx, header.RangeId, putReq)
	if pbErr == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if putResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	getReq := &kvrpcpb.KvRawGetRequest{
		Key: []byte("ccccc"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr := svr.handleRawGet(ctx, header.RangeId, getReq)
	if pbErr == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if getResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	delReq := &kvrpcpb.KvRawDeleteRequest{
		Key: []byte("ccccc"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	delResp, pbErr := svr.handleRawDelete(ctx, header.RangeId, delReq)
	if pbErr == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if delResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	putReq = &kvrpcpb.KvRawPutRequest{
		Key:   []byte("ccccc"),
		Value: []byte("ddddddd"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	putResp, pbErr = svr.handleRawPut(ctx, header.RangeId, putReq)
	if pbErr != nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if putResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	getReq = &kvrpcpb.KvRawGetRequest{
		Key: []byte("ccccc"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr = svr.handleRawGet(ctx, header.RangeId, getReq)
	if pbErr != nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if getResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	delReq = &kvrpcpb.KvRawDeleteRequest{
		Key: []byte("ccccc"),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	delResp, pbErr = svr.handleRawDelete(ctx, header.RangeId, delReq)
	if pbErr != nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if delResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	t.Log("test success")
}

func TestRangeInsertDelete(t *testing.T) {
	conf := &Config{
		AppName:       "data-server",
		AppVersion:    "v1",
		AppManagePort: 6060,
		DataPath:      "/export/Data/data-server",
		NodeID:        1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   nil,
		EndKey:     nil,
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng1)
	defer rng1.Clean()
	r2 := &metapb.Range{
		Id:         2,
		StartKey:   &metapb.Key{Type: metapb.KeyType_KT_NegativeInfinity},
		EndKey:     &metapb.Key{Key: []byte("e"), Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng2, err := NewRange(r2, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng2)
	defer rng2.Clean()
	r3 := &metapb.Range{
		Id:         3,
		StartKey:   &metapb.Key{Key: []byte("e"), Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Type: metapb.KeyType_KT_PositiveInfinity},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng3, err := NewRange(r3, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng3)
	defer rng3.Clean()
	// 等待选举产生leader
	time.Sleep(time.Second)
	var key, value []byte
	col := &metapb.Column{
		Name:       "test",
		Id:         1,
		DataType:   metapb.DataType_Varchar,
		PrimaryKey: 1,
	}
	key, err = util.EncodePrimaryKey(key, col, []byte("bbbbbbb"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("bbbbbbb"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq := &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header := &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	ctx := context.Background()
	insertResp, pbErr := svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	key = nil
	value = nil
	key, err = util.EncodePrimaryKey(key, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq = &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	getReq := &kvrpcpb.KvSelectRequest{
		Key:          key,
		FieldList:    []*kvrpcpb.SelectField{&kvrpcpb.SelectField{Typ: kvrpcpb.SelectField_Column, Column: col}},
		WhereFilters: []*kvrpcpb.Match{&kvrpcpb.Match{Column: col, Threshold: []byte("zzzzzz"), MatchType: kvrpcpb.MatchType_Equal}},
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr := svr.handleSelect(ctx, header.RangeId, getReq)
	if pbErr != nil {
		t.Errorf("raw get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if bytes.Compare(getResp.GetRows()[0].Fields, value) != 0 {
		t.Errorf("test failed, value[%s]", string(getResp.GetRows()[0].Fields))
		time.Sleep(time.Second)
		return
	}

	delReq := &kvrpcpb.KvDeleteRequest{
		Key: key,
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	delResp, pbErr := svr.handleDelete(ctx, header.RangeId, delReq)
	if pbErr != nil {
		t.Errorf("raw put failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if delResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	getReq = &kvrpcpb.KvSelectRequest{
		Key:          key,
		FieldList:    []*kvrpcpb.SelectField{&kvrpcpb.SelectField{Typ: kvrpcpb.SelectField_Column, Column: col}},
		WhereFilters: []*kvrpcpb.Match{&kvrpcpb.Match{Column: col, Threshold: []byte("zzzzzz"), MatchType: kvrpcpb.MatchType_Equal}},
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr = svr.handleSelect(ctx, header.RangeId, getReq)
	if pbErr != nil {
		t.Errorf("raw get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if getResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	if len(getResp.GetRows()) != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	t.Log("test success")
}

func TestRangeSplitInsertDelete(t *testing.T) {
	conf := &Config{
		AppName:       "data-server",
		AppVersion:    "v1",
		AppManagePort: 6060,
		DataPath:      "/export/Data/data-server",
		NodeID:        1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   nil,
		EndKey:     nil,
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng1)
	defer rng1.Clean()
	var split []byte
	col := &metapb.Column{
		Name:       "test",
		Id:         1,
		DataType:   metapb.DataType_Varchar,
		PrimaryKey: 1,
	}
	split, err = util.EncodePrimaryKey(split, col, []byte("eeeeeee"))
	r2 := &metapb.Range{
		Id:         2,
		StartKey:   &metapb.Key{Type: metapb.KeyType_KT_NegativeInfinity},
		EndKey:     &metapb.Key{Key: split, Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng2, err := NewRange(r2, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng2)
	defer rng2.Clean()
	r3 := &metapb.Range{
		Id:         3,
		StartKey:   &metapb.Key{Key: split, Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Type: metapb.KeyType_KT_PositiveInfinity},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng3, err := NewRange(r3, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng3)
	defer rng3.Clean()
	// 等待选举产生leader
	time.Sleep(time.Second)
	var key, value []byte

	key, err = util.EncodePrimaryKey(key, col, []byte("bbbbbbb"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("bbbbbbb"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq := &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header := &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	ctx := context.Background()
	insertResp, pbErr := svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}

	key = nil
	value = nil
	key, err = util.EncodePrimaryKey(key, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq = &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   3,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}
	if insertResp.AffectedKeys == 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}

	// 模拟分裂
	rng1.region.State = metapb.RangeState_R_Offline
	rng1.left = rng2.region
	rng1.right = rng3.region

	getReq := &kvrpcpb.KvSelectRequest{
		Key:          key,
		FieldList:    []*kvrpcpb.SelectField{&kvrpcpb.SelectField{Typ: kvrpcpb.SelectField_Column, Column: col}},
		WhereFilters: []*kvrpcpb.Match{&kvrpcpb.Match{Column: col, Threshold: []byte("zzzzzz"), MatchType: kvrpcpb.MatchType_Equal}},
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	getResp, pbErr := svr.handleSelect(ctx, header.RangeId, getReq)
	if pbErr == nil {
		t.Errorf("get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Errorf("get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if len(getResp.GetRows()) == 0 {
		t.Error("get failed")
		time.Sleep(time.Second)
		return
	}
	if bytes.Compare(getResp.GetRows()[0].Fields, value) != 0 {
		t.Errorf("test failed, value[%s]", string(getResp.GetRows()[0].Fields))
		time.Sleep(time.Second)
		return
	}

	delReq := &kvrpcpb.KvDeleteRequest{
		Key: key,
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	delResp, pbErr := svr.handleDelete(ctx, header.RangeId, delReq)
	if pbErr == nil {
		t.Errorf("delete failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Errorf("get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if delResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}

	key = nil
	value = nil
	key, err = util.EncodePrimaryKey(key, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq = &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr == nil {
		t.Errorf("raw get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Errorf("get failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	t.Log("test success")
}

func TestRangeQuery(t *testing.T) {

}

func TestRangeSplitQuery(t *testing.T) {
	conf := &Config{
		AppName:       "data-server",
		AppVersion:    "v1",
		AppManagePort: 6060,
		DataPath:      "/export/Data/data-server",
		NodeID:        1,

		LogDir:    "",
		LogModule: "node",
		LogLevel:  "debug",

		MasterServerAddrs: []string{"127.0.0.1:8887"},
		HeartbeatInterval: 10,
		RaftHeartbeatAddr: "127.0.0.1:1234",
		RaftReplicaAddr:   "127.0.0.1:1235",
	}
	svr := InitServer(conf)
	r1 := &metapb.Range{
		Id:         1,
		StartKey:   nil,
		EndKey:     nil,
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng1, err := NewRange(r1, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng1)
	defer rng1.Clean()
	var split []byte
	col := &metapb.Column{
		Name:       "test",
		Id:         1,
		DataType:   metapb.DataType_Varchar,
		PrimaryKey: 1,
	}
	split, err = util.EncodePrimaryKey(split, col, []byte("eeeeeee"))
	r2 := &metapb.Range{
		Id:         2,
		StartKey:   &metapb.Key{Type: metapb.KeyType_KT_NegativeInfinity},
		EndKey:     &metapb.Key{Key: split, Type: metapb.KeyType_KT_Ordinary},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng2, err := NewRange(r2, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng2)
	defer rng2.Clean()
	r3 := &metapb.Range{
		Id:         3,
		StartKey:   &metapb.Key{Key: split, Type: metapb.KeyType_KT_Ordinary},
		EndKey:     &metapb.Key{Type: metapb.KeyType_KT_PositiveInfinity},
		RangeEpoch: &metapb.RangeEpoch{1, 1},
		Peers:      []*metapb.Peer{&metapb.Peer{Id: 1, NodeId: svr.node.Id}},
		State:      metapb.RangeState_R_Init,
		DbName:     "test",
		TableName:  "test",
		CreateTime: time.Now().Unix(),
	}
	rng3, err := NewRange(r3, svr, nil, false)
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
		time.Sleep(time.Second)
		return
	}
	svr.AddRange(rng3)
	defer rng3.Clean()
	// 等待选举产生leader
	time.Sleep(time.Second)
	var key, value []byte

	key, err = util.EncodePrimaryKey(key, col, []byte("bbbbbbb"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("bbbbbbb"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq := &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header := &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	ctx := context.Background()
	insertResp, pbErr := svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   2,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}

	key = nil
	value = nil
	key, err = util.EncodePrimaryKey(key, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	value, err = util.EncodeColumnValue(value, col, []byte("zzzzzz"))
	if err != nil {
		t.Errorf("test failed, err[%v]", err)
	}
	insertReq = &kvrpcpb.KvInsertRequest{
		Rows:           []*kvrpcpb.KeyValue{&kvrpcpb.KeyValue{Key: key, Value: value}},
		CheckDuplicate: false,
		Timestamp:      svr.clock.Now(),
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   3,
		Timestamp: svr.clock.Now(),
	}
	insertResp, pbErr = svr.handleInsert(ctx, header.RangeId, insertReq)
	if pbErr != nil {
		t.Errorf("insert failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if insertResp.Code != 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}
	if insertResp.AffectedKeys == 0 {
		t.Errorf("insert failed")
		time.Sleep(time.Second)
		return
	}

	// 模拟分裂
	rng1.region.State = metapb.RangeState_R_Offline
	rng1.left = rng2.region
	rng1.right = rng3.region

	queryReq := &kvrpcpb.KvSelectRequest{
		Scope:        &kvrpcpb.Scope{Start: nil, Limit: nil},
		FieldList:    []*kvrpcpb.SelectField{&kvrpcpb.SelectField{Typ: kvrpcpb.SelectField_Column, Column: col}},
		WhereFilters: []*kvrpcpb.Match{&kvrpcpb.Match{Column: col, Threshold: []byte("zzzzzz"), MatchType: kvrpcpb.MatchType_Equal}},
		Limit:        &kvrpcpb.Limit{Offset: 0, Count: 10},
	}
	header = &kvrpcpb.RequestHeader{
		ClusterId: 1,
		RangeId:   1,
		Timestamp: svr.clock.Now(),
	}
	queryResp, pbErr := svr.handleSelect(ctx, header.RangeId, queryReq)
	if pbErr == nil {
		t.Errorf("query failed, resp[%v], err[%v]", queryResp, pbErr)
		time.Sleep(time.Second)
		return
	}
	if pbErr.RangeOffline == nil {
		t.Errorf("query failed, err[%v]", pbErr)
		time.Sleep(time.Second)
		return
	}
	if queryResp.GetRows() == nil {
		t.Error("query failed")
		time.Sleep(time.Second)
		return
	}

	t.Log("test success")
}
