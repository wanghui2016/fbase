package client

import (
	"sync"
	"errors"
	"time"

	"model/pkg/metapb"
	"google.golang.org/grpc"
	"util/log"
	"model/pkg/schpb"
	"golang.org/x/net/context"
)

const (
	RequestDsTimeout   = time.Second
	ConnectDsTimeout   = time.Second * 3
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type SchClient interface {
	// Close should release all data.
	Close() error
	// SendKVReq sends kv request.
	CreateRange(addr string, r *metapb.Range) error
	DeleteRange(addr string, rangeId uint64) error
	OfflineRange(addr string, rangeId uint64) error
	SplitRangePrepare(addr string, rangeId uint64) (bool, error)
	TransferRangeLeader(addr string, rangeId uint64) error
	// 关闭分片的存储[紧急修复使用]
	CloseRange(addr string, rangeId uint64) error
	// 停止读写[紧急修复使用]
	StopRange(addr string, rangeId uint64) error
}

type SchRpcClient struct {
	lock      sync.RWMutex
	pool      map[string]*SchConn
}

func NewSchClient() SchClient {
	return &SchRpcClient{pool: make(map[string]*SchConn)}
}

func (c *SchRpcClient) Close() error {
	for _, conn := range c.pool {
		conn.Close()
	}
	return nil
}

func (c *SchRpcClient) CreateRange(addr string, r *metapb.Range) error {
	conn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	req := &schpb.CreateRangeRequest{
		Header: &schpb.RequestHeader{},
		Range: r,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	_, err = conn.Cli.CreateRange(ctx, req)
	if err != nil {
		return errors.New(grpc.ErrorDesc(err))
	}

	return nil
}

func (c *SchRpcClient) DeleteRange(addr string, rangeId uint64) error {
	conn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	req := &schpb.DeleteRangeRequest{
		Header: &schpb.RequestHeader{},
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	_, err = conn.Cli.DeleteRange(ctx, req)
	if err != nil {
		return errors.New(grpc.ErrorDesc(err))
	}
	return nil
}

func (c *SchRpcClient) OfflineRange(addr string, rangeId uint64) error {
	conn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	req := &schpb.OfflineRangeRequest{
		Header: &schpb.RequestHeader{},
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	_, err = conn.Cli.OfflineRange(ctx, req)
	if err != nil {
		return errors.New(grpc.ErrorDesc(err))
	}
	return nil
}

func (c *SchRpcClient) SplitRangePrepare(addr string, rangeId uint64) (bool, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return false, err
	}
	req := &schpb.SplitRangePrepareRequest{
		Header: &schpb.RequestHeader{},
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	resp, err := conn.Cli.SplitRangePrepare(ctx, req)
	if err != nil {
		return false, errors.New(grpc.ErrorDesc(err))
	}
	return resp.GetReady(), nil
}

func (c *SchRpcClient) TransferRangeLeader(addr string, rangeId uint64) error {
	conn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	req := &schpb.TransferRangeLeaderRequest{
		Header: &schpb.RequestHeader{},
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	_, err = conn.Cli.TransferRangeLeader(ctx, req)
	if err != nil {
		return errors.New(grpc.ErrorDesc(err))
	}
	return nil
}

func (c *SchRpcClient) CloseRange(addr string, rangeId uint64) error {
	conn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	req := &schpb.CloseRangeRequest{
		Header: &schpb.RequestHeader{},
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	_, err = conn.Cli.CloseRange(ctx, req)
	if err != nil {
		return errors.New(grpc.ErrorDesc(err))
	}
	return nil
}

func (c *SchRpcClient) StopRange(addr string, rangeId uint64) error {
	conn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	req := &schpb.StopRangeRequest{
		Header: &schpb.RequestHeader{},
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestDsTimeout)
	defer cancel()
	_, err = conn.Cli.StopRange(ctx, req)
	if err != nil {
		return errors.New(grpc.ErrorDesc(err))
	}
	return nil
}

func (c *SchRpcClient) getConn(addr string) (*SchConn, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	var conn *SchConn
	var ok bool
	var err error
	c.lock.RLock()
	if conn, ok = c.pool[addr]; ok {
		c.lock.RUnlock()
		return conn, nil
	}
	c.lock.RUnlock()
	c.lock.Lock()
	defer c.lock.Unlock()
	// 已经建立链接了
	if conn, ok = c.pool[addr]; ok {
		return conn, nil
	}
	gc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(ConnectDsTimeout))
	if err != nil {
		log.Error("did not connect addr[%s], err[%v]", addr, err)
		return nil, err
	}
	cli := schpb.NewSchServerClient(gc)
	conn = &SchConn{addr: addr, conn: gc, Cli: cli}
	c.pool[addr] = conn
	return conn, nil
}

