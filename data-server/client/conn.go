package client

import (
	"time"

	"model/pkg/kvrpcpb"
	"model/pkg/schpb"
	"google.golang.org/grpc"
)

type SchConn struct {
	addr   string
	conn   *grpc.ClientConn
	Cli    schpb.SchServerClient

	closed bool
}

func (c *SchConn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.conn.Close()
}

type KvConn struct {
	addr   string
	conn   *grpc.ClientConn
	Cli    kvrpcpb.KvServerClient

	closed bool
}

func (c *KvConn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.conn.Close()
}

type createConnFunc func(addr string) (*KvConn, error)

type Pool struct {
	size int64
	pool  []*KvConn
}

func NewPool(size int, addr string, fun createConnFunc) (*Pool, error) {
	var pool []*KvConn
	for i := 0; i < size; i++ {
		conn, err := fun(addr)
		if err != nil {
			return nil, err
		}
		pool = append(pool, conn)
	}
	return &Pool{size: int64(size), pool: pool}, nil
}

func (p *Pool) GetConn() *KvConn {
	index := time.Now().UnixNano() % p.size
	return p.pool[index]
}

func (p *Pool) Close() {
	for _, c := range p.pool {
		c.Close()
	}
}