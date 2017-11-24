package client

import (
	"time"
	"sync"
	"errors"

	"model/pkg/kvrpcpb"
	"util/log"
	"google.golang.org/grpc"
	"util/context"
	"google.golang.org/grpc/codes"
)

const (
	WriteTimeout      = 10 * time.Second
	ReadTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.
)

const (
	dialTimeout       = 5 * time.Second
	// 128 KB
	DefaultInitialWindowSize int32 = 1024 * 64
	DefaultPoolSize           int = 1
)

// errInvalidResponse represents response message is invalid.
var (
	ErrGrpcError = errors.New("grpc error")
	ErrNoLeader = errors.New("no leader")
	ErrInvalidResponse = errors.New("invalid response")
	ErrInvalidResponseHeader = errors.New("response header not set")

	// ErrClientConnClosing indicates that the operation is illegal because
	// the ClientConn is closing.
	ErrClientConnClosing = errors.New("grpc: the client connection is closing")
	// ErrClientConnTimeout indicates that the ClientConn cannot establish the
	// underlying connections within the specified timeout.
	// DEPRECATED: Please use context.DeadlineExceeded instead.
	ErrClientConnTimeout = errors.New("grpc: timed out when dialing")

	// errNoTransportSecurity indicates that there is no transport security
	// being set for ClientConn. Users should either set one or explicitly
	// call WithInsecure DialOption to disable security.
	ErrNoTransportSecurity = errors.New("grpc: no transport security set (use grpc.WithInsecure() explicitly or set credentials)")
	// errTransportCredentialsMissing indicates that users want to transmit security
	// information (e.g., oauth2 token) which requires secure connection on an insecure
	// connection.
	ErrTransportCredentialsMissing = errors.New("grpc: the credentials require transport level security (use grpc.WithTransportCredentials() to set)")
	// errCredentialsConflict indicates that grpc.WithTransportCredentials()
	// and grpc.WithInsecure() are both called for a connection.
	ErrCredentialsConflict = errors.New("grpc: transport credentials are set for an insecure connection (grpc.WithTransportCredentials() and grpc.WithInsecure() are both called)")
	// errNetworkIO indicates that the connection is down due to some network I/O error.
	ErrNetworkIO = errors.New("grpc: failed with network I/O error")
	// errConnDrain indicates that the connection starts to be drained and does not accept any new RPCs.
	ErrConnDrain = errors.New("grpc: the connection is drained")
	// errConnClosing indicates that the connection is closing.
	ErrConnClosing = errors.New("grpc: the connection is closing")
	// errConnUnavailable indicates that the connection is unavailable.
	ErrConnUnavailable = errors.New("grpc: the connection is unavailable")
	// errBalancerClosed indicates that the balancer is closed.
	ErrBalancerClosed = errors.New("grpc: balancer is closed")
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type KvClient interface {
	// Close should release all data.
	Close() error
	// SendKVReq sends kv request.
	RawPut(addr string, req *kvrpcpb.DsKvRawPutRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawPutResponse, error)
	RawGet(addr string, req *kvrpcpb.DsKvRawGetRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawGetResponse, error)
	RawDelete(addr string, req *kvrpcpb.DsKvRawDeleteRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawDeleteResponse, error)
	Insert(addr string, req *kvrpcpb.DsKvInsertRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvInsertResponse, error)
	Select(addr string, req *kvrpcpb.DsKvSelectRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvSelectResponse, error)
	Delete(addr string, req *kvrpcpb.DsKvDeleteRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvDeleteResponse, error)
}

type KvRpcClient struct {
	lock      sync.RWMutex
	poolSize  int
	winSize   int32
	set       map[string]*Pool
}


func NewRPCClient(opts ...int) (KvClient) {
	var size int
	if len(opts) == 0 {
		size = DefaultPoolSize
	} else if len(opts) > 1 {
		log.Panic("invalid client param!!!")
		return nil
	} else {
		size = opts[0]
		if size == 0 {
			log.Panic("invalid client param!!!")
			return nil
		}
	}
	set := make(map[string]*Pool)
	return &KvRpcClient{poolSize: size, winSize: DefaultInitialWindowSize, set: set}
}

func NewRPCClientWithOpts(size int, winSize int32) (KvClient) {
	if size == 0 {
		log.Panic("invalid client param size !!!")
		return nil
	}
	if winSize < DefaultInitialWindowSize {
		log.Panic("invalid client param win size!!!")
		return nil
	}
	set := make(map[string]*Pool)
	return &KvRpcClient{poolSize: size, winSize: winSize, set: set}
}

func (c *KvRpcClient) Close() error {
	for _, conn := range c.set {
		conn.Close()
	}
	return nil
}

func (c *KvRpcClient) RawPut(addr string, req *kvrpcpb.DsKvRawPutRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawPutResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx := context.GetContext()
	ctx.Reset(readTimeout)
	resp, err := conn.Cli.KvRawPut(ctx, req)
	context.PutContext(ctx)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			return nil, ErrConnUnavailable
		}
		return nil, errors.New(grpc.ErrorDesc(err))
	}
	return resp, nil
}

func (c *KvRpcClient) RawGet(addr string, req *kvrpcpb.DsKvRawGetRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawGetResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx := context.GetContext()
	ctx.Reset(readTimeout)
	resp, err := conn.Cli.KvRawGet(ctx, req)
	context.PutContext(ctx)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			return nil, ErrConnUnavailable
		}
		return nil, errors.New(grpc.ErrorDesc(err))
	}
	return resp, nil
}

func (c *KvRpcClient) RawDelete(addr string, req *kvrpcpb.DsKvRawDeleteRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvRawDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx := context.GetContext()
	ctx.Reset(readTimeout)
	resp, err := conn.Cli.KvRawDelete(ctx, req)
	context.PutContext(ctx)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			return nil, ErrConnUnavailable
		}
		return nil, errors.New(grpc.ErrorDesc(err))
	}
	return resp, nil
}

func (c *KvRpcClient) Insert(addr string, req *kvrpcpb.DsKvInsertRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvInsertResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx := context.GetContext()
	ctx.Reset(readTimeout)
	resp, err := conn.Cli.KvInsert(ctx, req)
	context.PutContext(ctx)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			return nil, ErrConnUnavailable
		}
		return nil, errors.New(grpc.ErrorDesc(err))
	}
	return resp, nil
}

func (c *KvRpcClient) Select(addr string, req *kvrpcpb.DsKvSelectRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvSelectResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx := context.GetContext()
	ctx.Reset(readTimeout)
	resp, err := conn.Cli.KvSelect(ctx, req)
	context.PutContext(ctx)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			return nil, ErrConnUnavailable
		}
		return nil, errors.New(grpc.ErrorDesc(err))
	}
	return resp, nil
}

func (c *KvRpcClient) Delete(addr string, req *kvrpcpb.DsKvDeleteRequest, writeTimeout, readTimeout time.Duration) (*kvrpcpb.DsKvDeleteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx := context.GetContext()
	ctx.Reset(readTimeout)
	resp, err := conn.Cli.KvDelete(ctx, req)
	context.PutContext(ctx)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			return nil, ErrConnUnavailable
		}
		return nil, errors.New(grpc.ErrorDesc(err))
	}
	return resp, nil
}

func (c *KvRpcClient) getConn(addr string) (*KvConn, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	var pool *Pool
	var ok bool
	var err error
	c.lock.RLock()
	if pool, ok = c.set[addr]; ok {
		c.lock.RUnlock()
		return pool.GetConn(), nil
	}
	c.lock.RUnlock()
	c.lock.Lock()
	defer c.lock.Unlock()
	// 已经建立链接了
	if pool, ok = c.set[addr]; ok {
		return pool.GetConn(), nil
	}
	pool, err = NewPool(c.poolSize, addr, func(_addr string) (*KvConn, error){
		gc, _err := grpc.Dial(_addr, grpc.WithInsecure(), grpc.WithTimeout(dialTimeout), grpc.WithInitialWindowSize(c.winSize))
		if _err != nil {
			log.Error("did not connect addr[%s], err[%v]", _addr, _err)
			return nil, err
		}
		cli := kvrpcpb.NewKvServerClient(gc)
		return &KvConn{addr: addr, conn: gc, Cli: cli}, nil
	})
	if err != nil {
		log.Error("new pool[address %s] failed, err[%v]", addr, err)
		return nil, err
	}

	c.set[addr] = pool
	return pool.GetConn(), nil
}

