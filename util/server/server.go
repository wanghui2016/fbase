package server

import (
	"bufio"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"io"
	"os"
	"net"
	"net/http/pprof"

	"util/gogc"
	"util/log"
	"util/metrics"
	"golang.org/x/net/netutil"
)

type ServerConfig struct {
	Ip      string
	Port    string
	Version string
	ConnLimit   int
}

type ServiceHttpHandler struct {
	Verifier func(w http.ResponseWriter, r *http.Request) bool
	Proxy func(w http.ResponseWriter, r *http.Request) bool
	Handler func(w http.ResponseWriter, r *http.Request)
}

//type ServiceRpcHandler func( /*req*/ *messagepb.Message) (/*resp*/ *messagepb.Message)
type ServiceRpcHandler func(/*wr*/ *bufio.ReadWriter)
type ServiceTcpHandler func(/*wr*/ *bufio.ReadWriter)

type Stats struct {
	Rangeid            uint64
	Calls              uint64
	Microseconds       uint64
	InputBytes         uint64
	InputMicroseconds  uint64
	OutputBytes        uint64
	OutputMicroseconds uint64
}

// Server is a http server
type Server struct {
	name    string
	ip      string
	port    string
	version string

	debugLock sync.Mutex

	wg sync.WaitGroup

	connsLock sync.Mutex
	conns     map[*conn]struct{}

	connLimit int
	l      net.Listener
	closed int64

	// handles.
	handlers  map[string]ServiceHttpHandler
	rpcMethod string
	handleRpc ServiceRpcHandler
	tcpMethod string
	handleTcp ServiceTcpHandler

	metricMeter *metrics.MetricMeter
}

// NewServer creates the server with given configuration.
func NewServer() *Server {
	return &Server{name: "", handlers: nil}
}

func (s *Server) GetConnectNumber() int {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()
	return len(s.conns)
}

func (s *Server) Init(name string, config *ServerConfig, output metrics.Output) {
	if config == nil {
		panic("invalid server config")
	}
	s.name = name
	s.ip = config.Ip
	s.port = config.Port
	s.version = config.Version
	s.connLimit = config.ConnLimit
	s.handlers = make(map[string]ServiceHttpHandler, 10)
	s.conns = make(map[*conn]struct{}, 10000)
	if output != nil {
		s.metricMeter = metrics.NewMetricMeter(s.name+s.ip+":"+s.port, output)
	}
	s.rpcMethod = "/" + s.name + "/rpc"
	s.tcpMethod = "/" + s.name + "/tcp"

	s.Handle("/debug/ping", ServiceHttpHandler{Handler: func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
		return
	}})
	//s.Handle("/debug/pprof/", ServiceHttpHandler(netPprof.Index))
	//s.Handle("/debug/pprof/cmdline", ServiceHttpHandler(netPprof.Cmdline))
	//s.Handle("/debug/pprof/profile", ServiceHttpHandler(netPprof.Profile))
	//s.Handle("/debug/pprof/symbol", ServiceHttpHandler(netPprof.Symbol))
	//s.Handle("/debug/pprof/trace", ServiceHttpHandler(netPprof.Trace))
	s.Handle("/debug/pprof/", ServiceHttpHandler{Handler: DebugPprofHandler})
	s.Handle("/debug/pprof/cmdline", ServiceHttpHandler{Handler: DebugPprofCmdlineHandler})
	s.Handle("/debug/pprof/profile", ServiceHttpHandler{Handler: DebugPprofProfileHandler})
	s.Handle("/debug/pprof/symbol", ServiceHttpHandler{Handler: DebugPprofSymbolHandler})
	s.Handle("/debug/pprof/trace", ServiceHttpHandler{Handler: DebugPprofTraceHandler})
	s.Handle("/debug/gc", ServiceHttpHandler{Handler: func(w http.ResponseWriter, r *http.Request) {
		gogc.PrintGCSummary(w)
	    return
	}})

	//s.Handle("/fbase-debug", ServiceHttpHandler(func(w http.ResponseWriter, r *http.Request) {
	//	// setup output
	//	var output io.Writer = w
	//	var fd uintptr
	//	if file := r.FormValue("file"); file != "" {
	//		f, err := os.Create(file)
	//		if err != nil {
	//			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
	//			return
	//		}
	//		fd = f.Fd()
	//		defer f.Close()
	//		output = f
	//	}
	//
	//	option := r.FormValue("option")
	//	var debugLevel int
	//	switch option {
	//	case "memprof":
	//		runtime.GC()
	//		pprof.WriteHeapProfile(output)
	//		return
	//	case "cpu":
	//		s.debugLock.Lock()
	//		defer s.debugLock.Unlock()
	//		pprof.StartCPUProfile(output)
	//		time.Sleep(time.Second * 60)
	//		pprof.StopCPUProfile()
	//		return
	//	case "heap":
	//		if fd > 0 {
	//			debug.WriteHeapDump(fd)
	//		}
	//		debugLevel = 1
	//	case "threadcreate":
	//		debugLevel = 1
	//	case "block":
	//		debugLevel = 1
	//	case "goroutine":
	//		debugLevel = 2
	//	case "gc":
	//		gogc.PrintGCSummary(w)
	//		return
	//	default:
	//		return
	//	}
	//	p := pprof.Lookup(option)
	//	if p == nil {
	//		log.Error("fbase-debug: pprof option not exist(%v)", option)
	//		return
	//	}
	//	if err := p.WriteTo(output, debugLevel); err != nil {
	//		log.Error("fbase-debug: write pprof failed(%v)", err)
	//	}
	//}))
}

func (s *Server) Handle(name string, handler ServiceHttpHandler) {
	if _, ok := s.handlers[name]; ok {
		panic("duplicate register http handler")
	}
	s.handlers[name] = handler
}

func (s *Server) RpcHandle(handler ServiceRpcHandler) {
	rpc := s.handlers[s.rpcMethod]
	if rpc.Handler == nil && s.handleRpc == nil {
		s.handlers[s.rpcMethod] = ServiceHttpHandler{Handler: s.rpcHandler}
		s.handleRpc = handler
	} else {
		panic("duplicate register rpc handler")
	}
}

func (s *Server) TcpHandle(handler ServiceTcpHandler) {
	tcp := s.handlers[s.tcpMethod]
	if tcp.Handler == nil && s.handleTcp == nil {
		s.handlers[s.tcpMethod] = ServiceHttpHandler{Handler: s.tcpHandler}
		s.handleTcp = handler
	} else {
		panic("duplicate register tcp handler")
	}
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	log.Info("closing server")
	s.closeAllConnections()
	if s.metricMeter != nil {
		s.metricMeter.Stop()
	}
	if s.l != nil {
		s.l.Close()
	}
	s.wg.Wait()

	log.Info("close server")
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

func (s *Server) rpcHandler(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}

	err = conn.SetDeadline(time.Time{})
	if err != nil {
		log.Error("conn set deadline failed, err[%v]", err)
		conn.Close()
		return
	}

	c, err := newConn(s, conn, bufrw)
	if err != nil {
		log.Error("create rpc conn failed, err[%v]", err)
		conn.Close()
		return
	}

	s.wg.Add(1)
	go c.rpc()
}

func (s *Server) tcpHandler(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Error("server doesn't support hijacking: conn %v", w)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error("http hijack failed, err[%v]", err)
		return
	}

	err = conn.SetDeadline(time.Time{})
	if err != nil {
		log.Error("conn set deadline failed, err[%v]", err)
		conn.Close()
		return
	}

	c, err := newConn(s, conn, bufrw)
	if err != nil {
		log.Error("create tcp conn failed, err[%v]", err)
		conn.Close()
		return
	}

	s.wg.Add(1)
	go c.work()
}

// Run runs the server.
func (s *Server) Run() {
	var l net.Listener
	var err error
	host := s.ip+":"+s.port
	l, err = net.Listen("tcp", host)
	if err != nil {
		log.Fatal("Listen: %v", err)
	}
	if s.connLimit > 0 {
		l = netutil.LimitListener(l, s.connLimit)
	}

	err = http.Serve(l, s)
	if err != nil {
		log.Fatal("http.listenAndServe failed: %s", err.Error())
	}
	s.l = l
	return
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("! ", r)
		}
	}()
	// Before hijacke any connection, make sure the ms is initialized.
	if s.isClosed() {
		return
	}

	method := s.getReqMethod(r.RequestURI)
	handler := s.handlers[method]
	if handler.Handler == nil {
		if strings.HasPrefix(method, "/debug/pprof/") {
			handler = s.handlers["/debug/pprof/"]
		}
		if handler.Handler == nil {
			NotFound(w, r)
			return
		}
	}
	if handler.Verifier != nil {
		if ok := handler.Verifier(w, r); !ok {
			return
		}
	}
	if handler.Proxy != nil {
		if ok := handler.Proxy(w, r); ok {
			return
		}
	}
	start := time.Now()
	handler.Handler(w, r)
	end := time.Now()
	if s.metricMeter != nil {
		if method != s.rpcMethod && method != s.tcpMethod {
			s.metricMeter.AddApiWithDelay(method, true, end.Sub(start))
		}
	}

	return
}

func (s *Server) Name() string {
	return s.name
}

func (s *Server) getReqMethod(url string) string {
	var invalidMethod string
	index := strings.Index(url, "?")
	if index <= 0 {
		index = len(url)
	}
	method := url[0:index]
	if len(method) <= 0 {
		log.Warn("check method failed:handle[%s]", method)
		return invalidMethod
	}
	return method
}

func (s *Server) closeAllConnections() {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()

	if len(s.conns) == 0 {
		return
	}

	for conn := range s.conns {
		err := conn.close()
		if err != nil {
			log.Warn("close conn failed - %v", err)
		}
	}

	s.conns = make(map[*conn]struct{})
}

// Helper handlers

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to w.
// The error message should be plain text.
func Error(w http.ResponseWriter, error string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, error)
}

// NotFound replies to the request with an HTTP 404 not found error.
func NotFound(w http.ResponseWriter, r *http.Request) { Error(w, "404 page not found", http.StatusNotFound) }

// NotFoundHandler returns a simple request handler
// that replies to each request with a ``404 page not found'' reply.
func NotFoundHandler() ServiceHttpHandler { return ServiceHttpHandler{Handler: NotFound} }

type ResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

func NewResponseWriter(w http.ResponseWriter, writer io.Writer) *ResponseWriter {
	return &ResponseWriter{ResponseWriter:w, writer: writer }
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.writer == nil {
		return w.Write(b)
	} else  {
		return w.writer.Write(b)
	}
}

func DebugPprofHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Index(ww, r)
}

func DebugPprofCmdlineHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Cmdline(ww, r)
}

func DebugPprofProfileHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Profile(ww, r)
}

func DebugPprofSymbolHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Symbol(ww, r)
}

func DebugPprofTraceHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("fbase-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Trace(ww, r)
}