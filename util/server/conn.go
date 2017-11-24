package server

import (
	"bufio"
	"io"
	"net"
	"strings"

	"github.com/juju/errors"
)

const (
	readBufferSize  = 8 * 1024
	writeBufferSize = 8 * 1024
)

type conn struct {
	s *Server

	rb   *bufio.Reader
	wb   *bufio.Writer
	conn net.Conn
}

func newConn(s *Server, netConn net.Conn, bufrw *bufio.ReadWriter) (*conn, error) {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()
	c := &conn{
		s:    s,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: netConn,
	}

	s.conns[c] = struct{}{}

	return c, nil
}

func (c *conn) rpc() {
	defer func() {
		c.s.wg.Done()
		c.close()

		c.s.connsLock.Lock()
		delete(c.s.conns, c)
		c.s.connsLock.Unlock()
	}()

	rw := bufio.NewReadWriter(c.rb, c.wb)
	c.s.handleRpc(rw)
}

func (c *conn) work() {
	defer func() {
		c.s.wg.Done()
		c.close()

		c.s.connsLock.Lock()
		delete(c.s.conns, c)
		c.s.connsLock.Unlock()
	}()
	wr := bufio.NewReadWriter(c.rb, c.wb)
	c.s.handleTcp(wr)
}

func (c *conn) close() error {
	if err := c.conn.Close(); isUnexpectedConnError(err) {
		return errors.Trace(err)
	}
	return nil
}

var errClosed = errors.New("use of closed network connection")

func isUnexpectedConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Cause(err) == io.EOF {
		return false
	}
	if strings.Contains(err.Error(), errClosed.Error()) {
		return false
	}
	return true
}
