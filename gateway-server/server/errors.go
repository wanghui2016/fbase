package server

import (
	"errors"
)

var (
	ErrInternalError      = errors.New("internal error")
	ErrNotExistDatabase   = errors.New("database not exist")
	ErrNotExistTable      = errors.New("table not exist")
	ErrNotExistNode       = errors.New("node not exist")
	ErrNotExistRange      = errors.New("range not exist")
	ErrNotExistPeer       = errors.New("range peer not exist")
	ErrInvalidColumn      = errors.New("invalid column")
	ErrNoRoute            = errors.New("no route")
	ErrExceedMaxLimit     = errors.New("exceeding the maximum limit")
	ErrEmptyRow           = errors.New("empty row")
	ErrHttpCmdUnknown 	= errors.New("invalid command")
	ErrHttpCmdParse 	= errors.New("parse error")
	ErrHttpCmdRun 		= errors.New("run error")
	ErrHttpCmdEmpty 	= errors.New("command empty")
)

const (
	errCommandUnknown 	= 1
	errCommandParse 	= 2
	errCommandRun 		= 3
	errCommandNoDb 		= 4
	errCommandNoTable 	= 5
	errCommandEmpty 	= 6
)

