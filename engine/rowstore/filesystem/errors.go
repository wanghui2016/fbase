// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package filesystem

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidFile = errors.New("fbase/engine/filesystem: invalid file for argument")
	ErrLocked      = errors.New("fbase/engine/filesystem: already locked")
	ErrClosed      = errors.New("fbase/engine/filesystem: closed")
)

type ErrCorrupted struct {
	Fd  FileDesc
	Err error
}

func (e *ErrCorrupted) Error() string {
	if !e.Fd.Zero() {
		return fmt.Sprintf("%v [file=%v]", e.Err, e.Fd)
	}
	return e.Err.Error()
}

func NewErrCorrupted(fd FileDesc, err error) error {
	return &ErrCorrupted{fd, err}
}

func IsCorrupted(err error) bool {
	switch err.(type) {
	case *ErrCorrupted:
		return true
	}
	return false
}

type ErrMissingFiles struct {
	Fds []FileDesc
}

func (e *ErrMissingFiles) Error() string { return "file missing" }

func SetFd(err error, fd FileDesc) error {
	switch x := err.(type) {
	case *ErrCorrupted:
		x.Fd = fd
		return x
	}
	return err
}
