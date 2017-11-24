// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"fmt"
	"os"

	"util/log"
)

// 日志前面加上dbID前缀
type dbLogger struct {
	dbID   uint64
	prefix string
	*log.Log
}

func newDBLogger(dbID uint64) *dbLogger {
	return &dbLogger{
		dbID:   dbID,
		prefix: fmt.Sprintf("DB(%d) ", dbID),
		Log:    log.GetFileLogger(),
	}
}

func (l *dbLogger) Debug(format string, v ...interface{}) {
	if l.IsEnableDebug() {
		l.Output(4, "[DEBUG]: "+l.prefix+fmt.Sprintf(format, v...), false)
	}
}

func (l *dbLogger) Info(format string, v ...interface{}) {
	if l.IsEnableInfo() {
		l.Output(4, "[INFO]: "+l.prefix+fmt.Sprintf(format, v...), false)
	}
}

func (l *dbLogger) Warn(format string, v ...interface{}) {
	if l.IsEnableWarn() {
		l.Output(4, "[WARN]: "+l.prefix+fmt.Sprintf(format, v...), false)
	}
}

func (l *dbLogger) Error(format string, v ...interface{}) {
	if l.IsEnableError() {
		l.Output(4, "[ERROR]: "+l.prefix+fmt.Sprintf(format, v...), false)
	}
}

func (l *dbLogger) Fatal(format string, v ...interface{}) {
	l.Output(4, "[FATAL]: "+l.prefix+fmt.Sprintf(format, v...), false)
	os.Exit(1)
}

func (l *dbLogger) Panic(format string, v ...interface{}) {
	l.Output(4, "[FATAL]: "+l.prefix+fmt.Sprintf(format, v...), false)
	panic(l.prefix + fmt.Sprintf(format, v...))
}
