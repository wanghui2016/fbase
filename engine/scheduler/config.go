// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package scheduler

const (
	DefaultConcurrency    = 4
	DefaultWriteRateLimit = 1024 * 1024 * 30
)

// Config scheduler configs
type Config struct {
	Concurrency    int
	WriteRateLimit int64 // 每秒多少字节, 每个并发（协程）
}

// GetConcurrency get concurrency
func (c *Config) GetConcurrency() int {
	if c == nil || c.Concurrency == 0 {
		return DefaultConcurrency
	}
	return c.Concurrency
}

// GetWriteRateLimit get write rate limit
func (c *Config) GetWriteRateLimit() int64 {
	if c == nil || c.WriteRateLimit == 0 {
		return DefaultWriteRateLimit
	}
	return c.WriteRateLimit
}
