// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"encoding/binary"
	"fmt"
	"time"
)

/* 内部value的组成:
 * | user value | expire at(uint64) | ttl flag(一字节) |
 * 如果没有ttl，则ttl flag为0；有的话，ttl flag为过期时间编码后的长度
 */

func internalValueSuffixLen(expireAt int64) int {
	if expireAt <= 0 {
		return 1 // only one byte flag
	}
	return 8 + 1
}

func makeInternalValueSuffix(dst []byte, expireAt int64) {
	if expireAt <= 0 {
		dst[0] = 0 // no expire
		return
	}
	// put expire at
	binary.BigEndian.PutUint64(dst, uint64(expireAt))
	dst[8] = 1
}

func parseInternalValue(ivalue []byte) (uvalue []byte, expiredAt int64, err error) {
	if len(ivalue) == 0 {
		err = fmt.Errorf("invalid internal value length(zero)")
		return
	}
	flag := int(ivalue[len(ivalue)-1])
	if flag == 0 { // no ttl
		return ivalue[:len(ivalue)-1], 0, nil
	}
	if len(ivalue) < 9 {
		err = fmt.Errorf("invalid internal value length(%d)", len(ivalue))
		return
	}
	expiredAt = int64(binary.BigEndian.Uint64(ivalue[len(ivalue)-9:]))
	uvalue = ivalue[:len(ivalue)-9]
	return
}

func isExpired(expiredAt int64) bool {
	return expiredAt > 0 && expiredAt < time.Now().UnixNano()
}

func isInternalValueExpired(ivalue []byte) (bool, error) {
	_, expireAt, err := parseInternalValue(ivalue)
	if err != nil {
		return false, err
	}
	return isExpired(expireAt), nil
}
