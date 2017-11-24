// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package opt

// CompactTimestampLimiter  上层对compact的timestamp约束
type CompactTimestampLimiter interface {
	//  AtMost 大于timestamp的不要compact
	AtMost() (timestamp uint64)
}
