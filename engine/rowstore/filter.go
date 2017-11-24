// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"engine/rowstore/filter"
)

// 对filter的封装，实现从user key 到 internalkety的适配
type iFilter struct {
	filter.Filter
}

func (f iFilter) NewGenerator() filter.FilterGenerator {
	return iFilterGenerator{f.Filter.NewGenerator()}
}

func (f iFilter) Contains(filter, key []byte) bool {
	return f.Filter.Contains(filter, internalKey(key).ukey())
}

type iFilterGenerator struct {
	filter.FilterGenerator
}

func (g iFilterGenerator) Add(key []byte) {
	g.FilterGenerator.Add(internalKey(key).ukey())
}
