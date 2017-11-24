// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// Copyright (c) 2017, JD FBASE Team <fbase@jd.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package rowstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	ts "model/pkg/timestamp"
)

// ManifestVersion vesrion
const ManifestVersion = 1

// TableFileMeta  table file meta
type TableFileMeta struct {
	Num       int64       `json:"num"`
	FileSize  int64       `json:"fsize"`
	StartIKey internalKey `json:"start_ikey"`
	EndIKey   internalKey `json:"end_ikey"`
	KVSize    uint64      `json:"kvsize"`
	Shared    bool        `json:"shared"`
}

// ManifestMeta  manifest meta
type ManifestMeta struct {
	Applied     uint64          `json:"applied"`
	Timestamp   ts.Timestamp    `json:"timestamp"`
	NextFileNum int64           `json:"next_file_num"`
	Version     int             `json:"version"`
	TableFiles  []TableFileMeta `json:"table_files"`
}

func newMainfestMeta() *ManifestMeta {
	return &ManifestMeta{
		Version: ManifestVersion,
	}
}

// AddTableFile add table file
func (m *ManifestMeta) AddTableFile(t *tFile) {
	m.TableFiles = append(m.TableFiles, TableFileMeta{
		Num:       t.fd.Num,
		FileSize:  t.size,
		StartIKey: t.imin,
		EndIKey:   t.imax,
		KVSize:    t.kvsize,
		Shared:    t.shared,
	})
}

// LoadManifestMeta load manifest meta
func LoadManifestMeta(rd io.Reader) (*ManifestMeta, error) {
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, fmt.Errorf("read manifest failed: %v", err)
	}
	meta := &ManifestMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, fmt.Errorf("json unmarshal failed: %v", err)
	}

	// 检查字段是否齐全
	if !meta.Timestamp.Valid() {
		return nil, errors.New("timestamp is misssing")
	}
	if meta.Applied == 0 {
		return nil, errors.New("applied is misssing")
	}
	if meta.NextFileNum == 0 {
		return nil, errors.New("next-file-num is missing")
	}
	if len(meta.TableFiles) == 0 {
		return nil, errors.New("table files is missing")
	}
	if meta.Version == 0 {
		return nil, errors.New("meta version is missing")
	}
	return meta, nil
}

// StoreManifestMeta store manifest meta
func StoreManifestMeta(meta *ManifestMeta, wr io.Writer) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("json marshal failed(%v)", err)
	}
	_, err = wr.Write(data)
	return err
}
