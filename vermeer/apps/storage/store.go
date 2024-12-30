/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package storage

import "fmt"

type StoreType int

const (
	StoreTypeLevelDB StoreType = iota
	StoreTypePebble
	StoreTypePD
)

type Store interface {
	NewBatch() Batch

	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	// Scan: return a channel of KeyValue. scan channel and get all kv pairs. only for small data.
	Scan() chan KeyValue
	// NewIterator: return a iterator. can be used to scan large data.
	NewIterator() Iterator
	Delete(key []byte) error
	FlushDB() error
	Close() error
	Path() string
	Compact() error
}

type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	BatchFull() bool
	Commit() error
}

type StoreOption struct {
	StoreName StoreType
	Path      string
	Fsync     bool
	ReadOnly  bool
	UseFilter bool
}

func StoreMaker(option StoreOption) (Store, error) {
	var store Store
	var err error
	switch option.StoreName {
	case StoreTypeLevelDB:
		store, err = NewLevelDBStore(option)
	case StoreTypePebble:
		store, err = NewPebbleStore(option)
	case StoreTypePD:

	default:
		err = fmt.Errorf("unknown store type: %v", option.StoreName)
	}
	if err != nil {
		return nil, err
	}
	return store, nil
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type Iterator interface {
	First() bool
	Valid() bool
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}
