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

import (
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbStore struct {
	mu    sync.RWMutex
	db    *leveldb.DB
	path  string
	fsync bool
	wo    *opt.WriteOptions
	// batch *leveldb.Batch
}

func NewLevelDBStore(option StoreOption) (Store, error) {
	opts := &opt.Options{NoSync: !option.Fsync, Compression: opt.NoCompression}
	db, err := leveldb.OpenFile(option.Path+"-leveldb.db", opts)
	if err != nil {
		return nil, err
	}
	return &LeveldbStore{
		db:    db,
		path:  option.Path,
		fsync: option.Fsync,
		wo:    &opt.WriteOptions{Sync: option.Fsync},
	}, nil
}

func (s *LeveldbStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Close()
	return nil
}

func (s *LeveldbStore) Path() string {
	return s.path
}

func (s *LeveldbStore) Compact() error {
	return s.db.CompactRange(util.Range{Start: nil, Limit: nil})
}
func (s *LeveldbStore) NewBatch() Batch {
	return &leveldbBatch{
		batch: &leveldb.Batch{},
		db:    s.db,
		wo:    s.wo,
	}
}

// func (s *LeveldbStore) useBatch() bool {
// 	return s.batch != nil
// }

func (s *LeveldbStore) Set(key, value []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// if !s.useBatch() {
	return s.db.Put(key, value, s.wo)
	// }
	// s.batch.Put(key, value)
	// return nil
}

func (s *LeveldbStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, err := s.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s *LeveldbStore) Scan() chan KeyValue {
	resultChan := make(chan KeyValue, 100)
	iter := s.db.NewIterator(nil, nil)
	go func() {
		defer func() {
			close(resultChan)
		}()
		ok := iter.First()
		if !ok {
			return
		}
		for {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			value := make([]byte, len(iter.Value()))
			copy(value, iter.Value())
			resultChan <- KeyValue{
				Key:   key,
				Value: value,
			}
			ok := iter.Next()
			if !ok {
				break
			}
		}
	}()
	return resultChan
}

func (s *LeveldbStore) NewIterator() Iterator {
	iter := s.db.NewIterator(nil, nil)
	levelIter := &levelIterator{iter}
	return levelIter
}

func (s *LeveldbStore) Delete(key []byte) error {
	// if !s.useBatch() {
	return s.db.Delete(key, s.wo)
	// }
	// s.batch.Delete(key)
	// return nil
}

func (s *LeveldbStore) FlushDB() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Close()
	err := os.RemoveAll(s.path)
	if err != nil {
		return err
	}
	s.db = nil
	var opts *opt.Options
	if !s.fsync {
		opts = &opt.Options{NoSync: !s.fsync}
	}
	db, err := leveldb.OpenFile(s.path, opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

type levelIterator struct {
	iterator.Iterator
}

func (li *levelIterator) Close() error {
	li.Release()
	return nil
}

type leveldbBatch struct {
	db    *leveldb.DB
	wo    *opt.WriteOptions
	batch *leveldb.Batch
}

func (s *leveldbBatch) Set(key, value []byte) error {
	s.batch.Put(key, value)
	return nil
}

func (s *leveldbBatch) Delete(key []byte) error {
	s.batch.Delete(key)
	return nil
}

func (s *leveldbBatch) Commit() error {
	if err := s.db.Write(s.batch, s.wo); err != nil {
		return err
	}

	return nil
}

func (s *leveldbBatch) BatchFull() bool {
	return s.batch.Len() >= MaxBatchSize
}
