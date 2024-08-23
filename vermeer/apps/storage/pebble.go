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
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const MaxBatchSize = 3 << 30

type PebbleStore struct {
	db   *pebble.DB
	wo   *pebble.WriteOptions
	path string
}

func NewPebbleStore(option StoreOption) (Store, error) {

	opts := &pebble.Options{
		L0CompactionThreshold:       4,
		L0CompactionFileThreshold:   10,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MemTableSize:                256 << 20, // 256 MB
		MemTableStopWritesThreshold: 4,
		MaxOpenFiles:                10240,
		MaxManifestFileSize:         128 * 1024 * 1024,
		MaxConcurrentCompactions:    func() int { return 5 },
		BytesPerSync:                512 * 1024 * 1024,
	}
	if !option.Fsync {
		opts.DisableWAL = true
	}
	c := pebble.NewCache(512 * 1024 * 1024)
	defer c.Unref()
	opts.Cache = c
	opts.EnsureDefaults()
	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		if option.UseFilter {
			if i < len(opts.Levels)-1 {
				l.FilterPolicy = bloom.FilterPolicy(10)
				l.FilterType = pebble.TableFilter
			}
		}
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}
	if option.ReadOnly {
		opts.ReadOnly = true
	}

	wo := &pebble.WriteOptions{}
	wo.Sync = option.Fsync

	db, err := pebble.Open(option.Path+"-pebble.db", opts)
	if err != nil {
		return nil, err
	}

	return &PebbleStore{
		db:   db,
		wo:   wo,
		path: option.Path,
	}, nil
}

func (s *PebbleStore) Close() error {
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}
func (s *PebbleStore) Path() string {
	return s.path
}

func (s *PebbleStore) NewBatch() Batch {
	return &PebbleBatch{
		batch: s.db.NewBatch(),
		wo:    s.wo,
	}
}

//func (s *PebbleStore) BatchSet(keys, values [][]byte) error {
//	wb := s.db.NewBatch()
//
//	for i, k := range keys {
//		err := wb.Set(k, values[i], s.wo)
//		if err != nil {
//			return err
//		}
//	}
//	return wb.Commit(s.wo)
//}

func (s *PebbleStore) Set(key, value []byte) error {
	// if s.useBatch() {
	// 	s.batch.Len()
	// 	return s.batch.Set(key, value, s.wo)
	// }
	return s.db.Set(key, value, s.wo)
}

func (s *PebbleStore) Get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	vByte := make([]byte, len(v))
	copy(vByte, v)
	_ = closer.Close()

	return vByte, nil
}

func (s *PebbleStore) BatchGet(keys [][]byte) ([][]byte, error) {
	var values = make([][]byte, len(keys))

	var err error
	var closer io.Closer
	for i, k := range keys {
		var v []byte
		v, closer, err = s.db.Get(k)
		if err != nil {
			return nil, err
		}
		values[i] = make([]byte, len(v))
		copy(values[i], v)
		_ = closer.Close()
	}
	return values, nil
}

func (s *PebbleStore) Scan() chan KeyValue {
	resultChan := make(chan KeyValue, 100)
	iter := s.db.NewIter(nil)
	go func() {
		defer func() {
			_ = iter.Close()
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

func (s *PebbleStore) NewIterator() Iterator {
	iter := s.db.NewIter(nil)
	return iter
}

func (s *PebbleStore) Delete(key []byte) error {
	// if s.useBatch() {
	// 	return s.batch.Delete(key, s.wo)
	// }
	return s.db.Delete(key, s.wo)
}

func (s *PebbleStore) FlushDB() error {
	return s.db.Flush()
}

func (s *PebbleStore) Compact() error {
	iter := s.db.NewIter(nil)
	var first, last []byte
	if iter.First() {
		first = append(first, iter.Key()...)
	}
	if iter.Last() {
		last = append(last, iter.Key()...)
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if err := s.db.Compact(first, last, true); err != nil {
		return err
	}
	return nil
}

type PebbleBatch struct {
	batch *pebble.Batch
	wo    *pebble.WriteOptions
}

func (s *PebbleBatch) Set(key, value []byte) error {
	return s.batch.Set(key, value, s.wo)
}

func (s *PebbleBatch) Delete(key []byte) error {
	return s.batch.Delete(key, s.wo)
}

func (s *PebbleBatch) Commit() error {
	return s.batch.Commit(s.wo)
}

func (s *PebbleBatch) BatchFull() bool {
	return s.batch.Len() >= MaxBatchSize
}
