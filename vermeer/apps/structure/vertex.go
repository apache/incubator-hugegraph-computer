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

package structure

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"
	"unsafe"
	"vermeer/apps/common"
	"vermeer/apps/serialize"
	"vermeer/apps/storage"

	"github.com/allegro/bigcache/v3"
	"github.com/sirupsen/logrus"
)

type Vertex struct {
	ID string
}

func (v *Vertex) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint16(buffer, uint16(len(v.ID)))
	offset += 2
	copy(buffer[2:], v.ID)
	offset += len(v.ID)
	return offset, nil
}

func (v *Vertex) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	size := binary.BigEndian.Uint16(buffer)
	offset += 2
	b := make([]byte, size)
	copy(b, buffer[2:])
	v.ID = *(*string)(unsafe.Pointer(&b))
	offset += int(size)
	return offset, nil
}

func (v *Vertex) ToString() string {
	return ""
}

func (v *Vertex) PredictSize() int {
	return 2 + len(v.ID)
}

type SliceVertex []Vertex

func (sv *SliceVertex) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*sv)))
	offset += 4

	for _, v := range *sv {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (sv *SliceVertex) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*sv = make([]Vertex, length)
	for i := range *sv {
		var v Vertex
		n, err := v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		(*sv)[i] = v
		offset += n
	}
	return offset, nil
}

func (sv *SliceVertex) ToString() string {
	return ""
}

func (sv *SliceVertex) PredictSize() int {
	size := 4
	for _, v := range *sv {
		size += v.PredictSize()
	}
	return size
}

func (sv *SliceVertex) Partition(limit int) []serialize.SlicePartition {

	p := make([]serialize.SlicePartition, 0)

	length := len(*sv)

	index := 0
	size := 4
	for i, v := range *sv {
		size += v.PredictSize()
		if size >= limit {
			var one serialize.SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne serialize.SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p
}

// Partition 不可用 因为[]Vertex不可以转换成[]serialize.MarshalAble
func Partition(s []serialize.MarshalAble, limit int) []serialize.SlicePartition {
	p := make([]serialize.SlicePartition, 0)

	length := len(s)

	index := 0
	size := 4
	for i, v := range s {
		size += v.PredictSize()
		if size >= limit {
			var one serialize.SlicePartition
			one.Start = index
			one.Size = size
			one.End = i
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne serialize.SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length - 1
			p = append(p, lastOne)
		}

	}
	return p
}

type VertexMem struct {
	TotalVertex     []Vertex
	VertexLongIDMap map[string]uint32
}

func (vm *VertexMem) Init(dataDir string) {
	vm.TotalVertex = make([]Vertex, 0)
	_ = dataDir
}

func (vm *VertexMem) TotalVertexCount() uint32 {
	return uint32(len(vm.TotalVertex))
}

func (vm *VertexMem) GetVertex(vertexID uint32) Vertex {
	return vm.TotalVertex[vertexID]
}

func (vm *VertexMem) GetVertexIndex(vertex string) (uint32, bool) {
	value, ok := vm.VertexLongIDMap[vertex]
	return value, ok
}

func (vm *VertexMem) AppendVertices(vertex ...Vertex) {
	vm.TotalVertex = append(vm.TotalVertex, vertex...)
}

func (vm *VertexMem) SetVertex(vertexID uint32, vertex Vertex) {
	vm.TotalVertex[vertexID] = vertex
}

func (vm *VertexMem) SetVertices(offset uint32, vertex ...Vertex) {
	vID := serialize.SUint32(offset)
	for _, v := range vertex {
		vm.TotalVertex[vID] = v
		vID++
	}
}

func (vm *VertexMem) RecastVertex(totalCount int64, vertStart uint32, workers []*GraphWorker) {
	if len(workers) > 1 {
		oldVerts := vm.TotalVertex
		vm.TotalVertex = make([]Vertex, totalCount)
		logrus.Infof("recast make total vertex complete")
		for i := range oldVerts {
			vm.TotalVertex[vertStart+uint32(i)] = oldVerts[i]
		}
	}
}

func (vm *VertexMem) BuildVertexMap() {
	vm.VertexLongIDMap = make(map[string]uint32, len(vm.TotalVertex))

	for i, v := range vm.TotalVertex {
		vm.VertexLongIDMap[v.ID] = uint32(i)
	}
}

func (vm *VertexMem) deleteData() {}

func (vm *VertexMem) freeMem() {}

var cacheSize = 10 * 1024 * 1024 * 1024

type VertexInDB struct {
	sliceStores      []storage.Store
	mapStores        []storage.Store
	mapCache         *bigcache.BigCache
	workers          []*GraphWorker
	totalVertexCount uint32
	idSeed           int32
	dataDir          string
}

func (vm *VertexInDB) Init(dataDir string) {
	var err error
	vm.dataDir = vm.makeDataDir(dataDir)
	vm.sliceStores = make([]storage.Store, 1)
	vm.sliceStores[0], err = storage.StoreMaker(storage.StoreOption{
		StoreName: storage.StoreTypePebble,
		Path:      path.Join(vm.dataDir, "self_vertex_slice"),
		Fsync:     false,
		ReadOnly:  false,
		UseFilter: false,
	})
	if err != nil {
		logrus.Errorf("init vertex store error:%v", err)
		return
	}
}

func (vm *VertexInDB) makeDataDir(dataDir string) string {
	return path.Join(dataDir, "graph_db")
}

func (vm *VertexInDB) TotalVertexCount() uint32 {
	return vm.totalVertexCount
}

func (vm *VertexInDB) vertexStoreID(vertexID uint32) int {
	if len(vm.workers) == 1 {
		return 0
	}
	for i, worker := range vm.workers {
		if vertexID >= worker.VertIdStart && vertexID < worker.VertIdStart+worker.VertexCount {
			return i
		}
	}
	return 0
}

func (vm *VertexInDB) longVertexStoreID(vertex string) int {
	if len(vm.workers) == 1 {
		return 0
	}
	return common.HashBKDR(vertex) % len(vm.workers)
}

func (vm *VertexInDB) GetVertex(vertexID uint32) Vertex {
	storeID := vm.vertexStoreID(vertexID)
	vID := serialize.SUint32(vertexID)
	if vID > serialize.SUint32(vm.totalVertexCount) {
		logrus.Errorf("vertex id:%d is out of range", vertexID)
		return Vertex{}
	}
	if vm.workers[storeID].IsSelf {
		vID -= serialize.SUint32(vm.workers[storeID].VertIdStart)
	}

	bytes := make([]byte, vID.PredictSize())
	_, err := vID.Marshal(bytes)
	if err != nil {
		logrus.Errorf("marshal vertex error:%v", err)
		return Vertex{}
	}
	longID, err := vm.sliceStores[storeID].Get(bytes)
	if err != nil {
		logrus.Errorf("get vertex error:%v", err)
		return Vertex{}
	}
	return Vertex{ID: string(longID)}
}

func (vm *VertexInDB) GetVertexIndex(vertex string) (uint32, bool) {
	storeID := vm.longVertexStoreID(vertex)

	start := vm.workers[storeID].VertIdStart
	end := start + vm.workers[storeID].VertexCount

	if vm.mapCache != nil {
		// vByte := []byte(vertex)
		if sID, err := vm.mapCache.Get(vertex); err == nil {
			vID := serialize.SUint32(0)
			_, err = vID.Unmarshal(sID)
			if err != nil {
				logrus.Errorf("unmarshal vertex error:%v", err)
				return 0, false
			}
			if vID < serialize.SUint32(start) || vID >= serialize.SUint32(end) {
				logrus.Warnf("get vertex index from cache is out of range, read from storage. vertex:%v vid:%v , start:%v, end:%v", vertex, vID, start, end)
			} else {
				return uint32(vID), true
			}
		}
	}

	// storeID := vm.longVertexStoreID(vertex)
	sID, err := vm.mapStores[storeID].Get([]byte(vertex))
	if err != nil {
		// logrus.Errorf("get vertex error:%v", err)
		return 0, false
	}
	vID := serialize.SUint32(0)
	_, err = vID.Unmarshal(sID)
	if err != nil {
		logrus.Errorf("unmarshal vertex error:%v", err)
		return 0, false
	}
	if vm.mapCache != nil {
		err = vm.mapCache.Set(vertex, sID)
		if err != nil {
			logrus.Errorf("set vertex cache error:%v", err)
		}
	}
	if vID < serialize.SUint32(start) || vID >= serialize.SUint32(end) {
		logrus.Errorf("get vertex index from db:%v vertex:%v vid:%v is out of range, start:%v, end:%v", vm.mapStores[storeID].Path(), vertex, vID, start, end)
		return 0, false
	}
	return uint32(vID), true
}

func (vm *VertexInDB) AppendVertices(vertex ...Vertex) {
	batch := vm.sliceStores[0].NewBatch()
	vertexCopy := make([]Vertex, len(vertex))
	copy(vertexCopy, vertex)
	for _, v := range vertexCopy {
		vID := serialize.SUint32(vm.idSeed)
		vm.idSeed++
		bytes := make([]byte, vID.PredictSize())
		_, err := vID.Marshal(bytes)
		if err != nil {
			logrus.Errorf("marshal vertex error:%v", err)
			continue
		}
		err = batch.Set(bytes, []byte(v.ID))
		if err != nil {
			logrus.Errorf("set vertex error:%v", err)
			continue
		}
	}
	err := batch.Commit()
	if err != nil {
		logrus.Errorf("commit vertex error:%v", err)
	}
	// update vertex count
	vm.totalVertexCount = uint32(vm.idSeed)
}

func (vm *VertexInDB) SetVertex(vertexID uint32, vertex Vertex) {
	vID := serialize.SUint32(vertexID)
	bytes := make([]byte, vID.PredictSize())
	_, err := vID.Marshal(bytes)
	if err != nil {
		logrus.Errorf("marshal vertex error:%v", err)
		return
	}
	err = vm.sliceStores[vm.vertexStoreID(vertexID)].Set(bytes, []byte(vertex.ID))
	if err != nil {
		logrus.Errorf("set vertex error:%v", err)
		return
	}
}

func (vm *VertexInDB) SetVertices(offset uint32, vertex ...Vertex) {
	vID := serialize.SUint32(offset)
	storeID := vm.vertexStoreID(uint32(vID))
	sliceBatch := vm.sliceStores[storeID].NewBatch()
	mapBatch := vm.mapStores[storeID].NewBatch()
	vertexCopy := make([]Vertex, len(vertex))
	copy(vertexCopy, vertex)
	for _, v := range vertexCopy {
		bytes := make([]byte, vID.PredictSize())
		_, err := vID.Marshal(bytes)
		vID++
		if err != nil {
			logrus.Errorf("marshal vertex error:%v", err)
			continue
		}
		err = sliceBatch.Set(bytes, []byte(v.ID))
		if err != nil {
			logrus.Errorf("set vertex error:%v", err)
			continue
		}
		err = mapBatch.Set([]byte(v.ID), bytes)
		if err != nil {
			logrus.Errorf("set vertex map error:%v", err)
			continue
		}
	}
	err := sliceBatch.Commit()
	if err != nil {
		logrus.Errorf("commit vertex error:%v", err)
	}
	err = mapBatch.Commit()
	if err != nil {
		logrus.Errorf("commit vertex map error:%v", err)
	}
}

func (vm *VertexInDB) RecastVertex(totalCount int64, vertStart uint32, workers []*GraphWorker) {
	_ = totalCount
	_ = vertStart
	vm.workers = workers
	selfStore := vm.sliceStores[0]
	vm.sliceStores = make([]storage.Store, len(vm.workers))
	var err error
	for i := range vm.sliceStores {
		if workers[i].IsSelf {
			vm.sliceStores[i] = selfStore
			err := vm.sliceStores[i].Compact()
			if err != nil {
				logrus.Errorf("compact vertex slice store error:%v", err)
			}
		} else {
			vm.sliceStores[i], err = storage.StoreMaker(storage.StoreOption{
				StoreName: storage.StoreTypePebble,
				Path:      path.Join(vm.dataDir, fmt.Sprintf("%v_vertex_slice", i)),
				Fsync:     false,
				ReadOnly:  false,
				UseFilter: false,
			})
			if err != nil {
				logrus.Errorf("create vertex slice store error:%v", err)
				continue
			}
		}
	}
	// sum vertex count
	vm.totalVertexCount = 0
	for _, w := range workers {
		vm.totalVertexCount += w.VertexCount
	}

	// build vertex map
	vm.mapStores = make([]storage.Store, len(vm.workers))
	for id := range workers {
		vm.mapStores[id], err = storage.StoreMaker(storage.StoreOption{
			StoreName: storage.StoreTypePebble,
			Path:      path.Join(vm.dataDir, fmt.Sprintf("%v_vertex_map", id)),
			Fsync:     false,
			ReadOnly:  false,
			UseFilter: false,
		})
		if err != nil {
			logrus.Errorf("create vertex map store error:%v", err)
			return
		}
	}
}

func (vm *VertexInDB) BuildVertexMap() {
	var err error
	config := bigcache.Config{
		Shards:             1024,
		LifeWindow:         10 * time.Minute,
		CleanWindow:        0,
		MaxEntriesInWindow: 1000 * 10 * 60,
		MaxEntrySize:       500,
		Verbose:            false,
		HardMaxCacheSize:   cacheSize / 1024 / 1024,
		OnRemove:           nil,
		OnRemoveWithReason: nil,
	}
	vm.mapCache, err = bigcache.New(context.Background(), config)
	if err != nil {
		logrus.Errorf("create vertex map cache error:%v", err)
		return
	}
	id := 0
	for i, worker := range vm.workers {
		if worker.IsSelf {
			id = i
			break
		}
	}
	// sizePerStore := uint64(cacheSize) / uint64(len(vm.sliceStores))
	// wg := &sync.WaitGroup{}
	// vm.mapStores = make([]storage.Store, len(vm.sliceStores))
	// for i, store := range vm.sliceStores {
	// wg.Add(1)
	// go func(id int, sliceStore storage.Store) {
	// defer wg.Done()

	// err = sliceStore.FlushDB()
	// if err != nil {
	// 	logrus.Errorf("flush vertex slice store error:%v", err)
	// 	return
	// }
	// isSelf := vm.workers[id].IsSelf

	// make map store
	// vm.mapStores[id], err = storage.StoreMaker(storage.StoreOption{
	// 	StoreName: storage.StoreTypePebble,
	// 	Path:      path.Join(vm.dataDir, fmt.Sprintf("%v_vertex_map", id)),
	// 	Fsync:     false,
	// 	ReadOnly:  false,
	// 	UseFilter: false,
	// })
	// if err != nil {
	// 	logrus.Errorf("create vertex map store error:%v", err)
	// 	return
	// }
	// make batch
	batch := vm.mapStores[id].NewBatch()

	// get new iterator
	iter := vm.sliceStores[id].NewIterator()
	if iter == nil {
		logrus.Errorf("create vertex map store iterator error:%v", err)
		return
	}
	var size uint64
	for iter.First(); iter.Valid(); iter.Next() {
		// get vertex
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())

		vID := serialize.SUint32(0)
		_, err = vID.Unmarshal(key)
		if err != nil {
			logrus.Errorf("unmarshal vertex error:%v", err)
			continue
		}
		// if isSelf {
		vID += serialize.SUint32(vm.workers[id].VertIdStart)
		// }
		bytes := make([]byte, vID.PredictSize())
		_, err = vID.Marshal(bytes)
		if err != nil {
			logrus.Errorf("marshal vertex error:%v", err)
			continue
		}

		if size < uint64(cacheSize) {
			size += uint64(len(value) + len(bytes))
			// set map cache
			err = vm.mapCache.Set(string(value), bytes)
			if err != nil {
				logrus.Errorf("set vertex map cache, longID:%v ,error:%v", string(value), err)
				continue
			}
		}
		// set map store
		err = batch.Set(value, bytes)
		if err != nil {
			logrus.Errorf("set vertex map store, longID:%v ,error:%v", string(value), err)
			continue
		}
		// commit full batch
		if batch.BatchFull() {
			err = batch.Commit()
			if err != nil {
				logrus.Errorf("commit vertex map store error:%v", err)
			}
			batch = vm.mapStores[id].NewBatch()
		}
	}
	err = iter.Close()
	if err != nil {
		logrus.Errorf("close vertex map store iterator error:%v", err)
	}
	err = batch.Commit()
	if err != nil {
		logrus.Errorf("commit vertex map store error:%v", err)
	}
	// logrus.Infof("build vertex map store:%v done", id)
	// }(i, store)
	// }
	// wg.Wait()

	logrus.Infof("map cache entry count:%v", vm.mapCache.Len())
	// vm.mapCache.ResetStatistics()

	// compact all map store
	wg := &sync.WaitGroup{}
	for i := range vm.mapStores {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := vm.mapStores[idx].Compact()
			if err != nil {
				logrus.Errorf("compact vertex map store error:%v", err)
			}
			logrus.Infof("compact vertex map store:%v done", idx)
		}(i)
	}
	wg.Wait()
}

type VertexInDBMeta struct {
	Workers          []*GraphWorker `json:"workers"`
	TotalVertexCount uint32         `json:"total_vertex_count"`
	DataDir          string         `json:"data_dir"`
}

func (vm *VertexInDB) save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup) {
	if vm.mapCache != nil {
		logrus.Infof("cache len:%v", vm.mapCache.Len())
		logrus.Infof("hit rate:%.2f", float64(vm.mapCache.Stats().Hits)/float64(vm.mapCache.Stats().Hits+vm.mapCache.Stats().Misses))
		vm.mapCache.Close()
		vm.mapCache = nil
	}
	// 1. save graph and store meta
	dataMeta := VertexInDBMeta{}
	dataMeta.DataDir = vm.dataDir
	dataMeta.TotalVertexCount = vm.totalVertexCount
	dataMeta.Workers = vm.workers
	bytes, err := json.Marshal(dataMeta)
	if err != nil {
		logrus.Errorf("marshal vertex meta error:%v", err)
	}
	err = os.WriteFile(path.Join(dir, "vertex_in_db__meta"), bytes, 0644)
	if err != nil {
		logrus.Errorf("write vertex meta error:%v", err)
	}

	// 2. close all store
	for i, worker := range vm.workers {
		wg.Add(1)
		go func(id int, worker *GraphWorker) {
			defer wg.Done()
			defer logrus.Infof("save store:%v-%v done", vm.dataDir, id)
			err := vm.sliceStores[id].FlushDB()
			if err != nil {
				logrus.Errorf("flush vertex slice store error:%v,store:%v", err, vm.dataDir)
			}
			err = vm.mapStores[id].FlushDB()
			if err != nil {
				logrus.Errorf("flush vertex map store error:%v,store:%v", err, vm.dataDir)
			}
		}(i, worker)

	}
}

func (vm *VertexInDB) freeMem() {
	for id := range vm.sliceStores {
		err := vm.sliceStores[id].Close()
		if err != nil {
			logrus.Errorf("close vertex slice store error:%v,store:%v", err, vm.dataDir)
		}
		logrus.Infof("close vertex slice store:%v", vm.dataDir)
	}

	for id := range vm.mapStores {
		err := vm.mapStores[id].Close()
		if err != nil {
			logrus.Errorf("close vertex map store error:%v,store:%v", err, vm.dataDir)
		}
		logrus.Infof("close vertex map store:%v", vm.dataDir)
	}
}

func (vm *VertexInDB) load(meta GraphMeta, dir string, wg *sync.WaitGroup) {
	// 1. load graph and store meta
	bytes, err := os.ReadFile(path.Join(dir, "vertex_in_db__meta"))
	if err != nil {
		logrus.Errorf("read vertex meta error:%v,store:%v", err, vm.dataDir)
	}
	dataMeta := VertexInDBMeta{}
	err = json.Unmarshal(bytes, &dataMeta)
	if err != nil {
		logrus.Errorf("unmarshal vertex meta error:%v,store:%v", err, vm.dataDir)
	}
	vm.dataDir = dataMeta.DataDir
	vm.totalVertexCount = dataMeta.TotalVertexCount
	vm.workers = dataMeta.Workers

	// 2. open all store
	vm.sliceStores = make([]storage.Store, len(vm.workers))
	vm.mapStores = make([]storage.Store, len(vm.workers))

	for i, worker := range vm.workers {
		wg.Add(1)
		go func(id int, worker *GraphWorker) {
			defer wg.Done()
			defer logrus.Infof("open store:%v-%v done", vm.dataDir, id)
			if worker.IsSelf {
				vm.sliceStores[id], err = storage.StoreMaker(storage.StoreOption{
					StoreName: storage.StoreTypePebble,
					Path:      path.Join(vm.dataDir, "self_vertex_slice"),
					Fsync:     false,
					ReadOnly:  false,
					UseFilter: false,
				})
			} else {
				vm.sliceStores[id], err = storage.StoreMaker(storage.StoreOption{
					StoreName: storage.StoreTypePebble,
					Path:      path.Join(vm.dataDir, fmt.Sprintf("%v_vertex_slice", id)),
					Fsync:     false,
					ReadOnly:  false,
					UseFilter: false,
				})
			}
			if err != nil {
				logrus.Errorf("open vertex slice store error:%v,store:%v", err, vm.dataDir)
			}
			vm.mapStores[id], err = storage.StoreMaker(storage.StoreOption{
				StoreName: storage.StoreTypePebble,
				Path:      path.Join(vm.dataDir, fmt.Sprintf("%v_vertex_map", id)),
				Fsync:     false,
				ReadOnly:  false,
				UseFilter: false,
			})
			if err != nil {
				logrus.Errorf("open vertex map store error:%v,store:%v", err, vm.dataDir)
			}
		}(i, worker)

	}
}

func (vm *VertexInDB) deleteData() {
	err := os.RemoveAll(vm.dataDir)
	if err != nil {
		logrus.Errorf("delete vertex data error:%v", err)
	}
}
