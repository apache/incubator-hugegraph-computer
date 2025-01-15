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
	"encoding/binary"
	"sync/atomic"
	"unsafe"
	"vermeer/apps/common"
	"vermeer/apps/serialize"
)

type Edge struct {
	Source string
	Target string
}

func (e *Edge) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint16(buffer, uint16(len(e.Source)))
	offset += 2
	copy(buffer[offset:], e.Source)
	offset += len(e.Source)
	binary.BigEndian.PutUint16(buffer[offset:], uint16(len(e.Target)))
	offset += 2
	copy(buffer[offset:], e.Target)
	offset += len(e.Target)
	return offset, nil
}

func (e *Edge) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	size := binary.BigEndian.Uint16(buffer)
	offset += 2
	b := make([]byte, size)
	copy(b, buffer[offset:])
	e.Source = *(*string)(unsafe.Pointer(&b))
	offset += int(size)

	size = binary.BigEndian.Uint16(buffer[offset:])
	offset += 2
	b = make([]byte, size)
	copy(b, buffer[offset:])
	e.Target = *(*string)(unsafe.Pointer(&b))
	offset += int(size)

	return offset, nil
}

func (e *Edge) ToString() string {
	return ""
}

func (e *Edge) PredictSize() int {
	return 0
}

type IntEdge struct {
	Source uint32
	Target uint32
}

func (e *IntEdge) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint32(buffer, e.Source)
	binary.BigEndian.PutUint32(buffer[4:], e.Target)

	return 8, nil
}

func (e *IntEdge) Unmarshal(buffer []byte) (int, error) {
	e.Source = binary.BigEndian.Uint32(buffer)
	e.Target = binary.BigEndian.Uint32(buffer[4:])

	return 8, nil
}

func (e *IntEdge) ToString() string {
	return ""
}

func (e *IntEdge) PredictSize() int {
	return 0
}

type EdgeMem struct {
	useOutEdges  bool
	useOutDegree bool
	InEdges      serialize.TwoDimSliceUint32
	OutEdges     serialize.TwoDimSliceUint32
	OutDegree    []serialize.SUint32
	EdgeLocker   []common.SpinLocker
}

func (em *EdgeMem) Init(useOutEdges bool, useOutDegree bool) {
	em.useOutEdges = useOutEdges
	em.useOutDegree = useOutDegree
}

func (em *EdgeMem) GetInEdges(vertID uint32) serialize.SliceUint32 {
	return em.InEdges[vertID]
}

func (em *EdgeMem) GetOutEdges(vertID uint32) serialize.SliceUint32 {
	return em.OutEdges[vertID]
}

func (em *EdgeMem) UseOutEdges() bool {
	return em.useOutEdges
}

func (em *EdgeMem) UseOutDegree() bool {
	return em.useOutDegree
}

func (em *EdgeMem) GetOutDegree(vertexID uint32) serialize.SUint32 {
	return em.OutDegree[vertexID]
}

func (em *EdgeMem) AppendInEdge(vertID uint32, edge serialize.SUint32) {
	em.EdgeLocker[vertID].Lock()
	defer em.EdgeLocker[vertID].UnLock()
	em.InEdges[vertID] = append(em.InEdges[vertID], edge)
}

func (em *EdgeMem) AppendOutEdge(vertID uint32, edge serialize.SUint32) {
	em.EdgeLocker[vertID].Lock()
	defer em.EdgeLocker[vertID].UnLock()
	em.OutEdges[vertID] = append(em.OutEdges[vertID], edge)
}

func (em *EdgeMem) EdgeLockFunc(vertID uint32, fun func()) {
	em.EdgeLocker[vertID].Lock()
	defer em.EdgeLocker[vertID].UnLock()
	fun()
}

func (em *EdgeMem) AddOutDegree(vertexID uint32, degree uint32) {
	atomic.AddUint32((*uint32)(&em.OutDegree[vertexID]), degree)
}

func (em *EdgeMem) SetOutDegree(vertexID uint32, degree serialize.SUint32) {
	em.OutDegree[vertexID] = degree
}

func (em *EdgeMem) BuildEdge(edgeNums int, vertexCount uint32) {
	if edgeNums < 0 {
		edgeNums = 0
	}
	if edgeNums > 1000 {
		edgeNums = 1000
	}
	em.InEdges = make(serialize.TwoDimSliceUint32, vertexCount)
	for i := range em.InEdges {
		em.InEdges[i] = make(serialize.SliceUint32, 0, edgeNums)
	}
	if em.useOutEdges {
		em.OutEdges = make(serialize.TwoDimSliceUint32, vertexCount)
		for i := range em.OutEdges {
			em.OutEdges[i] = make(serialize.SliceUint32, 0, edgeNums)
		}
	}
	em.EdgeLocker = make([]common.SpinLocker, vertexCount)
}

func (em *EdgeMem) OptimizeEdgesMemory() {
	em.EdgeLocker = nil
	for i, e := range em.InEdges {
		ne := make(serialize.SliceUint32, 0, len(e))
		ne = append(ne, e...)
		em.InEdges[i] = ne
	}

	if em.useOutEdges {
		for i, e := range em.OutEdges {
			ne := make(serialize.SliceUint32, 0, len(e))
			ne = append(ne, e...)
			em.OutEdges[i] = ne
		}
	}
}

func (em *EdgeMem) OptimizeOutEdgesMemory() {
	em.EdgeLocker = nil
	for i, e := range em.OutEdges {
		ne := make(serialize.SliceUint32, 0, len(e))
		ne = append(ne, e...)
		em.OutEdges[i] = ne
	}
}

func (em *EdgeMem) BuildOutEdges(edgeNums int, vertexCount uint32) {
	em.OutEdges = make(serialize.TwoDimSliceUint32, vertexCount)
	for i := range em.OutEdges {
		em.OutEdges[i] = make(serialize.SliceUint32, 0, edgeNums)
	}
	em.EdgeLocker = make([]common.SpinLocker, vertexCount)
}

func (em *EdgeMem) BuildOutDegree(totalVertexCount uint32) {
	em.OutDegree = make([]serialize.SUint32, totalVertexCount)
}
