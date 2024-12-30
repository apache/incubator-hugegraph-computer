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

package algorithms

import (
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/serialize"
)

func init() {
	Algorithms = append(Algorithms, new(TriangleCountMaker))
}

type TriangleCountMaker struct {
	compute.AlgorithmMakerBase
}

func (tc *TriangleCountMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &TriangleCountWorker{}
}

func (tc *TriangleCountMaker) CreateMasterComputer() compute.MasterComputer {
	return &TriangleCountMaster{}
}

func (tc *TriangleCountMaker) Name() string {
	return "triangle_count"
}

func (tc *TriangleCountMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

type TriangleCountWorker struct {
	compute.WorkerComputerBase
	triangleCount    []serialize.SUint32
	totalNeighborSet []serialize.MapUint32Struct
	selfNeighborSet  []map[serialize.SUint32]struct{}
}

func (tcw *TriangleCountWorker) VertexValue(i uint32) serialize.MarshalAble {
	if tcw.WContext.Step == 1 {
		return &tcw.totalNeighborSet[i]
	}
	return &tcw.triangleCount[i]
}

func (tcw *TriangleCountWorker) Init() error {
	tcw.triangleCount = make([]serialize.SUint32, tcw.WContext.GraphData.Vertex.TotalVertexCount())
	tcw.totalNeighborSet = make([]serialize.MapUint32Struct, tcw.WContext.GraphData.Vertex.TotalVertexCount())
	tcw.selfNeighborSet = make([]map[serialize.SUint32]struct{}, tcw.WContext.GraphData.VertexCount)
	return nil
}

func (tcw *TriangleCountWorker) Compute(vertexID uint32, pID int) {
	_ = pID
	vertID := vertexID - tcw.WContext.GraphData.VertIDStart
	switch tcw.WContext.Step {
	case 1:
		//if tcw.WContext.GraphData.BothEdges != nil {
		//	tcw.selfNeighborSet[vertID] = make(map[serialize.SUint32]struct{},
		//		len(tcw.WContext.GraphData.BothEdges[vertID]))
		//	tcw.totalNeighborSet[vertexID] = make(map[serialize.SUint32]struct{},
		//		len(tcw.WContext.GraphData.BothEdges[vertID])/2)
		//	for _, nID := range tcw.WContext.GraphData.BothEdges[vertID] {
		//		if uint32(nID) == vertexID {
		//			continue
		//		}
		//		tcw.selfNeighborSet[vertID][nID] = struct{}{}
		//		if uint32(nID) < vertexID {
		//			tcw.totalNeighborSet[vertexID][nID] = struct{}{}
		//		}
		//	}
		//} else {
		inEdges := tcw.WContext.GraphData.Edges.GetInEdges(vertID)
		outEdges := tcw.WContext.GraphData.Edges.GetOutEdges(vertID)
		tcw.selfNeighborSet[vertID] = make(map[serialize.SUint32]struct{},
			len(inEdges)+len(outEdges))
		tcw.totalNeighborSet[vertexID] = make(map[serialize.SUint32]struct{},
			(len(inEdges)+len(outEdges))/2)
		for _, nID := range inEdges {
			if uint32(nID) == vertexID {
				continue
			}
			tcw.selfNeighborSet[vertID][nID] = struct{}{}
			if uint32(nID) < vertexID {
				tcw.totalNeighborSet[vertexID][nID] = struct{}{}
			}
		}
		for _, nID := range outEdges {
			if uint32(nID) == vertexID {
				continue
			}
			tcw.selfNeighborSet[vertID][nID] = struct{}{}
			if uint32(nID) < vertexID {
				tcw.totalNeighborSet[vertexID][nID] = struct{}{}
			}
		}
		//}
	case 2:
		for fID := range tcw.selfNeighborSet[vertID] {
			tcw.triangleCount[vertexID] += tcw.findCross(tcw.selfNeighborSet[vertID], tcw.totalNeighborSet[fID])
		}
	}
}

func (tcw *TriangleCountWorker) findCross(a map[serialize.SUint32]struct{}, b map[serialize.SUint32]struct{}) serialize.SUint32 {
	var count serialize.SUint32 = 0
	if len(a) > len(b) {
		for k := range b {
			if _, ok := a[k]; ok {
				count++
			}
		}
	} else {
		for k := range a {
			if _, ok := b[k]; ok {
				count++
			}
		}
	}
	return count
}
func (tcw *TriangleCountWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type TriangleCountMaster struct {
	compute.MasterComputerBase
}

func (tcm *TriangleCountMaster) Compute() bool {
	return tcm.MContext.Step != 2
}
