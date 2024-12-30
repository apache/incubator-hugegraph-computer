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
	Algorithms = append(Algorithms, new(ClusteringCoefficientMaker))
}

type ClusteringCoefficientMaker struct {
	compute.AlgorithmMakerBase
}

func (cc *ClusteringCoefficientMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &ClusteringCoefficientWorker{}
}

func (cc *ClusteringCoefficientMaker) CreateMasterComputer() compute.MasterComputer {
	return &ClusteringCoefficientMaster{}
}

func (cc *ClusteringCoefficientMaker) Name() string {
	return "clustering_coefficient"
}

func (cc *ClusteringCoefficientMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

type ClusteringCoefficientWorker struct {
	compute.WorkerComputerBase
	ccValues         []serialize.SFloat32
	totalNeighborSet []serialize.MapUint32Struct
	selfNeighborSet  []map[serialize.SUint32]struct{}
}

func (ccw *ClusteringCoefficientWorker) VertexValue(i uint32) serialize.MarshalAble {
	if ccw.WContext.Step == 1 {
		return &ccw.totalNeighborSet[i]
	}
	return &ccw.ccValues[i]
}

func (ccw *ClusteringCoefficientWorker) Init() error {
	ccw.ccValues = make([]serialize.SFloat32, ccw.WContext.GraphData.Vertex.TotalVertexCount())
	ccw.totalNeighborSet = make([]serialize.MapUint32Struct, ccw.WContext.GraphData.Vertex.TotalVertexCount())
	ccw.selfNeighborSet = make([]map[serialize.SUint32]struct{}, ccw.WContext.GraphData.VertexCount)
	return nil
}

func (ccw *ClusteringCoefficientWorker) Compute(vertexID uint32, pID int) {
	_ = pID
	vertID := vertexID - ccw.WContext.GraphData.VertIDStart
	switch ccw.WContext.Step {
	case 1:
		inEdges := ccw.WContext.GraphData.Edges.GetInEdges(vertID)
		outEdges := ccw.WContext.GraphData.Edges.GetOutEdges(vertID)
		ccw.selfNeighborSet[vertID] = make(map[serialize.SUint32]struct{},
			len(inEdges)+len(outEdges))
		ccw.totalNeighborSet[vertexID] = make(map[serialize.SUint32]struct{},
			(len(inEdges)+len(outEdges))/2)
		for _, nID := range inEdges {
			if uint32(nID) == vertexID {
				continue
			}
			ccw.selfNeighborSet[vertID][nID] = struct{}{}
			if uint32(nID) < vertexID {
				ccw.totalNeighborSet[vertexID][nID] = struct{}{}
			}
		}
		for _, nID := range outEdges {
			if uint32(nID) == vertexID {
				continue
			}
			ccw.selfNeighborSet[vertID][nID] = struct{}{}
			if uint32(nID) < vertexID {
				ccw.totalNeighborSet[vertexID][nID] = struct{}{}
			}
		}
	case 2:
		length := len(ccw.selfNeighborSet[vertID])
		if length > 1 {
			var triangleCount serialize.SUint32
			for fID := range ccw.selfNeighborSet[vertID] {
				triangleCount += ccw.findCross(ccw.selfNeighborSet[vertID], ccw.totalNeighborSet[fID])
			}
			ccw.ccValues[vertexID] = 2 * serialize.SFloat32(triangleCount) /
				serialize.SFloat32((length)*(length-1))
		} else {
			ccw.ccValues[vertexID] = 0
		}
	}
}

func (ccw *ClusteringCoefficientWorker) findCross(a map[serialize.SUint32]struct{}, b map[serialize.SUint32]struct{}) serialize.SUint32 {
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
func (ccw *ClusteringCoefficientWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type ClusteringCoefficientMaster struct {
	compute.MasterComputerBase
}

func (ccm *ClusteringCoefficientMaster) Compute() bool {
	return ccm.MContext.Step != 2
}
