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
	"fmt"
	"math/rand"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(ClosenessCentralityMaker))
}

//normalized closeness centrality

type ClosenessCentralityMaker struct {
	compute.AlgorithmMakerBase
}

func (cc *ClosenessCentralityMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &ClosenessCentralityWorker{}
}

func (cc *ClosenessCentralityMaker) CreateMasterComputer() compute.MasterComputer {
	return &ClosenessCentralityMaster{}
}

func (cc *ClosenessCentralityMaker) Name() string {
	return "closeness_centrality"
}

func (cc *ClosenessCentralityMaker) DataNeeded() []string {
	return []string{}
}

type ClosenessCentralityWorker struct {
	compute.WorkerComputerBase
	wfImproved      bool
	sampleRate      float64
	stopSum         []serialize.SInt32
	closenessValues []serialize.SFloat32
	tempEdges       [][]serialize.SUint32
	dfsOutsideEdge  [][]serialize.SUint32
	neighborEdges   []serialize.SliceUint32
	pMap            []map[uint32]int32
}

func (ccw *ClosenessCentralityWorker) VertexValue(i uint32) serialize.MarshalAble {
	if ccw.WContext.Step == 1 {
		return &ccw.neighborEdges[i]
	}

	return &ccw.closenessValues[i]
}

func (ccw *ClosenessCentralityWorker) Init() error {
	ccw.closenessValues = make([]serialize.SFloat32, ccw.WContext.GraphData.Vertex.TotalVertexCount())
	ccw.neighborEdges = make([]serialize.SliceUint32, ccw.WContext.GraphData.Vertex.TotalVertexCount())
	ccw.dfsOutsideEdge = make([][]serialize.SUint32, ccw.WContext.GraphData.VertexCount)
	ccw.stopSum = make([]serialize.SInt32, ccw.WContext.Parallel)
	ccw.pMap = make([]map[uint32]int32, ccw.WContext.GraphData.VertexCount)
	for i := range ccw.pMap {
		ccw.pMap[i] = make(map[uint32]int32, len(ccw.WContext.GraphData.Edges.GetInEdges(uint32(i))))
		ccw.pMap[i][uint32(i)] = 0
	}
	ccw.tempEdges = make([][]serialize.SUint32, ccw.WContext.Parallel)
	for i := range ccw.tempEdges {
		ccw.tempEdges[i] = make([]serialize.SUint32, 0, 100)
	}
	ccw.sampleRate = options.GetFloat(ccw.WContext.Params, "closeness_centrality.sample_rate")
	if ccw.sampleRate > 1.0 || ccw.sampleRate <= 0 {
		logrus.Errorf("The param closeness_centrality.sample_rate must be in (0.0, 1.0], actual got '%v",
			ccw.sampleRate)
		return fmt.Errorf("the param closeness_centrality.sample_rate must be in (0.0, 1.0], actual got '%v",
			ccw.sampleRate)
	}
	//wf_improved:bool. default true
	//		If True, scale by the fraction of nodes reachable. This gives the
	//      Wasserman and Faust improved formula. For single component graphs
	//      it is the same as the original formula.
	ccw.wfImproved = true
	wfImproved := options.GetInt(ccw.WContext.Params, "closeness_centrality.wf_improved")
	if wfImproved != 1 {
		ccw.wfImproved = false
	}
	ccw.WContext.CreateValue("stop_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	ccw.WContext.SetValue("stop_sum", serialize.SInt32(0))
	return nil
}

func (ccw *ClosenessCentralityWorker) BeforeStep() {
	for i := range ccw.stopSum {
		ccw.stopSum[i] = 0
	}
}

func (ccw *ClosenessCentralityWorker) Compute(vertexID uint32, pID int) {
	vertID := vertexID - ccw.WContext.GraphData.VertIDStart
	switch ccw.WContext.Step {
	case 1:
		inEdges := ccw.WContext.GraphData.Edges.GetInEdges(vertID)
		ccw.neighborEdges[vertexID] = make([]serialize.SUint32, 0, len(inEdges))
		for _, edge := range inEdges {
			if !ccw.sample() || uint32(edge) == vertexID {
				continue
			}
			ccw.neighborEdges[vertexID] = append(ccw.neighborEdges[vertexID], edge)
			ccw.pMap[vertID][uint32(edge)] = 1
		}
		ccw.dfsOutsideEdge[vertID] = ccw.neighborEdges[vertexID]
		ccw.stopSum[pID] = 1
	default:
		for _, value := range ccw.dfsOutsideEdge[vertID] {
			for _, nId := range ccw.neighborEdges[value] {
				if _, ok := ccw.pMap[vertID][uint32(nId)]; !ok {
					if !ccw.sample() {
						continue
					}
					ccw.pMap[vertID][uint32(nId)] = ccw.pMap[vertID][uint32(value)] + 1
					ccw.tempEdges[pID] = append(ccw.tempEdges[pID], nId)
					ccw.stopSum[pID] = 1
				}
			}
		}
		ccw.dfsOutsideEdge[vertID] = make([]serialize.SUint32, len(ccw.tempEdges[pID]))
		copy(ccw.dfsOutsideEdge[vertID], ccw.tempEdges[pID])
	}
	ccw.tempEdges[pID] = ccw.tempEdges[pID][0:0]
	var stopSum serialize.SInt32
	for _, v := range ccw.stopSum {
		stopSum += v
	}
	if stopSum == 0 || options.GetInt(ccw.WContext.Params, "compute.max_step") == int(ccw.WContext.Step) {
		//计算closenessValues
		var sum int32
		for _, v := range ccw.pMap[vertID] {
			sum += v
		}
		if sum > 0 && ccw.WContext.GraphData.Vertex.TotalVertexCount() > 1 {
			ccw.closenessValues[vertexID] =
				serialize.SFloat32(len(ccw.pMap[vertID])-1) / serialize.SFloat32(sum)
			if ccw.wfImproved {
				ccw.closenessValues[vertexID] *= serialize.SFloat32(len(ccw.pMap[vertID])-1) /
					serialize.SFloat32(ccw.WContext.GraphData.Vertex.TotalVertexCount()-1)
			}
			ccw.closenessValues[vertexID] *= serialize.SFloat32(ccw.sampleRate)
		}
	}
}

func (ccw *ClosenessCentralityWorker) sample() bool {
	return rand.Float64() < ccw.sampleRate
}

func (ccw *ClosenessCentralityWorker) AfterStep() {
	var stopSum serialize.SInt32
	for i := range ccw.stopSum {
		stopSum += ccw.stopSum[i]
	}
	ccw.WContext.SetValue("stop_sum", stopSum)
}

func (ccw *ClosenessCentralityWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type ClosenessCentralityMaster struct {
	compute.MasterComputerBase
}

func (ccm *ClosenessCentralityMaster) Compute() bool {
	stopSum := ccm.MContext.GetValue("stop_sum")
	logrus.Infof("stopSum:%v", stopSum)
	return stopSum.(serialize.SInt32) != 0
}
