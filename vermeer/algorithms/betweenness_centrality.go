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
	Algorithms = append(Algorithms, new(BetweennessCentralityMaker))
}

type BetweennessCentralityMaker struct {
	compute.AlgorithmMakerBase
}

func (bc *BetweennessCentralityMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &BetweennessCentralityWorker{}
}

func (bc *BetweennessCentralityMaker) CreateMasterComputer() compute.MasterComputer {
	return &BetweennessCentralityMaster{}
}

func (bc *BetweennessCentralityMaker) Name() string {
	return "betweenness_centrality"
}

func (bc *BetweennessCentralityMaker) DataNeeded() []string {
	return []string{}
}

type BetweennessCentralityWorker struct {
	compute.WorkerComputerBase
	useEndPoint         bool
	stopFlag            bool
	scale               serialize.SFloat32
	sampleRate          float64
	stopSum             []serialize.SInt32
	betweennessValues   []serialize.SFloat32
	tempEdges           [][]serialize.SUint32
	neighborEdges       []serialize.SliceUint32
	betweennessValueMap []serialize.MapUint32Float32
	bfsNewValues        []map[uint32][][]uint32
	bfsOldValues        []map[uint32][][]uint32
}

func (bcw *BetweennessCentralityWorker) VertexValue(i uint32) serialize.MarshalAble {
	if bcw.WContext.Step == 1 {
		return &bcw.neighborEdges[i]
	}
	if bcw.stopFlag && bcw.WContext.Step > 1 {
		return &bcw.betweennessValues[i]
	}
	return &bcw.betweennessValueMap[i]
}

func (bcw *BetweennessCentralityWorker) Init() error {
	bcw.betweennessValues = make([]serialize.SFloat32, bcw.WContext.GraphData.Vertex.TotalVertexCount())
	bcw.neighborEdges = make([]serialize.SliceUint32, bcw.WContext.GraphData.Vertex.TotalVertexCount())
	bcw.betweennessValueMap = make([]serialize.MapUint32Float32, bcw.WContext.GraphData.Vertex.TotalVertexCount())
	for i := range bcw.betweennessValueMap {
		bcw.betweennessValueMap[i] = make(map[serialize.SUint32]serialize.SFloat32)
	}
	bcw.tempEdges = make([][]serialize.SUint32, bcw.WContext.Parallel)
	for i := range bcw.tempEdges {
		bcw.tempEdges[i] = make([]serialize.SUint32, 0, 100)
	}
	bcw.bfsNewValues = make([]map[uint32][][]uint32, bcw.WContext.GraphData.VertexCount)
	bcw.bfsOldValues = make([]map[uint32][][]uint32, bcw.WContext.GraphData.VertexCount)
	bcw.stopSum = make([]serialize.SInt32, bcw.WContext.Parallel)
	bcw.sampleRate = options.GetFloat(bcw.WContext.Params, "betweenness_centrality.sample_rate")
	if bcw.sampleRate > 1.0 || bcw.sampleRate <= 0 {
		logrus.Errorf("The param betweenness_centrality.sample_rate must be in (0.0, 1.0], actual got '%v",
			bcw.sampleRate)
		return fmt.Errorf("the param betweenness_centrality.sample_rate must be in (0.0, 1.0], actual got '%v",
			bcw.sampleRate)
	}
	bcw.useEndPoint = false
	if options.GetInt(bcw.WContext.Params, "betweenness_centrality.use_endpoint") == 1 {
		bcw.useEndPoint = true
	}
	bcw.stopFlag = false

	n := serialize.SFloat32(bcw.WContext.GraphData.Vertex.TotalVertexCount())
	if n < 2 {
		bcw.scale = 1
	} else if bcw.useEndPoint {
		n *= serialize.SFloat32(bcw.sampleRate)
		bcw.scale = 1 / serialize.SFloat32(n*(n-1))
	} else {
		n *= serialize.SFloat32(bcw.sampleRate)
		bcw.scale = 1 / serialize.SFloat32((n-1)*(n-2))
	}

	bcw.WContext.CreateValue("stop_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	bcw.WContext.SetValue("stop_sum", serialize.SInt32(0))
	bcw.WContext.CreateValue("stop_flag", compute.ValueTypeInt32, compute.CValueActionAggregate)
	bcw.WContext.SetValue("stop_flag", serialize.SInt32(0))
	return nil
}

func (bcw *BetweennessCentralityWorker) BeforeStep() {
	for i := range bcw.stopSum {
		bcw.stopSum[i] = 0
	}
	if bcw.WContext.Step > 1 && bcw.WContext.GetValue("stop_flag").(serialize.SInt32) == 1 ||
		options.GetInt(bcw.WContext.Params, "compute.max_step") == int(bcw.WContext.Step) {
		bcw.stopFlag = true
	}
	for i := range bcw.bfsNewValues {
		bcw.bfsOldValues[i] = make(map[uint32][][]uint32)
		for k, v := range bcw.bfsNewValues[i] {
			bcw.bfsOldValues[i][k] = v
		}
		bcw.bfsNewValues[i] = make(map[uint32][][]uint32)
	}

	if bcw.stopFlag {
		rangeStart := bcw.WContext.GraphData.VertIDStart
		rangeEnd := bcw.WContext.GraphData.VertIDStart + bcw.WContext.GraphData.VertexCount
		for _, valueMap := range bcw.betweennessValueMap {
			for k, v := range valueMap {
				key := uint32(k)
				if key >= rangeStart && key < rangeEnd {
					bcw.betweennessValues[key] += v
				}
			}
		}
	}

}

func (bcw *BetweennessCentralityWorker) Compute(vertexID uint32, pID int) {
	if bcw.stopFlag {
		return
	}
	vertID := vertexID - bcw.WContext.GraphData.VertIDStart
	switch bcw.WContext.Step {
	case 1:
		inEdges := bcw.WContext.GraphData.Edges.GetInEdges(vertID)
		bcw.neighborEdges[vertexID] = make([]serialize.SUint32, 0, len(inEdges))
		for _, edge := range inEdges {
			if !bcw.sample() || uint32(edge) == vertexID {
				continue
			}
			bcw.neighborEdges[vertexID] = append(bcw.neighborEdges[vertexID], edge)
			bcw.bfsNewValues[vertID][uint32(edge)] = [][]uint32{}
			bcw.betweennessValueMap[vertexID][edge] = 0
			if bcw.useEndPoint {
				bcw.betweennessValueMap[vertexID][edge] += 1 * bcw.scale
			}
		}
		bcw.stopSum[pID] = 1
	default:
		for k, v := range bcw.bfsOldValues[vertID] {
			for _, nId := range bcw.neighborEdges[k] {
				if !bcw.sample() || nId == serialize.SUint32(vertexID) {
					continue
				}
				if _, ok := bcw.betweennessValueMap[vertexID][nId]; !ok {
					bcw.stopSum[pID] = 1
					bcw.tempEdges[pID] = append(bcw.tempEdges[pID], nId)
					for _, vi := range v {
						newV := append(vi, k)
						bcw.bfsNewValues[vertID][uint32(nId)] = append(bcw.bfsNewValues[vertID][uint32(nId)], newV)
					}
					if len(v) == 0 {
						bcw.bfsNewValues[vertID][uint32(nId)] = append(bcw.bfsNewValues[vertID][uint32(nId)], []uint32{k})
					}
				}
			}
		}
		for _, edge := range bcw.tempEdges[pID] {
			bcw.betweennessValueMap[vertexID][edge] = 0
			if bcw.useEndPoint {
				bcw.betweennessValueMap[vertexID][edge] += 1 * bcw.scale
			}
		}
	}
	bcw.tempEdges[pID] = bcw.tempEdges[pID][0:0]
	for _, values := range bcw.bfsNewValues[vertID] {
		for _, v := range values {
			for _, u := range v {
				bcw.betweennessValueMap[vertexID][serialize.SUint32(u)] +=
					1.0 / serialize.SFloat32(len(values)) * bcw.scale
			}
		}
	}

}

func (bcw *BetweennessCentralityWorker) AfterStep() {
	var stopSum serialize.SInt32
	for i := range bcw.stopSum {
		stopSum += bcw.stopSum[i]
	}
	bcw.WContext.SetValue("stop_sum", stopSum)
}

func (bcw *BetweennessCentralityWorker) sample() bool {
	return rand.Float64() < bcw.sampleRate
}

func (bcw *BetweennessCentralityWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type BetweennessCentralityMaster struct {
	compute.MasterComputerBase
}

func (bcm *BetweennessCentralityMaster) Compute() bool {
	logrus.Infof("stopFlag:%v", bcm.MContext.GetValue("stop_flag").(serialize.SInt32))
	if bcm.MContext.GetValue("stop_flag").(serialize.SInt32) == serialize.SInt32(len(bcm.MContext.WorkerCValues)) {
		return false
	}
	stopSum := bcm.MContext.GetValue("stop_sum")
	logrus.Infof("stopSum:%v", stopSum)
	if stopSum.(serialize.SInt32) == serialize.SInt32(0) {
		bcm.MContext.SetValue("stop_flag", serialize.SInt32(1))
	}
	return true
}
