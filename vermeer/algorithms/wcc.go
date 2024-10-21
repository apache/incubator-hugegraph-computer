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

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(WccMaker))
}

type WccMaker struct {
	compute.AlgorithmMakerBase
}

func (wm *WccMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &WccWorker{}
}

func (wm *WccMaker) CreateMasterComputer() compute.MasterComputer {
	return &WccMaster{}
}

func (wm *WccMaker) Name() string {
	return "wcc"
}

func (wm *WccMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

func (wm *WccMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeCount: {}, compute.StatisticsTypeModularity: {}, compute.StatisticsTypeSketchCount: {}}
}

type WccWorker struct {
	compute.WorkerComputerBase
	connectedMinID []serialize.SUint32
	diffSum        []serialize.SInt32
}

func (wcw *WccWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &wcw.connectedMinID[i]
}
func (wcw *WccWorker) Init() error {
	wcw.connectedMinID = make([]serialize.SUint32, wcw.WContext.GraphData.Vertex.TotalVertexCount())
	for i := uint32(0); i < wcw.WContext.GraphData.Vertex.TotalVertexCount(); i++ {
		wcw.connectedMinID[i] = serialize.SUint32(i)
	}
	wcw.WContext.CreateValue("diff_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	wcw.WContext.SetValue("diff_sum", serialize.SInt32(0))
	wcw.diffSum = make([]serialize.SInt32, wcw.WContext.Parallel)
	return nil
}

func (wcw *WccWorker) BeforeStep() {
	for i := range wcw.diffSum {
		wcw.diffSum[i] = 0
	}
}

func (wcw *WccWorker) Compute(vertexID uint32, pID int) {
	_ = pID
	vertIDx := vertexID - wcw.WContext.GraphData.VertIDStart
	oldValue := wcw.connectedMinID[vertexID]
	//if wcw.WContext.GraphData.BothEdges != nil {
	//	for _, nID := range wcw.WContext.GraphData.BothEdges[vertIDx] {
	//		if wcw.connectedMinID[vertexID] > wcw.connectedMinID[nID] {
	//			wcw.connectedMinID[vertexID] = wcw.connectedMinID[nID]
	//		}
	//	}
	//} else {
	for _, nID := range wcw.WContext.GraphData.Edges.GetInEdges(vertIDx) {
		if wcw.connectedMinID[vertexID] > wcw.connectedMinID[nID] {
			wcw.connectedMinID[vertexID] = wcw.connectedMinID[nID]
		}
	}
	for _, nID := range wcw.WContext.GraphData.Edges.GetOutEdges(vertIDx) {
		if wcw.connectedMinID[vertexID] > wcw.connectedMinID[nID] {
			wcw.connectedMinID[vertexID] = wcw.connectedMinID[nID]
		}
	}
	//}
	if oldValue != wcw.connectedMinID[vertexID] {
		wcw.diffSum[pID]++
	}
}

func (wcw *WccWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

func (wcw *WccWorker) AfterStep() {
	var diffSum serialize.SInt32
	for _, v := range wcw.diffSum {
		diffSum += v
	}
	wcw.WContext.SetValue("diff_sum", diffSum)
}

type WccMaster struct {
	compute.MasterComputerBase
}

func (wcm *WccMaster) Compute() bool {
	diffSum := wcm.MContext.GetValue("diff_sum").(serialize.SInt32)
	logrus.Infof("different sum: %v", diffSum)
	return diffSum != 0
}
