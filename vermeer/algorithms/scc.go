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
	Algorithms = append(Algorithms, new(SccMaker))
}

type SccMaker struct {
	compute.AlgorithmMakerBase
}

func (sm *SccMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &SccWorker{}
}

func (sm *SccMaker) CreateMasterComputer() compute.MasterComputer {
	return &SccMaster{}
}

func (sm *SccMaker) Name() string {
	return "scc"
}

func (sm *SccMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}
func (sm *SccMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeCount: {}, compute.StatisticsTypeModularity: {}}
}

type sccStepType int

const (
	stepColoring sccStepType = iota
	stepBackward
)

type SccWorker struct {
	compute.WorkerComputerBase
	sccStep    sccStepType
	isTrimmed  []serialize.SUint8
	backwardID []serialize.SUint8
	colorID    []serialize.SUint32
	sccID      []serialize.SUint32
	diffSum    []serialize.SInt32
}

func (scw *SccWorker) VertexValue(i uint32) serialize.MarshalAble {
	if scw.WContext.Output {
		return &scw.sccID[i]
	}
	switch scw.sccStep {
	case stepColoring:
		return &scw.colorID[i]
	case stepBackward:
		return &scw.backwardID[i]
	}
	return &scw.sccID[i]
}

func (scw *SccWorker) Init() error {
	scw.sccStep = stepColoring
	scw.isTrimmed = make([]serialize.SUint8, scw.WContext.GraphData.Vertex.TotalVertexCount())
	scw.sccID = make([]serialize.SUint32, scw.WContext.GraphData.Vertex.TotalVertexCount())
	for v := range scw.sccID {
		scw.sccID[v] = serialize.SUint32(v)
	}
	scw.colorID = make([]serialize.SUint32, scw.WContext.GraphData.Vertex.TotalVertexCount())
	for v := range scw.colorID {
		scw.colorID[v] = serialize.SUint32(v)
	}
	scw.diffSum = make([]serialize.SInt32, scw.WContext.Parallel)
	scw.WContext.CreateValue("step", compute.ValueTypeInt32, compute.CValueActionAggregate)
	scw.WContext.SetValue("step", serialize.SInt32(stepColoring))
	scw.WContext.CreateValue("diff_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	scw.WContext.SetValue("diff_sum", serialize.SInt32(0))
	scw.WContext.CreateValue("active_count", compute.ValueTypeInt32, compute.CValueActionAggregate)
	scw.WContext.SetValue("active_count", serialize.SInt32(0))
	return nil
}

func (scw *SccWorker) BeforeStep() {
	oldSccStep := scw.sccStep
	scw.sccStep = sccStepType(scw.WContext.GetValue("step").(serialize.SInt32))
	if oldSccStep != scw.sccStep {
		switch oldSccStep {
		case stepColoring:
			scw.backwardID = make([]serialize.SUint8, scw.WContext.GraphData.Vertex.TotalVertexCount())
			for v, color := range scw.colorID {
				if scw.isTrimmed[v] == 1 {
					continue
				}
				if serialize.SUint32(v) == color {
					scw.backwardID[v] = 1
				}
			}
		case stepBackward:
			for v, bw := range scw.backwardID {
				if scw.isTrimmed[v] == 1 {
					continue
				}
				if bw == 1 {
					scw.sccID[v] = scw.colorID[v]
					scw.isTrimmed[v] = 1
				}
			}
			scw.colorID = make([]serialize.SUint32, scw.WContext.GraphData.Vertex.TotalVertexCount())
			for v := range scw.colorID {
				scw.colorID[v] = serialize.SUint32(v)
			}
		}
	}
	for i := range scw.diffSum {
		scw.diffSum[i] = 0
	}
}

func (scw *SccWorker) Compute(vertexID uint32, pID int) {
	vertID := vertexID - scw.WContext.GraphData.VertIDStart
	if scw.isTrimmed[vertexID] == 1 {
		return
	}
	switch scw.sccStep {
	case stepColoring:
		oldValue := scw.colorID[vertexID]
		for _, nID := range scw.WContext.GraphData.Edges.GetOutEdges(vertID) {
			if scw.isTrimmed[nID] == 1 {
				continue
			}
			if scw.colorID[vertexID] > scw.colorID[nID] {
				scw.colorID[vertexID] = scw.colorID[nID]
			}
		}
		if scw.colorID[vertexID] != oldValue {
			scw.diffSum[pID]++
		}
	case stepBackward:
		if scw.backwardID[vertexID] == 1 {
			return
		}
		colorID := scw.colorID[vertexID]
		for _, nID := range scw.WContext.GraphData.Edges.GetInEdges(vertID) {
			if scw.isTrimmed[nID] == 1 || scw.colorID[nID] != colorID {
				continue
			}
			if scw.backwardID[nID] == 1 {
				scw.backwardID[vertexID] = 1
				scw.diffSum[pID]++
				break
			}
		}
	}
}

func (scw *SccWorker) AfterStep() {
	var count serialize.SInt32
	for _, c := range scw.diffSum {
		count += c
	}
	scw.WContext.SetValue("diff_sum", count)
	activeCount := 0
	for vID := scw.WContext.GraphData.VertIDStart; vID <
		scw.WContext.GraphData.VertIDStart+scw.WContext.GraphData.VertexCount; vID++ {
		if scw.isTrimmed[vID] == 0 {
			activeCount++
		}
	}
	scw.WContext.SetValue("active_count", serialize.SInt32(activeCount))
	//scc := make(map[serialize.SUint32]int)
	//for _, value := range scw.sccID {
	//	scc[value]++
	//}
	//max := 0
	//for _, count := range scc {
	//	if count > max {
	//		max = count
	//	}
	//}
	//logrus.Infof("scc len:%v, max scc:%v", len(scc), max)
}

func (scw *SccWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type SccMaster struct {
	compute.MasterComputerBase
	sccStep sccStepType
}

func (scm *SccMaster) Init() error {
	scm.sccStep = stepColoring
	return nil
}

func (scm *SccMaster) Compute() bool {
	activeCount := scm.MContext.GetValue("active_count").(serialize.SInt32)
	logrus.Infof("active count:%v", activeCount)
	if activeCount == 0 {
		return false
	}
	switch scm.sccStep {
	case stepColoring:
		diffSum := scm.MContext.GetValue("diff_sum").(serialize.SInt32)
		logrus.Infof("coloring diff sum: %v", diffSum)
		if diffSum == 0 {
			scm.MContext.SetValue("step", serialize.SInt32(stepBackward))
			scm.sccStep = stepBackward
		} else {
			scm.MContext.SetValue("step", serialize.SInt32(stepColoring))
		}
	case stepBackward:
		diffSum := scm.MContext.GetValue("diff_sum").(serialize.SInt32)
		logrus.Infof("backward diff sum: %v", diffSum)
		if diffSum == 0 {
			scm.MContext.SetValue("step", serialize.SInt32(stepColoring))
			scm.sccStep = stepColoring
		} else {
			scm.MContext.SetValue("step", serialize.SInt32(stepBackward))
		}
	}

	return true
}
