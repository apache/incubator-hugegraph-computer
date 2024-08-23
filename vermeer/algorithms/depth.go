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
	Algorithms = append(Algorithms, new(DepthMaker))
}

type DepthMaker struct {
	compute.AlgorithmMakerBase
}

func (dg *DepthMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &DepthWorker{}
}

func (dg *DepthMaker) CreateMasterComputer() compute.MasterComputer {
	return &DepthMaster{}
}

func (dg *DepthMaker) Name() string {
	return "depth"
}

func (dg *DepthMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

func (dg *DepthMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeSketchDepth: {}}
}

type DepthWorker struct {
	compute.WorkerComputerBase
	endWccStep bool
	isFindEnd  bool
	endPoint   uint32
	depth      []serialize.SUint32
	diffs      []int32
	baseStep   int32
}

func (dg *DepthWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &dg.depth[i]
}

func (dg *DepthWorker) Init() error {
	dg.isFindEnd = false
	dg.depth = make([]serialize.SUint32, dg.WContext.GraphData.Vertex.TotalVertexCount())
	dg.diffs = make([]int32, dg.WContext.Parallel)

	dg.endWccStep = false
	for i := uint32(0); i < dg.WContext.GraphData.Vertex.TotalVertexCount(); i++ {
		dg.depth[i] = serialize.SUint32(i)
	}

	dg.WContext.CreateValue("diff_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	dg.WContext.SetValue("diff_sum", serialize.SInt32(0))
	dg.WContext.CreateValue("find_end", compute.ValueTypeInt32, compute.CValueActionAggregate)
	dg.WContext.SetValue("find_end", serialize.SInt32(0))
	dg.WContext.CreateValue("wcc_end", compute.ValueTypeInt32, compute.CValueActionAggregate)
	dg.WContext.SetValue("wcc_end", serialize.SInt32(0))
	return nil
}

func (dg *DepthWorker) BeforeStep() {
	for i := range dg.diffs {
		dg.diffs[i] = 0
	}
	wccEnd := dg.WContext.GetValue("wcc_end").(serialize.SInt32) > 0
	if !dg.endWccStep && wccEnd {
		dg.endWccStep = true
		dg.baseStep = dg.WContext.Step - 1
		for i, v := range dg.depth {
			dg.depth[i] = 0
			dg.depth[v] = 1
		}
	}
	endFind := dg.WContext.GetValue("find_end").(serialize.SInt32) > 0
	if !dg.isFindEnd && endFind {
		dg.isFindEnd = true
		dg.baseStep = dg.WContext.Step - 1
		maxEnd := 0
		for vertexID, depth := range dg.depth {
			if depth > serialize.SUint32(maxEnd) {
				maxEnd = int(depth)
				dg.endPoint = uint32(vertexID)
			}
		}
		logrus.Infof("find end point: %v, vertexID:%v", dg.endPoint, dg.WContext.GraphData.Vertex.GetVertex(dg.endPoint).ID)
		// clear depth
		for i := range dg.depth {
			dg.depth[i] = serialize.SUint32(0)
		}
		dg.depth[dg.endPoint] = serialize.SUint32(1)
	}

}

func (dg *DepthWorker) Compute(vertexID uint32, pID int) {
	vertIDx := vertexID - dg.WContext.GraphData.VertIDStart
	if !dg.endWccStep {
		// do wcc
		oldValue := dg.depth[vertexID]
		for _, nID := range dg.WContext.GraphData.Edges.GetInEdges(vertIDx) {
			if dg.depth[vertexID] > dg.depth[nID] {
				dg.depth[vertexID] = dg.depth[nID]
			}
		}
		for _, nID := range dg.WContext.GraphData.Edges.GetOutEdges(vertIDx) {
			if dg.depth[vertexID] > dg.depth[nID] {
				dg.depth[vertexID] = dg.depth[nID]
			}
		}
		//}
		if oldValue != dg.depth[vertexID] {
			dg.diffs[pID]++
		}
	} else {
		// find max depth
		if dg.depth[vertexID] > 0 {
			return
		}
		for _, nID := range dg.WContext.GraphData.Edges.GetInEdges(vertIDx) {
			if dg.depth[nID] == serialize.SUint32(dg.WContext.Step-dg.baseStep) {
				dg.depth[vertexID] = serialize.SUint32(dg.WContext.Step - dg.baseStep + 1)
				dg.diffs[pID]++
				return
			}
		}
		for _, nID := range dg.WContext.GraphData.Edges.GetOutEdges(vertIDx) {
			if dg.depth[nID] == serialize.SUint32(dg.WContext.Step-dg.baseStep) {
				dg.depth[vertexID] = serialize.SUint32(dg.WContext.Step - dg.baseStep + 1)
				dg.diffs[pID]++
				return
			}
		}
	}
}

func (dg *DepthWorker) AfterStep() {
	var diffSum int32
	for _, v := range dg.diffs {
		diffSum += v
	}
	dg.WContext.SetValue("diff_sum", serialize.SInt32(diffSum))
}

func (dg *DepthWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type DepthMaster struct {
	compute.MasterComputerBase
	endWccStep bool
	isFindEnd  bool
}

func (dg *DepthMaster) Compute() bool {
	diffSum := dg.MContext.GetValue("diff_sum").(serialize.SInt32)
	logrus.Infof("different sum: %v", diffSum)
	if diffSum == 0 {
		if !dg.endWccStep {
			logrus.Infof("wcc step end!")
			dg.MContext.SetValue("wcc_end", serialize.SInt32(1))
			dg.endWccStep = true
		} else if !dg.isFindEnd {
			logrus.Infof("find end point!")
			dg.MContext.SetValue("find_end", serialize.SInt32(1))
			dg.isFindEnd = true
		} else {
			return false
		}
	}
	return true
}
