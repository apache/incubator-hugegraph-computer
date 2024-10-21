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
	"math"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(PersonalizedPagerankMaker))
}

type PersonalizedPagerankMaker struct {
	compute.AlgorithmMakerBase
}

func (prm *PersonalizedPagerankMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &PersonalizedPagerankWorker{}
}

func (prm *PersonalizedPagerankMaker) CreateMasterComputer() compute.MasterComputer {
	return &PersonalizedPagerankMaster{}
}

func (prm *PersonalizedPagerankMaker) Name() string {
	return "ppr"
}

func (prm *PersonalizedPagerankMaker) DataNeeded() []string {
	return []string{compute.UseOutDegree}
}

type PersonalizedPagerankWorker struct {
	compute.WorkerComputerBase
	source            uint32
	damping           serialize.SFloat32
	danglingSumWorker serialize.SFloat32
	danglingSum       serialize.SFloat32
	diffSum           []serialize.SFloat32
	newValues         []serialize.SFloat32
	oldValues         []serialize.SFloat32
}

func (pgw *PersonalizedPagerankWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &pgw.newValues[i]
}

func (pgw *PersonalizedPagerankWorker) Init() error {
	pgw.newValues = make([]serialize.SFloat32, pgw.WContext.GraphData.Vertex.TotalVertexCount())
	pgw.oldValues = make([]serialize.SFloat32, pgw.WContext.GraphData.Vertex.TotalVertexCount())
	pgw.diffSum = make([]serialize.SFloat32, pgw.WContext.Parallel)
	initValue := 1.0 / serialize.SFloat32(pgw.WContext.GraphData.Vertex.TotalVertexCount())
	for i := range pgw.oldValues {
		pgw.oldValues[i] = initValue
	}
	pgw.damping = serialize.SFloat32(
		options.GetFloat(pgw.WContext.Params, "ppr.damping"))
	logrus.Infof("damping:%v", pgw.damping)
	var ok bool
	pgw.source, ok = pgw.WContext.GraphData.Vertex.GetVertexIndex(options.GetString(pgw.WContext.Params, "ppr.source"))
	if !ok {
		logrus.Errorf("ppr.source:%v not exist", options.GetString(pgw.WContext.Params, "ppr.source"))
		return fmt.Errorf("ppr.source:%v not exist", options.GetString(pgw.WContext.Params, "ppr.source"))
	}
	pgw.WContext.CreateValue("dangling_sum", compute.ValueTypeFloat32, compute.CValueActionAggregate)
	pgw.WContext.SetValue("dangling_sum", serialize.SFloat32(0))
	pgw.WContext.CreateValue("diff_sum", compute.ValueTypeFloat32, compute.CValueActionAggregate)
	pgw.WContext.SetValue("diff_sum", serialize.SFloat32(0))
	return nil
}

func (pgw *PersonalizedPagerankWorker) BeforeStep() {
	for i := range pgw.diffSum {
		pgw.diffSum[i] = 0
	}
	pgw.danglingSum = 0
	for i := uint32(0); i < pgw.WContext.GraphData.Vertex.TotalVertexCount(); i++ {
		if pgw.WContext.GraphData.Edges.GetOutDegree(i) == 0 {
			pgw.danglingSum += pgw.oldValues[i]
		}
	}
}

func (pgw *PersonalizedPagerankWorker) Compute(vertexID uint32, pID int) {
	newRank := serialize.SFloat32(0.0)
	vertIdx := vertexID - pgw.WContext.GraphData.VertIDStart
	for _, nID := range pgw.WContext.GraphData.Edges.GetInEdges(vertIdx) {
		out := pgw.WContext.GraphData.Edges.GetOutDegree(uint32(nID))
		newRank += pgw.oldValues[nID] / serialize.SFloat32(out)
	}
	newRank = pgw.damping * newRank
	if vertexID == pgw.source {
		newRank += (1.0 - pgw.damping) + pgw.damping*pgw.danglingSum
	}

	pgw.diffSum[pID] += serialize.SFloat32(math.Abs(float64(newRank - pgw.newValues[vertexID])))
	pgw.newValues[vertexID] = newRank
}

func (pgw *PersonalizedPagerankWorker) AfterStep() {
	pgw.WContext.SetValue("dangling_sum", pgw.danglingSumWorker)
	//endIdx := pgw.WContext.GraphData.VertIDStart + pgw.WContext.GraphData.VertexCount
	diffSum := serialize.SFloat32(0.0)
	for _, v := range pgw.diffSum {
		diffSum += v
	}
	//for i := pgw.WContext.GraphData.VertIDStart; i < endIdx; i++ {
	//	diffSum += serialize.SFloat32(math.Abs(float64(pgw.oldValues[i] - pgw.newValues[i])))
	//}
	pgw.WContext.SetValue("diff_sum", diffSum)
	copy(pgw.oldValues, pgw.newValues)
}

func (pgw *PersonalizedPagerankWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

type PersonalizedPagerankMaster struct {
	compute.MasterComputerBase
	diffThreshold serialize.SFloat32
}

func (pgm *PersonalizedPagerankMaster) Init() error {
	pgm.diffThreshold = serialize.SFloat32(options.GetFloat(pgm.MContext.Params, "ppr.diff_threshold"))
	return nil
}

func (pgm *PersonalizedPagerankMaster) Compute() bool {
	diffSum := pgm.MContext.GetValue("diff_sum").(serialize.SFloat32)
	logrus.Infof("different sum: %f, threshold: %f", diffSum, pgm.diffThreshold)
	return pgm.MContext.GetValue("diff_sum").(serialize.SFloat32) >= pgm.diffThreshold
}
