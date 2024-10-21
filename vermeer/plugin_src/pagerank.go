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

package main

import (
	"math"

	"github.com/sirupsen/logrus"

	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
)

func init() {
	_ = AlgoMaker
	logrus.Infof("pagerank plugin init")
}

var AlgoMaker = PageRankMaker{}

type PageRankMaker struct {
}

func (prm *PageRankMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &PageRankWorker{}
}

func (prm *PageRankMaker) CreateMasterComputer() compute.MasterComputer {
	return &PageRankMaster{}
}

func (prm *PageRankMaker) Name() string {
	return "pagerank"
}

func (prm *PageRankMaker) DataNeeded() []string {
	return []string{"use_out_degree"}
}

type PageRankWorker struct {
	compute.WorkerComputerBase
	damping           serialize.SFloat32
	initialRank       serialize.SFloat32
	danglingSumWorker serialize.SFloat32
	danglingSum       serialize.SFloat32
	preDangling       serialize.SFloat32
	diffSum           []serialize.SFloat32
	newValues         []serialize.SFloat32
	oldValues         []serialize.SFloat32
}

func (pgw *PageRankWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &pgw.newValues[i]
}

func (pgw *PageRankWorker) Init() error {
	pgw.newValues = make([]serialize.SFloat32, pgw.WContext.GraphData.Vertex.TotalVertexCount())
	pgw.oldValues = make([]serialize.SFloat32, pgw.WContext.GraphData.Vertex.TotalVertexCount())
	pgw.diffSum = make([]serialize.SFloat32, pgw.WContext.Parallel)
	initValue := 1.0 / serialize.SFloat32(pgw.WContext.GraphData.Vertex.TotalVertexCount())
	for i := range pgw.oldValues {
		pgw.oldValues[i] = initValue
	}
	pgw.damping = serialize.SFloat32(
		options.GetFloat(pgw.WContext.Params, "pagerank.damping"))
	pgw.initialRank = (1.0 - pgw.damping) / serialize.SFloat32(pgw.WContext.GraphData.Vertex.TotalVertexCount())
	pgw.preDangling = pgw.damping / serialize.SFloat32(pgw.WContext.GraphData.Vertex.TotalVertexCount())
	pgw.WContext.CreateValue("dangling_sum", compute.ValueTypeFloat32, compute.CValueActionAggregate)
	pgw.WContext.SetValue("dangling_sum", serialize.SFloat32(0))
	pgw.WContext.CreateValue("diff_sum", compute.ValueTypeFloat32, compute.CValueActionAggregate)
	pgw.WContext.SetValue("diff_sum", serialize.SFloat32(0))
	return nil
}

func (pgw *PageRankWorker) BeforeStep() {
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

func (pgw *PageRankWorker) Compute(vertexID uint32, pID int) {
	newRank := serialize.SFloat32(0.0)
	vertIdx := vertexID - pgw.WContext.GraphData.VertIDStart
	inEdges := pgw.WContext.GraphData.Edges.GetInEdges(vertIdx)
	for _, nID := range inEdges {
		out := pgw.WContext.GraphData.Edges.GetOutDegree(uint32(nID))
		newRank += pgw.oldValues[nID] / serialize.SFloat32(out)
	}
	newRank = pgw.initialRank + pgw.damping*newRank + pgw.preDangling*pgw.danglingSum
	pgw.diffSum[pID] += serialize.SFloat32(math.Abs(float64(newRank - pgw.newValues[vertexID])))
	pgw.newValues[vertexID] = newRank
}

func (pgw *PageRankWorker) AfterStep() {
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
	for i := range pgw.newValues {
		pgw.oldValues[i] = pgw.newValues[i]
	}
}

func (pgw *PageRankWorker) OutputValueType() string {
	return "FLOAT"
}

type PageRankMaster struct {
	compute.MasterComputerBase
	diffThreshold serialize.SFloat32
}

func (pgm *PageRankMaster) Init() error {
	pgm.diffThreshold = serialize.SFloat32(options.GetFloat(pgm.MContext.Params, "pagerank.diff_threshold"))
	return nil
}

func (pgm *PageRankMaster) Compute() bool {
	diffSum := pgm.MContext.GetValue("diff_sum").(serialize.SFloat32)
	logrus.Infof("different sum: %f, threshold: %f", diffSum, pgm.diffThreshold)
	if pgm.MContext.GetValue("diff_sum").(serialize.SFloat32) < pgm.diffThreshold {
		return false
	}
	return true
}
