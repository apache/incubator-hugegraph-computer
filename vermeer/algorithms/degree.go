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
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(DegreeMaker))
}

type DegreeMaker struct {
	compute.AlgorithmMakerBase
}

func (dg *DegreeMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &DegreeWorker{}
}

func (dg *DegreeMaker) CreateMasterComputer() compute.MasterComputer {
	return &DegreeMaster{}
}

func (dg *DegreeMaker) Name() string {
	return "degree"
}

func (dg *DegreeMaker) DataNeeded() []string {
	return []string{compute.UseOutDegree}
}

func (dg *DegreeMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeSketchDegree: {}}
}

type DegreeWorker struct {
	compute.WorkerComputerBase
	direction serialize.SInt32
	newValues []serialize.SUint32
}

func (dg *DegreeWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &dg.newValues[i]
}

func (dg *DegreeWorker) Init() error {
	dg.newValues = make([]serialize.SUint32, dg.WContext.GraphData.Vertex.TotalVertexCount())
	drt := options.GetString(dg.WContext.Params, "degree.direction")
	switch drt {
	case "out":
		dg.direction = 0
	case "in":
		dg.direction = 1
	case "both":
		dg.direction = 2
	default:
		logrus.Errorf("please input 'out', 'in' or 'both'.")
		return fmt.Errorf("please input 'out', 'in' or 'both'")
	}
	return nil
}

func (dg *DegreeWorker) BeforeStep() {
}

func (dg *DegreeWorker) Compute(vertexId uint32, pId int) {
	_ = pId
	var vertexIdx = vertexId - dg.WContext.GraphData.VertIDStart
	switch dg.direction {
	case 0:
		dg.newValues[vertexId] = dg.WContext.GraphData.Edges.GetOutDegree(vertexId)
	case 1:
		dg.newValues[vertexId] = serialize.SUint32(len(dg.WContext.GraphData.Edges.GetInEdges(vertexIdx)))
	case 2:
		dg.newValues[vertexId] = dg.WContext.GraphData.Edges.GetOutDegree(vertexId) +
			serialize.SUint32(len(dg.WContext.GraphData.Edges.GetInEdges(vertexIdx)))
	}
}

func (dg *DegreeWorker) AfterStep() {
}

func (dg *DegreeWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type DegreeMaster struct {
	compute.MasterComputerBase
}

func (dg *DegreeMaster) Compute() bool {
	return dg.MContext.Step < 1
}
