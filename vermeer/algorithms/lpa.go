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
	"strings"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(LpaMaker))
}

type LpaMaker struct {
	compute.AlgorithmMakerBase
}

func (lm *LpaMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &LpaWorker{}
}

func (lm *LpaMaker) CreateMasterComputer() compute.MasterComputer {
	return &LpaMaster{}
}

func (lm *LpaMaker) Name() string {
	return "lpa"
}

func (lm *LpaMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}
func (lm *LpaMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeCount: {}, compute.StatisticsTypeModularity: {}}
}

type LpaWorker struct {
	compute.WorkerComputerBase
	grandpaLabels  []serialize.SUint32
	oldLabels      []serialize.SUint32
	newLabels      []serialize.SUint32
	grandpaDiffs   []serialize.SInt32
	diffSum        []serialize.SInt32
	useProperty    bool
	propertyType   structure.ValueType
	vertexProperty string
	compareOption  int
	updateMethod   int
}

func (lw *LpaWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &lw.newLabels[i]
}

const (
	UpdateMethodSync     = "sync"
	UpdateMethodSemiSync = "semi_sync"
)

const (
	CompareVetexID        = "id"
	CompareVertexOriginID = "origin"
)

func (lw *LpaWorker) Init() error {
	// 初始化label为自增id
	lw.oldLabels = make([]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())
	lw.newLabels = make([]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())
	lw.grandpaLabels = make([]serialize.SUint32, lw.WContext.GraphData.Vertex.TotalVertexCount())
	for id := range lw.oldLabels {
		lw.oldLabels[id] = serialize.SUint32(id)
		lw.newLabels[id] = serialize.SUint32(id)
	}

	lw.diffSum = make([]serialize.SInt32, lw.WContext.Parallel)
	lw.grandpaDiffs = make([]serialize.SInt32, lw.WContext.Parallel)

	lw.WContext.CreateValue("diff_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	lw.WContext.SetValue("diff_sum", serialize.SInt32(0))
	lw.WContext.CreateValue("grandpa_diff_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	lw.WContext.SetValue("grandpa_diff_sum", serialize.SInt32(0))

	switch options.GetString(lw.WContext.Params, "lpa.compare_option") {
	case CompareVetexID:
		lw.compareOption = 0
	case CompareVertexOriginID:
		lw.compareOption = 1
	default:
		logrus.Errorf("illegal compare option:%v, use id/origin", options.GetString(lw.WContext.Params, "lpa.compare_option"))
		return fmt.Errorf("illegal compare option:%v, use id/origin", options.GetString(lw.WContext.Params, "lpa.compare_option"))
	}

	switch options.GetString(lw.WContext.Params, "lpa.update_method") {
	case UpdateMethodSync:
		lw.updateMethod = 0
	case UpdateMethodSemiSync:
		lw.updateMethod = 1
	default:
		logrus.Errorf("illegal update method:%v, use sync/semi_sync", options.GetString(lw.WContext.Params, "lpa.update_method"))
		return fmt.Errorf("illegal update method:%v, use sync/semi_sync", options.GetString(lw.WContext.Params, "lpa.update_method"))
	}

	lw.vertexProperty = options.GetString(lw.WContext.Params, "lpa.vertex_weight_property")
	if len(lw.vertexProperty) > 0 {
		vType, ok := lw.WContext.GraphData.VertexProperty.GetValueType(lw.vertexProperty)
		if !ok {
			logrus.Errorf("unknown vertex weighted property:%v", lw.vertexProperty)
			return fmt.Errorf("unknown vertex weighted property:%v", lw.vertexProperty)
		}
		switch vType {
		case structure.ValueTypeInt32, structure.ValueTypeFloat32:
			lw.propertyType = vType
		case structure.ValueTypeString:
			logrus.Errorf("illegal vertex weighted property type:%v", lw.vertexProperty)
			return fmt.Errorf("illegal vertex weighted property type:%v", lw.vertexProperty)
		}
		lw.useProperty = true
	}
	return nil
}

func (lw *LpaWorker) BeforeStep() {
	for i := range lw.diffSum {
		lw.diffSum[i] = 0
		lw.grandpaDiffs[i] = 0
	}
}

func (lw *LpaWorker) inSelfWorker(vertexID uint32) bool {
	return vertexID >= lw.WContext.GraphData.VertIDStart && vertexID < (lw.WContext.GraphData.VertexCount+lw.WContext.GraphData.VertIDStart)
}

func (lw *LpaWorker) Compute(vertexId uint32, pId int) {
	_ = pId
	vertexIdx := vertexId - lw.WContext.GraphData.VertIDStart

	//count key:自增label value:frequency
	count := make(map[serialize.SUint32]float64)
	//if lw.WContext.GraphData.BothEdges != nil {
	//	for _, node := range lw.WContext.GraphData.BothEdges[vertexIdx] {
	//		count[lw.oldLabels[node]]++
	//	}
	//} else {
	for _, inNode := range lw.WContext.GraphData.Edges.GetInEdges(vertexIdx) {
		if lw.updateMethod == 1 && lw.inSelfWorker(uint32(inNode)) {
			count[lw.newLabels[inNode]]++
		} else {
			count[lw.oldLabels[inNode]]++
		}
	}
	for _, outNode := range lw.WContext.GraphData.Edges.GetOutEdges(vertexIdx) {
		if lw.updateMethod == 1 && lw.inSelfWorker(uint32(outNode)) {
			count[lw.newLabels[outNode]]++
		} else {
			count[lw.oldLabels[outNode]]++
		}
	}
	//}
	if lw.useProperty {
		for node := range count {
			switch lw.propertyType {
			case structure.ValueTypeFloat32:
				count[node] *= float64(lw.WContext.GraphData.VertexProperty.GetFloat32Value(lw.vertexProperty, vertexId))
			case structure.ValueTypeInt32:
				count[node] *= float64(lw.WContext.GraphData.VertexProperty.GetInt32Value(lw.vertexProperty, vertexId))
			}
		}
	}

	//得到maxCount
	maxCount := float64(0)
	for _, labelCount := range count {
		if maxCount < labelCount {
			maxCount = labelCount
		}
	}

	//得到满足maxCount的所有自增label
	candidate := make([]serialize.SUint32, 0)
	for originLabel, labelCount := range count {
		if labelCount == maxCount {
			candidate = append(candidate, originLabel)
		}
	}
	if len(candidate) == 0 {
		return
	}

	// // 如果原label在候选中，则不更新
	// for _, can := range candidate {
	// 	if can == lw.oldLabels[vertexId] {
	// 		lw.newLabels[vertexId] = can
	// 		return
	// 	}
	// }

	// 比较originLabel
	if lw.compareOption == 0 {
		minLabel := candidate[0]
		for _, candiLabel := range candidate {
			originLabel := candiLabel
			if originLabel < minLabel {
				minLabel = originLabel
			}
		}
		lw.newLabels[vertexId] = minLabel
	} else if lw.compareOption == 1 {
		minLabel := lw.WContext.GraphData.Vertex.GetVertex(uint32(candidate[0])).ID
		for _, candiLabel := range candidate {
			originLabel := lw.WContext.GraphData.Vertex.GetVertex(uint32(candiLabel)).ID
			if strings.Compare(originLabel, minLabel) < 0 {
				minLabel = originLabel
			}
		}
		vertexIndex, ok := lw.WContext.GraphData.Vertex.GetVertexIndex(minLabel)
		if !ok {
			logrus.Errorf("unknown vertex %v", minLabel)
		}
		lw.newLabels[vertexId] = serialize.SUint32(vertexIndex)
	}
	// }
	if lw.newLabels[vertexId] != lw.oldLabels[vertexId] {
		lw.diffSum[pId]++
	}
	if lw.newLabels[vertexId] != lw.grandpaLabels[vertexId] {
		lw.grandpaDiffs[pId]++
	}
}

func (lw *LpaWorker) AfterStep() {
	// 更新oldLabels
	copy(lw.grandpaLabels, lw.oldLabels)
	copy(lw.oldLabels, lw.newLabels)

	var diffSum serialize.SInt32
	for _, diff := range lw.diffSum {
		diffSum += diff
	}
	lw.WContext.SetValue("diff_sum", diffSum)

	var grandpaDiffSum serialize.SInt32
	for _, diff := range lw.grandpaDiffs {
		grandpaDiffSum += diff
	}
	lw.WContext.SetValue("grandpa_diff_sum", grandpaDiffSum)
}

func (lw *LpaWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type LpaMaster struct {
	compute.MasterComputerBase
}

func (wcm *LpaMaster) Compute() bool {
	diffSum := wcm.MContext.GetValue("diff_sum").(serialize.SInt32)
	logrus.Infof("different sum: %v", diffSum)

	grandpaDiffs := wcm.MContext.GetValue("grandpa_diff_sum").(serialize.SInt32)
	logrus.Infof("grandpa different sum: %v", grandpaDiffs)

	return diffSum != 0 && grandpaDiffs != 0
}
