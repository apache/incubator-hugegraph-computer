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
	"container/heap"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	"vermeer/apps/serialize"

	"github.com/bytedance/gopkg/lang/fastrand"
	"github.com/sirupsen/logrus"
)

func init() {
	Algorithms = append(Algorithms, new(SlpaMaker))
}

type SlpaMaker struct {
	compute.AlgorithmMakerBase
}

func (lm *SlpaMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &SlpaWorker{}
}

func (lm *SlpaMaker) CreateMasterComputer() compute.MasterComputer {
	return &SlpaMaster{}
}

func (lm *SlpaMaker) Name() string {
	return "slpa"
}

func (lm *SlpaMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}
func (lm *SlpaMaker) SupportedStatistics() map[compute.StatisticsType]struct{} {
	return map[compute.StatisticsType]struct{}{compute.StatisticsTypeCount: {}}
}

type SlpaWorker struct {
	compute.WorkerComputerBase
	// 全部prevlabels，供计算时读取，在afterstep中更新
	prevlabels []serialize.MapUint32Float32
	// 仅当前worker的newlabels，当前轮计算出的新newlabels
	newlabels []serialize.MapUint32Float32
	// 总轮次数
	maxStep int
	// 最多保留的标签数量
	k int
	// 选举标签的方法
	selectMethod int
}

const (
	selectMethodMax = iota
	selectMethodRand
)

func (sw *SlpaWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &sw.newlabels[i]
}

func (sw *SlpaWorker) Init() error {
	// 初始化label为自增id
	sw.prevlabels = make([]serialize.MapUint32Float32, sw.WContext.GraphData.Vertex.TotalVertexCount())
	sw.newlabels = make([]serialize.MapUint32Float32, sw.WContext.GraphData.Vertex.TotalVertexCount())

	sw.maxStep = options.GetInt(sw.WContext.Params, "compute.max_step")
	if sw.maxStep > 1000 {
		logrus.Infof("max_step:%v is too large, set to 1000", sw.maxStep)
		sw.maxStep = 1000
	}
	sw.k = options.GetInt(sw.WContext.Params, "slpa.k")

	selectMethod := options.GetString(sw.WContext.Params, "slpa.select_method")
	switch selectMethod {
	case "max":
		sw.selectMethod = selectMethodMax
	case "random":
		sw.selectMethod = selectMethodRand
	default:
		logrus.Errorf("slpa.select_method:%v is not supported, use default", selectMethod)
	}

	// init labels
	for i := uint32(0); i < sw.WContext.GraphData.Vertex.TotalVertexCount(); i++ {
		sw.prevlabels[i] = make(serialize.MapUint32Float32)
		sw.prevlabels[i][serialize.SUint32(i)] = 1
		sw.newlabels[i] = make(serialize.MapUint32Float32)
		sw.newlabels[i][serialize.SUint32(i)] = 1
	}

	return nil
}

func (sw *SlpaWorker) BeforeStep() {
	if sw.WContext.Step == int32(sw.maxStep) {
		logrus.Infof("step:%v, finish slpa, post processing", sw.WContext.Step)
	}
}

func (sw *SlpaWorker) selectNeighborLabel(listener uint32, speaker uint32) uint32 {
	switch sw.selectMethod {
	case selectMethodMax:
		return sw.selectNeighborLabelMax(listener, speaker)
	case selectMethodRand:
		return sw.selectNeighborLabelRand(listener, speaker)
	default:
		logrus.Errorf("slpa.select_method:%v is not supported", sw.selectMethod)
	}
	return 0
}

func (sw *SlpaWorker) selectNeighborLabelMax(lisenser uint32, speaker uint32) uint32 {
	maxWeight := serialize.SFloat32(0)
	maxLabel := 0
	for label, weight := range sw.prevlabels[speaker] {
		if weight > maxWeight {
			maxWeight = weight
			maxLabel = int(label)
		}
	}
	return uint32(maxLabel)
}

func (sw *SlpaWorker) selectNeighborLabelRand(listener uint32, speaker uint32) uint32 {
	var sum float64
	for _, weight := range sw.prevlabels[speaker] {
		sum += float64(weight)
	}

	randNum := fastrand.Float64()

	var current float64
	for label, weight := range sw.prevlabels[speaker] {
		next := current + float64(weight)
		if randNum >= (current/sum) && randNum < (next/sum) {
			return uint32(label)
		}
		current = next

	}

	logrus.Errorf("error in selectNeighborLabel, listener=%v, speaker=%v,labels[speaker]=%v, ", listener, speaker, sw.prevlabels[speaker])
	return 0
}

func (sw *SlpaWorker) Compute(vertexID uint32, pID int) {
	_ = pID
	vertexIdx := vertexID - sw.WContext.GraphData.VertIDStart

	if sw.WContext.Step < int32(sw.maxStep) {
		// count key:自增label value:frequency
		count := make(map[uint32]float64)
		for _, inNode := range sw.WContext.GraphData.Edges.GetInEdges(vertexIdx) {
			count[sw.selectNeighborLabel(vertexID, uint32(inNode))]++
		}
		for _, outNode := range sw.WContext.GraphData.Edges.GetOutEdges(vertexIdx) {
			count[sw.selectNeighborLabel(vertexID, uint32(outNode))]++
		}

		// 得到maxCount
		maxCount := float64(0)
		for _, labelCount := range count {
			if maxCount < labelCount {
				maxCount = labelCount
			}
		}

		// 得到满足maxCount的所有自增label
		candidate := make([]uint32, 0)
		for originLabel, labelCount := range count {
			if labelCount == maxCount {
				candidate = append(candidate, originLabel)
			}
		}
		if len(candidate) == 0 {
			return
		}

		// 比较originLabel
		minLabel := candidate[0]
		for _, candiLabel := range candidate {
			originLabel := candiLabel
			if originLabel < minLabel {
				minLabel = originLabel
			}
		}
		sw.newlabels[vertexID][serialize.SUint32(minLabel)]++
	} else {
		//  post-processing, select final labels
		if sw.k < 0 || sw.k >= len(sw.newlabels[vertexID]) {
			return
		}

		labelFreqs := make(common.VertexWeights, 0, sw.k)

		heap.Init(&labelFreqs)

		for label, freq := range sw.newlabels[vertexID] {
			heap.Push(&labelFreqs, common.VertexWeight{Vertex: uint32(label), Weight: float64(freq)})
			if labelFreqs.Len() > sw.k {
				heap.Pop(&labelFreqs)
			}
		}

		sw.newlabels[vertexID] = make(serialize.MapUint32Float32, sw.k)
		for _, labelFreq := range labelFreqs {
			sw.newlabels[vertexID][serialize.SUint32(labelFreq.Vertex)] = serialize.SFloat32(labelFreq.Weight)
		}
	}

}

func (sw *SlpaWorker) AfterStep() {
	for vertexID, LabelFreq := range sw.newlabels {
		for label, freq := range LabelFreq {
			sw.prevlabels[vertexID][label] = freq
		}
	}
}

func (sw *SlpaWorker) OutputValueType() string {
	return common.HgValueTypeString
}

type SlpaMaster struct {
	compute.MasterComputerBase
}

func (sm *SlpaMaster) Compute() bool {
	return true
}
