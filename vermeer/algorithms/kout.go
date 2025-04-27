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
	Algorithms = append(Algorithms, new(KoutMaker))
}

type KoutMaker struct {
	compute.AlgorithmMakerBase
}

func (km *KoutMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &KoutWorker{}
}

func (km *KoutMaker) CreateMasterComputer() compute.MasterComputer {
	return &KoutMaster{}
}

func (km *KoutMaker) Name() string {
	return "kout"
}

func (km *KoutMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

type directionType int

const (
	directionBoth directionType = iota
	directionIn
	directionOut
)

type KoutWorker struct {
	compute.WorkerComputerBase
	vertValues []serialize.SUint8
	direction  directionType
	diff       []int32
}

// Init 初始化函数
func (kw *KoutWorker) Init() error {
	kw.vertValues = make([]serialize.SUint8, kw.WContext.GraphData.Vertex.TotalVertexCount())
	source := options.GetString(kw.WContext.Params, "kout.source")
	srcVertID, ok := kw.WContext.GraphData.Vertex.GetVertexIndex(source)
	if !ok {
		logrus.Errorf("source not exists: %s", source)
		return fmt.Errorf("source not exists: %s", source)
	}
	kw.vertValues[srcVertID] = 1
	direction := options.GetString(kw.WContext.Params, "kout.direction")
	// direction can be "in", "out", "both"
	switch direction {
	case "both":
		kw.direction = directionBoth
	case "in":
		// direction is "in" means use out-edge
		kw.direction = directionOut
	case "out":
		// direction is "out" means use in-edge
		kw.direction = directionIn
	default:
		logrus.Errorf("direction not exists: %s", direction)
		return fmt.Errorf("direction not exists: %s", direction)
	}
	kw.diff = make([]int32, kw.WContext.Parallel)
	kw.WContext.CreateValue("diff_sum", compute.ValueTypeInt32, compute.CValueActionAggregate)
	kw.WContext.SetValue("diff_sum", serialize.SInt32(0))
	logrus.Infof("kout init source: %s, shortId: %d, direction: %v", source, srcVertID, direction)
	return nil
}

// VertexValue 返回与给定索引i对应的顶点值的序列化接口
// 参数：
// i：给定的索引
// 返回值：
// 返回一个指向kw.vertValues[i]的指针，该元素实现了serialize.MarshalAble接口
func (kw *KoutWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &kw.vertValues[i]
}

// BeforeStep 方法在执行任务前调用，将kw.diff数组的每个元素清零
func (kw *KoutWorker) BeforeStep() {
	for i := 0; i < len(kw.diff); i++ {
		kw.diff[i] = 0
	}
}

// Compute 函数是KoutWorker类型的方法，用于执行计算操作
//
// 参数：
//   - vertexID：待计算的顶点ID
//   - pID：用于标识当前计算的进程ID
//
// 返回值：
//   - 无返回值
func (kw *KoutWorker) Compute(vertexID uint32, pID int) {
	if kw.vertValues[vertexID] > 0 {
		return
	}
	vertIDx := vertexID - kw.WContext.GraphData.VertIDStart
	switch kw.direction {
	case directionBoth:
		if kw.computeInEdges(vertexID, vertIDx) {
			kw.diff[pID]++
			return
		}
		if kw.computeOutEdges(vertexID, vertIDx) {
			kw.diff[pID]++
			return
		}
	case directionIn:
		if kw.computeInEdges(vertexID, vertIDx) {
			kw.diff[pID]++
			return
		}
	case directionOut:
		if kw.computeOutEdges(vertexID, vertIDx) {
			kw.diff[pID]++
			return
		}
	}
}

// computeInEdges 函数接收两个参数，分别表示顶点ID和短ID，返回一个bool值
// 该函数会遍历当前顶点的所有入边，检查每个入边的顶点的值是否与当前步骤相同
// 如果相同，则将当前顶点的值更新为当前步骤+1，并返回true；否则返回false
func (kw *KoutWorker) computeInEdges(vertexID uint32, vShortID uint32) (changed bool) {
	for _, nID := range kw.WContext.GraphData.Edges.GetInEdges(vShortID) {
		if kw.vertValues[nID] == serialize.SUint8(kw.WContext.Step) {
			kw.vertValues[vertexID] = serialize.SUint8(kw.WContext.Step + 1)
			return true
		}
	}
	return false
}

// computeOutEdges 计算给定顶点ID和短ID的出边，如果存在一条出边对应的顶点值与当前步骤相同，则将当前顶点的值更新为当前步骤+1，返回true，否则返回false
func (kw *KoutWorker) computeOutEdges(vertexID uint32, vShortID uint32) (changed bool) {
	for _, nID := range kw.WContext.GraphData.Edges.GetOutEdges(vShortID) {
		if kw.vertValues[nID] == serialize.SUint8(kw.WContext.Step) {
			kw.vertValues[vertexID] = serialize.SUint8(kw.WContext.Step + 1)
			return true
		}
	}
	return false
}

// AfterStep 方法计算所有 diff 的和，并将结果保存在 WContext 中
func (kw *KoutWorker) AfterStep() {
	var diffSum int32
	for _, v := range kw.diff {
		diffSum += v
	}
	kw.WContext.SetValue("diff_sum", serialize.SInt32(diffSum))
}

func (kw *KoutWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type KoutMaster struct {
	compute.MasterComputerBase
}

// Compute 方法用于计算KoutMaster实例中的不同值的总和，并返回一个布尔值，表示是否存在不同值
func (km *KoutMaster) Compute() bool {
	diffSum := km.MContext.GetValue("diff_sum").(serialize.SInt32)
	logrus.Infof("different sum: %v", diffSum)
	return diffSum != 0
}
