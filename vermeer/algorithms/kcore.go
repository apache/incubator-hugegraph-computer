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
	"vermeer/apps/options"
	"vermeer/apps/serialize"
)

func init() {
	Algorithms = append(Algorithms, new(KcoreMaker))
}

type KcoreMaker struct {
	compute.AlgorithmMakerBase
}

func (kc *KcoreMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &KcoreWorker{}
}

func (kc *KcoreMaker) CreateMasterComputer() compute.MasterComputer {
	return &KcoreMaster{}
}

func (kc *KcoreMaker) Name() string {
	return "kcore"
}

func (kc *KcoreMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

type KcoreWorker struct {
	compute.WorkerComputerBase
	oldDegrees []serialize.SUint32
	newDegrees []serialize.SUint32
	node2neigh [][]serialize.SUint32
	stopFlag   serialize.SInt32
	k          serialize.SUint32
}

func (kw *KcoreWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &kw.newDegrees[i]
}

func (kw *KcoreWorker) Init() error {
	kw.k = serialize.SUint32(options.GetInt(kw.WContext.Params, "kcore.degree_k"))
	kw.oldDegrees = make([]serialize.SUint32, kw.WContext.GraphData.Vertex.TotalVertexCount())
	kw.newDegrees = make([]serialize.SUint32, kw.WContext.GraphData.Vertex.TotalVertexCount())
	kw.node2neigh = make([][]serialize.SUint32, kw.WContext.GraphData.Vertex.TotalVertexCount())
	kw.WContext.CreateValue("is_stop", compute.ValueTypeInt32, compute.CValueActionAggregate)
	return nil
}

func (kw *KcoreWorker) BeforeStep() {
	kw.stopFlag = 1
}

func (kw *KcoreWorker) Compute(vertexId uint32, pId int) {
	_ = pId
	vertexIdx := vertexId - kw.WContext.GraphData.VertIDStart
	if kw.WContext.Step == 1 {
		kw.stopFlag = 0
		inEdges := kw.WContext.GraphData.Edges.GetInEdges(vertexIdx)
		outEdges := kw.WContext.GraphData.Edges.GetOutEdges(vertexIdx)
		//neighNode存储邻接节点集合, key:neighNode
		neighNodes := make(map[uint32]int8, len(inEdges)+len(outEdges))
		for _, inNode := range inEdges {
			neighNodes[uint32(inNode)] = 1
		}
		for _, outNode := range outEdges {
			neighNodes[uint32(outNode)] = 1
		}
		//}
		//初始化每个节点newDegree的值为邻接节点的个数
		kw.newDegrees[vertexId] = serialize.SUint32(len(neighNodes))
		//node2neigh保存每个节点的邻接节点
		kw.node2neigh[vertexId] = make([]serialize.SUint32, len(neighNodes))
		i := 0
		for neighNode := range neighNodes {
			kw.node2neigh[vertexId][i] = serialize.SUint32(neighNode)
			i++
		}
		return
	}
	//节点度数为0,说明已被剔除,不再计算该节点
	if kw.oldDegrees[vertexId] == 0 {
		return
	}
	//节点度数小于k,度数置0剔除且step继续
	if kw.oldDegrees[vertexId] < kw.k {
		kw.stopFlag = 0
		kw.newDegrees[vertexId] = 0
		return
	}
	//节点度数大于等于k, 若邻接节点中有度数小于k的, 自身newDegree减一
	for _, neighNode := range kw.node2neigh[vertexId] {
		if kw.oldDegrees[neighNode] > 0 && kw.oldDegrees[neighNode] < kw.k {
			kw.stopFlag = 0
			kw.newDegrees[vertexId]--
		}
	}
}

func (kw *KcoreWorker) AfterStep() {
	kw.WContext.SetValue("is_stop", kw.stopFlag)
	//更新oldDegree
	for i := range kw.oldDegrees {
		kw.oldDegrees[i] = kw.newDegrees[i]
	}
}

func (kw *KcoreWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type KcoreMaster struct {
	compute.MasterComputerBase
}

func (km *KcoreMaster) Compute() bool {
	//stopFlag =0 表示继续迭代；=1 表示终止
	stopFlag := km.MContext.GetValue("is_stop").(serialize.SInt32)
	//所有worker的stopFlag全为1时，task终止
	return stopFlag != serialize.SInt32(len(km.MContext.WorkerCValues))
}
