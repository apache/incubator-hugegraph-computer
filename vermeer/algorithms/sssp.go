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
	Algorithms = append(Algorithms, new(SsspMaker))
}

type SsspMaker struct {
	compute.AlgorithmMakerBase
}

func (sp *SsspMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &SsspWorker{}
}

func (sp *SsspMaker) CreateMasterComputer() compute.MasterComputer {
	return &SsspMaster{}
}

func (sp *SsspMaker) Name() string {
	return "sssp"
}

func (sp *SsspMaker) DataNeeded() []string {
	return []string{}
}

type SsspWorker struct {
	compute.WorkerComputerBase
	srcID      serialize.SUint32
	newValues  []serialize.SInt32
	isContinue serialize.SInt32
	srcExist   serialize.SInt32
}

func (sw *SsspWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &sw.newValues[i]
}

func (sw *SsspWorker) Init() error {
	sw.newValues = make([]serialize.SInt32, sw.WContext.GraphData.Vertex.TotalVertexCount())
	// 不可达节点的最短路径为-1
	for i := uint32(0); i < sw.WContext.GraphData.Vertex.TotalVertexCount(); i++ {
		sw.newValues[i] = -1
	}

	originID := options.GetString(sw.WContext.Params, "sssp.source")
	srcID, ok := sw.WContext.GraphData.Vertex.GetVertexIndex(originID)
	if !ok {
		sw.srcExist = 0
		logrus.Errorf("no node id named:%v", originID)
		return fmt.Errorf(`unknown source node id named:%v`, originID)
	} else {
		sw.srcExist = 1
		sw.srcID = serialize.SUint32(srcID)
		logrus.Infof("sssp init originID: %s, shortId: %d", originID, sw.srcID)
		sw.newValues[sw.srcID] = 0
	}
	sw.WContext.CreateValue("is_continue", compute.ValueTypeInt32, compute.CValueActionAggregate)
	return nil
}

func (sw *SsspWorker) BeforeStep() {
	sw.isContinue = 0
}

func (sw *SsspWorker) Compute(vertexId uint32, pId int) {
	_ = pId
	if sw.srcExist == 0 || sw.newValues[vertexId] != -1 {
		return
	}
	vertexIdx := vertexId - sw.WContext.GraphData.VertIDStart
	for _, nid := range sw.WContext.GraphData.Edges.GetInEdges(vertexIdx) {
		if sw.newValues[nid] == serialize.SInt32(sw.WContext.Step-1) {
			sw.newValues[vertexId] = serialize.SInt32(sw.WContext.Step)
			sw.isContinue = 1
			return
		}
	}
}

func (sw *SsspWorker) OutputValueType() string {
	return common.HgValueTypeFloat
}

func (sw *SsspWorker) AfterStep() {
	sw.WContext.SetValue("is_continue", sw.isContinue)

}

type SsspMaster struct {
	compute.MasterComputerBase
}

func (sm *SsspMaster) Compute() bool {
	//is_continue >1 表示继续迭代；=0 表示终止
	isContinue := sm.MContext.GetValue("is_continue").(serialize.SInt32)
	return isContinue != 0
}
