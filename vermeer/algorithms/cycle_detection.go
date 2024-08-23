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
	Algorithms = append(Algorithms, new(CycleDetectionMaker))
}

type CycleDetectionMaker struct {
	compute.AlgorithmMakerBase
}

func (cd *CycleDetectionMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &CycleDetectionWorker{}
}

func (cd *CycleDetectionMaker) CreateMasterComputer() compute.MasterComputer {
	return &CycleDetectionMaster{}
}

func (cd *CycleDetectionMaker) Name() string {
	return "cycle_detection"
}

func (cd *CycleDetectionMaker) DataNeeded() []string {
	return []string{}
}

type cyclesMode string

const (
	All     cyclesMode = "all"
	Limit   cyclesMode = "limit"
	Boolean cyclesMode = "boolean"
)

type CycleDetectionWorker struct {
	compute.WorkerComputerBase
	mode            cyclesMode
	maxLen          int
	minLen          int
	limit           int
	hasCycle        []serialize.SUint8
	inEdges         []serialize.SliceUint32
	cycleList       []serialize.TwoDimSliceString
	useVertexFilter bool
	useEdgeFilter   bool
	vertexFilter    *compute.VertexFilter
	edgeFilter      *compute.EdgeFilter
}

func (cdw *CycleDetectionWorker) Init() error {
	cdw.inEdges = make([]serialize.SliceUint32, cdw.WContext.GraphData.Vertex.TotalVertexCount())
	cdw.maxLen = options.GetInt(cdw.WContext.Params, "cycle.max_length")
	cdw.minLen = options.GetInt(cdw.WContext.Params, "cycle.min_length")
	cdw.mode = cyclesMode(options.GetString(cdw.WContext.Params, "cycle.mode"))
	switch cdw.mode {
	case All:
		cdw.cycleList = make([]serialize.TwoDimSliceString, cdw.WContext.GraphData.VertexCount)
	case Limit:
		cdw.cycleList = make([]serialize.TwoDimSliceString, cdw.WContext.GraphData.VertexCount)
		cdw.limit = options.GetInt(cdw.WContext.Params, "cycle.max_cycles")
	case Boolean:
		cdw.hasCycle = make([]serialize.SUint8, cdw.WContext.GraphData.VertexCount)
	default:
		return fmt.Errorf("cycle detection mode must be 'all', 'limit', 'boolean'. not: %v", cdw.mode)
	}
	cdw.WContext.CreateValue("cycle_num", compute.ValueTypeInt32, compute.CValueActionAggregate)
	cdw.WContext.SetValue("cycle_num", serialize.SInt32(0))
	vertexExprStr := options.GetString(cdw.WContext.Params, "filter.vertex_expr")
	filterVertexProps := options.GetSliceString(cdw.WContext.Params, "filter.vertex_properties")
	edgeExprStr := options.GetString(cdw.WContext.Params, "filter.edge_expr")
	filterEdgeProps := options.GetSliceString(cdw.WContext.Params, "filter.edge_properties")
	if vertexExprStr != "" && len(filterVertexProps) > 0 {
		cdw.useVertexFilter = true
		cdw.vertexFilter = &compute.VertexFilter{}
		err := cdw.vertexFilter.Init(vertexExprStr, filterVertexProps, cdw.WContext.GraphData.VertexProperty)
		if err != nil {
			logrus.Errorf("vertex filter init error:%v", err)
			return fmt.Errorf("vertex filter init error:%w", err)
		}
	}
	if edgeExprStr != "" && len(filterEdgeProps) > 0 {
		cdw.useEdgeFilter = true
		cdw.edgeFilter = &compute.EdgeFilter{}
		err := cdw.edgeFilter.Init(edgeExprStr, filterEdgeProps, cdw.WContext.GraphData.InEdgesProperty)
		if err != nil {
			logrus.Errorf("edge filter init error:%v", err)
			return fmt.Errorf("edge filter init error:%w", err)
		}
	}
	return nil
}

func (cdw *CycleDetectionWorker) VertexValue(i uint32) serialize.MarshalAble {
	if cdw.WContext.Output {
		switch cdw.mode {
		case All, Limit:
			return &cdw.cycleList[i-cdw.WContext.GraphData.VertIDStart]
		case Boolean:
			return &cdw.hasCycle[i-cdw.WContext.GraphData.VertIDStart]
		}
	}
	if cdw.WContext.Step == 1 {
		return &cdw.inEdges[i]
	}
	nilValue := serialize.SInt32(0)
	return &nilValue
}

func (cdw *CycleDetectionWorker) BeforeStep() {
}

func (cdw *CycleDetectionWorker) Compute(vertexID uint32, pID int) {
	if cdw.useVertexFilter && !cdw.vertexFilter.Filter(vertexID) {
		return
	}
	_ = pID
	vIDx := vertexID - cdw.WContext.GraphData.VertIDStart
	if cdw.WContext.Step == 1 {
		cdw.inEdges[vertexID] = make([]serialize.SUint32, 0, len(cdw.WContext.GraphData.Edges.GetInEdges(vIDx)))
		tempMap := make(map[serialize.SUint32]struct{})
		for idx, edge := range cdw.WContext.GraphData.Edges.GetInEdges(vIDx) {
			if cdw.useVertexFilter && !cdw.vertexFilter.Filter(uint32(edge)) ||
				cdw.useEdgeFilter && !cdw.edgeFilter.Filter(vIDx, uint32(idx)) {
				continue
			}
			if _, ok := tempMap[edge]; ok || vertexID == uint32(edge) {
				continue
			}
			tempMap[edge] = struct{}{}
			cdw.inEdges[vertexID] = append(cdw.inEdges[vertexID], edge)
		}
	} else {
		switch cdw.mode {
		case All, Limit:
			for _, edge := range cdw.inEdges[vertexID] {
				if cdw.WContext.GraphData.Vertex.GetVertex(vertexID).ID < cdw.WContext.GraphData.Vertex.GetVertex(uint32(edge)).ID {
					cdw.dfs(serialize.SUint32(vertexID), edge, []serialize.SUint32{edge})
				}
			}
		case Boolean:
			for _, edge := range cdw.inEdges[vertexID] {
				cdw.dfs(serialize.SUint32(vertexID), edge, []serialize.SUint32{edge})
			}
		}
	}
}

func (cdw *CycleDetectionWorker) dfs(rootID serialize.SUint32, vertexID serialize.SUint32, stack []serialize.SUint32) {
	//logrus.Infof("root:%v, vID:%v, stack:%v", rootID, vertexID, stack)
	if len(stack) > cdw.maxLen {
		return
	}
	temp := make(map[serialize.SUint32]struct{}, len(stack))
	for _, node := range stack {
		if _, ok := temp[node]; ok {
			return
		}
		temp[node] = struct{}{}
	}

	for _, edge := range cdw.inEdges[vertexID] {
		if _, ok := temp[edge]; ok {
			continue
		}
		switch cdw.mode {
		case All:
			if cdw.WContext.GraphData.Vertex.GetVertex(uint32(rootID)).ID < cdw.WContext.GraphData.Vertex.GetVertex(uint32(edge)).ID {
				cdw.dfs(rootID, edge, append(stack, edge))
			} else if rootID == edge && len(stack) >= cdw.minLen {
				vIDx := uint32(rootID) - cdw.WContext.GraphData.VertIDStart
				newList := make([]serialize.SString, 0, len(stack))
				for _, node := range stack {
					newList = append(newList, serialize.SString(cdw.WContext.GraphData.Vertex.GetVertex(uint32(node)).ID))
				}
				cdw.cycleList[vIDx] = append(cdw.cycleList[vIDx], newList)
			}
		case Limit:
			vIDx := uint32(rootID) - cdw.WContext.GraphData.VertIDStart
			if len(cdw.cycleList[vIDx]) >= cdw.limit {
				return
			}
			if cdw.WContext.GraphData.Vertex.GetVertex(uint32(rootID)).ID < cdw.WContext.GraphData.Vertex.GetVertex(uint32(edge)).ID {
				cdw.dfs(rootID, edge, append(stack, edge))
			} else if rootID == edge && len(stack) >= cdw.minLen {
				vIDx := uint32(rootID) - cdw.WContext.GraphData.VertIDStart
				newList := make([]serialize.SString, 0, len(stack))
				for _, node := range stack {
					newList = append(newList, serialize.SString(cdw.WContext.GraphData.Vertex.GetVertex(uint32(node)).ID))
				}
				cdw.cycleList[vIDx] = append(cdw.cycleList[vIDx], newList)
				if len(cdw.cycleList[vIDx]) >= cdw.limit {
					return
				}
			}
		case Boolean:
			if cdw.hasCycle[uint32(rootID)-cdw.WContext.GraphData.VertIDStart] == 1 {
				return
			}
			if rootID != edge {
				cdw.dfs(rootID, edge, append(stack, edge))
			} else if len(stack) >= cdw.minLen {
				cdw.hasCycle[uint32(rootID)-cdw.WContext.GraphData.VertIDStart] = 1
				return
			}
		}
	}
}

func (cdw *CycleDetectionWorker) AfterStep() {
	if cdw.WContext.Step > 1 {
		cycleCount := 0
		switch cdw.mode {
		case All, Limit:
			for _, list := range cdw.cycleList {
				cycleCount += len(list)
			}
		case Boolean:
			for _, has := range cdw.hasCycle {
				if has == 1 {
					cycleCount++
				}
			}
		}

		//logrus.Infof("cycle count:%v", cycleCount)
		cdw.WContext.SetValue("cycle_num", serialize.SInt32(cycleCount))
	}
}

func (cdw *CycleDetectionWorker) OutputValueType() string {
	return common.HgValueTypeString
}

type CycleDetectionMaster struct {
	compute.MasterComputerBase
}

func (cdm *CycleDetectionMaster) Init() error {
	return nil
}

func (cdm *CycleDetectionMaster) Compute() bool {
	if cdm.MContext.Step == 2 {
		cycleNum := cdm.MContext.GetValue("cycle_num").(serialize.SInt32)
		logrus.Infof("cycles num:%v", cycleNum)
		return false
	}
	return true
}
