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
	Algorithms = append(Algorithms, new(JaccardMaker))
}

type JaccardMaker struct {
	compute.AlgorithmMakerBase
}

func (jm *JaccardMaker) CreateWorkerComputer() compute.WorkerComputer {
	return &JaccardWorker{}
}

func (jm *JaccardMaker) CreateMasterComputer() compute.MasterComputer {
	return &JaccardMaster{}
}

func (jm *JaccardMaker) Name() string {
	return "jaccard"
}

func (jm *JaccardMaker) DataNeeded() []string {
	return []string{compute.UseOutEdge}
}

type JaccardWorker struct {
	compute.WorkerComputerBase
	sourceID          uint32
	sourceNeighbors   map[serialize.SUint32]struct{}
	jaccardSimilarity []serialize.SFloat32
}

func (jw *JaccardWorker) VertexValue(i uint32) serialize.MarshalAble {
	return &jw.jaccardSimilarity[i]
}

func (jw *JaccardWorker) Init() error {
	var ok bool
	jw.sourceID, ok = jw.WContext.GraphData.Vertex.GetVertexIndex(options.GetString(jw.WContext.Params, "jaccard.source"))
	if !ok {
		logrus.Errorf("jaccard.source not exist:%v", options.GetString(jw.WContext.Params, "jaccard.source"))
		return fmt.Errorf("jaccard.source not exist:%v", options.GetString(jw.WContext.Params, "jaccard.source"))
	}
	jw.WContext.CreateValue("source_neighbors", compute.ValueTypeSliceUint32, compute.CValueActionAggregate)
	jw.WContext.SetValue("source_neighbors", serialize.SliceUint32{})
	jw.jaccardSimilarity = make([]serialize.SFloat32, jw.WContext.GraphData.Vertex.TotalVertexCount())
	return nil
}

func (jw *JaccardWorker) BeforeStep() {
	if jw.WContext.Step > 1 {
		sourceNeighborSlice := jw.WContext.GetValue("source_neighbors").(serialize.SliceUint32)
		jw.sourceNeighbors = make(map[serialize.SUint32]struct{}, len(sourceNeighborSlice))
		for _, neighbor := range sourceNeighborSlice {
			jw.sourceNeighbors[neighbor] = struct{}{}
		}
	}
}

func (jw *JaccardWorker) Compute(vertexId uint32, pId int) {
	_ = pId
	vertID := vertexId - jw.WContext.GraphData.VertIDStart
	inEdges := jw.WContext.GraphData.Edges.GetInEdges(vertID)
	outEdges := jw.WContext.GraphData.Edges.GetOutEdges(vertID)
	if jw.WContext.Step == 1 {
		if vertexId == jw.sourceID {
			neighbors := make(map[serialize.SUint32]struct{}, len(inEdges)+len(outEdges))
			for _, edge := range inEdges {
				neighbors[edge] = struct{}{}
			}
			for _, edge := range outEdges {
				neighbors[edge] = struct{}{}
			}
			sourceNeighbors := make([]serialize.SUint32, 0, len(neighbors))
			for edge := range neighbors {
				sourceNeighbors = append(sourceNeighbors, edge)
			}
			jw.WContext.SetValue("source_neighbors", serialize.SliceUint32(sourceNeighbors))
		}
	} else {
		selfNeighbors := make(map[serialize.SUint32]struct{}, len(inEdges)+len(outEdges))
		for _, edge := range inEdges {
			selfNeighbors[edge] = struct{}{}
		}
		for _, edge := range outEdges {
			selfNeighbors[edge] = struct{}{}
		}
		count := 0
		if len(jw.sourceNeighbors) > len(selfNeighbors) {
			for selfNeighbor := range selfNeighbors {
				if _, ok := jw.sourceNeighbors[selfNeighbor]; ok {
					count++
				}
			}
		} else {
			for sourceNeighbor := range jw.sourceNeighbors {
				if _, ok := selfNeighbors[sourceNeighbor]; ok {
					count++
				}
			}
		}
		jw.jaccardSimilarity[vertexId] =
			serialize.SFloat32(count) / serialize.SFloat32(len(selfNeighbors)+len(jw.sourceNeighbors)-count)
	}
}

func (jw *JaccardWorker) OutputValueType() string {
	return common.HgValueTypeInt
}

type JaccardMaster struct {
	compute.MasterComputerBase
}

func (dg *JaccardMaster) Compute() bool {
	return dg.MContext.Step < 2
}
