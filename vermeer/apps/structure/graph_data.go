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

package structure

import (
	"sync"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

type VertexInterface interface {
	Init(dataDir string)
	TotalVertexCount() uint32
	GetVertex(vertexID uint32) Vertex
	GetVertexIndex(vertex string) (uint32, bool)
	AppendVertices(vertex ...Vertex)
	SetVertex(vertexID uint32, vertex Vertex)
	SetVertices(offset uint32, vertex ...Vertex)
	BuildVertexMap()
	RecastVertex(totalCount int64, vertStart uint32, workers []*GraphWorker)
	save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup)
	load(meta GraphMeta, dataDir string, wg *sync.WaitGroup)
	deleteData()
	freeMem()
}

type EdgesInterface interface {
	Init(useOutEdges bool, useOutDegree bool)
	GetInEdges(vertID uint32) serialize.SliceUint32
	GetOutEdges(vertID uint32) serialize.SliceUint32
	GetOutDegree(vertexID uint32) serialize.SUint32
	UseOutEdges() bool
	UseOutDegree() bool
	AppendInEdge(vertID uint32, edge serialize.SUint32)
	AppendOutEdge(vertID uint32, edge serialize.SUint32)
	EdgeLockFunc(vertID uint32, fun func())
	AddOutDegree(vertexID uint32, degree uint32)
	SetOutDegree(vertexID uint32, degree serialize.SUint32)
	BuildEdge(edgeNums int, vertexCount uint32)
	BuildOutDegree(totalVertexCount uint32)
	BuildOutEdges(edgeNums int, vertexCount uint32)
	OptimizeEdgesMemory()
	OptimizeOutEdgesMemory()
	save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup)
	load(meta GraphMeta, dataDir string, wg *sync.WaitGroup)
}

type VertexPropertyInterface interface {
	Init(schema PropertySchema)
	AppendProp(prop PropertyValue, schema PropertySchema)
	AppendProps(prop VertexProperties)
	GetValueType(propKey string) (ValueType, bool)
	GetInt32Value(propKey string, idx uint32) serialize.SInt32
	GetStringValue(propKey string, idx uint32) serialize.SString
	GetFloat32Value(propKey string, idx uint32) serialize.SFloat32
	SetValue(propKey string, idx uint32, value serialize.MarshalAble)
	GetValue(propKey string, idx uint32) serialize.MarshalAble
	Recast(totalCount int64, vertStart uint32, schema PropertySchema)
	save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup)
	load(wg *sync.WaitGroup, meta GraphMeta, dir string)
}

type EdgesPropertyInterface interface {
	Init(schema PropertySchema, vertexCount uint32)
	GetInt32Value(propKey string, vertID, idx uint32) serialize.SInt32
	GetStringValue(propKey string, vertID, idx uint32) serialize.SString
	GetFloat32Value(propKey string, vertID, idx uint32) serialize.SFloat32
	GetValue(propKey string, vertID, idx uint32) (serialize.MarshalAble, error)
	GetValueType(propKey string) (ValueType, bool)
	AppendProp(prop PropertyValue, inIdx uint32, schema PropertySchema)
	OptimizeMemory()
	save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup)
	load(wg *sync.WaitGroup, meta GraphMeta, dir string)
}

type GraphData struct {
	graphName             string
	spaceName             string
	VertIDStart           uint32
	VertexCount           uint32
	EdgeCount             int64
	VertexPropertySchema  PropertySchema
	InEdgesPropertySchema PropertySchema

	Vertex          VertexInterface
	VertexProperty  VertexPropertyInterface
	Edges           EdgesInterface
	InEdgesProperty EdgesPropertyInterface

	//TotalVertex     []Vertex
	//InEdges         serialize.TwoDimSliceUint32
	//OutEdges        serialize.TwoDimSliceUint32
	//OutDegree       []serialize.SUint32
	//VertexLongIDMap map[string]uint32
	//EdgeLocker      []common.SpinLocker
	//VertexProperty  VertexProperties
	//InEdgesProperty EdgeProperties
}

var DataBackendInMem string = "mem"
var DataBackendInDB string = "db"

type GraphDataBackendOption struct {
	VertexDataBackend string `json:"vertex_data_backend"`
	// VertexPropertyBackend string
	// EdgesDataBackend      string
	// EdgesPropertyBackend  string
}

type GraphDataOption struct {
	spaceName    string
	graphName    string
	dataDir      string
	firstInit    bool
	useOutEdges  bool
	useOutDegree bool
}

func (gd *GraphData) MallocData(option GraphDataBackendOption) {
	switch option.VertexDataBackend {
	case DataBackendInMem:
		gd.Vertex = &VertexMem{}
	case DataBackendInDB:
		gd.Vertex = &VertexInDB{}
	}
	gd.Edges = &EdgeMem{}
	gd.VertexProperty = &VertexProperties{}
	gd.InEdgesProperty = &EdgeProperties{}
}

func (gd *GraphData) SetOption(option GraphDataOption) {
	gd.graphName = option.graphName
	gd.spaceName = option.spaceName
	if option.firstInit {
		gd.Vertex.Init(option.dataDir)
	}
	gd.Edges.Init(option.useOutEdges, option.useOutDegree)
}

//func (gd *GraphData) Name() string {
//	return gd.name
//}
//
//func (gd *GraphData) SelfVertexStart() uint32 {
//	return gd.VertIDStart
//}
//
//func (gd *GraphData) SelfVertexCount() uint32 {
//	return gd.VertexCount
//}
//
//func (gd *GraphData) GetVertexPropertySchema() PropertySchema {
//	return gd.VertexPropertySchema
//}
//
//func (gd *GraphData) GetEdgePropertySchema() PropertySchema {
//	return gd.InEdgesPropertySchema
//}
//
//func (gd *GraphData) SetVertexPropertySchema(schema PropertySchema) {
//	gd.InEdgesPropertySchema = schema
//}
//
//func (gd *GraphData) SetEdgePropertySchema(schema PropertySchema) {
//	gd.InEdgesPropertySchema = schema
//}

func (gd *GraphData) RecastCount(workers []*GraphWorker) (totalCount int64) {
	vSize := int64(0)
	for _, w := range workers {
		if w.IsSelf {
			w.ScatterOffset += w.VertexCount
			gd.VertexCount = w.VertexCount
			gd.VertIDStart = w.VertIdStart
		}
		vSize += int64(w.VertexCount)
	}

	if vSize < int64(gd.Vertex.TotalVertexCount()) {
		logrus.Errorf("RecastVertex TotalVertex not enough: %d<%d", vSize, gd.Vertex.TotalVertexCount())
		return 0
	}
	return vSize
}

func (gd *GraphData) Delete() {
	gd.Vertex.deleteData()
}

func (gd *GraphData) FreeMem() {
	gd.Vertex.freeMem()
}
