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

package worker

import (
	"fmt"
	"net/http"
	"vermeer/apps/common"
	pb "vermeer/apps/protos"

	"github.com/gin-gonic/gin"
)

type EdgesHandler struct {
	common.SenHandler
}

type EdgesResponse struct {
	common.BaseResp
	InEdges  []string `json:"in_edges"`
	OutEdges []string `json:"out_edges"`
	//BothEdges      []string           `json:"both_edges"`
	InEdgeProperty []*pb.EdgeProperty `json:"in_edge_property,omitempty"`
}

func (eh *EdgesHandler) GET(ctx *gin.Context) {
	space := ctx.Param("space_name")
	graph := ctx.Param("graph_name")
	vertId := ctx.Query("vertex_id")
	direction := ctx.Query("direction")
	resp := EdgesResponse{}
	if vertId == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": fmt.Sprintf("vertex_id not exist: %s", vertId),
		})
		return
	}
	eb := EdgesBl{}
	inEdges, outEdges, edgeProperty, err := eb.GetEdges(space, graph, vertId, direction)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": fmt.Sprintf("get edge, err:%v", err),
		})
		return
	}
	resp.InEdges = inEdges
	resp.OutEdges = outEdges
	//resp.BothEdges = bothEdges
	resp.InEdgeProperty = edgeProperty
	ctx.JSON(http.StatusOK, resp)
}

type DegreeHandler struct {
	common.SenHandler
}

type DegreeResponse struct {
	common.BaseResp
	Degree uint32 `json:"degree"`
}

func (dh *DegreeHandler) GET(ctx *gin.Context) {
	space := ctx.Param("space_name")
	graph := ctx.Param("graph_name")
	vertId := ctx.Query("vertex_id")
	direction := ctx.Query("direction")
	resp := DegreeResponse{}

	db := DegreeBl{}
	resp.Degree = db.GetDegree(space, graph, vertId, direction)
	ctx.JSON(http.StatusOK, resp)
}

type DbSerializeHandler struct {
	common.BaseHandler
}

//
//func addVertexPropertyToGraphData(data *structure.GraphData) {
//
//	var vertexPropertyInt structure.VValues
//	vertexPropertyInt.VType = structure.ValueTypeInt32
//	vv := make([]serialize.SInt32, len(data.TotalVertex))
//	for i := range vv {
//		vv[i] = serialize.SInt32(i)
//	}
//	vertexPropertyInt.Values = vv
//
//	var vertexPropertyFloat structure.VValues
//	vertexPropertyFloat.VType = structure.ValueTypeFloat32
//	vf := make([]serialize.SFloat32, len(data.TotalVertex))
//	for i := range vf {
//		vf[i] = serialize.SFloat32(i)
//	}
//	vertexPropertyFloat.Values = vf
//
//	var vertexPropertyString structure.VValues
//	vertexPropertyString.VType = structure.ValueTypeString
//	vs := make([]serialize.SString, len(data.TotalVertex))
//	for i := range vs {
//		vs[i] = serialize.SString(strconv.Itoa(i))
//	}
//	vertexPropertyString.Values = vs
//
//	data.VertexProperty = make(map[string]*structure.VValues)
//	data.VertexProperty["int"] = &vertexPropertyInt
//	data.VertexProperty["float"] = &vertexPropertyFloat
//	data.VertexProperty["string"] = &vertexPropertyString
//
//}
//
//func addInEdgePropertyToGraphData(data *structure.GraphData) {
//
//	var inEdgePropertyInt structure.VValues
//	inEdgePropertyInt.VType = structure.ValueTypeInt32
//	vv := make([][]serialize.SInt32, len(data.InEdges))
//	for i := range vv {
//		vvv := make([]serialize.SInt32, len(data.InEdges[i]))
//		for j := range vvv {
//			vvv[j] = serialize.SInt32(j)
//		}
//		vv[i] = vvv
//
//	}
//	inEdgePropertyInt.Values = vv
//
//	var inEdgePropertyFloat structure.VValues
//	inEdgePropertyFloat.VType = structure.ValueTypeFloat32
//	vf := make([][]serialize.SFloat32, len(data.InEdges))
//	for i := range vf {
//		vvv := make([]serialize.SFloat32, len(data.InEdges[i]))
//		for j := range vvv {
//			vvv[j] = serialize.SFloat32(j)
//		}
//		vf[i] = vvv
//	}
//	inEdgePropertyFloat.Values = vf
//
//	var inEdgePropertyString structure.VValues
//	inEdgePropertyString.VType = structure.ValueTypeString
//	vs := make([][]serialize.SString, len(data.InEdges))
//	for i := range vs {
//		vvv := make([]serialize.SString, len(data.InEdges[i]))
//		for j := range vvv {
//			vvv[j] = serialize.SString(strconv.Itoa(j))
//		}
//		vs[i] = vvv
//	}
//	inEdgePropertyString.Values = vs
//
//	data.InEdgesProperty = make(map[string]*structure.VValues)
//	data.InEdgesProperty["int"] = &inEdgePropertyInt
//	data.InEdgesProperty["float"] = &inEdgePropertyFloat
//	data.InEdgesProperty["string"] = &inEdgePropertyString
//
//}
