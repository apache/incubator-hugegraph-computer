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

package services

import (
	"fmt"
	"net/http"
	"vermeer/apps/common"
	. "vermeer/apps/master/graphs"
	"vermeer/apps/structure"

	"github.com/gin-gonic/gin"
)

type GraphsResponse struct {
	common.BaseResp
	Graphs []*structure.VermeerGraph `json:"graphs,omitempty"`
}

type GraphsHandler struct {
	common.SenHandler
}

func (gh *GraphsHandler) GET(ctx *gin.Context) {
	graphs, err := graphBiz(ctx).GetGraphs()
	if isErr(err, ctx) {
		return
	}
	gs := make([]*structure.VermeerGraph, 0, len(graphs))
	for _, graph := range graphs {
		gs = append(gs, HideParams(graph))
	}
	ctx.JSON(http.StatusOK, GraphsResponse{Graphs: gs})
}

type GraphCreateRequest struct {
	Name string `json:"name,omitempty"`
}

type GraphCreateResponse struct {
	common.BaseResp
}

type GraphCreateHandler struct {
	common.SenHandler
}

func (gh *GraphCreateHandler) POST(ctx *gin.Context) {
	req := GraphCreateRequest{}
	err := ctx.BindJSON(&req)
	if isBad(err != nil, ctx, func() string { return "request body not correct" }) {
		return
	}

	// if _, err = graphBiz(ctx).CreateGraph(req.Name, nil); isErr(err, ctx) {
	// 	return
	// }

	// For the purpose of compatibility
	ok(ctx, "Graph creation successful.")
}

type GraphPersistenceRequest struct {
	Name string `json:"name,omitempty"`
}

type GraphPersistenceResponse struct {
	common.BaseResp
	Workers []*structure.GraphPersistenceInfo `json:"workers,omitempty"`
}

type GraphResponse struct {
	common.BaseResp
	Graph *structure.VermeerGraph `json:"graph,omitempty"`
}

type GraphHandler struct {
	common.SenHandler
}

func (gh *GraphHandler) GET(ctx *gin.Context) {
	name := ctx.Param("graph_name")
	g, err := graphBiz(ctx).GetGraph(name)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, GraphResponse{Graph: HideParams(g)})
}

func (gh *GraphHandler) DELETE(ctx *gin.Context) {
	name := ctx.Param("graph_name")
	if err := graphBiz(ctx).DeleteGraph(name); isErr(err, ctx) {
		return
	}

	ok(ctx, "deleted ok")
}

type EdgesHandler struct {
	common.SenHandler
}

type edgeProperty struct {
	Edge     string            `json:"edge"`
	Property map[string]string `json:"property"`
}

type EdgesResponse struct {
	common.BaseResp
	InEdges  []string `json:"in_edges"`
	OutEdges []string `json:"out_edges"`
	//BothEdges      []string       `json:"both_edges"`
	InEdgeProperty []EdgeProperty `json:"in_edge_property,omitempty"`
}

func (eh *EdgesHandler) GET(ctx *gin.Context) {
	graphName := ctx.Param("graph_name")
	vertexId := ctx.Query("vertex_id")
	direction := ctx.Query("direction")
	if isBad(vertexId == "", ctx, func() string { return fmt.Sprintf("vertex_id not exist: %s", vertexId) }) {
		return
	}

	resp := EdgesResponse{}
	var err error
	resp.InEdges, resp.OutEdges, resp.InEdgeProperty, err = graphBiz(ctx).GetEdges(graphName, vertexId, direction)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, resp)
}

type VerticesHandler struct {
	common.SenHandler
}

type VerticesRequest struct {
	VertexIds []string `json:"vertices"`
}

// type vertex struct {
// 	ID       string            `json:"id"`
// 	Property map[string]string `json:"property,omitempty"`
// }

type VerticesResponse struct {
	common.BaseResp
	Vertices []Vertex `json:"vertices"`
}

const getVertexLimitNum = 10000

// type VerticesGoResponse struct {
// 	err      error
// 	Vertices []vertex `json:"vertices"`
// }

func (vh *VerticesHandler) POST(ctx *gin.Context) {
	req := VerticesRequest{}
	graphName := ctx.Param("graph_name")
	err := ctx.BindJSON(&req)
	//校验参数
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}

	//校验顶点数量
	if isBad(len(req.VertexIds) == 0 || len(req.VertexIds) > getVertexLimitNum,
		ctx, func() string { return fmt.Sprintf("vertex_ids num can't be 0 and can't over %d", getVertexLimitNum) }) {
		return
	}

	vertices, err := graphBiz(ctx).GetVertices(graphName, req.VertexIds)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, VerticesResponse{Vertices: vertices})
}
