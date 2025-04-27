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
	"strconv"
	"strings"
	"vermeer/apps/common"
	"vermeer/apps/master/threshold"
	"vermeer/apps/master/workers"

	//. "vermeer/apps/master/graphs"
	. "vermeer/apps/master/workers"

	"github.com/gin-gonic/gin"
)

type AdminGraphSaveHandler struct {
	common.SenHandler
}

// POST
func (gh *AdminGraphSaveHandler) POST(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	graphName := ctx.Param("graph_name")
	workers, err := adminBiz(ctx).SaveGraph(spaceName, graphName)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, GraphPersistenceResponse{Workers: workers})
}

type AdminGraphReadHandler struct {
	common.SenHandler
}

// POST
func (gh *AdminGraphReadHandler) POST(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	graphName := ctx.Param("graph_name")
	workers, err := adminBiz(ctx).ReadGraph(spaceName, graphName)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, GraphPersistenceResponse{Workers: workers})
}

type AdminGraphCreateHandler struct {
	common.SenHandler
}

type AdminGraphsHandler struct {
	common.SenHandler
}

func (gh *AdminGraphsHandler) GET(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	biz := adminGraphBiz(ctx, spaceName)
	graphs, err := biz.GetSpaceGraphs(spaceName)
	if isErr(err, ctx) {
		return
	}
	ctx.JSON(http.StatusOK, GraphsResponse{Graphs: graphs})
}

func (gh *AdminGraphsHandler) DELETE(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	deleteCount := 0
	failedGraphs := make([]string, 0)
	if spaceName != "" {
		biz := adminGraphBiz(ctx, spaceName)
		graphs, err := biz.GetGraphs()
		if err != nil {
			return
		}
		for _, graph := range graphs {
			if graph.SpaceName != spaceName {
				continue
			}
			err := biz.DeleteGraph(graph.Name)
			if err == nil {
				deleteCount++
			} else {
				failedGraphs = append(failedGraphs,
					fmt.Sprintf("space: %v graph: %v delete error: %v", spaceName, graph.Name, err))
			}
		}
	}
	if len(failedGraphs) == 0 {
		ok(ctx, "deleted ok")
	} else {
		ok(ctx, fmt.Sprintf("deleted count:%v,failed:%v", deleteCount, failedGraphs))
	}
}

// POST
func (gh *AdminGraphCreateHandler) POST(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	biz := adminGraphBiz(ctx, spaceName)

	req := GraphCreateRequest{}
	err := ctx.BindJSON(&req)
	if isBad(err != nil, ctx, func() string { return "request body not correct" }) {
		return
	}

	if _, err = biz.CreateGraph(req.Name, nil); isErr(err, ctx) {
		return
	}

	ok(ctx, "Graph creation successful.")
}

type AdminGraphHandler struct {
	common.SenHandler
}

// GET
func (gh *AdminGraphHandler) GET(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	name := ctx.Param("graph_name")
	biz := adminGraphBiz(ctx, spaceName)

	g, err := biz.GetGraph(name)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, GraphResponse{Graph: g})
}

// DELETE
func (gh *AdminGraphHandler) DELETE(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	name := ctx.Param("graph_name")
	biz := adminGraphBiz(ctx, spaceName)

	if err := biz.DeleteGraph(name); isErr(err, ctx) {
		return
	}

	ok(ctx, "deleted ok")
}

type AdminEdgesHandler struct {
	common.SenHandler
}

func (eh *AdminEdgesHandler) GET(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	graphName := ctx.Param("graph_name")
	biz := adminGraphBiz(ctx, spaceName)

	vertexId := ctx.Query("vertex_id")
	direction := ctx.Query("direction")
	if isBad(vertexId == "", ctx, func() string { return fmt.Sprintf("vertex_id not exist: %s", vertexId) }) {
		return
	}

	resp := EdgesResponse{}
	var err error
	resp.InEdges, resp.OutEdges, resp.InEdgeProperty, err = biz.GetEdges(graphName, vertexId, direction)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, resp)
}

type AdminVerticesHandler struct {
	common.SenHandler
}

func (vh *AdminVerticesHandler) POST(ctx *gin.Context) {
	spaceName := ctx.Param("space_name")
	graphName := ctx.Param("graph_name")
	biz := adminGraphBiz(ctx, spaceName)

	req := VerticesRequest{}
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

	vertices, err := biz.GetVertices(graphName, req.VertexIds)
	if isErr(err, ctx) {
		return
	}

	ctx.JSON(http.StatusOK, VerticesResponse{Vertices: vertices})
}

type AdminWorkersHandler struct {
	common.SenHandler
}
type AdminWorkersResponse struct {
	common.BaseResp
	AllWorkers    map[string][]*WorkerClient `json:"all_workers,omitempty"`
	GroupWorkers  map[string][]*WorkerClient `json:"group_workers,omitempty"`
	SpaceWorkers  map[string][]*WorkerClient `json:"space_workers,omitempty"`
	GraphWorkers  map[string][]*WorkerClient `json:"graph_workers,omitempty"`
	CommonWorkers map[string][]*WorkerClient `json:"common_workers,omitempty"`
	DutyWorkers   map[string][]*WorkerClient `json:"duty_workers,omitempty"`
}

func (vh *AdminWorkersHandler) GET(ctx *gin.Context) {
	adminBiz(ctx) // for checking auth
	dutyWorkers := make(map[string][]*WorkerClient)
	offlineWorkers := make([]*WorkerClient, 0)
	workerMap := make(map[string]*WorkerClient)

	for _, g := range GraphMgr.GetAllGraphs() {
		workers := make([]*WorkerClient, 0)
		for _, w := range g.Workers {

			worker := WorkerMgr.GetWorkerInfo(w.Name)

			if worker != nil {
				workers = append(workers, worker)

				if _, ok := workerMap[worker.Name]; !ok && worker.State == "OFFLINE" {
					offlineWorkers = append(offlineWorkers, worker)
					workerMap[worker.Name] = worker
				}

			} else {
				workers = append(workers, &WorkerClient{Name: w.Name, State: "NOT_FOUND"})
			}

		}
		dutyWorkers[toSpaceGraph(g.SpaceName, g.Name)] = SortWorkersAsc(workers)
	}

	ctx.JSON(http.StatusOK, AdminWorkersResponse{
		AllWorkers: map[string][]*WorkerClient{
			"online":  WorkerMgr.GetAllWorkers(),
			"offline": offlineWorkers,
		},
		GroupWorkers:  WorkerMgr.AllGroupWorkers(),
		SpaceWorkers:  WorkerMgr.AllSpaceWorkers(),
		GraphWorkers:  WorkerMgr.AllGraphWorkers(),
		CommonWorkers: WorkerMgr.CommonWorkers(),
		DutyWorkers:   dutyWorkers,
	})

}

type AdminWorkerGroupAllocHandler struct {
	common.SenHandler
}

func (wg *AdminWorkerGroupAllocHandler) DELETE(ctx *gin.Context) {
	adminBiz(ctx) // for checking auth

	workerGroup := ctx.Param("worker_group")

	// unallocate the worker group from both space and graph
	num, err := WorkerMgr.UnallocGroup(workerGroup)
	if isErr(err, ctx) {
		return
	}

	ok(ctx, fmt.Sprintf("unallocated worker group: '%s' success, updated: %d", workerGroup, num))

}

type AdminWorkerGroupSpaceAllocHandler struct {
	common.SenHandler
}

func (vh *AdminWorkerGroupSpaceAllocHandler) POST(ctx *gin.Context) {
	adminBiz(ctx) // for checking auth
	workerGroup := ctx.Param("worker_group")
	spaceName := ctx.Param("space_name")

	err := WorkerMgr.AllocGroupSpace(workerGroup, spaceName)
	if isErr(err, ctx) {
		return
	}

	ok(ctx, fmt.Sprintf("worker group:'%s' allocated to space: '%s' success", workerGroup, spaceName))
}

type AdminWorkerGroupGraphAllocHandler struct {
	common.SenHandler
}

func (vh *AdminWorkerGroupGraphAllocHandler) POST(ctx *gin.Context) {
	adminBiz(ctx) // for checking auth
	workerGroup := ctx.Param("worker_group")
	spaceName := ctx.Param("space_name")
	graphName := ctx.Param("graph_name")

	err := WorkerMgr.AllocGroupGraph(workerGroup, spaceName, graphName)
	if isErr(err, ctx) {
		return
	}

	ok(ctx, fmt.Sprintf("worker group:'%s' allocated to graph: '%s/%s' success", workerGroup, spaceName, graphName))
}

type AdminWorkerGroupHandler struct {
	common.SenHandler
}

func (vh *AdminWorkerGroupHandler) POST(ctx *gin.Context) {
	adminBiz(ctx) // for checking auth
	workerName := ctx.Param("worker_name")
	workerGroup := ctx.Param("worker_group")

	err := WorkerMgr.SetWorkerGroup(workerName, strings.TrimSpace(workerGroup))
	if isErr(err, ctx) {
		return
	}

	ok(ctx, fmt.Sprintf("set worker group success, worker: '%s' group: %s", workerName, workerGroup))
}

type AdminDispatchPauseHandler struct {
	common.SenHandler
}

func (h *AdminDispatchPauseHandler) POST(ctx *gin.Context) {
	err := adminBiz(ctx).PauseDispatchTask()

	if isErr(err, ctx) {
		return
	}

	ok(ctx, "paused the scheduler dispatch successfully")
}

func (h *AdminDispatchPauseHandler) GET(ctx *gin.Context) {
	ok(ctx, strconv.FormatBool(adminBiz(ctx).IsDispatchPaused()))
}

type AdminDispatchResumeHandler struct {
	common.SenHandler
}

func (h *AdminDispatchResumeHandler) POST(ctx *gin.Context) {
	err := adminBiz(ctx).ResumeDispatchTask()

	if isErr(err, ctx) {
		return
	}

	ok(ctx, "resume the scheduler dispatch successfully")
}

// ----------------------------Threshold-----------------------------------

type AdminThresholdMemHandler struct {
	common.SenHandler
}

type AdminThresholdMemRequest struct {
	GroupName string  `json:"group_name"`
	MaxMem    *uint32 `json:"max_mem"`
	MinFree   *uint32 `json:"min_free"`
	GcPct     *uint32 `json:"gc_pct"`
}

type AdminThresholdMemResponse struct {
	common.BaseResp
	AllMaxMem  map[string]uint32 `json:"all_max_mem,omitempty"`
	AllMinFree map[string]uint32 `json:"all_min_free,omitempty"`
	AllGcPct   map[string]uint32 `json:"all_gc_pct,omitempty"`
}

func (h *AdminThresholdMemHandler) POST(ctx *gin.Context) {
	req := AdminThresholdMemRequest{}
	err := ctx.BindJSON(&req)

	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}
	if isBad(req.GroupName == "", ctx, func() string { return "the `group name` should not be empty" }) {
		return
	}

	flag := false
	if req.MaxMem != nil || req.MinFree != nil || req.GcPct != nil {
		flag = true
	}

	if isBad(!flag, ctx, func() string { return "there is nothing to set" }) {
		return
	}

	if req.MaxMem != nil && isErr(threshold.SetGroupMaxMem(req.GroupName, *req.MaxMem), ctx) {
		return
	}
	if req.MinFree != nil && isErr(threshold.SetGroupMinFree(req.GroupName, *req.MinFree), ctx) {
		return
	}
	if req.GcPct != nil && isErr(threshold.SetGroupGcPct(req.GroupName, *req.GcPct), ctx) {
		return
	}

	// send to workers via the group
	total, success, err := workers.SendMemLimitGroup(req.GroupName, &workers.WorkerMemLimit{
		MaxMem:  *req.MaxMem,
		MinFree: *req.MinFree,
		GcRatio: float32(*req.GcPct) / 100,
	})

	if isBad(err != nil, ctx, func() string {
		return fmt.Sprintf("send group '%s' mem limit failed: %s, total: %d, success: %d", req.GroupName, err, total, success)
	}) {
		return
	}

	ok(ctx, fmt.Sprintf("set the threshold for `memory` successfully with group: '%s', total: %d, success: %d", req.GroupName, total, success))
}

func (h *AdminThresholdMemHandler) GET(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, AdminThresholdMemResponse{
		AllMaxMem:  threshold.AllGroupMaxMem(),
		AllMinFree: threshold.AllGroupMinFree(),
		AllGcPct:   threshold.AllGroupGcPct(),
	})
}
