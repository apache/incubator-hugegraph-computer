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
	"io"
	"net/http"
	"strconv"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/master/bl"
	. "vermeer/apps/master/bl"
	"vermeer/apps/structure"

	"github.com/gin-gonic/gin"
)

type TasksHandler struct {
	common.SenHandler
}

type TasksResp struct {
	common.BaseResp
	Tasks []taskInJson `json:"tasks,omitempty"`
}

func (th *TasksHandler) GET(ctx *gin.Context) {
	queryType := ctx.DefaultQuery("type", "all")
	limit := ctx.DefaultQuery("limit", "100")
	limitNum, err := strconv.Atoi(limit)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, TasksResp{BaseResp: common.BaseResp{ErrCode: -1, Message: fmt.Errorf("limit convert to int error:%w", err).Error()}})
	}
	tasks, err := taskBiz(ctx).QueryTasks(queryType, limitNum)
	if isBad(err != nil, ctx, func() string { return err.Error() }) {
		return
	}
	filteredTasks := taskBiz(ctx).FilteringTasks(tasks)
	ctx.JSON(http.StatusOK, TasksResp{Tasks: taskInfo2TaskJsons(filteredTasks)})
}

func (th *TasksHandler) POST(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"code": http.StatusOK,
	})
}

type TaskCreateHandler struct {
	common.SenHandler
}

type TaskCreateRequest struct {
	TaskType  string            `json:"task_type"`
	GraphName string            `json:"graph"`
	Params    map[string]string `json:"params"`
}

type TaskCreateResponse struct {
	common.BaseResp
	Task taskInJson `json:"task,omitempty"`
}

// POST Create a task and execute it in queue order.
func (th *TaskCreateHandler) POST(ctx *gin.Context) {
	handleTaskCreation(ctx, QueueExecuteTask)
}

type TaskCreateSyncHandler struct {
	common.SenHandler
}

// POST Create a task, execute it immediately, and wait until it's done.
func (th *TaskCreateSyncHandler) POST(ctx *gin.Context) {
	handleTaskCreation(ctx, SyncExecuteTask)
}

func handleTaskCreation(ctx *gin.Context, exeFunc func(*structure.TaskInfo) error) {
	req := TaskCreateRequest{}
	err := ctx.BindJSON(&req)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}

	task, err := taskBiz(ctx).CreateTaskInfo(req.GraphName, req.TaskType, req.Params, bl.IsCheck)

	if err != ErrorTaskDuplicate {
		if isBad(err != nil, ctx, func() string { return err.Error() }) {
			common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(req.TaskType).Dec()
			return
		}
		err = exeFunc(task)
		if isBad(err != nil, ctx, func() string { return err.Error() }) {
			common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(req.TaskType).Dec()
			return
		}
	}

	filteredTask := taskBiz(ctx).FilteringTask(task)
	ctx.JSON(http.StatusOK, TaskResp{Task: taskInfo2TaskJson(filteredTask)})
}

type TaskCreateBatchHandler struct {
	common.SenHandler
}

type TaskCreateBatchRequest []TaskCreateRequest

type TaskCreateBatchResponse []TaskCreateResponse

// POST Create batch tasks and execute it in queue order.
func (th *TaskCreateBatchHandler) POST(ctx *gin.Context) {
	// todo: 批量任务创建

	reqs := TaskCreateBatchRequest{}
	err := ctx.BindJSON(&reqs)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}
	resps := TaskCreateBatchResponse{}
	tasks := make([]*structure.TaskInfo, 0, len((reqs)))
	for _, req := range reqs {
		task, err := taskBiz(ctx).CreateTaskInfo(req.GraphName, req.TaskType, req.Params, bl.NoCheck)
		if err != ErrorTaskDuplicate && err != nil {
			// handle error, ignore duplicate error
			resps = append(resps, TaskCreateResponse{
				BaseResp: common.BaseResp{
					ErrCode: 1,
					Message: err.Error(),
				},
			})
			continue
		}
		resps = append(resps, TaskCreateResponse{})
		tasks = append(tasks, task)
	}
	errs := QueueExecuteTasks(tasks)
	index := 0
	for i, resp := range resps {
		if resp.BaseResp.ErrCode == 0 {
			if errs[i] != nil {
				resps[i].BaseResp.ErrCode = 1
				resps[i].BaseResp.Message = errs[i].Error()
			} else {
				filteredTask := taskBiz(ctx).FilteringTask(tasks[index])
				resps[i].Task = taskInfo2TaskJson(filteredTask)
			}
			index++
		}
	}
	ctx.JSON(http.StatusOK, resps)
}

/*
	ComputeValueHandler
*/

type ComputeValueHandler struct {
	common.SenHandler
}

type ComputeValueResponse struct {
	common.BaseResp
	Vertices []compute.VertexValue `json:"vertices,omitempty"`
	Cursor   int32                 `json:"cursor,omitempty"`
}

func (ch *ComputeValueHandler) GET(ctx *gin.Context) {
	resp := ComputeValueResponse{}
	var err error
	limitQuery := ctx.Query("limit")
	limit := 1000
	if limitQuery != "" {
		limit, err = strconv.Atoi(limitQuery)
		//default limit=1000
		if err != nil {
			resp.BaseResp.Message = fmt.Sprintf("limit value not correct:%v , set to default value:10000", limit)
		}
		if limit > 100000 || limit <= 0 {
			resp.BaseResp.Message = fmt.Sprintf("limit:%v, must be > 0 and <= 100,000, set to default value: 1000", limit)
			limit = 1000
		}
	}

	cursorQuery := ctx.Query("cursor")
	var cursor int
	if cursorQuery == "" {
		cursor = 0
	} else {
		cursor, err = strconv.Atoi(cursorQuery)
		if isBad(err != nil, ctx, func() string { return fmt.Sprintf("query cursor not correct: %s", err) }) {
			return
		}
	}

	taskIDString := ctx.Param("task_id")
	if isBad(taskIDString == "", ctx, func() string { return fmt.Sprintf("Task_id must be available:%s", taskIDString) }) {
		return
	}

	taskID, err := strconv.Atoi(taskIDString)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("task_id not correct: %s", err) }) {
		return
	}

	//执行分页查询
	resp.Vertices, resp.Cursor, err = taskBiz(ctx).QueryResults(int32(taskID), cursor, limit)
	if err == io.EOF {
		resp.BaseResp.ErrCode = 0
		resp.BaseResp.Message = err.Error()
		ctx.JSON(http.StatusOK, resp)
		return
	} else if isBad(err != nil, ctx, func() string { return err.Error() }) {
		return
	}

	ctx.JSON(http.StatusOK, resp)
}
