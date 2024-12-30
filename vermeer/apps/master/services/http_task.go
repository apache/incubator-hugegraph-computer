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
	"time"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/gin-gonic/gin"
)

type taskInJson struct {
	ID               int32                `json:"id,omitempty"`
	Status           structure.TaskStatus `json:"status,omitempty"`
	State            structure.TaskState  `json:"state,omitempty"`
	CreateUser       string               `json:"create_user,omitempty"`
	CreateType       string               `json:"create_type,omitempty"`
	CreateTime       time.Time            `json:"create_time,omitempty"`
	StartTime        time.Time            `json:"start_time,omitempty"`
	UpdateTime       time.Time            `json:"update_time,omitempty"`
	GraphName        string               `json:"graph_name,omitempty"`
	SpaceName        string               `json:"space_name,omitempty"`
	Type             string               `json:"task_type,omitempty"`
	Params           map[string]string    `json:"params,omitempty"`
	Workers          []taskWorker         `json:"workers,omitempty"`
	ErrMessage       string               `json:"error_message,omitempty"`
	StatisticsResult map[string]any       `json:"statistics_result,omitempty"`
}

type taskWorker struct {
	Name  string              `json:"name,omitempty"`
	State structure.TaskState `json:"state,omitempty"`
}

func taskInfo2TaskJsons(tasks []*structure.TaskInfo) []taskInJson {
	jsons := make([]taskInJson, len(tasks))
	for i, task := range tasks {
		jsons[i] = taskInfo2TaskJson(task)
	}
	return jsons
}

func taskInfo2TaskJson(task *structure.TaskInfo) taskInJson {
	taskInJson := taskInJson{
		ID:               task.ID,
		Status:           task.State.Converter(),
		State:            task.State,
		CreateUser:       task.CreateUser,
		CreateType:       task.CreateType,
		StartTime:        task.StartTime,
		CreateTime:       task.CreateTime,
		UpdateTime:       task.UpdateTime,
		GraphName:        task.GraphName,
		SpaceName:        task.SpaceName,
		Type:             task.Type,
		Workers:          make([]taskWorker, 0, len(task.Workers)),
		ErrMessage:       task.ErrMessage,
		Params:           make(map[string]string, len(task.Workers)),
		StatisticsResult: task.StatisticsResult,
	}

	for s, s2 := range task.Params {
		taskInJson.Params[s] = s2
	}

	for _, worker := range task.Workers {
		taskInJson.Workers = append(taskInJson.Workers, taskWorker{
			Name:  worker.Name,
			State: worker.State,
		})
	}

	return taskInJson
}

type TaskHandler struct {
	common.SenHandler
}

type TaskResp struct {
	common.BaseResp
	Task taskInJson `json:"task,omitempty"`
}

func (th *TaskHandler) GET(ctx *gin.Context) {
	taskIDString := ctx.Param("task_id")
	taskID, err := strconv.Atoi(taskIDString)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("task_id type convert to int error: %v", err) }) {
		return
	}

	task, err := taskBiz(ctx).GetTaskInfo(int32(taskID))
	if isErr(err, ctx) {
		return
	}
	filteredTask := taskBiz(ctx).FilteringTask(task)
	ctx.JSON(http.StatusOK, TaskResp{Task: taskInfo2TaskJson(filteredTask)})
}

func (th *TaskHandler) POST(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"code": http.StatusOK,
	})
}

type TaskCancelHandler struct {
	common.SenHandler
}

type TaskCancelResp struct {
	common.BaseResp
}

func (tch *TaskCancelHandler) GET(ctx *gin.Context) {
	taskIDString := ctx.Param("task_id")
	taskID, err := strconv.Atoi(taskIDString)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("task_id:%s type convert to int error: %v", taskIDString, err) }) {
		return
	}

	//任务调度器取消任务
	err = taskBiz(ctx).CancelTask(int32(taskID))
	if isErr(err, ctx) {
		return
	}

	ok(ctx, "cancel task: ok")
}
