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
	"math/rand"
	"net/http"
	"strconv"
	"vermeer/apps/common"
	"vermeer/apps/master/bl"

	"github.com/gin-gonic/gin"
)

type OltpHandler struct {
	common.SenHandler
}

func (o *OltpHandler) POST(ctx *gin.Context) {
	req := TaskCreateRequest{}
	err := ctx.BindJSON(&req)
	if isBad(err != nil, ctx, func() string { return fmt.Sprintf("request body not correct: %s", err) }) {
		return
	}
	req.Params["random"] = strconv.FormatFloat(rand.Float64(), 'f', 10, 64)
	task, err := taskBiz(ctx).CreateTaskInfo(req.GraphName, req.TaskType, req.Params, bl.IsCheck)
	if isBad(err != nil, ctx, func() string { return err.Error() }) {
		common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(req.TaskType).Dec()
		return
	}

	err = bl.SyncExecuteTask(task)
	if isBad(err != nil, ctx, func() string { return err.Error() }) {
		common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(req.TaskType).Dec()
		return
	}

	ctx.JSON(http.StatusOK, ComputerTaskMgr.GetOltpResult(task.ID))
}
