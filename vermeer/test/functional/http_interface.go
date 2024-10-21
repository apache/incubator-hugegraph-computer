//go:build vermeer_test

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

package functional

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"vermeer/client"

	"github.com/stretchr/testify/require"
)

type DeleteGraph struct {
}

func (dg DeleteGraph) DeleteGraph(t *testing.T, method *client.VermeerClient, graphName string) {
	ok, err := method.DeleteGraph(graphName)
	require.NoError(t, err)
	require.Equal(t, true, ok)
	_, err = method.GetGraph(graphName)
	require.Error(t, err, fmt.Errorf("response:{\"errcode\":-1,\"message\":\"graph not exists.\"}"))
}

type GetWorkers struct {
}

func (g GetWorkers) GetWorkers(t *testing.T, method *client.VermeerClient) {
	workersResp, err := method.GetWorkers()
	require.NoError(t, err)
	require.Equal(t, 3, len(workersResp.Workers))
	for _, worker := range workersResp.Workers {
		require.Equal(t, "127.0.0.1", worker.IPAddr)
		require.Equal(t, "READY", worker.State)
	}
}

type CancelTask struct {
}

func (ct CancelTask) CancelTask(t *testing.T, master *client.VermeerClient, graphName string) {
	lpaTest := LpaTest{}
	taskResponse, err := master.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: graphName,
		Params:    lpaTest.TaskComputeBody(),
	})
	require.NoError(t, err)
	//发送完请求随机等待0-5s后发送取消任务
	time.Sleep(time.Duration(rand.Float32()*5) * time.Second)

	ok, err := master.GetTaskCancel(int(taskResponse.Task.ID))
	require.NoError(t, err)
	require.Equal(t, true, ok)

	task, err := master.GetTask(int(taskResponse.Task.ID))
	require.NoError(t, err)
	require.Equal(t, "canceled", task.Task.Status)
}

type GetGraphs struct {
}

func (gg GetGraphs) GetGraphs(t *testing.T, master *client.VermeerClient) {
	graphsResponse, err := master.GetGraphs()
	require.NoError(t, err)
	require.Equal(t, 1, len(graphsResponse.Graphs))
	require.Equal(t, "testGraph", graphsResponse.Graphs[0].Name)
	require.Equal(t, "loaded", graphsResponse.Graphs[0].Status)
}

type GetTasks struct {
}

func (gt GetTasks) GetTasks(t *testing.T, master *client.VermeerClient, nums int) {
	tasksResponse, err := master.GetTasks()
	require.NoError(t, err)
	require.LessOrEqual(t, nums, len(tasksResponse.Tasks))
	require.Equal(t, "testGraph", tasksResponse.Tasks[0].GraphName)
	require.Equal(t, "complete", tasksResponse.Tasks[0].Status)
}

type GetMaster struct {
}

func (gm GetMaster) GetMaster(t *testing.T, method *client.VermeerClient, masterIPAddr string) {
	masterResponse, err := method.GetMaster()
	require.NoError(t, err)
	require.Equal(t, masterIPAddr, masterResponse.Master.IPAddr)
}
