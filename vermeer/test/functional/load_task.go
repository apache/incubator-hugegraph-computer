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
	"testing"
	"time"
	"vermeer/client"

	"github.com/stretchr/testify/require"
)

type LoadTaskTestBase struct {
	graphName   string
	masterHttp  *client.VermeerClient
	waitSecond  int
	expectRes   *ExpectRes
	t           *testing.T
	healthCheck *HealthCheck
}

func (lt *LoadTaskTestBase) Init(graphName string, expectRes *ExpectRes,
	masterHttp *client.VermeerClient, waitSecond int, t *testing.T, check *HealthCheck) {
	lt.graphName = graphName
	lt.expectRes = expectRes
	lt.masterHttp = masterHttp
	lt.t = t
	lt.waitSecond = waitSecond
	lt.healthCheck = check
}

type LoadTask interface {
	Init(graphName string, expectRes *ExpectRes,
		masterHttp *client.VermeerClient, waitSecond int, t *testing.T, check *HealthCheck)
	TaskLoadBody() map[string]string
	LoadTest()
	CheckGraph()
}

func (lt *LoadTaskTestBase) SendLoadRequest(TaskLoadBody map[string]string) {
	////create Test Graph
	//ok, err := lt.masterHttp.CreateGraph(client.GraphCreateRequest{Name: lt.graphName})
	//require.NoError(lt.t, err)
	//require.Equal(lt.t, true, ok)

	//create Load Task
	taskResponse, err := lt.masterHttp.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "load",
		GraphName: lt.graphName,
		Params:    TaskLoadBody,
	})
	require.NoError(lt.t, err)
	taskID := int(taskResponse.Task.ID)
	//若成功启动load Task，开始轮询tasksGet，解析response，当得到statues为loaded代表完成。
	var task *client.TaskResponse
	for i := 0; i < lt.waitSecond; i++ {
		lt.healthCheck.DoHealthCheck()
		task, err = lt.masterHttp.GetTask(taskID)
		require.NoError(lt.t, err)
		if task.Task.Status == "loaded" {
			break
		}
		require.NotEqual(lt.t, "error", task.Task.Status)
		time.Sleep(2 * time.Second)
	}
	require.Equal(lt.t, "loaded", task.Task.Status)

	fmt.Printf("load graph [OK]\n")
	time.Sleep(2 * time.Second)
}

func (lt *LoadTaskTestBase) CheckGraph() {
	//load任务结束后，校验结果是否正确
	//校验总数：得到TestGraph的response数据的点和边的总数是否正确。
	graphResponse, err := lt.masterHttp.GetGraph(lt.graphName)
	require.NoError(lt.t, err)

	require.Equal(lt.t, lt.expectRes.VertexCount, graphResponse.Graph.VertexCount)
	require.Equal(lt.t, lt.expectRes.EdgeCount, graphResponse.Graph.EdgeCount)

	//校验某些点的入边和出边
	//入边
	for k, v := range lt.expectRes.InEdges {
		edges, err := lt.masterHttp.GetEdges(lt.graphName, k, "in")
		require.NoError(lt.t, err)

		require.Equal(lt.t, true, edgeEqual(edges.InEdges, v))
	}

	//出边

	//for k, v := range lt.expectRes.OutEdges {
	//	edges, err := lt.masterHttp.GetEdges(lt.graphName, k, "out")
	//	require.NoError(lt.t, err)
	//	require.Equal(lt.t, true, edgeEqual(edges.OutEdges, v))
	//
	//}

	//校验取点接口
	for key := range lt.expectRes.InEdges {
		verticesResponse, err := lt.masterHttp.GetVertices(lt.graphName, client.VerticesRequest{VerticesIds: []string{key}})
		require.NoError(lt.t, err)
		require.Equal(lt.t, key, verticesResponse.Vertices[0].ID)
	}
	for key := range lt.expectRes.OutEdges {
		verticesResponse, err := lt.masterHttp.GetVertices(lt.graphName, client.VerticesRequest{VerticesIds: []string{key}})
		require.NoError(lt.t, err)
		require.Equal(lt.t, key, verticesResponse.Vertices[0].ID)
	}
	fmt.Printf("check graph [OK]\n")
}

func edgeEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	diff := make(map[string]int, len(a))
	for _, x := range a {
		diff[x]++
	}
	for _, y := range b {
		if _, ok := diff[y]; !ok {
			return false
		}
		diff[y] -= 1
		if diff[y] == 0 {
			delete(diff, y)
		}
	}
	if len(diff) == 0 {
		return true
	}
	return false
}
