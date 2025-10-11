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
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"vermeer/client"

	"github.com/stretchr/testify/require"
)

type ComputeTaskBase struct {
	taskID      int
	waitSecond  int
	outputType  int
	errorRange  float64
	graphName   string
	algoName    string
	masterHttp  *client.VermeerClient
	t           *testing.T
	healthCheck *HealthCheck
	expectRes   *ExpectRes
	computeRes  []interface{}
}

// Init
//
//	@Description: 初始化对象。若子类有其他变量，则需要重写。
func (ctb *ComputeTaskBase) Init(graphName string, algoName string, expectRes *ExpectRes,
	waitSecond int, masterHttp *client.VermeerClient, t *testing.T, healthCheck *HealthCheck) {
	ctb.graphName = graphName
	ctb.algoName = algoName
	ctb.masterHttp = masterHttp
	ctb.expectRes = expectRes
	ctb.waitSecond = waitSecond
	ctb.t = t
	ctb.healthCheck = healthCheck
}

// TaskComputeBody
//
//	@Description: 自定义compute任务需要发送的json body，需要重写。参考compute_pagerank.go、compute_wcc.go
func (ctb *ComputeTaskBase) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return nil
}

// SendComputeReqAsync
//
//	@Description: 发送Http请求，无需重写，异步请求
func (ctb *ComputeTaskBase) SendComputeReqAsync(params map[string]string) {
	//create Compute Task
	resp, err := ctb.masterHttp.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: ctb.graphName,
		Params:    params,
	})
	require.NoError(ctb.t, err)

	taskInfo := resp.Task
	ctb.taskID = int(taskInfo.ID)
	//若成功启动Compute Task，开始轮询tasksGet，解析response，得到状态为完成时break。
	var taskResp *client.TaskResponse
	for i := 0; i < ctb.waitSecond; i++ {
		ctb.healthCheck.DoHealthCheck()
		taskResp, err = ctb.masterHttp.GetTask(ctb.taskID)
		require.NoError(ctb.t, err)
		if taskResp.Task.Status == "complete" {
			break
		}
		require.NotEqual(ctb.t, "error", taskResp.Task.Status)
		time.Sleep(1 * time.Second)
	}
	require.Equal(ctb.t, "complete", taskResp.Task.Status)
}

/*
* @Description: SendComputeReqAsyncNotWait sends a compute request asynchronously and returns the task ID.
* @Param params
* @Return int32
 */
func (ctb *ComputeTaskBase) SendComputeReqAsyncNotWait(params map[string]string) int32 {
	//create Compute Task
	resp, err := ctb.masterHttp.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: ctb.graphName,
		Params:    params,
	})
	require.NoError(ctb.t, err)
	return int32(resp.Task.ID)
}

/*
* @Description: SendComputeReqAsyncNotWaitWithError sends a compute request asynchronously and returns the task ID and error.
* @Param params
* @Return int32, error
 */
func (ctb *ComputeTaskBase) SendComputeReqAsyncNotWaitWithError(params map[string]string) (int32, error) {
	//create Compute Task
	resp, err := ctb.masterHttp.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: ctb.graphName,
		Params:    params,
	})
	if err != nil {
		return -1, err
	}
	return int32(resp.Task.ID), nil
}

/*
* @Description: SendComputeReqAsyncBatchPriority sends a compute request asynchronously and returns the task ID and sequence.
* @Note: This function will block the main thread until all tasks are completed.
* @Param params
* @Return []int32, []int32
 */
func (ctb *ComputeTaskBase) SendComputeReqAsyncBatchPriority(params []map[string]string) ([]int32, []int32) {
	//create Compute Task
	tasks := make([]client.TaskInfo, 0, len(params))
	taskIds := make([]int32, 0, len(params))
	createTasksParams := client.TaskCreateBatchRequest{}
	for i := 0; i < len(params); i++ {
		graph := ctb.graphName
		if params[i]["graph_name"] != "" {
			graph = params[i]["graph_name"]
		}
		createTasksParams = append(createTasksParams, client.TaskCreateRequest{
			TaskType:  "compute",
			GraphName: graph,
			Params:    params[i],
		})
	}
	resp, err := ctb.masterHttp.CreateTaskBatch(createTasksParams)
	require.NoError(ctb.t, err)

	for i, r := range *resp {
		if r.BaseResponse.ErrCode != 0 {
			ctb.t.Fatalf("create compute task %d failed: %s", i, r.BaseResponse.Message)
		}
		tasks = append(tasks, r.Task)
		taskIds = append(taskIds, r.Task.ID)
	}

	for i := 0; i < len(params); i++ {
		ctb.taskID = int(tasks[i].ID)
		//若成功启动Compute Task，开始轮询tasksGet，解析response，得到状态为完成时break。
		var taskResp *client.TaskResponse
		var err error
		for attempt := 0; attempt < ctb.waitSecond; attempt++ {
			ctb.healthCheck.DoHealthCheck()
			taskResp, err = ctb.masterHttp.GetTask(ctb.taskID)
			require.NoError(ctb.t, err)
			if taskResp.Task.Status == "complete" {
				break
			}
			require.NotEqual(ctb.t, "error", taskResp.Task.Status)
			time.Sleep(1 * time.Second)
		}
		require.Equal(ctb.t, "complete", taskResp.Task.Status)
		fmt.Printf("Compute Task %d completed successfully\n", ctb.taskID)
	}

	response, err := ctb.masterHttp.GetTaskStartSequence(taskIds)
	require.NoError(ctb.t, err)
	sequence := response.Sequence
	for i, id := range sequence {
		fmt.Printf("Task %d started at position %d in the sequence\n", id, i+1)
	}
	require.Equal(ctb.t, len(taskIds), len(sequence))

	return taskIds, sequence
}

// SendComputeReqSync
//
//	@Description: 发送Http请求，无需重写,同步请求
func (ctb *ComputeTaskBase) SendComputeReqSync(params map[string]string) {
	//create Compute Task
	resp, err := ctb.masterHttp.CreateTaskSync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: ctb.graphName,
		Params:    params,
	})
	require.NoError(ctb.t, err)
	taskInfo := resp.Task
	ctb.taskID = int(taskInfo.ID)
	require.Equal(ctb.t, "complete", taskInfo.Status)
}

// LoadComputeRes
//
//	@Description: 读取计算出的结果。
func (ctb *ComputeTaskBase) LoadComputeRes() ([]interface{}, error) {
	dir, err := os.ReadDir("data/")
	if err != nil {
		return nil, err
	}
	res := make([]interface{}, ctb.expectRes.VertexCount)
	var count int64
	for _, file := range dir {
		if !strings.HasPrefix(file.Name(), ctb.algoName) {
			continue
		}
		f, err := os.Open("data/" + file.Name())
		require.NoError(ctb.t, err)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			count++
			ss := strings.Split(scanner.Text(), ",")
			vertex, err := strconv.Atoi(ss[0])
			if err != nil {
				return nil, err
			}
			switch ctb.outputType {
			case OutputTypeInt:
				res[vertex], err = strconv.Atoi(ss[1])
			case OutputTypeFloat:
				res[vertex], err = strconv.ParseFloat(ss[1], 10)
			case OutputTypeString:
				res[vertex] = strings.TrimSpace(ss[1])
			default:
				return nil, fmt.Errorf("no match outputType:%v", ctb.outputType)
			}
			if err != nil {
				return nil, err
			}
		}
		_ = f.Close()
	}
	require.Equal(ctb.t, ctb.expectRes.VertexCount, count)
	return res, nil
}

// LoadExpectRes
//
//	@Description: 读取expect_xxx文件，该文件应当是正确的计算结果文件合成的一个文件，该文件的书写方式同计算的输出文件。
//
// 若expect文件组织形式不同，则需重写LoadExpectRes方法。
func (ctb *ComputeTaskBase) LoadExpectRes(filepath string) ([]interface{}, error) {
	res := make([]interface{}, ctb.expectRes.VertexCount)
	var count int64

	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		count++
		ss := strings.Split(scanner.Text(), ",")
		vertex, err := strconv.Atoi(ss[0])
		require.NoError(ctb.t, err)
		switch ctb.outputType {
		case OutputTypeInt:
			res[vertex], err = strconv.Atoi(ss[1])
		case OutputTypeFloat:
			res[vertex], err = strconv.ParseFloat(ss[1], 10)
		case OutputTypeString:
			res[vertex] = strings.TrimSpace(ss[1])
		default:
			return nil, fmt.Errorf("no match outputType:%v", ctb.outputType)
		}
		if err != nil {
			return nil, err
		}
	}

	require.Equal(ctb.t, ctb.expectRes.VertexCount, count)
	return res, nil
}

// CheckRes
//
//	@Description: 是校验每一个vertex对应的结果值是否正确或在误差范围内。若使用其他校验方法，需重写CheckRes()
func (ctb *ComputeTaskBase) CheckRes() {
	//解析输出结果，校验结果正确性。
	var err error
	ctb.computeRes, err = ctb.LoadComputeRes()
	require.NoError(ctb.t, err)
	expectRes, err := ctb.LoadExpectRes("test/case/expect_" + ctb.algoName)
	require.NoError(ctb.t, err)
	err = ctb.Compare(ctb.computeRes, expectRes)
	require.NoError(ctb.t, err)
}

func (ctb *ComputeTaskBase) CheckGetComputeValue() {
	//check interface
	var err error
	if len(ctb.computeRes) == 0 {
		ctb.computeRes, err = ctb.LoadComputeRes()
		require.NoError(ctb.t, err)
	}

	taskValueRes := ctb.LoadTaskValue()
	for i, value := range taskValueRes {
		require.Equal(ctb.t, ctb.computeRes[i], value)
	}
}

// Compare
//
//	@Description: 比对每一个vertex的计算值与期望是否符合。
func (ctb *ComputeTaskBase) Compare(compute, expect []interface{}) error {
	if len(compute) != len(expect) {
		return fmt.Errorf("compute and expect result length not equal")
	}
	for i := range expect {
		switch ctb.outputType {
		case OutputTypeFloat:
			require.LessOrEqual(ctb.t, expect[i].(float64)*(1-ctb.errorRange), compute[i].(float64))
			require.GreaterOrEqual(ctb.t, expect[i].(float64)*(1+ctb.errorRange), compute[i].(float64))
		case OutputTypeInt, OutputTypeString:
			require.Equal(ctb.t, expect[i], compute[i])
		default:
			return fmt.Errorf("no match outputType:%v", ctb.outputType)
		}
	}
	return nil
}

func (ctb *ComputeTaskBase) LoadTaskValue() []interface{} {
	cursor := 0
	var count int64
	limit := 100000
	computeValueRes := make([]interface{}, ctb.expectRes.VertexCount)
	for {
		computeValueResp, err := ctb.masterHttp.GetComputeValue(ctb.taskID, cursor, limit)
		require.NoError(ctb.t, err)
		if computeValueResp.Message == "EOF" {
			break
		}
		cursor = int(computeValueResp.Cursor)
		vertices := computeValueResp.Vertices
		for _, vertex := range vertices {
			vertexID, err := strconv.Atoi(vertex.ID)
			require.NoError(ctb.t, err)
			switch ctb.outputType {
			case OutputTypeInt:
				computeValueRes[vertexID], err = strconv.Atoi(vertex.Value)
			case OutputTypeFloat:
				computeValueRes[vertexID], err = strconv.ParseFloat(vertex.Value, 10)
			case OutputTypeString:
				computeValueRes[vertexID] = strings.TrimSpace(vertex.Value)
			default:
				ctb.t.Fatalf("no match outputType:%v", ctb.outputType)
			}
			require.NoError(ctb.t, err)
			count++
		}
	}

	require.Equal(ctb.t, ctb.expectRes.VertexCount, count)
	return computeValueRes
}
