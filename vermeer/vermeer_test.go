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

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vermeer/client"
	"vermeer/test/functional"
	"vermeer/test/scheduler"
)

var (
	testMode         = "algorithms"
	masterHttpAddr   = "0.0.0.0:6688"
	worker01HttpAddr = "0.0.0.0:6788"
	worker02HttpAddr = "0.0.0.0:6888"
	worker03HttpAddr = "0.0.0.0:6988"
	useAuth          = false
	user             = "user"
	space            = "space"
	factor           = "1234"
	expectResPath    = "test/case/expect_res.json"
	waitSecond       = 1200
	graphName        = "testGraph"
)

func init() {
	masterHttp := flag.String("master", "", "master http listen address")
	worker01Http := flag.String("worker01", "", "worker01 http listen address")
	worker02Http := flag.String("worker02", "", "worker02 http listen address")
	worker03Http := flag.String("worker03", "", "worker03 http listen address")
	mode := flag.String("mode", "", "vermeer test mode")
	auth := flag.String("auth", "", "vermeer use auth")
	userName := flag.String("user", "", "vermeer user name")
	spaceName := flag.String("space", "", "vermeer space name")
	tokenFactor := flag.String("factor", "", "token factor")
	flag.Parse()

	if *masterHttp != "" {
		masterHttpAddr = *masterHttp
	}
	if *worker01Http != "" {
		worker01HttpAddr = *worker01Http
	}
	if *worker02Http != "" {
		worker02HttpAddr = *worker02Http
	}
	if *worker03Http != "" {
		worker03HttpAddr = *worker03Http
	}
	if *mode != "" {
		testMode = *mode
	}
	if *auth != "" {
		useAuth = (*auth) == "token"
	}
	if *userName != "" {
		user = *userName
	}
	if *spaceName != "" {
		space = *spaceName
	}
	if *tokenFactor != "" {
		factor = *tokenFactor
	}
}

func TestVermeer(t *testing.T) {
	switch testMode {
	case "algorithms":
		t.Run("algorithms", testAlgorithms)
	case "function":
		t.Run("function", testFunction)
	case "scheduler":
		t.Run("scheduler", testScheduler)
	}
}

func testFunction(t *testing.T) {
	functional.TestFunction(t, expectResPath, masterHttpAddr, graphName, factor, waitSecond)
}

func testScheduler(t *testing.T) {
	scheduler.TestScheduler(t, expectResPath, masterHttpAddr, graphName, factor, waitSecond)
}

func testAlgorithms(t *testing.T) {
	// todo: 增加算法名称
	var computeTasks = []string{"pagerank", "lpa", "wcc", "degree_out", "degree_in", "degree_both", "triangle_count",
		"sssp", "closeness_centrality", "betweenness_centrality", "kcore", "jaccard", "ppr", "clustering_coefficient", "scc", "louvain"}
	// var computeTasks = []string{"pagerank"}

	startTime := time.Now()
	expectRes, err := functional.GetExpectRes(expectResPath)
	require.NoError(t, err)

	masterHttp := client.VermeerClient{}
	masterHttp.Init("http://"+masterHttpAddr, http.DefaultClient)
	if useAuth {
		err := masterHttp.SetAuth(client.AuthOptions{
			User:            user,
			Space:           space,
			AuthTokenFactor: factor,
		})
		require.NoError(t, err)
	}
	worker01Http := client.VermeerClient{}
	worker01Http.Init("http://"+worker01HttpAddr, http.DefaultClient)
	worker02Http := client.VermeerClient{}
	worker02Http.Init("http://"+worker02HttpAddr, http.DefaultClient)
	worker03Http := client.VermeerClient{}
	worker03Http.Init("http://"+worker03HttpAddr, http.DefaultClient)

	// health check
	healthCheck := functional.HealthCheck{}
	healthCheck.Init(t, &masterHttp, &worker01Http, &worker02Http, &worker03Http)
	healthCheck.DoHealthCheck()
	// loadTest
	loadTest := functional.LoadTaskLocal{}
	loadTest.Init(graphName, expectRes, &masterHttp, waitSecond, t, &healthCheck)
	loadTest.SendLoadRequest(loadTest.TaskLoadBody())
	loadTest.CheckGraph()

	// computeTest (rand:async/sync)
	rand.Seed(time.Now().UnixNano())
	for i := range computeTasks {
		bTime := time.Now()
		sendType := "async"
		if rand.Float64() > 0.5 {
			sendType = "sync "
		}
		needQuery := "0"
		if rand.Float64() > 0.5 {
			needQuery = "1"
		}
		fmt.Printf("%s run algorithm: %-30s [start]\n", sendType, computeTasks[i])
		computeTest, err := functional.MakeComputeTask(computeTasks[i])
		require.NoError(t, err)
		computeTest.Init(graphName, computeTasks[i], expectRes, waitSecond, &masterHttp, t, &healthCheck)
		taskComputeBody := computeTest.TaskComputeBody()
		taskComputeBody["output.need_query"] = needQuery
		if sendType == "async" {
			computeTest.SendComputeReqAsync(taskComputeBody)
			// computeTest.SendComputeReqAsyncBatchPriority(10, taskComputeBody) // 异步发送多个请求
		} else {
			computeTest.SendComputeReqSync(taskComputeBody)
		}
		computeTest.CheckRes()
		if needQuery == "1" {
			computeTest.CheckGetComputeValue()
			fmt.Printf("check value algorithm: %-27s  [OK]\n", computeTasks[i])
		}
		fmt.Printf("%s run algorithm: %-30s [OK], cost: %v\n", sendType, computeTasks[i], time.Since(bTime))
	}

	// test get graphs
	getGraphs := functional.GetGraphs{}
	getGraphs.GetGraphs(t, &masterHttp)
	fmt.Print("test get graphs [OK]\n")

	// test get tasks
	getTasks := functional.GetTasks{}
	getTasks.GetTasks(t, &masterHttp, len(computeTasks)+1)
	fmt.Print("test get tasks [OK]\n")

	// test cancel task
	cancelTask := functional.CancelTask{}
	cancelTask.CancelTask(t, &masterHttp, graphName)
	fmt.Print("test cancel task [OK]\n")

	// test delete graph
	deleteGraph := functional.DeleteGraph{}
	deleteGraph.DeleteGraph(t, &masterHttp, graphName)
	fmt.Print("test delete graph [OK]\n")

	// test get workers
	getWorkers := functional.GetWorkers{}
	getWorkers.GetWorkers(t, &masterHttp)
	fmt.Print("test get workers [OK]\n")

	// test get master
	getMaster := functional.GetMaster{}
	getMaster.GetMaster(t, &masterHttp, masterHttpAddr)
	fmt.Print("test get master [OK]\n")

	fmt.Printf("client test finished, cost time:%v\n", time.Since(startTime))
}
