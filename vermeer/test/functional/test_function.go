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
	"net/http"
	"os/exec"
	"sync"
	"testing"
	"time"
	"vermeer/client"

	"github.com/stretchr/testify/require"
)

func TestFunction(t *testing.T, expectResPath string, masterHttpAddr string, graphName string, factor string, waitSecond int) {
	expectRes, err := GetExpectRes(expectResPath)
	require.NoError(t, err)
	userList := make([]client.VermeerClient, 0, 5)
	fmt.Print("start function test\n")
	//init 5 user and 3 space
	//every user check one interface
	//user1
	var initUser func(userName string, spaceName string) (client.VermeerClient, error)
	initUser = func(userName string, spaceName string) (client.VermeerClient, error) {
		userClient := client.VermeerClient{}
		userClient.Init("http://"+masterHttpAddr, http.DefaultClient)
		err = userClient.SetAuth(client.AuthOptions{
			User:            userName,
			Space:           spaceName,
			AuthTokenFactor: factor,
		})
		return userClient, err
	}

	user1, err := initUser("user1", "space1")
	require.NoError(t, err)
	_, err = user1.GetWorkers()
	require.NoError(t, err)
	userList = append(userList, user1)
	//user2
	user2, err := initUser("user2", "space2")
	require.NoError(t, err)
	_, err = user2.GetMaster()
	require.NoError(t, err)
	userList = append(userList, user2)
	//user3
	user3, err := initUser("user3", "space3")
	require.NoError(t, err)
	_, err = user3.GetGraphs()
	require.NoError(t, err)
	userList = append(userList, user3)
	//user4
	user4, err := initUser("user4", "space1")
	require.NoError(t, err)
	_, err = user4.GetTasks()
	require.NoError(t, err)
	userList = append(userList, user4)
	//user5
	user5, err := initUser("user5", "space1")
	require.NoError(t, err)
	_, err = user5.HealthCheck()
	require.NoError(t, err)
	userList = append(userList, user5)
	fmt.Print("user login success\n")

	fakeUser := client.VermeerClient{}
	fakeUser.Init("http://"+masterHttpAddr, http.DefaultClient)
	err = fakeUser.SetAuth(client.AuthOptions{
		User:            "fakeUser",
		Space:           "fakespace",
		AuthTokenFactor: "9867",
	})
	_, err = fakeUser.GetTasks()
	require.Equal(t, "response:{\"errcode\":-1,\"message\":\"Invalid Token\"}", err.Error())

	err = fakeUser.SetAuth(client.AuthOptions{
		User:            "fakeUser",
		Space:           "fakespace",
		AuthTokenFactor: "1382",
	})
	_, err = fakeUser.GetGraphs()
	require.Equal(t, "response:{\"errcode\":-1,\"message\":\"Invalid Token\"}", err.Error())

	fmt.Print("fake token check success\n")

	fmt.Print("start load graph\n")
	// health check
	healthCheck := HealthCheck{}
	healthCheck.Init(t, &user1, &user2, &user3, &user4)
	healthCheck.DoHealthCheck()

	wg := &sync.WaitGroup{}
	//user1（local），user2（hugegraph），user3（afs）发起图的导入，都成功并结果正确。
	wg.Add(3)
	go func() {
		defer wg.Done()
		loadTest := LoadTaskLocal{}
		loadTest.Init(graphName, expectRes, &user1, waitSecond, t, &healthCheck)
		loadTest.SendLoadRequest(loadTest.TaskLoadBody())
		loadTest.CheckGraph()
		fmt.Print("user1 load graph local success\n")
	}()

	go func() {
		defer wg.Done()
		loadTest := LoadTaskLocal{}
		loadTest.Init(graphName, expectRes, &user2, waitSecond, t, &healthCheck)
		loadTest.SendLoadRequest(loadTest.TaskLoadBody())
		loadTest.CheckGraph()
		fmt.Print("user2 load graph hugegraph success\n")
	}()

	go func() {
		defer wg.Done()
		loadTest := LoadTaskLocal{}
		loadTest.Init(graphName, expectRes, &user3, waitSecond, t, &healthCheck)
		loadTest.SendLoadRequest(loadTest.TaskLoadBody())
		loadTest.CheckGraph()
		fmt.Print("user3 load graph afs success\n")
	}()

	wg.Wait()
	fmt.Print("load graph all success\n")

	graphs, err := user4.GetGraphs()
	require.NoError(t, err)
	require.Equal(t, 1, len(graphs.Graphs))
	require.Equal(t, graphName, graphs.Graphs[0].Name)
	require.Equal(t, "space1", graphs.Graphs[0].SpaceName)

	graphs, err = user5.GetGraphs()
	require.NoError(t, err)
	require.Equal(t, 1, len(graphs.Graphs))
	require.Equal(t, graphName, graphs.Graphs[0].Name)
	require.Equal(t, "space1", graphs.Graphs[0].SpaceName)

	fmt.Print("check graph space success\n")
	fmt.Print("start run algorithm\n")

	algoList := []string{"wcc", "degree_out", "degree_in", "jaccard", "ppr"}
	for i, user := range userList {
		wg.Add(1)
		go func(idx int, user client.VermeerClient) {
			defer wg.Done()
			bTime := time.Now()
			needQuery := "0"
			if rand.Float64() > 0.5 {
				needQuery = "1"
			}
			fmt.Printf("run algorithm %-30s user%v [start]\n", algoList[idx], idx+1)
			computeTest, err := MakeComputeTask(algoList[idx])
			require.NoError(t, err)
			computeTest.Init(graphName, algoList[idx], expectRes, waitSecond, &user, t, &healthCheck)
			taskComputeBody := computeTest.TaskComputeBody()
			taskComputeBody["output.need_query"] = needQuery
			computeTest.SendComputeReqAsync(taskComputeBody)
			computeTest.CheckRes()
			if needQuery == "1" {
				computeTest.CheckGetComputeValue()
				fmt.Printf("check value algorithm: %-30s  [OK]\n", algoList[idx])
			}
			fmt.Printf("run algorithm %-30s user%v [OK], cost: %v\n", algoList[idx], idx+1, time.Since(bTime))
		}(i, user)
	}
	wg.Wait()
	fmt.Print("run algorithm all success\n")

	computeTest, err := MakeComputeTask("lpa")
	computeTest.Init(graphName, "lpa", expectRes, waitSecond, nil, t, nil)
	require.NoError(t, err)
	resp1, err := user1.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: graphName,
		Params:    computeTest.TaskComputeBody(),
	})
	require.NoError(t, err)

	resp4, err := user4.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: graphName,
		Params:    computeTest.TaskComputeBody(),
	})
	require.NoError(t, err)

	resp5, err := user5.CreateTaskAsync(client.TaskCreateRequest{
		TaskType:  "compute",
		GraphName: graphName,
		Params:    computeTest.TaskComputeBody(),
	})
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	fmt.Print("killing master\n")
	output, err := exec.Command("./vermeer.sh", "stop", "master").Output()
	fmt.Println(string(output))
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	_, err = exec.Command("./vermeer.sh", "start", "master").Output()
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	isAvailable := false
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		graphResponse, err := user4.GetGraph(graphName)
		if err == nil {
			if graphResponse.Graph.Status == "loaded" || graphResponse.Graph.Status == "disk" {
				isAvailable = true
				break
			}
			require.NotEqual(t, "error", graphResponse.Graph.Status)
		}
		time.Sleep(1 * time.Second)
	}
	require.Equal(t, true, isAvailable)
	taskResponse, err := user1.GetTask(int(resp1.Task.ID))
	require.NoError(t, err)
	require.Equal(t, "error", taskResponse.Task.Status)
	for {
		taskResponse, err = user4.GetTask(int(resp4.Task.ID))
		require.NoError(t, err)
		require.NotEqual(t, "error", taskResponse.Task.Status)
		if taskResponse.Task.Status == "complete" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	lpaTest, err := MakeComputeTask("lpa")
	require.NoError(t, err)
	lpaTest.Init(graphName, "lpa", expectRes, waitSecond, &user4, t, nil)
	lpaTest.CheckRes()

	for {
		taskResponse, err = user5.GetTask(int(resp5.Task.ID))
		require.NoError(t, err)
		require.NotEqual(t, "error", taskResponse.Task.Status)
		if taskResponse.Task.Status == "complete" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	lpaTest, err = MakeComputeTask("lpa")
	require.NoError(t, err)
	lpaTest.Init(graphName, "lpa", expectRes, waitSecond, &user5, t, &healthCheck)
	lpaTest.CheckRes()
	fmt.Print("kill master recover success\n")

	fmt.Print("killing worker\n")
	_, err = exec.Command("./vermeer.sh", "stop", "worker").Output()
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	_, err = exec.Command("./vermeer.sh", "start", "worker").Output()
	require.NoError(t, err)
	isAvailable = false
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		graphResponse, err := user1.GetGraph(graphName)
		if err == nil && (graphResponse.Graph.Status == "loaded" || graphResponse.Graph.Status == "disk") {
			isAvailable = true
			break
		}
		require.NotEqual(t, "error", graphResponse.Graph.Status)
		time.Sleep(1 * time.Second)
	}
	require.Equal(t, true, isAvailable)

	algoList = []string{"pagerank", "ppr", "degree_both"}
	for i := 0; i < 3; i++ {
		//wg.Add(1)
		//go func(idx int) {
		//defer wg.Done()
		computeTask, err := MakeComputeTask(algoList[i])
		require.NoError(t, err)
		computeTask.Init(graphName, algoList[i], expectRes, waitSecond, &userList[i], t, &healthCheck)
		computeTask.SendComputeReqAsync(computeTask.TaskComputeBody())
		computeTask.CheckRes()
		fmt.Printf("algo:%v success\n", algoList[i])
		//}(i)
	}
	//wg.Wait()
	//check graph
	//for i := 0; i < 3; i++ {
	//	loadTest := LoadTaskTestBase{}
	//	loadTest.Init(graphName, expectRes, &userList[i], waitSecond, t, &healthCheck)
	//	loadTest.CheckGraph()
	//}
	fmt.Print("kill worker recover success\n")

	for _, user := range userList {
		wg.Add(1)
		go func(user client.VermeerClient) {
			defer wg.Done()
			cancelTask := CancelTask{}
			cancelTask.CancelTask(t, &user, graphName)
		}(user)
	}
	wg.Wait()
	fmt.Print("cancel task ok\n")
	time.Sleep(5 * time.Second)
	for i := 0; i < 3; i++ {
		user := userList[i]
		graphsResponse, err := user.GetGraphs()
		require.NoError(t, err)
		require.Equal(t, 1, len(graphsResponse.Graphs))
		require.Equal(t, graphName, graphsResponse.Graphs[0].Name)
		ok, err := user.DeleteGraph(graphName)
		require.NoError(t, err)
		require.Equal(t, true, ok)
		graphsResponse, err = user.GetGraphs()
		require.NoError(t, err)
		require.Equal(t, 0, len(graphsResponse.Graphs))
	}
	fmt.Printf("delete graph ok\n")
}
