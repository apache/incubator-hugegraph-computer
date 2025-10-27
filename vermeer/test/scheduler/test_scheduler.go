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

package scheduler

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vermeer/client"
	"vermeer/test/functional"
)

/*
* @Description: This is the main test function for scheduler.
* @Param t
* @Param expectResPath
* @Param masterHttpAddr
* @Param graphName
* @Param factor
* @Param waitSecond
* @Note: You must start at least two worker, named worker01 and worker04 in your config.yaml
 */
func TestScheduler(t *testing.T, expectResPath string, masterHttpAddr string, graphName string, factor string, waitSecond int) {
	fmt.Print("start test scheduler\n")

	startTime := time.Now()
	expectRes, err := functional.GetExpectRes(expectResPath)
	require.NoError(t, err)

	masterHttp := client.VermeerClient{}
	masterHttp.Init("http://"+masterHttpAddr, http.DefaultClient)

	// health check
	healthCheck := functional.HealthCheck{}
	healthCheck.Init(t, &masterHttp)
	healthCheck.DoHealthCheck()

	// load graph first
	loadTest1 := functional.LoadTaskLocal{}
	loadTest1.Init(graphName+"_1", expectRes, &masterHttp, waitSecond, t, &healthCheck)
	loadTest1.SendLoadRequest(loadTest1.TaskLoadBodyWithNum(0))
	loadTest1.CheckGraph()

	loadTest2 := functional.LoadTaskLocal{}
	loadTest2.Init(graphName+"_2", expectRes, &masterHttp, waitSecond, t, &healthCheck)
	loadTest2.SendLoadRequest(loadTest2.TaskLoadBodyWithNum(20))
	// loadTest2.CheckGraph()

	TestPriority(t, expectRes, &healthCheck, &masterHttp, []string{graphName + "_1", graphName + "_2"}, factor, waitSecond)

	TestBatch(t, expectRes, &healthCheck, &masterHttp, []string{graphName + "_1"}, factor, waitSecond)

	TestRoutine(t, expectRes, &healthCheck, &masterHttp, []string{graphName + "_2"}, factor, waitSecond)

	// Error handling: cancel task
	cancelTask := functional.CancelTask{}
	cancelTask.CancelTask(t, &masterHttp, graphName+"_1")
	cancelTask.CancelTask(t, &masterHttp, graphName+"_2")
	fmt.Print("test cancel task [OK]\n")

	// Finally, delete graph
	deleteGraph := functional.DeleteGraph{}
	deleteGraph.DeleteGraph(t, &masterHttp, graphName+"_1")
	deleteGraph.DeleteGraph(t, &masterHttp, graphName+"_2")
	fmt.Print("test delete graph [OK]\n")

	fmt.Printf("client test finished, cost time:%v\n", time.Since(startTime))
}
