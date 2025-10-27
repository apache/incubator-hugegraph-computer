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
	"sync"
	"testing"
	"time"

	"vermeer/apps/structure"
	"vermeer/client"
	"vermeer/test/functional"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

/*
* @Description: SubTestPriority tests the scheduler's behavior when submitting tasks with different priorities.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestPriority(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Priority start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// send two tasks with different priority
	params := make([]map[string]string, 0)

	for i := 0; i < 2; i++ {
		param := make(map[string]string)
		param["priority"] = fmt.Sprintf("%d", i)
		for k, v := range taskComputeBody {
			param[k] = v
		}
		params = append(params, param)
	}

	logrus.Infof("params for priority test: %+v", params)

	taskids, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))
	for i := 0; i < 2; i++ {
		require.Equal(t, taskids[1-i], sequence[i]) // expect task with priority 1 executed before priority 0
	}

	computeTest.CheckRes()
	fmt.Printf("Test Priority: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

/*
* @Description: SubTestSmall tests the scheduler's behavior when submitting tasks with different sizes.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestSmall(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Small start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	computeTaskSmall, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()
	computeTaskSmall.Init(graphName[1], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBodySmall := computeTaskSmall.TaskComputeBody()

	// send two tasks with different size
	params := make([]map[string]string, 0)
	taskComputeBody["graph_name"] = graphName[0]
	taskComputeBodySmall["graph_name"] = graphName[1]
	params = append(params, taskComputeBody)
	params = append(params, taskComputeBodySmall)

	logrus.Infof("params for small test: %+v", params)

	taskids, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))
	for i := 0; i < 2; i++ {
		require.Equal(t, taskids[1-i], sequence[i]) // expect task smaller executed before larger
	}

	computeTest.CheckRes()
	fmt.Printf("Test Small: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

/*
* @Description: SubTestConcurrent tests the scheduler's behavior when submitting tasks with different sizes.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestConcurrent(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Concurrent start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[1], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// send two tasks with different size
	params := make([]map[string]string, 0)
	// default is false, actually do not need to set
	taskComputeBody["exclusive"] = "false"
	params = append(params, taskComputeBody)
	params = append(params, taskComputeBody)

	logrus.Infof("params for concurrent test: %+v", params)

	_, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))

	fmt.Printf("Test Concurrent: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
	// cost should be less than 2 * single task time
}

/*
* @Description: SubTestDepends tests the scheduler's behavior when submitting tasks with different dependencies.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestDepends(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Depends start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// first alloc worker 4 for graph 3
	masterHttp.AllocGroupGraph(graphName[0]+"_3", "test")

	loadTest3 := functional.LoadTaskLocal{}
	loadTest3.Init(graphName[0]+"_3", expectRes, masterHttp, waitSecond, t, healthCheck)
	loadTest3.SendLoadRequest(loadTest3.TaskLoadBodyWithNum(10))

	// send a large task to $ worker group
	taskid := computeTest.SendComputeReqAsyncNotWait(taskComputeBody)

	// send two tasks with different dependency to the same graph
	taskComputeBody["graph_name"] = graphName[0] + "_3"
	params := make([]map[string]string, 0)
	new_body := make(map[string]string)
	for k, v := range taskComputeBody {
		new_body[k] = v
	}
	new_body["preorders"] = fmt.Sprintf("%d", taskid)
	params = append(params, new_body)
	params = append(params, taskComputeBody)

	logrus.Infof("params for depends test: %+v", params)

	taskids, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))
	for i := 0; i < 2; i++ {
		require.Equal(t, taskids[1-i], sequence[i]) // expect task not depend executed first
	}

	// computeTest.CheckRes()
	fmt.Printf("Test Depends: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

/*
* @Description: SubTestInvalidDependency tests the scheduler's behavior when a compute task is submitted with a dependency on a non-existent (invalid) task ID.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestInvalidDependency(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Invalid Dependency start with task: %s\n", computeTask)
	bTime := time.Now()

	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)

	taskBody := computeTest.TaskComputeBody()
	// set preorders to a very large, theoretically nonexistent task ID
	invalidTaskID := 999999999
	taskBody["preorders"] = fmt.Sprintf("%d", invalidTaskID)

	logrus.Infof("Attempting to submit a task with invalid dependency on ID: %d", invalidTaskID)

	// try to submit task asynchronously and check if it returns an error
	taskID, err := computeTest.SendComputeReqAsyncNotWaitWithError(taskBody)

	// assert that the submission operation failed
	require.Error(t, err, "Submitting a task with a non-existent dependency should return an error.")
	// assert that the returned task ID is 0 or other failed values
	require.Equal(t, int32(-1), taskID, "The task ID should be zero or invalid on failure.")

	fmt.Printf("Test Invalid Dependency: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

/*
* @Description: SubTestConcurrentCancellation tests the scheduler's behavior when submitting tasks concurrently and canceling them.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestConcurrentCancellation(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Concurrent Cancellation start with task: %s\n", computeTask)
	bTime := time.Now()

	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)

	// set task number
	const numTasks = 20
	taskBodies := make([]map[string]string, numTasks)
	for i := 0; i < numTasks; i++ {
		taskBodies[i] = computeTest.TaskComputeBody()
	}

	taskIDs := make(chan int32, numTasks)
	var wg sync.WaitGroup

	// 1. submit tasks concurrently
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(body map[string]string) {
			defer wg.Done()
			taskID := computeTest.SendComputeReqAsyncNotWait(body)
			if taskID != 0 {
				taskIDs <- taskID
			} else {
				logrus.Errorf("Failed to submit task: %v", err)
			}
		}(taskBodies[i])
	}

	wg.Wait()
	close(taskIDs)

	submittedTaskIDs := make([]int32, 0, numTasks)
	for id := range taskIDs {
		submittedTaskIDs = append(submittedTaskIDs, id)
	}

	logrus.Infof("Submitted %d tasks concurrently: %+v", len(submittedTaskIDs), submittedTaskIDs)
	require.Equal(t, numTasks, len(submittedTaskIDs), "Not all tasks were successfully submitted.")

	cancelTask := functional.CancelTask{}
	cancelTask.DirectCancelTask(t, masterHttp, submittedTaskIDs[len(submittedTaskIDs)-1])

	// 3. verify task status
	// wait for tasks to settle
	logrus.Info("Waiting for tasks to settle...")
	time.Sleep(time.Duration(waitSecond) * time.Second)

	checkTask, err := masterHttp.GetTask(int(submittedTaskIDs[numTasks-1]))

	require.NoError(t, err, "Error fetching task status after cancellation.")
	require.NotNil(t, checkTask, "Task should exist after cancellation.")

	if structure.TaskState(checkTask.Task.Status) != structure.TaskStateCanceled {
		logrus.Warn("No tasks were cancelled; check scheduler behavior.")
		require.Fail(t, "Expected at least some tasks to be cancelled.")
	}

	fmt.Printf("Test Concurrent Cancellation: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

/*
* @Description: This is the main test function for priority.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func TestPriority(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	fmt.Print("start test priority\n")

	// for scheduler, just test a simple task
	var computeTask = "pagerank"

	// TEST GROUP: PRIORITY
	// 1. send priority tasks to single graph
	// expect: the tasks should be executed in order of priority

	SubTestPriority(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 2. send small tasks and large tasks to single graph
	// expect: the small tasks should be executed first

	SubTestSmall(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 3. send support concurrent tasks to single graph
	// expect: the tasks should be executed concurrently
	SubTestConcurrent(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 4. send dependency-tasks to single graph
	// expect: the tasks should be executed in order of dependency

	SubTestDepends(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 5. send same priority tasks to single graph
	// expect: the tasks should be executed in order of time
	// skipped, too fragile

	// 6. send tasks to different graphs
	// expect: the tasks should be executed concurrently
	// have been tested in SubTestSmall and SubTestDepends

	// 7. send tasks with invalid dependency to single graph
	// expect: the tasks should not be executed
	SubTestInvalidDependency(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 8. send tasks concurrently and cancel them
	// expect: the tasks should be cancelled
	SubTestConcurrentCancellation(t, expectRes, healthCheck, masterHttp, graphName, computeTask, 3)
}
