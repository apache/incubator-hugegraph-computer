package scheduler

import (
	"fmt"
	"testing"
	"time"
	"vermeer/client"
	"vermeer/test/functional"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

/*
* @Description: SubTestRoutine tests the scheduler's behavior when submitting tasks with cron expression.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param computeTask
* @Param waitSecond
 */
func SubTestRoutine(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Routine start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// every 1 minute
	taskComputeBody["cron_expr"] = "* * * * *"

	logrus.Infof("params for routine test: %+v", taskComputeBody)

	taskid := computeTest.SendComputeReqAsyncNotWait(taskComputeBody)
	// computeTest.CheckRes()

	// wait for a while and check again
	time.Sleep(2 * time.Minute)

	// check if deployed
	queue := []int32{}
	queue = append(queue, int32(taskid+1))
	result, err := masterHttp.GetTaskStartSequence(queue)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.Sequence))
	require.Greater(t, result.Sequence[0], int32(0))

	masterHttp.GetTaskCancel(int(taskid))

	fmt.Printf("Test Routine: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

func TestRoutine(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	var computeTask = "pagerank"

	// TEST GROUP: ROUTINE
	// 1. send tasks to single graph
	// expect: the tasks should be executed timely

	SubTestRoutine(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)
}
