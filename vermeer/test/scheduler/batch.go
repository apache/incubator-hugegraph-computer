package scheduler

import (
	"testing"
	"vermeer/client"
	"vermeer/test/functional"
)

/*
* @Description: This is the main test function for batch.
* @Param t
* @Param expectRes
* @Param healthCheck
* @Param masterHttp
* @Param graphName
* @Param factor
* @Param waitSecond
 */
func TestBatch(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	// TEST GROUP: BATCH
	// 1. send batch tasks to single graph
	// expect: the tasks should be executed in order of time
	// have been tested in priority.go
}
