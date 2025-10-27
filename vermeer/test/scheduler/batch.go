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
