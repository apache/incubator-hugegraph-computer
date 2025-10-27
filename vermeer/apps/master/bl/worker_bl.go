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

package bl

import (
	"fmt"
	"sync"
	. "vermeer/apps/master/graphs"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

var workerBl = &WorkerBl{}

type WorkerBl struct {
	//cred *structure.Credential
	//gbiz *GraphBl
}

// KickOffline Checking the worker state and performing a forced termination if it's offline.
// Return false if the worker is alive.
func (wb *WorkerBl) KickOffline(workerName string) bool {
	if !workerMgr.CheckWorkerAlive(workerName) {

		if err := wb.ReleaseWorker(workerName); err != nil {
			logrus.Errorf("failed to perform ReleaseWorker with the name: %s as the worker is offline, caused by: %v", workerName, err)
		}

		return true
	}

	return false
}

// ReleaseWorker Release the graph loaded by the worker, cancel the task running in the worker, and remove the worker from the WorkerManager.
func (wb *WorkerBl) ReleaseWorker(workerName string) error {
	graphs := graphMgr.GetGraphsByWorker(workerName)

	if len(graphs) == 0 {
		logrus.Infof("there are no graphs loaded by the worker with name: '%s'", workerName)
	} else {
		wg := &sync.WaitGroup{}
		for _, graph := range graphs {
			wg.Add(1)
			go wb.releaseWorkerGraph(workerName, graph, wg)
		}
		wg.Wait()
	}

	for _, taskInfo := range taskMgr.GetAllRunningTasks() {
		for _, worker := range taskInfo.Workers {
			if worker.Name == workerName {
				//taskInfo.SetState(structure.TaskStateError)
				//taskInfo.SetErrMsg(fmt.Sprintf("worker %v is offline", workerName))
				taskMgr.SetError(taskInfo, fmt.Sprintf("worker %v is offline", workerName))
				logrus.Warnf("set task %v status:error", taskInfo.ID)
				if err := Scheduler.CloseCurrent(taskInfo.ID, workerName); err != nil {
					logrus.Errorf("failed to close task with ID: %d,err:%v", taskInfo.ID, err)
				}
				break
			}
		}
	}

	workerMgr.RemoveWorker(workerName)

	return nil
}

func (wb *WorkerBl) releaseWorkerGraph(workerName string, graph *structure.VermeerGraph, wg *sync.WaitGroup) {
	if err := wb.graphBiz(graph.SpaceName).ReleaseGraph(graph.Name); err != nil {
		logrus.Errorf("graph release failed with name %s/%s during the execution of ReleaseWorker with worker name:%s",
			graph.SpaceName, graph.Name, workerName)
	}

	wg.Done()
}

func (wb *WorkerBl) graphBiz(spaceName string) *GraphBl {
	return &GraphBl{Cred: structure.NewMasterCred(spaceName)}
}
