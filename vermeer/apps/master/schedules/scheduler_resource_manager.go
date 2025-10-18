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

package schedules

import (
	"errors"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

/*
* @Description: WorkerOngoingStatus is the status of the worker ongoing.
* @Note: This is the status of the worker ongoing.
 */
type WorkerOngoingStatus string

const (
	WorkerOngoingStatusIdle              WorkerOngoingStatus = "idle"
	WorkerOngoingStatusRunning           WorkerOngoingStatus = "running"
	WorkerOngoingStatusConcurrentRunning WorkerOngoingStatus = "concurrent_running"
	WorkerOngoingStatusPaused            WorkerOngoingStatus = "paused"
	WorkerOngoingStatusDeleted           WorkerOngoingStatus = "deleted"
)

/*
* @Description: SchedulerResourceManager is the manager for the scheduler resource.
* @Note: This is the manager for the scheduler resource.
 */
type SchedulerResourceManager struct {
	structure.MutexLocker
	workerStatus            map[string]WorkerOngoingStatus
	workerGroupStatus       map[string]WorkerOngoingStatus
	runningWorkerGroupTasks map[string][]int32 // worker group name to list of running task IDs
	// broker just responsible for communication with workers
	// it can not apply tasks to workers directly
	broker *Broker
}

/*
* @Description: Init initializes the SchedulerResourceManager.
* @Note: This function will initialize the SchedulerResourceManager.
 */
func (rm *SchedulerResourceManager) Init() {
	rm.workerStatus = make(map[string]WorkerOngoingStatus)
	rm.workerGroupStatus = make(map[string]WorkerOngoingStatus)
	rm.runningWorkerGroupTasks = make(map[string][]int32)
	rm.broker = new(Broker).Init()
}

/*
* @Description: ReleaseByTaskID releases the resource by task ID.
* @Note: This function will release the resource by task ID.
* @Param taskID
 */
func (rm *SchedulerResourceManager) ReleaseByTaskID(taskID int32) {
	defer rm.Unlock(rm.Lock())

	for workerGroup, status := range rm.workerGroupStatus {
		if (status == WorkerOngoingStatusRunning || status == WorkerOngoingStatusConcurrentRunning) && rm.isTaskRunningOnWorkerGroup(workerGroup, taskID) {
			delete(rm.workerGroupStatus, workerGroup)
			if tasks, exists := rm.runningWorkerGroupTasks[workerGroup]; exists {
				for i, id := range tasks {
					if id == taskID {
						rm.runningWorkerGroupTasks[workerGroup] = append(tasks[:i], tasks[i+1:]...)
						if len(rm.runningWorkerGroupTasks[workerGroup]) == 0 {
							delete(rm.runningWorkerGroupTasks, workerGroup)
						}
						break
					}
				}
			}
			if tasks, exists := rm.runningWorkerGroupTasks[workerGroup]; !exists || len(tasks) == 0 {
				for _, worker := range workerMgr.GetGroupWorkers(workerGroup) {
					rm.changeWorkerStatus(worker.Name, WorkerOngoingStatusIdle)
				}
			} else {
				for _, worker := range workerMgr.GetGroupWorkers(workerGroup) {
					rm.changeWorkerStatus(worker.Name, WorkerOngoingStatusConcurrentRunning)
				}
			}
		}
	}
}

/*
* @Description: isTaskRunningOnWorkerGroup checks if the task is running on the worker group.
* @Note: This function will check if the task is running on the worker group.
* @Param workerGroup
* @Param taskID
* @Return bool
 */
func (rm *SchedulerResourceManager) isTaskRunningOnWorkerGroup(workerGroup string, taskID int32) bool {
	if tasks, exists := rm.runningWorkerGroupTasks[workerGroup]; exists {
		for _, id := range tasks {
			if id == taskID {
				return true
			}
		}
	}
	return false
}

/*
* @Description: GetAgentAndAssignTask gets the agent and assigns the task.
* @Note: This function will get the agent and assigns the task.
* @Param taskInfo
* @Return *Agent, AgentStatus, error
 */
func (rm *SchedulerResourceManager) GetAgentAndAssignTask(taskInfo *structure.TaskInfo) (*Agent, AgentStatus, error) {
	if taskInfo == nil {
		return nil, AgentStatusError, errors.New("taskInfo is nil")
	}

	defer rm.Unlock(rm.Lock())

	agent, status, workers, err := rm.broker.ApplyAgent(taskInfo, !taskInfo.Exclusive)
	if err != nil {
		return nil, AgentStatusError, err
	}
	if agent == nil {
		return nil, status, nil
	}

	// Assign the task to the agent
	agent.AssignTask(taskInfo)

	exclusive := taskInfo.Exclusive
	runningStatus := WorkerOngoingStatusRunning
	if _, exists := rm.runningWorkerGroupTasks[agent.GroupName()]; !exists && exclusive {
		rm.runningWorkerGroupTasks[agent.GroupName()] = []int32{}
		runningStatus = WorkerOngoingStatusRunning
		rm.workerGroupStatus[agent.GroupName()] = runningStatus
	} else {
		runningStatus = WorkerOngoingStatusConcurrentRunning
		rm.workerGroupStatus[agent.GroupName()] = runningStatus
	}
	rm.runningWorkerGroupTasks[agent.GroupName()] = append(rm.runningWorkerGroupTasks[agent.GroupName()], taskInfo.ID)

	for _, worker := range workers {
		if worker == nil {
			continue
		}
		rm.workerStatus[worker.Name] = runningStatus
	}

	return agent, status, nil
}

/*
* @Description: GetIdleWorkerGroups gets the idle worker groups.
* @Note: This function will get the idle worker groups.
* @Return []string
 */
func (rm *SchedulerResourceManager) GetIdleWorkerGroups() []string {
	defer rm.Unlock(rm.Lock())

	idleWorkerGroups := make([]string, 0)
	for workerGroup, status := range rm.workerGroupStatus {
		if status == WorkerOngoingStatusIdle {
			idleWorkerGroups = append(idleWorkerGroups, workerGroup)
		}
	}
	return idleWorkerGroups
}

/*
* @Description: GetConcurrentWorkerGroups gets the concurrent worker groups.
* @Note: This function will get the concurrent worker groups.
* @Return []string
 */
func (rm *SchedulerResourceManager) GetConcurrentWorkerGroups() []string {
	defer rm.Unlock(rm.Lock())

	concurrentWorkerGroups := make([]string, 0)
	for workerGroup, status := range rm.workerGroupStatus {
		if status == WorkerOngoingStatusConcurrentRunning {
			concurrentWorkerGroups = append(concurrentWorkerGroups, workerGroup)
		}
	}
	return concurrentWorkerGroups
}

/*
* @Description: changeWorkerStatus changes the worker status.
* @Note: This function will change the worker status.
* @Param workerName
* @Param status
 */
func (rm *SchedulerResourceManager) changeWorkerStatus(workerName string, status WorkerOngoingStatus) {
	rm.workerStatus[workerName] = status

	if status == WorkerOngoingStatusIdle || status == WorkerOngoingStatusConcurrentRunning {
		workerInfo := workerMgr.GetWorkerInfo(workerName)

		if workerInfo == nil {
			logrus.Warnf("worker '%s' not found", workerName)
			return
		}

		// get worker group name
		groupName := workerInfo.Group
		if groupName != "" {
			gws := workerMgr.GetGroupWorkers(groupName)
			allIdle := true
			allConcurrent := true
			for _, w := range gws {
				st := rm.workerStatus[w.Name]
				if st != WorkerOngoingStatusIdle {
					allIdle = false
				}
				if st != WorkerOngoingStatusConcurrentRunning {
					allConcurrent = false
				}
			}
			if allConcurrent || allIdle {
				newStatus := WorkerOngoingStatusIdle
				if allConcurrent {
					newStatus = WorkerOngoingStatusConcurrentRunning
				}
				logrus.Debugf("Change worker group '%s' status to '%s' (derived from %d workers)", groupName, newStatus, len(gws))
				rm.changeWorkerGroupStatus(groupName, newStatus)
			}
		}

	} else if status == WorkerOngoingStatusDeleted {
		delete(rm.workerStatus, workerName)
	}

	// TODO: Other status changes can be handled here if needed
}

func (rm *SchedulerResourceManager) changeWorkerGroupStatus(workerGroup string, status WorkerOngoingStatus) {
	logrus.Infof("Change worker group '%s' status to '%s'", workerGroup, status)
	rm.workerGroupStatus[workerGroup] = status
}

// TODO: when sync task created, need to alloc worker?
func (rm *SchedulerResourceManager) ChangeWorkerStatus(workerName string, status WorkerOngoingStatus) {
	defer rm.Unlock(rm.Lock())

	rm.changeWorkerStatus(workerName, status)
}
