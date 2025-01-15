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
	"errors"
	"fmt"
	"sort"
	"time"
	"vermeer/apps/compute"

	"vermeer/apps/master/tasks"
	. "vermeer/apps/master/workers"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

var ErrorTaskDuplicate error = errors.New("task already exists")

type TaskBl struct {
	Cred *structure.Credential
}

func (tb *TaskBl) CreateTaskInfo(
	graphName string,
	taskType string,
	params map[string]string, isCheck bool) (taskInfo *structure.TaskInfo, err error) {

	var creator TaskCreator
	if creator, err = NewTaskCreator(tb.Cred, taskType); err != nil {
		return nil, err
	}

	// doing duplicate check
	if newTask, err := creator.NewTaskInfo(graphName, params, -1); err != nil {
		return nil, err
	} else if isCheck {
		if prevTask, err := tb.checkDuplicate(newTask); err != nil {
			logrus.Infof("checkDuplicate :%v", err)
			return prevTask, ErrorTaskDuplicate
		}
	}

	if taskInfo, err = creator.CreateTaskInfo(graphName, params, isCheck); err != nil {
		return nil, err
	}

	return taskInfo, nil
}

func (tb *TaskBl) checkDuplicate(taskInfo *structure.TaskInfo) (*structure.TaskInfo, error) {
	tailTask := Scheduler.PeekSpaceTail(taskInfo.SpaceName)
	if tailTask == nil {
		return nil, nil
	}
	if tailTask.Equivalent(taskInfo) {
		return tailTask, fmt.Errorf("task [ %v ] already exists", tailTask.ID)
	}
	return nil, nil
}

func (tb *TaskBl) QueryTasks(queryType string, limit int) (tasks []*structure.TaskInfo, err error) {
	switch queryType {
	case "all":
		if tb.Cred.IsAdmin() {
			tasks = taskMgr.GetAllTasks(limit)
		} else {
			tasks = taskMgr.GetTasks(tb.Cred.Space(), limit)
		}
	case "todo":
		if tb.Cred.IsAdmin() {
			tasks = Scheduler.AllTasksInQueue()
		} else {
			tasks = Scheduler.TasksInQueue(tb.Cred.Space())
		}
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].ID > tasks[j].ID
		})
	case "running":
		if tb.Cred.IsAdmin() {
			tasks = taskMgr.GetAllRunningTasks()
		} else {
			//tasks = scheduleBl.TasksInQueue(tb.cred.Space())
		}
	default:
		return nil, fmt.Errorf("unsupported query type: %s", queryType)
	}
	if len(tasks) > limit {
		return tasks[:limit], nil
	}
	return tasks, nil
}

// QueryResults
// limit: [0 - 100,000]
func (tb *TaskBl) QueryResults(taskID int32, cursor, limit int) (results []compute.VertexValue, end int32, err error) {
	// check permissions
	if !tb.Cred.IsAdmin() {
		if _, err := tb.GetTaskInfo(taskID); err != nil {
			return nil, 0, err
		}
	}

	return computerTaskMgr.GetComputeValues(taskID, cursor, limit)
}

// GetTaskInfo
func (tb *TaskBl) GetTaskInfo(taskID int32) (taskInfo *structure.TaskInfo, err error) {
	taskInfo = taskMgr.GetTaskByID(taskID)
	if taskInfo == nil {
		return nil, fmt.Errorf("there is no task with taskID: %v", taskID)
	}

	if tb.Cred.IsAdmin() || taskInfo.SpaceName == tb.Cred.Space() {
		return taskInfo, nil
	}

	return nil, fmt.Errorf("permission required for this task with ID: %v", taskID)
}

// CancelTask
func (tb *TaskBl) CancelTask(taskID int32) error {
	task, err := tb.GetTaskInfo(taskID)
	if err != nil {
		return err
	}

	if task.CreateUser != tb.Cred.User() {
		return fmt.Errorf("cannot cancel the task with id '%v' as it was not created by you", taskID)
	}

	if task.State == structure.TaskStateCanceled {
		return fmt.Errorf("task had been in state canceled")
	}

	if task.State == structure.TaskStateError {
		return fmt.Errorf("task status is error")
	}

	if task.Type == structure.TaskTypeLoad && task.State == structure.TaskStateLoaded ||
		task.Type == structure.TaskTypeCompute && task.State == structure.TaskStateComplete {
		return fmt.Errorf("task already complete")
	}

	err = Scheduler.CancelTask(task)
	if err != nil {
		return err
	}
	err = taskMgr.FinishTask(task.ID)
	if err != nil {
		logrus.Errorf("cancel task finished error:%v", err.Error())
	}
	return nil
}

// FilteringTasks 根据用户过滤任务参数
func (tb *TaskBl) FilteringTasks(tasksInfo []*structure.TaskInfo) []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(tasksInfo))
	for _, info := range tasksInfo {
		tasks = append(tasks, tb.FilteringTask(info))
	}

	return tasks
}

// FilteringTask 根据用户过滤任务参数
func (tb *TaskBl) FilteringTask(taskInfo *structure.TaskInfo) *structure.TaskInfo {
	task := structure.TaskInfo{}
	task = *taskInfo
	// 非任务本用户和admin不展示任务信息, 不展示统计结果
	if !tb.Cred.IsAdmin() && tb.Cred.User() != taskInfo.CreateUser {
		task.Params = nil
		task.StatisticsResult = nil
	}

	return &task
}

// func queueExecuteTask(taskInfo *structure.TaskInfo) error {
// 	taskInfo.CreateType = structure.TaskCreateAsync
// 	_, err := TaskScheduleMgr.QueueTask(taskInfo)
// 	return err
// }

func QueueExecuteTask(taskInfo *structure.TaskInfo) error {
	taskInfo.CreateType = structure.TaskCreateAsync
	_, err := Scheduler.QueueTask(taskInfo)
	return err
}

func QueueExecuteTasks(tasksInfo []*structure.TaskInfo) []error {
	defer Scheduler.Unlock(Scheduler.Lock())
	errs := make([]error, 0, len(tasksInfo))
	for _, task := range tasksInfo {
		task.CreateType = structure.TaskCreateAsync
		_, err := Scheduler.QueueTask(task)
		errs = append(errs, err)
	}
	return errs
}

func SyncExecuteTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	taskStarter, err := NewTaskStarter(taskInfo, "$")
	if err != nil {
		logrus.Errorf("SyncExecuting,failed to construct a TaskStarter with task type: %s; task ID: %d", taskInfo.Type, taskInfo.ID)
		//taskInfo.SetState(structure.TaskStateError)
		taskMgr.SetError(taskInfo, err.Error())
		return err
	}

	taskInfo.CreateType = structure.TaskCreateSync
	taskInfo.GetWg().Add(1)
	taskInfo.StartTime = time.Now()
	if err := taskStarter.StartTask(); err != nil {
		taskInfo.GetWg().Done()
		return err
	}
	taskInfo.GetWg().Wait()
	if taskInfo.State != structure.TaskStateLoaded && taskInfo.State != structure.TaskStateComplete {
		return fmt.Errorf("task %v not complete: %s", taskInfo.ID, taskInfo.State)
	}
	return nil
}

func doLoadingTask(graph *structure.VermeerGraph, task *structure.TaskInfo, workerClients []*WorkerClient) error {
	//graph.SetState(structure.GraphStateLoading)
	if err := graphMgr.SetState(graph, structure.GraphStateLoading); err != nil {
		return err
	}

	workers, workersName := tasks.ToGraphWorkers(workerClients)
	graph.Workers = workers

	loadTask, err := loadGraphMgr.MakeLoadTasks(task)
	if err != nil {
		//task.SetState(structure.TaskStateError)
		//graph.SetState(structure.GraphStateError)
		graphMgr.SetError(graph)
		//todo异步通知任务创建者
		return err
	}

	req := &LoadingTaskReq{
		task.ID,
		pb.LoadStep_Vertex,
		loadTask.LoadType,
		task.GraphName,
		task.SpaceName,
		workersName,
		task.Params,
	}

	err = sendLoadingReq(task, workerClients, func(*WorkerClient) *LoadingTaskReq { return req })

	if err != nil {
		//graph.SetState(structure.GraphStateError)
		graphMgr.SetError(graph)
		return err
	}

	return nil
}

func sendLoadingReq(task *structure.TaskInfo, workerClients []*WorkerClient, apply Function[*WorkerClient, *LoadingTaskReq]) error {

	for _, wc := range workerClients {
		taskWorker := structure.TaskWorker{
			Name:  wc.Name,
			State: structure.TaskStateCreated,
		}
		task.Workers = append(task.Workers, &taskWorker)
		//err := wc.loadServer.SendLoadReq(apply(wc))
		err := ServerMgr.LoadServer(wc.Name).SendLoadReq(apply(wc))
		if err != nil {
			//task.SetState(structure.TaskStateError)
			taskMgr.SetError(task, err.Error())
			return err
		}
	}

	return nil
}
