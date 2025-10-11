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
	. "vermeer/apps/master/graphs"
	"vermeer/apps/master/tasks"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

// is or not check the task runable.
const (
	IsCheck = true
	NoCheck = false
)

// TaskCreator Create a new TaskInfo.
type TaskCreator interface {
	// CreateTaskInfo creates a new TaskInfo from the given task description and adds it to the TaskManager.
	CreateTaskInfo(graphName string, params map[string]string, isCheck bool) (*structure.TaskInfo, error)

	// NewTaskInfo just create a new TaskInfo without adding to the TaskManager.
	NewTaskInfo(graphName string, params map[string]string, taskId int32) (*structure.TaskInfo, error)
}

// NewTaskCreator Create different task creator depending on the task type.
func NewTaskCreator(cred *structure.Credential, taskType string) (TaskCreator, error) {
	if cred == nil {
		return nil, fmt.Errorf("cred should not be nil")
	}

	base := baseCreator{cred: cred, taskType: taskType}

	switch taskType {
	case structure.TaskTypeLoad:
		return &loadTaskCreator{baseCreator: base}, nil
	case structure.TaskTypeCompute:
		return &computeTaskCreator{baseCreator: base}, nil
	case structure.TaskTypeReload:
		return &reloadTaskCreator{baseCreator: base}, nil
	default:
		return nil, fmt.Errorf("task_type must be set to load/compute, get task_type:%v", taskType)
	}
}

type baseCreator struct {
	TaskCreator
	cred     *structure.Credential
	taskType string
	gbiz     *GraphBl
}

func (bc *baseCreator) graphBiz() *GraphBl {
	if bc.gbiz == nil {
		bc.gbiz = &GraphBl{Cred: bc.cred}
	}
	return bc.gbiz
}

func (bc *baseCreator) createTaskInfo(graphName string, params map[string]string, isCheck bool) (*structure.TaskInfo, error) {
	_ = isCheck
	taskInfo, err := bc.NewTaskInfo(graphName, params, 0)

	if err != nil {
		return nil, err
	}

	return bc.saveTaskInfo(taskInfo)
}

func (bc *baseCreator) NewTaskInfo(graphName string, params map[string]string, taskId int32) (*structure.TaskInfo, error) {
	task, err := taskMgr.CreateTask(bc.cred.Space(), bc.taskType, taskId)
	if err != nil {
		return nil, err
	}

	task.GraphName = graphName
	task.CreateUser = bc.cred.User()
	task.Params = params

	return task, nil
}

func (bc *baseCreator) CopyTaskInfo(src *structure.TaskInfo) (*structure.TaskInfo, error) {
	if src == nil {
		return nil, fmt.Errorf("the argument `src` should not be nil")
	}

	task, err := taskMgr.CreateTask(src.SpaceName, src.Type, 0)
	if err != nil {
		return nil, err
	}

	task.CreateType = structure.TaskCreateAsync
	task.GraphName = src.GraphName
	task.CreateUser = src.CreateUser
	task.Params = src.Params
	task.CronExpr = "" // clear cron expression for the new task
	task.Priority = src.Priority
	task.Preorders = src.Preorders
	task.Exclusive = src.Exclusive

	return task, nil
}

func (bc *baseCreator) saveTaskInfo(task *structure.TaskInfo) (*structure.TaskInfo, error) {
	if _, err := taskMgr.AddTask(task); err != nil {
		logrus.Errorf("failed to add a task to `TaskManager`, task: %v, cased by: %v", task, err)
		return nil, err
	}

	err := taskMgr.SaveTask(task.ID)
	if err != nil {
		if err := taskMgr.DeleteTask(task.ID); err != nil {
			logrus.Errorf("failed to delete the task with taskID: %d, caused by: %v", task.ID, err)
		}
		return nil, fmt.Errorf("save task info error: %w", err)
	}

	return task, nil
}

// It's for loading task creation.
type loadTaskCreator struct {
	baseCreator
}

// CreateTaskInfo Load Task
func (tc *loadTaskCreator) CreateTaskInfo(graphName string, params map[string]string, isCheck bool) (*structure.TaskInfo, error) {
	if isCheck {
		_, exists, err := tc.graphBiz().AppendGraph(graphName, params)

		if err != nil {
			return nil, err
		}

		if exists {
			return nil, fmt.Errorf("graph already exists: %s/%s", tc.cred.Space(), graphName)
		}
	}
	return tc.createTaskInfo(graphName, params, isCheck)
}

// It's for computing task creation.
type computeTaskCreator struct {
	baseCreator
}

// CreateTaskInfo Computer Task
func (tc *computeTaskCreator) CreateTaskInfo(graphName string, params map[string]string, isCheck bool) (*structure.TaskInfo, error) {
	if isCheck {
		graph := graphMgr.GetGraphByName(tc.cred.Space(), graphName)
		if graph == nil {
			return nil, fmt.Errorf("graph not exists: %s/%s", tc.cred.Space(), graphName)
		}

		if err := tasks.CheckAlgoComputable(graph, params); err != nil {
			return nil, err
		}
	}
	return tc.createTaskInfo(graphName, params, isCheck)
}

// It's for reload task creation.
type reloadTaskCreator struct {
	baseCreator
}

// CreateTaskInfo Reload Task
func (tc *reloadTaskCreator) CreateTaskInfo(graphName string, params map[string]string, isCheck bool) (*structure.TaskInfo, error) {
	if isCheck {
		graph := graphMgr.GetGraphByName(tc.cred.Space(), graphName)
		if graph == nil {
			return nil, fmt.Errorf("graph not exists: %s/%s", tc.cred.Space(), graphName)
		}

		if graph.State == structure.GraphStateError {
			return nil, fmt.Errorf("the graph is in the error state, graph: %s/%s", tc.cred.Space(), graphName)
		}

		if len(params) == 0 && len(graph.Params) == 0 {
			return nil, fmt.Errorf("the reloading task has no params in either the task or the graph: %s/%s", tc.cred.Space(), graphName)
		}
	}
	return tc.createTaskInfo(graphName, params, isCheck)
}
