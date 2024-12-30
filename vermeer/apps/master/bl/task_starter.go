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
	"vermeer/apps/compute"
	. "vermeer/apps/master/graphs"
	"vermeer/apps/master/tasks"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

// TaskStarter The starter of tasks
type TaskStarter interface {
	StartTask() error
	preStartTask() error
}

func NewTaskStarter(taskInfo *structure.TaskInfo, workerGroup string) (TaskStarter, error) {
	if taskInfo == nil {
		return nil, fmt.Errorf("the argument `taskInfo` should not be nil")
	}
	base := baseStarter{task: taskInfo, workerGroup: workerGroup}
	switch taskInfo.Type {
	case structure.TaskTypeLoad:
		return &loadTaskStarter{base}, nil
	case structure.TaskTypeCompute:
		return &computeTaskStarter{base}, nil
	case structure.TaskTypeReload:
		return &reloadTaskStarter{base}, nil
	default:
		return nil, fmt.Errorf("taskType must be set to load/compute/reload, get taskType:%v", taskInfo.Type)
	}
}

type baseStarter struct {
	TaskStarter
	task        *structure.TaskInfo
	gbiz        *GraphBl
	workerGroup string
}

func (bs *baseStarter) graphBiz() *GraphBl {
	if bs.gbiz == nil {
		bs.gbiz = &GraphBl{Cred: structure.NewMasterCred(bs.task.SpaceName)}
	}
	return bs.gbiz
}

func (bs *baseStarter) preStartTask() error {
	bs.graphBiz().SaveIdle(bs.task.GraphName)

	return nil
}

// The load task starter
type loadTaskStarter struct {
	baseStarter
}

// StartTask It's used to start a load task.
func (lts *loadTaskStarter) StartTask() error {
	task := lts.task
	accessMgr.Access(task.SpaceName, task.GraphName)

	graph, err := lts.graphBiz().GetGraph(task.GraphName)
	if err != nil {
		return err
	}

	if graph.State != structure.GraphStateCreated {
		return fmt.Errorf("graph already exists: %s/%s", task.SpaceName, task.GraphName)
	}

	if err := lts.preStartTask(); err != nil {
		logrus.Errorf("failed to call `preStartTask` of `loadTask`, caused by: %v", err)
	}

	if err := graphMgr.SaveWorkerGroup(graph, lts.workerGroup); err != nil {
		return fmt.Errorf("failed to save work group to the graph: %s/%s, taskID: %d, caused by: %w", graph.SpaceName, graph.Name, task.ID, err)
	}

	workerClients := workerMgr.GroupWorkers(graph.WorkerGroup)

	if len(workerClients) == 0 {
		//graph.SetState(structure.GraphStateError)
		graphMgr.SetError(graph)
		return fmt.Errorf("there are no workers available for this graph: %s/%s", task.SpaceName, task.GraphName)
	}

	if err := doLoadingTask(graph, task, workerClients); err != nil {
		return err
	}

	return nil
}

// The reloading task starter
type reloadTaskStarter struct {
	baseStarter
}

// StartTask It's used to start a reload task.
func (rts *reloadTaskStarter) StartTask() (err error) {
	task := rts.task
	accessMgr.Access(task.SpaceName, task.GraphName)

	graph, err := rts.graphBiz().GetGraph(task.GraphName)
	if err != nil {
		return nil
	}

	//defer graph.UnSync(graph.Sync())
	if graph.State == structure.GraphStateLoading {
		return fmt.Errorf("the graph is in the process of being loaded, current graph state: %s", graph.State)
	}
	if graph.State == structure.GraphStateError {
		return fmt.Errorf("the graph is in the error state, graph: %s/%s", graph.SpaceName, graph.Name)
	}

	if err := graphMgr.SaveWorkerGroup(graph, rts.workerGroup); err != nil {
		return fmt.Errorf("failed to save work group to the graph: %s/%s, taskID: %d, caused by: %w", graph.SpaceName, graph.Name, task.ID, err)
	}

	// use the workers assigned to the graph currently, not the workers owned by the graph.
	workerClients := workerMgr.GroupWorkers(graph.WorkerGroup)

	if len(workerClients) == 0 {
		return fmt.Errorf("there are no workers available for this graph: %s/%s", task.SpaceName, task.GraphName)
	}

	if err := rts.graphBiz().DeleteGraph(graph.Name); err != nil {
		return fmt.Errorf("failed to delete the graph: %s/%s, caused by:%w", task.SpaceName, task.GraphName, err)
	}

	params := graph.Params
	if params == nil {
		params = task.Params
	} else {
		tasks.MergeParams(params, task.Params)
	}

	if exists, err := rts.graphBiz().AppendGraphObj(graph, params); exists || err != nil {
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("state err: failed to delete the graph: %s/%s", task.SpaceName, task.GraphName)
		}
		return fmt.Errorf("unknown state: graph: %s/%s", task.SpaceName, task.GraphName)
	}

	// update the params to the merged version
	task.Params = params
	if err := doLoadingTask(graph, task, workerClients); err != nil {
		return err
	}

	return nil
}

// The computer task starter.
type computeTaskStarter struct {
	baseStarter
}

// StartTask It's used to start a computer task.
func (cts *computeTaskStarter) StartTask() error {
	task := cts.task

	accessMgr.Access(task.SpaceName, task.GraphName)

	if err := cts.preStartTask(); err != nil {
		logrus.Errorf("failed to call `preStartTask` of `computeTask`, caused by: %v", err)
	}

	graph := graphMgr.GetGraphByName(task.SpaceName, task.GraphName)
	if graph == nil {
		return fmt.Errorf("failed to retrieve graph with name: %s/%s", task.SpaceName, task.GraphName)
	}

	if len(graph.Workers) == 0 {
		//graph.SetState(structure.GraphStateError)
		graphMgr.SetError(graph)
		return fmt.Errorf("there are no GraphWorkers in this graph: %s/%s", task.SpaceName, task.GraphName)
	}

	if err := tasks.CheckAlgoComputable(graph, task.Params); err != nil {
		return err
	}

	// It'll load graph data from disk if the graph status is OnDisk.
	if graph.State == structure.GraphStateOnDisk {
		graph.State = structure.GraphStateLoading
		_, success := GraphPersistenceTask.Operate(task.SpaceName, task.GraphName, Read)
		if !success {
			//graph.SetState(structure.GraphStateError)
			graphMgr.SetError(graph)
			return fmt.Errorf("graph load from disk error %s/%s: %s", task.SpaceName, task.GraphName, graph.State)
		} else {
			graph.State = structure.GraphStateLoaded
		}
	}

	ct := computerTaskMgr.MakeTask(task)
	maker := algorithmMgr.GetMaker(ct.Algorithm)
	if maker == nil {
		return fmt.Errorf("algorithm not exists: %s", ct.Algorithm)
	}
	dataNeeded := maker.DataNeeded()
	var useOutDegree bool
	var useOutEdges bool
	for _, need := range dataNeeded {
		switch need {
		case compute.UseOutDegree:
			useOutDegree = true
		case compute.UseOutEdge:
			useOutEdges = true
		}
	}

	var action = pb.ComputeAction_Compute
	var settingOutEdges bool
	var settingOutDegree bool
	if !graph.UseOutDegree && useOutDegree {
		settingOutDegree = true
		action = pb.ComputeAction_SettingOutDegree
	}
	if !graph.UseOutEdges && useOutEdges {
		settingOutEdges = true
		action = pb.ComputeAction_SettingOutEdges
	}
	if settingOutEdges || settingOutDegree {
		ct.SettingOutEdges = settingOutEdges
		ct.SettingOutDegree = settingOutDegree
	}

	for _, w := range graph.Workers {
		taskWorker := structure.TaskWorker{
			Name: w.Name,
		}
		task.Workers = append(task.Workers, &taskWorker)
	}

	err := StartComputeTask(graph, ct, action)
	if err != nil {
		task.SetState(structure.TaskStateError)
		task.SetErrMsg(fmt.Sprintf("start compute task error:%v", err.Error()))
		return err
	}

	//atomic.AddInt32(&graph.UsingNum, 1)
	graph.AddUsingNum()
	return nil
}

func StartComputeTask(graph *structure.VermeerGraph, computeTask *compute.ComputerTask, action pb.ComputeAction) error {
	var workerState structure.TaskState
	switch action {
	case pb.ComputeAction_Compute:
		masterComputer, err := algorithmMgr.MakeMasterComputer(computeTask.Algorithm)
		if err != nil {
			return err
		}

		ctx := masterComputer.MakeContext(computeTask.Task.Params)
		err = masterComputer.Init()
		if err != nil {
			computeTask.Task.SetState(structure.TaskStateError)
			return fmt.Errorf("algorithm init err:%w", err)
		}

		computeTask.ComputeMaster = masterComputer
		masterComputer.BeforeStep()
		for _, w := range graph.Workers {
			ctx.WorkerCValues[w.Name] = make(map[string]*compute.CValue)
		}
		workerState = structure.TaskStateStepDoing
	case pb.ComputeAction_SettingOutEdges:
		workerState = structure.TaskStateSettingOutEdges
	case pb.ComputeAction_SettingOutDegree:
		workerState = structure.TaskStateSettingOutDegree
	}

	for _, w := range graph.Workers {
		wc := workerMgr.GetWorker(w.Name)
		if wc == nil {
			//graph.SetState(structure.GraphStateError)
			graphMgr.SetError(graph)
			return fmt.Errorf("worker %s is not found", w.Name)
		}
		computeTask.Task.SetWorkerState(w.Name, workerState)
		//err := wc.computeServer.AsyncCompute(
		err := ServerMgr.ComputeServer(wc.Name).AsyncCompute(
			computeTask.Algorithm,
			computeTask.Task.GraphName,
			computeTask.Task.SpaceName,
			computeTask.Task.ID,
			computeTask.Task.Params,
			action)

		if err != nil {
			return err
		}
	}
	return nil
}
