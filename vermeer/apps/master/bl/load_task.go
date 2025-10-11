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
	"time"
	"vermeer/apps/common"
	"vermeer/apps/graphio"
	. "vermeer/apps/master/graphs"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type LoadTaskBl struct {
}

func (lb *LoadTaskBl) FetchPartition(taskId int32, workName string) graphio.LoadPartition {
	worker := workerMgr.GetWorker(workName)
	part, err := loadGraphMgr.FetchPreparedPart(taskId, worker.IpAddr)
	if err != nil {
		return graphio.LoadPartition{}
	}
	return part
}

func (lb *LoadTaskBl) LoadPartStatus(taskId int32, partId int32, status string) {
	logrus.Infof("LoadPartStatus task: %d, part: %d, status: %s", taskId, taskId, status)
	if status == graphio.LoadPartStatusDone {
		loadGraphMgr.LoadTaskDone(taskId, partId)
	}
}

func (lb *LoadTaskBl) WorkerVertexCount(taskId int32, workerName string, count uint32) {
	loadTask := loadGraphMgr.GetLoadTask(taskId)
	graph := graphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	graph.Locker.Lock()
	defer graph.Locker.Unlock()

	graph.SetWorkerVertexCount(workerName, count, 0)
}

func (lb *LoadTaskBl) WorkerEdgeCount(taskId int32, workerName string, count int64) {
	loadTask := loadGraphMgr.GetLoadTask(taskId)
	graph := graphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	graph.Locker.Lock()
	defer graph.Locker.Unlock()

	graph.SetWorkerEdgeCount(workerName, count)
}

func (lb *LoadTaskBl) GetGraphWorkers(spaceName string, graphName string) []*structure.GraphWorker {
	graph := graphMgr.GetGraphByName(spaceName, graphName)
	return graph.Workers
}

func (lb *LoadTaskBl) LoadTaskStatus(taskId int32, state string, workerName string, errorMsg string) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("LoadTaskStatus panic recover taskID:%v, panic:%v, stack message: %s",
				taskId, r, common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := loadGraphMgr.GetLoadTask(taskId)
	if loadTask == nil || loadTask.Task.State == structure.TaskStateError || loadTask.Task.State == structure.TaskStateCanceled {
		return
	}
	loadTask.Task.SetWorkerState(workerName, structure.TaskState(state))
	if structure.TaskState(state) == structure.TaskStateError {
		logrus.Infof("LoadTaskStatus task: %d, worker: %s, state: %s", taskId, workerName, state)
		taskMgr.SetError(loadTask.Task, errorMsg)
		//loadTask.Task.SetState(structure.TaskStateError)
		//loadTask.Task.SetErrMsg(errorMsg)
		if loadTask.Task.CreateType == structure.TaskCreateSync {
			loadTask.Task.GetWg().Done()
		}
		graph := graphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
		//graph.SetState(structure.GraphStateError)
		graphMgr.SetError(graph)

		for _, wn := range loadTask.Task.Workers {
			//worker := WorkerMgr.GetWorker(wn.Name)
			//if err := worker.loadServer.AsyncLoad(
			if err := ServerMgr.LoadServer(wn.Name).AsyncLoad(
				taskId,
				pb.LoadStep_Error,
				"",
				loadTask.Task.GraphName,
				loadTask.Task.SpaceName,
				nil,
				nil); err != nil {
				logrus.Errorf("failed to perform the AsyncLoad through the worker with name: '%s' in the TaskStatusError state , caused by: %v", wn.Name, err)
			}
		}
		loadTask.FreeMemory()
		time.AfterFunc(1*time.Minute, func() { loadGraphMgr.DeleteTask(taskId) })
		common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(loadTask.Task.Type).Dec()
		if err := Scheduler.CloseCurrent(taskId); err != nil {
			logrus.Errorf("failed to close task with ID: %d,err:%v", taskId, err)
		}
		err := graphMgr.SaveInfo(graph.SpaceName, graph.Name)
		if err != nil {
			logrus.Errorf("save graph info error:%v", err)
		}
		err = taskMgr.SaveTask(loadTask.Task.ID)
		if err != nil {
			logrus.Errorf("save task info error:%v", err)
		}
	} else if loadTask.Task.CheckTaskState(structure.TaskState(state)) {
		logrus.Infof("LoadTaskStatus task: %d, worker: %s, state: %s", taskId, workerName, state)
		if structure.TaskState(state) == structure.TaskStateLoadVertexOK {
			logrus.Infof("load graph TaskStateLoadVertexOK task: %d, graph: %s",
				taskId, loadTask.Task.GraphName)

			loadTask.Task.SetState(structure.TaskStateLoadScatter)
			//TaskMgr.ForceState(loadTask.Task, structure.TaskStateLoadScatter)

			graph := graphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
			graph.DispatchVertexId()

			for _, wn := range loadTask.Task.Workers {
				//worker := WorkerMgr.GetWorker(wn.Name)
				//if err := worker.loadServer.AsyncLoad(
				if err := ServerMgr.LoadServer(wn.Name).AsyncLoad(
					taskId,
					pb.LoadStep_ScatterVertex,
					"",
					loadTask.Task.GraphName,
					loadTask.Task.SpaceName,
					nil,
					nil); err != nil {
					logrus.Errorf("failed to perform the AsyncLoad through the worker with name: %s, caused by: %v", wn.Name, err)
				}
			}
		} else if structure.TaskState(state) == structure.TaskStateLoadScatterOK {
			logrus.Infof("load graph TaskStateLoadScatterOK task: %d, graph: %s",
				taskId, loadTask.Task.GraphName)

			loadTask.Task.SetState(structure.TaskStateLoadEdge)
			//TaskMgr.ForceState(loadTask.Task, structure.TaskStateLoadEdge)

			for _, wn := range loadTask.Task.Workers {
				//worker := WorkerMgr.GetWorker(wn.Name)
				//if err := worker.loadServer.AsyncLoad(
				if err := ServerMgr.LoadServer(wn.Name).AsyncLoad(
					taskId,
					pb.LoadStep_Edge,
					"",
					loadTask.Task.GraphName,
					loadTask.Task.SpaceName,
					nil,
					nil); err != nil {
					logrus.Errorf("failed to perform the AsyncLoad through the worker with name: %s in the TaskStatusLoadScatterOK state, caused by: %v",
						wn.Name, err)
				}
			}
		} else if structure.TaskState(state) == structure.TaskStateLoadEdgeOK {
			logrus.Infof("load graph TaskStateLoadEdgeOK task: %d, graph: %s",
				taskId, loadTask.Task.GraphName)

			loadTask.Task.SetState(structure.TaskStateLoadDegree)
			//TaskMgr.ForceState(loadTask.Task, structure.TaskStateLoadDegree)

			for _, wn := range loadTask.Task.Workers {
				//worker := WorkerMgr.GetWorker(wn.Name)
				//if err := worker.loadServer.AsyncLoad(
				if err := ServerMgr.LoadServer(wn.Name).AsyncLoad(
					taskId,
					pb.LoadStep_OutDegree,
					"",
					loadTask.Task.GraphName,
					loadTask.Task.SpaceName,
					nil,
					nil); err != nil {
					logrus.Errorf("falied to perform the AsyncLoad through the worker with name: %s in the TaskStatusLoadEdgeOK state, caused by: %v",
						workerName, err)
				}
			}
		} else if structure.TaskState(state) == structure.TaskStateLoaded {
			logrus.Infof("load graph TaskStateLoaded task: %d, graph: %s, cost: %v",
				taskId, loadTask.Task.GraphName, time.Since(loadTask.Task.StartTime))
			graph := graphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)

			//graph.SetState(structure.GraphStateLoaded)
			graphMgr.ForceState(graph, structure.GraphStateLoaded)
			graph.Statics()

			loadTask.Task.SetState(structure.TaskStateLoaded)
			//TaskMgr.ForceState(loadTask.Task, structure.TaskStateLoaded)

			// for scheduler, mark task complete
			Scheduler.taskManager.MarkTaskComplete(taskId)

			logrus.Infof("graph: %s, vertex: %d, edge: %d", graph.Name, graph.VertexCount, graph.EdgeCount)
			for _, w := range graph.Workers {
				logrus.Infof(
					"graph: %s, worker: %s, vertex: %d, edge: %d",
					graph.Name, w.Name, w.VertexCount, w.EdgeCount)
			}

			for _, wn := range loadTask.Task.Workers {
				//worker := WorkerMgr.GetWorker(wn.Name)
				//if err := worker.loadServer.AsyncLoad(
				if err := ServerMgr.LoadServer(wn.Name).AsyncLoad(
					taskId,
					pb.LoadStep_Complete,
					"",
					loadTask.Task.GraphName,
					loadTask.Task.SpaceName,
					nil,
					nil); err != nil {
					logrus.Errorf("failed to perform the AsyncLoad through the worker with name: %s in the TaskStatusLoaded state, caused by %v",
						wn.Name, err)
				}
			}
			if loadTask.Task.CreateType == structure.TaskCreateSync {
				loadTask.Task.GetWg().Done()
			}
			loadTask.FreeMemory()
			time.AfterFunc(1*time.Minute, func() { loadGraphMgr.DeleteTask(taskId) })
			common.PrometheusMetrics.GraphLoadedCnt.WithLabelValues().Inc()
			common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(loadTask.Task.Type).Dec()
			if err := Scheduler.CloseCurrent(taskId); err != nil {
				logrus.Errorf("failed to close task with ID: %d,err:%v", taskId, err)
			}
			err := graphMgr.SaveInfo(graph.SpaceName, graph.Name)
			if err != nil {
				logrus.Errorf("save graph info error:%v", err)
			}
			err = taskMgr.SaveTask(loadTask.Task.ID)
			if err != nil {
				logrus.Errorf("save task info error:%v", err)
			}
			err = taskMgr.FinishTask(loadTask.Task.ID)
			if err != nil {
				logrus.Errorf("cancel task finished error:%v", err.Error())
			}
			//开始落盘
			go func() {
				_, ok := GraphPersistenceTask.Operate(graph.SpaceName, graph.Name, WriteDisk)
				if !ok {
					logrus.Errorf("graph %v write disk failed", graph.Name)
				}
			}()
		}
	}
}

func (lb *LoadTaskBl) Canceled(loadTask *graphio.LoadGraphTask) {
	if loadTask == nil {
		logrus.Errorf("cancel loadTask is nil")
		return
	}
	logrus.Infof("task has been canceled, task_id:%v", loadTask.Task.ID)
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(loadTask.Task.Type).Dec()
	graph := graphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	if graph != nil {
		//graph.SetState(structure.GraphStateError)
		graphMgr.SetError(graph)
		//_ = GraphMgr.SaveInfo(graph.SpaceName, graph.Name)
	}

	//loadTask.Task.SetState(structure.TaskStateCanceled)
	taskMgr.ForceState(loadTask.Task, structure.TaskStateCanceled)

	loadTask.FreeMemory()
	time.AfterFunc(1*time.Minute, func() { loadGraphMgr.DeleteTask(loadTask.Task.ID) })
}
