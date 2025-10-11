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
	"time"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	. "vermeer/apps/master/graphs"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type ComputeTaskBl struct {
}

func (ctb *ComputeTaskBl) ComputeTaskStatus(
	taskId int32, state string, workerName string, step int32, computeValues map[string][]byte, errorMsg string) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("ComputeTaskStatus panic recover taskID:%v, panic:%v，stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := computerTaskMgr.GetTask(taskId)
	if computeTask == nil || computeTask.Task.State == structure.TaskStateError || computeTask.Task.State == structure.TaskStateCanceled {
		return
	}
	computeTask.Task.SetWorkerState(workerName, structure.TaskState(state))
	if computeTask.ComputeMaster == nil {
		return
	}
	ctx := computeTask.ComputeMaster.Context()
	for k, v := range computeValues {
		cv := compute.CValue{}
		_, _ = cv.Unmarshal(v)
		ctx.WorkerCValues[workerName][k] = &cv
	}
	graph := graphMgr.GetGraphByName(computeTask.Task.SpaceName, computeTask.Task.GraphName)
	if graph == nil {
		logrus.Errorf("graph not exist")
		return
	}
	logrus.Infof("ComputeTaskStatus task: %d, worker: %s, state: %s, step: %d",
		taskId, workerName, state, step)
	if structure.TaskState(state) == structure.TaskStateError {
		logrus.Infof("ComputeTaskStatus task: %d, worker: %s, state: %s", taskId, workerName, state)

		//computeTask.Task.SetState(structure.TaskStateError)
		//computeTask.Task.SetErrMsg(errorMsg)
		taskMgr.SetError(computeTask.Task, errorMsg)

		if computeTask.Task.CreateType == structure.TaskCreateSync {
			computeTask.Task.GetWg().Done()
		}
		//atomic.AddInt32(&graph.UsingNum, -1)
		graph.SubUsingNum()
		computeTask.FreeMemory()
		time.AfterFunc(1*time.Minute, func() { computerTaskMgr.DeleteTask(taskId) })
		common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
		if computeTask.Task.CreateType == structure.TaskCreateAsync {
			if err := Scheduler.CloseCurrent(taskId); err != nil {
				logrus.Errorf("failed to close task with ID: %d,err:%v", taskId, err)
			}
		}
		err := taskMgr.SaveTask(computeTask.Task.ID)
		if err != nil {
			logrus.Errorf("save task info error:%v", err)
		}
		err = taskMgr.FinishTask(taskId)
		if err != nil {
			logrus.Errorf("compute task finished error:%v", err.Error())
		}
	} else if computeTask.Task.CheckTaskState(structure.TaskState(state)) {
		if structure.TaskState(state) == structure.TaskStateInitOK {
			computeTask.Task.SetState(structure.TaskStateStepDoing)
			for _, w := range computeTask.Task.Workers {
				wc := workerMgr.GetWorker(w.Name)
				//wc.SuperStepServer.AsyncSuperStep(
				ServerMgr.SuperStepServer(wc.Name).AsyncSuperStep(
					computeTask.Task.ID,
					computeTask.Step,
					false,
					nil)
			}
		} else if structure.TaskState(state) == structure.TaskStateStepDone {
			ctx.AggregateValue()
			output := false
			isContinue := computeTask.ComputeMaster.Compute()
			computeTask.Task.SetState(structure.TaskStateStepDoing)
			//TaskMgr.ForceState(computeTask.Task, structure.TaskStateStepDoing)
			computeTask.Step = step
			computeTask.ComputeMaster.Context().Step = step
			maxStep := options.GetInt(computeTask.Task.Params, "compute.max_step")
			if computeTask.Step >= int32(maxStep) || !isContinue {
				output = true
				logrus.Infof("compute task done, cost: %v", time.Since(computeTask.Task.StartTime))
			}
			computeTask.ComputeMaster.AfterStep()
			computeTask.Step = step + 1
			computeTask.ComputeMaster.Context().Step = step + 1
			cValues := ctx.MarshalValues()
			for _, w := range computeTask.Task.Workers {
				wc := workerMgr.GetWorker(w.Name)
				//wc.SuperStepServer.AsyncSuperStep(
				ServerMgr.SuperStepServer(wc.Name).AsyncSuperStep(
					computeTask.Task.ID,
					computeTask.Step,
					output,
					cValues)
			}
		} else if structure.TaskState(state) == structure.TaskStateComplete {

			if options.GetInt(computeTask.Task.Params, "output.need_statistics") == 1 {
				ctb.computeStatistics(computeTask, graph)
			}
			//computeTask.Task.SetState(structure.TaskStateComplete)

			logrus.Infof("compute task output complete, cost: %v", time.Since(computeTask.Task.StartTime))
			if computeTask.Task.CreateType == structure.TaskCreateSync {
				if algorithmMgr.GetMaker(computeTask.Algorithm).Type() == compute.AlgorithmOLTP {
					ctb.computeTpResult(computeTask)
				}
			}
			taskMgr.ForceState(computeTask.Task, structure.TaskStateComplete)
			// for scheduler, mark task complete
			Scheduler.taskManager.MarkTaskComplete(taskId)
			graph.SubUsingNum()
			computeTask.FreeMemory()
			needQuery := options.GetInt(computeTask.Task.Params, "output.need_query") == 1
			if !needQuery {
				time.AfterFunc(1*time.Minute, func() { computerTaskMgr.DeleteTask(taskId) })
			}
			common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
			if computeTask.Task.CreateType == structure.TaskCreateAsync {
				if err := Scheduler.CloseCurrent(taskId); err != nil {
					logrus.Errorf("failed to close task with ID: %d,err:%v", taskId, err)
				}
			} else if computeTask.Task.CreateType == structure.TaskCreateSync {
				computeTask.Task.GetWg().Done()
			}
			err := taskMgr.SaveTask(computeTask.Task.ID)
			if err != nil {
				logrus.Errorf("save task info error:%v", err)
			}
			err = taskMgr.FinishTask(taskId)
			if err != nil {
				logrus.Errorf("compute task finished error:%v", err.Error())
			}
		}
	}
}

func (ctb *ComputeTaskBl) computeStatistics(computeTask *compute.ComputerTask, graph *structure.VermeerGraph) {
	//master output result
	defer func() {
		computeTask.Statistics = nil
	}()
	mode := compute.StatisticsType(options.GetString(computeTask.Task.Params, "output.statistics_mode"))
	maker := algorithmMgr.GetMaker(computeTask.Algorithm)
	statistics := maker.SupportedStatistics()
	if mode == "" {
		for statisticsType := range statistics {
			mode = statisticsType
			break
		}
	}
	if _, ok := statistics[mode]; !ok {
		sKeys := make([]compute.StatisticsType, 0, len(statistics))
		for statisticsType := range statistics {
			sKeys = append(sKeys, statisticsType)
		}
		logrus.Errorf("algorithm %v not support statistics:%v. The options available are: %v ", computeTask.Algorithm, mode, sKeys)
		computeTask.Task.SetErrMsg(fmt.Sprintf("algorithm %v not support statistics:%v. The options available are: %v ", computeTask.Algorithm, mode, sKeys))
	}
	statisticsMaster := compute.StatisticsMasterMaker(mode)
	smContext := statisticsMaster.MakeContext()
	smContext.Graph = graph
	statisticsMaster.Init(computeTask.Task.Params)
	for _, statistic := range computeTask.Statistics {
		statisticsMaster.Aggregate(statistic)
	}
	output := statisticsMaster.Output()
	for k, v := range computeTask.ComputeMaster.Statistics() {
		output[k] = v
	}

	// write to task info
	computeTask.Task.StatisticsResult = output
	// outputType := options.GetString(computeTask.Task.Params, "output.type")
	// writer := graphio.MakeWriter(outputType)

	// writerInitInfo := graphio.WriterInitInfo{
	// 	Params: computeTask.Task.Params,
	// 	Mode:   graphio.WriteModeStatistics,
	// }
	// err := writer.Init(writerInitInfo)
	// if err != nil {
	// 	//computeTask.Task.SetState(structure.TaskStateError)
	// 	//computeTask.Task.SetErrMsg(fmt.Sprintf("write statistics init error:%v", err))
	// 	taskMgr.SetError(computeTask.Task, fmt.Sprintf("write statistics init error:%v", err))
	// }
	// err = writer.WriteStatistics(output)
	// if err != nil {
	// 	//computeTask.Task.SetState(structure.TaskStateError)
	// 	//computeTask.Task.SetErrMsg(fmt.Sprintf("write statistics error:%v", err))
	// 	taskMgr.SetError(computeTask.Task, fmt.Sprintf("write statistics error:%v", err))
	// }
	// writer.Close()
}

func (ctb *ComputeTaskBl) computeTpResult(computeTask *compute.ComputerTask) {
	//master output result
	computeTask.TpResult.Output = computeTask.ComputeMaster.Output(computeTask.TpResult.WorkerResult)
}

func (ctb *ComputeTaskBl) Canceled(computeTask *compute.ComputerTask) {
	if computeTask == nil {
		logrus.Errorf("cancel computeTask is nil")
		return
	}
	logrus.Infof("task has been canceled, task_id:%v", computeTask.Task.ID)
	graph := graphMgr.GetGraphByName(computeTask.Task.SpaceName, computeTask.Task.GraphName)
	if graph != nil {
		//atomic.AddInt32(&graph.UsingNum, -1)
		graph.SubUsingNum()
	}
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
	//computeTask.Task.SetState(structure.TaskStateCanceled)
	taskMgr.ForceState(computeTask.Task, structure.TaskStateCanceled)
	computeTask.FreeMemory()
	time.AfterFunc(1*time.Minute, func() { computerTaskMgr.DeleteTask(computeTask.Task.ID) })
}

func (ctb *ComputeTaskBl) SettingGraphStatus(
	taskId int32, state string, workerName string, errorMsg string) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("ComputeTask SettingGraph panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := computerTaskMgr.GetTask(taskId)
	if computeTask == nil || computeTask.Task.State == structure.TaskStateError || computeTask.Task.State == structure.TaskStateCanceled {
		return
	}
	computeTask.Task.SetWorkerState(workerName, structure.TaskState(state))
	graph := graphMgr.GetGraphByName(computeTask.Task.SpaceName, computeTask.Task.GraphName)
	if graph == nil {
		logrus.Errorf("graph not exist")
		return
	}
	logrus.Infof("ComputeTask SettingGraph task: %d, worker: %s, state: %s",
		taskId, workerName, state)
	if structure.TaskState(state) == structure.TaskStateError {
		logrus.Infof("ComputeTaskStatus task: %d, worker: %s, state: %s", taskId, workerName, state)
		computeTask.Task.SetState(structure.TaskStateError)
		computeTask.Task.SetErrMsg(errorMsg)
		if computeTask.Task.CreateType == structure.TaskCreateSync {
			computeTask.Task.GetWg().Done()
		}
		//atomic.AddInt32(&graph.UsingNum, -1)
		graph.SubUsingNum()
		computeTask.FreeMemory()
		common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
		if computeTask.Task.CreateType == structure.TaskCreateAsync {
			if err := Scheduler.CloseCurrent(taskId); err != nil {
				logrus.Errorf("failed to close task with ID: %d,err:%v", taskId, err)
			}
		}
		err := taskMgr.SaveTask(computeTask.Task.ID)
		if err != nil {
			logrus.Errorf("save task info error:%v", err)
		}
	} else if computeTask.Task.CheckTaskState(structure.TaskState(state)) {
		if structure.TaskState(state) == structure.TaskStateSettingOutEdgesOK {
			graph.UseOutEdges = true
			computeTask.Task.SetState(structure.TaskStateSettingOutEdgesOK)
			if computeTask.SettingOutDegree {
				err := StartComputeTask(graph, computeTask, pb.ComputeAction_SettingOutDegree)
				if err != nil {
					computeTask.Task.SetState(structure.TaskStateError)
					computeTask.Task.SetErrMsg(fmt.Sprintf("start compute task error:%v", err.Error()))
					return
				}
			} else {
				//开始落盘
				go func() {
					_, ok := GraphPersistenceTask.Operate(graph.SpaceName, graph.Name, WriteDisk)
					if !ok {
						logrus.Errorf("graph %v write disk failed", graph.Name)
					}
				}()
				err := StartComputeTask(graph, computeTask, pb.ComputeAction_Compute)
				if err != nil {
					computeTask.Task.SetState(structure.TaskStateError)
					computeTask.Task.SetErrMsg(fmt.Sprintf("start compute task error:%v", err.Error()))
					return
				}
			}
		} else if structure.TaskState(state) == structure.TaskStateSettingOutDegreeOK {
			graph.UseOutDegree = true
			//开始落盘
			go func() {
				_, ok := GraphPersistenceTask.Operate(graph.SpaceName, graph.Name, WriteDisk)
				if !ok {
					logrus.Errorf("graph %v write disk failed", graph.Name)
				}
			}()
			computeTask.Task.SetState(structure.TaskStateSettingOutDegreeOK)
			err := StartComputeTask(graph, computeTask, pb.ComputeAction_Compute)
			if err != nil {
				computeTask.Task.SetState(structure.TaskStateError)
				computeTask.Task.SetErrMsg(fmt.Sprintf("start compute task error:%v", err.Error()))
				return
			}
		}
	}
}
