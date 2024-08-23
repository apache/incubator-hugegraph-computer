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

package services

import (
	"time"
	"vermeer/algorithms"
	"vermeer/apps/compute"
	"vermeer/apps/graphio"
	"vermeer/apps/master/access"
	"vermeer/apps/master/bl"
	"vermeer/apps/master/store"
	"vermeer/apps/master/threshold"
	"vermeer/apps/master/workers"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

var GraphMgr = structure.GraphManager
var TaskMgr = structure.TaskManager
var LoadGraphMgr = graphio.LoadGraphMaster
var ComputerTaskMgr = compute.ComputerTaskManager
var AlgorithmMgr = compute.AlgorithmManager
var SpaceMgr = structure.SpaceManager
var WorkerMgr = workers.WorkerManager
var AccessMgr = access.AccessManager
var serviceStore = store.ServiceStore

var serverMgr = bl.ServerMgr
var scheduler = bl.Scheduler

var ServiceMaster = &service{}
var InfoMaster = &Info{}

type Info struct {
	GrpcPeer   string    `json:"grpc_peer,omitempty"`
	IpAddr     string    `json:"ip_addr,omitempty"`
	DebugMod   string    `json:"debug_mod,omitempty"`
	Version    string    `json:"version,omitempty"`
	LaunchTime time.Time `json:"launch_time,omitempty"`
}

type service struct {
	//LoadGraphHandler *LoadGraphServer
	//ComputerHandler  *ComputeTaskServer
	//SuperStepHandler *SuperStepServer
}

func (s *service) Init() error {
	//s.ComputerHandler = &ComputeTaskServer{}
	//s.SuperStepHandler = &SuperStepServer{}
	//s.LoadGraphHandler = &LoadGraphServer{}

	// init worker manager
	WorkerMgr.Init()
	GraphMgr.Init()
	TaskMgr.Init()
	LoadGraphMgr.Init()
	ComputerTaskMgr.Init()
	AlgorithmMgr.Init()
	AccessMgr.Init()
	SpaceMgr.Init()
	serverMgr.Init()

	scheduler.Init()
	threshold.Init()

	if err := serviceStore.Init(); err != nil {
		return err
	}

	//GraphPersistenceTask.Run()
	for _, maker := range algorithms.Algorithms {
		AlgorithmMgr.Register(maker, "built-in")
	}
	AlgorithmMgr.LoadPlugins()

	//恢复本地落盘数据
	err := GraphMgr.InitStore()
	if err != nil {
		return err
	}
	graphs, err := GraphMgr.ReadAllInfo()
	if err != nil {
		return err
	}
	for _, graph := range graphs {
		if graph == nil {
			logrus.Errorf("load graph info nil")
			continue
		}
		if graph.OnDisk && (graph.State == structure.GraphStateLoaded || graph.State == structure.GraphStateOnDisk) {
			// no need to save state
			graph.SetState(structure.GraphStateInComplete)
		} else {
			if graph.State != structure.GraphStateCreated {
				//graph.SetState(structure.GraphStateError)
				GraphMgr.SetError(graph)
			}
		}
		//graph.UsingNum = 0
		graph.ResetUsingNum()
		err = GraphMgr.AddGraph(graph)
		if err != nil {
			logrus.Errorf("add graph error:%v", err)
			return err
		}
	}

	err = TaskMgr.InitStore()
	if err != nil {
		return err
	}
	tasks, err := TaskMgr.ReadAllTask()
	if err != nil {
		return err
	}
	for _, task := range tasks {
		_, err := TaskMgr.AddTask(task)
		if err != nil {
			return err
		}
	}

	for _, task := range TaskMgr.GetAllWaitingTasks() {
		logrus.Infof("recover a waiting task '%d' of %s/%s", task.ID, task.SpaceName, task.GraphName)

		if _, err := scheduler.QueueTask(task); err != nil {
			logrus.Errorf("failed to recover a task into the queue tasks, caused by: %v", err)
		}
	}

	if serviceStore.GetDispatchPause() {
		scheduler.PauseDispatch()
		logrus.Info("recovered dispatching to be paused")
	}

	return nil
}

func (s *service) Run() {
	InfoMaster.LaunchTime = time.Now()
}

func (s *service) Close() {

}
