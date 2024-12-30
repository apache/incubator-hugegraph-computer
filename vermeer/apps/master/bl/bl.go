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
	"vermeer/apps/compute"
	"vermeer/apps/graphio"
	"vermeer/apps/master/access"
	"vermeer/apps/master/store"
	"vermeer/apps/master/workers"
	"vermeer/apps/structure"
)

type Supplier[T any] func() (t T)
type Function[T any, R any] func(t T) (r R)

var graphMgr = structure.GraphManager
var taskMgr = structure.TaskManager
var loadGraphMgr = graphio.LoadGraphMaster
var computerTaskMgr = compute.ComputerTaskManager
var algorithmMgr = compute.AlgorithmManager

var serviceStore = store.ServiceStore
var workerMgr = workers.WorkerManager

var accessMgr = access.AccessManager

//var serviceStore = store.ServiceStore

var ServerMgr = &ServerManager{}
var Scheduler = &ScheduleBl{}
