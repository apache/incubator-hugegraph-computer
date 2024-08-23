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

package structure

type TaskState string

const (
	TaskStateCreated            TaskState = "created"
	TaskStateInitOK             TaskState = "init_ok"
	TaskStateLoadVertex         TaskState = "load_vertex"
	TaskStateLoadVertexOK       TaskState = "load_vertex_ok"
	TaskStateLoadScatter        TaskState = "load_scatter"
	TaskStateLoadScatterOK      TaskState = "load_scatter_ok"
	TaskStateLoadDegree         TaskState = "load_degree"
	TaskStateLoadDegreeOK       TaskState = "load_degree_ok"
	TaskStateLoadEdge           TaskState = "load_edge"
	TaskStateLoadEdgeOK         TaskState = "load_edge_ok"
	TaskStateLoaded             TaskState = "loaded"
	TaskStateSettingOutDegree   TaskState = "scatter_degree"
	TaskStateSettingOutDegreeOK TaskState = "scatter_degree_ok"
	TaskStateSettingOutEdges    TaskState = "scatter_outedge"
	TaskStateSettingOutEdgesOK  TaskState = "scatter_outedge_ok"
	TaskStateStepDoing          TaskState = "step_doing"
	TaskStateStepDone           TaskState = "step_done"
	TaskStateOutput             TaskState = "output"
	TaskStateComplete           TaskState = "complete"
	TaskStateError              TaskState = "error"
	TaskStateCanceled           TaskState = "canceled"
	TaskStateWaiting            TaskState = "waiting"
)

func (s TaskState) Converter() TaskStatus {
	if s == TaskStateLoadVertex ||
		s == TaskStateLoadVertexOK ||
		s == TaskStateLoadScatter ||
		s == TaskStateLoadScatterOK ||
		s == TaskStateLoadDegree ||
		s == TaskStateLoadDegreeOK ||
		s == TaskStateLoadEdge ||
		s == TaskStateLoadEdgeOK {
		return TaskStatusLoading
	}
	if s == TaskStateStepDoing ||
		s == TaskStateStepDone ||
		s == TaskStateOutput {
		return TaskStatusComputing
	}
	if s == TaskStateSettingOutEdges ||
		s == TaskStateSettingOutDegree ||
		s == TaskStateSettingOutDegreeOK ||
		s == TaskStateSettingOutEdgesOK {
		return TaskStatusPreparing
	}
	return TaskStatus(s)
}

type TaskStatus string

const (
	TaskStatusCreated   TaskStatus = "created"
	TaskStatusLoading   TaskStatus = "loading"
	TaskStatusLoaded    TaskStatus = "loaded"
	TaskStatusPreparing TaskStatus = "preparing"
	TaskStatusComputing TaskStatus = "computing"
	TaskStatusComplete  TaskStatus = "complete"
	TaskStatusError     TaskStatus = "error"
	TaskStatusCanceled  TaskStatus = "canceled"
	TaskStatusWaiting   TaskStatus = "waiting"
)

const (
	TaskTypeLoad    = "load"
	TaskTypeCompute = "compute"
	TaskTypeReload  = "reload"
	TaskTypeScatter = "scatter"
)

const (
	TaskCreateSync  = "sync"
	TaskCreateAsync = "async"
)

type GraphState string

const (
	GraphStateCreated    GraphState = "created"
	GraphStateLoading    GraphState = "loading"
	GraphStateLoaded     GraphState = "loaded"
	GraphStateError      GraphState = "error"
	GraphStateOnDisk     GraphState = "disk"
	GraphStateInComplete GraphState = "incomplete"
)

func (g GraphState) Converter() GraphStatus {
	if g == GraphStateOnDisk {
		return GraphStatusLoaded
	}
	return GraphStatus(g)
}

type GraphStatus string

const (
	GraphStatusCreated    GraphStatus = "created"
	GraphStatusLoading    GraphStatus = "loading"
	GraphStatusLoaded     GraphStatus = "loaded"
	GraphStatusError      GraphStatus = "error"
	GraphStatusInComplete GraphStatus = "incomplete"
)
