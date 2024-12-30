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

package graphio

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/options"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

const (
	LoadPartStatusPrepared = "prepared"
	LoadPartStatusLoading  = "loading"
	LoadPartStatusDone     = "done"
)

const (
	LoadTypeLocal     = "local"
	LoadTypeHdfs      = "hdfs"
	LoadTypeHugegraph = "hugegraph"
	LoadTypeAFS       = "afs"
	LoadTypeNone      = "none"
)

var FetchEof = errors.New("EOF")
var LoadMakers = map[string]GraphLoadMaker{}

type GraphLoadMaker interface {
	CreateGraphLoader() GraphLoader
	CreateGraphWriter() GraphWriter
	MakeTasks(params map[string]string, taskID int32) ([]LoadPartition, error)
}

type GraphLoadMakerBase struct{}

func init() {
	LoadMakers[LoadTypeNone] = &GraphLoadMakerBase{}
}

func (g *GraphLoadMakerBase) CreateGraphLoader() GraphLoader {
	return &GraphLoaderBase{}
}

func (g *GraphLoadMakerBase) CreateGraphWriter() GraphWriter {
	return &GraphWriterBase{}
}

func (g *GraphLoadMakerBase) MakeTasks(params map[string]string, taskID int32) ([]LoadPartition, error) {
	return nil, errors.New("not implemented")
}

type GraphLoader interface {
	Init(params map[string]string, schema structure.PropertySchema) error
	ReadVertex(vertex *structure.Vertex, property *structure.PropertyValue) error
	ReadEdge(edge *structure.Edge, property *structure.PropertyValue) error
	Name() string
	ReadCount() int
	Close()
}

type GraphLoaderBase struct{}

func (g *GraphLoaderBase) Init(params map[string]string, schema structure.PropertySchema) error {
	return nil
}
func (g *GraphLoaderBase) ReadVertex(vertex *structure.Vertex, property *structure.PropertyValue) error {
	return nil
}
func (g *GraphLoaderBase) ReadEdge(edge *structure.Edge, property *structure.PropertyValue) error {
	return nil
}
func (g *GraphLoaderBase) Name() string {
	return "none"
}
func (g *GraphLoaderBase) ReadCount() int {
	return 0
}
func (g *GraphLoaderBase) Close() {}

type GraphWriter interface {
	Init(info WriterInitInfo) error
	WriteVertex(info WriteVertexValue)
	WriteCount() int
	WriteStatistics(statistics map[string]any) error
	Close()
}

type GraphWriterBase struct {
}

func (g *GraphWriterBase) Init(info WriterInitInfo) error {
	return nil
}
func (g *GraphWriterBase) WriteVertex(info WriteVertexValue) {}
func (g *GraphWriterBase) WriteCount() int {
	return 0
}
func (g *GraphWriterBase) WriteStatistics(statistics map[string]any) error {
	return nil
}
func (g *GraphWriterBase) Close() {}

type WriteMode int

const (
	WriteModeVertexValue WriteMode = iota
	WriteModeStatistics
)

type WriterInitInfo struct {
	Params         map[string]string
	Mode           WriteMode
	PartID         int
	MaxID          int
	HgVertexSchema structure.PropertySchema
	OutputType     string
}

type WriteVertexValue struct {
	VertexID string
	Value    serialize.MarshalAble
	HgLabel  string
}

func MakeLoader(loadType string) GraphLoader {
	maker, ok := LoadMakers[loadType]
	if !ok {
		logrus.Errorf("no matched loader: %s", loadType)
		return nil
	}
	return maker.CreateGraphLoader()
}

func MakeWriter(outType string) GraphWriter {
	maker, ok := LoadMakers[outType]
	if !ok {
		logrus.Errorf("no matched writer: %s", outType)
		return nil
	}
	return maker.CreateGraphWriter()
}

type LoadGraphTask struct {
	Task      *structure.TaskInfo
	loadParts []LoadPartition
	LoadType  string
	Parallel  *int32
	LoadWg    *sync.WaitGroup
	RecvWg    *common.SpinWaiter
	SendCount map[string]*int32
	RecvCount map[string]*int32
	Locker    *sync.Mutex
}

func (lt *LoadGraphTask) GetPartition(partIdx int32) LoadPartition {
	return lt.loadParts[partIdx]
}

func (lt *LoadGraphTask) MakeTask() error {
	lt.Parallel = new(int32)
	maker, ok := LoadMakers[lt.LoadType]
	if !ok {
		return fmt.Errorf("no matched load type: %s", lt.LoadType)
	}
	var err error
	lt.loadParts, err = maker.MakeTasks(lt.Task.Params, lt.Task.ID)
	if err != nil {
		return err
	}
	for i := range lt.loadParts {
		lt.loadParts[i].Params["load.use_property"] = lt.Task.Params["load.use_property"]
		lt.loadParts[i].Params["load.delimiter"] = lt.Task.Params["load.delimiter"]
	}

	return nil
}

//func (lt *LoadGraphTask) Install() {
//	ctx := context.Background()
//	parallel := options.GetInt(lt.Task.Params, "load.parallel")
//	for i := 0; i < parallel; i++ {
//		go lt.Run(ctx)
//	}
//}
//
//func (lt *LoadGraphTask) Run(ctx context.WContext) {
//	_ = ctx
//	for {
//		_, err := lt.handler.FetchPartition(lt.Task.Id)
//		if err != nil {
//			if err == FetchEof {
//				logrus.Infof("fetch partition eof")
//				break
//			}
//			logrus.Infof("fetch partition eof")
//		}
//		_ = lt.handler.ScatterGraph(lt.Task.Id, nil)
//	}
//}

func (lt *LoadGraphTask) SetPartStatus(partIdx int32, status string) {
	lt.loadParts[partIdx].SetStatus(status)
}

func (lt *LoadGraphTask) FreeMemory() {
	lt.loadParts = nil
	lt.SendCount = nil
	lt.RecvCount = nil
	lt.RecvWg = nil
	lt.LoadWg = nil
}

const (
	LoadPartTypeVertex = "vertex"
	LoadPartTypeEdge   = "edge"
)

type LoadPartition struct {
	Id         int32
	TaskId     int32
	CreateTime time.Time
	UpdateTime time.Time
	Status     string
	Type       string
	IpAddr     string
	Params     map[string]string
}

func (lp *LoadPartition) Init(id int32, taskId int32, partType string) {
	lp.Id = id
	lp.TaskId = taskId
	lp.CreateTime = time.Now()
	lp.UpdateTime = time.Now()
	lp.Status = LoadPartStatusPrepared
	lp.Type = partType
}

func (lp *LoadPartition) SetStatus(status string) {
	lp.Status = status
	lp.UpdateTime = time.Now()
}

// --------------------LoadGraphMaster---------------------

var LoadGraphMaster = &loadGraphMaster{}

type loadGraphMaster struct {
	//LoadGraphSpaceMap map[string]*loadGraphMasterSpace
	//sync.Mutex
	loadTasks map[int32]*LoadGraphTask
	locker    sync.RWMutex
}

// type LoadGraphMasterSpace struct {
// 	loadTasks map[int32]LoadGraphTask
// 	locker    sync.RWMutex
// }

func (lm *loadGraphMaster) Init() {
	lm.loadTasks = make(map[int32]*LoadGraphTask)
}

// func (lm *loadGraphMasterSpace) Init() {
// 	lm.locker = sync.RWMutex{}
// 	lm.loadTasks = make(map[int32]LoadGraphTask, 0)
// }

// func (lm *loadGraphMaster) AddSpace(spaceName string) error {
// 	lm.Lock()
// 	defer lm.Unlock()
// 	if lm.LoadGraphSpaceMap[spaceName] != nil {
// 		return fmt.Errorf("LoadGraphMaster space exists:%s", spaceName)
// 	}
// 	loadGraphMasterSpace := &LoadGraphMasterSpace{}
// 	loadGraphMasterSpace.Init()
// 	lm.LoadGraphSpaceMap[spaceName] = loadGraphMasterSpace
// 	return nil
// }

func (lm *loadGraphMaster) MakeLoadTasks(taskInfo *structure.TaskInfo) (*LoadGraphTask, error) {
	lm.locker.Lock()
	defer lm.locker.Unlock()
	if t, ok := lm.loadTasks[taskInfo.ID]; ok {
		msg := fmt.Sprintln("MakeLoadTasks error: task exists.")
		logrus.Errorf(msg)
		return t, errors.New(msg)
	}
	loadTask := &LoadGraphTask{}
	loadTask.Task = taskInfo
	loadTask.LoadWg = &sync.WaitGroup{}
	loadTask.RecvWg = new(common.SpinWaiter)
	loadTask.SendCount = make(map[string]*int32, len(loadTask.Task.Workers))
	loadTask.RecvCount = make(map[string]*int32, len(loadTask.Task.Workers))
	for _, worker := range loadTask.Task.Workers {
		loadTask.SendCount[worker.Name] = new(int32)
		loadTask.RecvCount[worker.Name] = new(int32)
	}
	loadTask.Locker = &sync.Mutex{}
	loadTask.LoadType = options.GetString(taskInfo.Params, "load.type")
	err := loadTask.MakeTask()
	if err != nil {
		return loadTask, err
	}
	lm.loadTasks[taskInfo.ID] = loadTask
	loadTask.Task.State = structure.TaskStateLoadVertex
	return loadTask, nil
}

func (lm *loadGraphMaster) GetLoadTask(taskID int32) *LoadGraphTask {
	lm.locker.Lock()
	defer lm.locker.Unlock()
	if v, ok := lm.loadTasks[taskID]; ok {
		return v
	}
	logrus.Errorf("GetLoadTask task not exists: %d", taskID)
	return nil
}

func (lm *loadGraphMaster) FetchPreparedPart(taskID int32, workerIP string) (LoadPartition, error) {
	lm.locker.Lock()
	defer lm.locker.Unlock()

	loadTask, ok := lm.loadTasks[taskID]
	if !ok {
		return LoadPartition{}, fmt.Errorf("load task not exists: %d", taskID)
	}
	if loadTask.Task.State == structure.TaskStateLoaded {
		return LoadPartition{}, FetchEof
	}

	for i := range loadTask.loadParts {
		if loadTask.LoadType == LoadTypeLocal &&
			loadTask.loadParts[i].IpAddr != workerIP {
			continue
		}
		if loadTask.Task.State == structure.TaskStateLoadVertex {
			if loadTask.loadParts[i].Type != LoadPartTypeVertex {
				continue
			}
		} else if loadTask.Task.State == structure.TaskStateLoadEdge {
			if loadTask.loadParts[i].Type != LoadPartTypeEdge {
				continue
			}
		} else {
			logrus.Errorf("fetch task status error: %s", loadTask.Task.State)
			return LoadPartition{}, fmt.Errorf("fetch task status error: %s", loadTask.Task.State)
		}

		if loadTask.loadParts[i].Status == LoadPartStatusPrepared {
			loadTask.SetPartStatus(int32(i), LoadPartStatusLoading)
			return loadTask.loadParts[i], nil
		}
	}

	return LoadPartition{}, FetchEof
}

func (lm *loadGraphMaster) LoadTaskDone(taskID, partID int32) {
	lm.locker.Lock()
	defer lm.locker.Unlock()

	loadTask, ok := lm.loadTasks[taskID]
	if !ok {
		return
	}

	loadTask.SetPartStatus(partID-1, LoadPartStatusDone)

	if loadTask.Task.State == structure.TaskStateLoadVertex {
		for _, t := range loadTask.loadParts {
			if t.Type == LoadPartTypeVertex && t.Status != LoadPartStatusDone {
				return
			}
		}
		loadTask.Task.SetState(structure.TaskStateLoadScatter)
	} else if loadTask.Task.State == structure.TaskStateLoadEdge {
		for _, t := range loadTask.loadParts {
			if t.Status != LoadPartStatusDone {
				return
			}
		}
		loadTask.Task.SetState(structure.TaskStateLoaded)
	}
}

func (lm *loadGraphMaster) CheckLoadTaskStatus(taskID int32) bool {
	lm.locker.Lock()
	defer lm.locker.Unlock()

	loadTask, ok := lm.loadTasks[taskID]
	if !ok {
		return false
	}

	for _, t := range loadTask.loadParts {
		if t.Status != LoadPartStatusDone {
			return false
		}
	}

	return true
}

func (lm *loadGraphMaster) DeleteTask(taskID int32) {
	lm.locker.Lock()
	defer lm.locker.Unlock()
	delete(lm.loadTasks, taskID)
}

// --------------------LoadGraphWorker--------------------

var LoadGraphWorker = &loadGraphWorker{}

type loadGraphWorker struct {
	loadTasks map[int32]*LoadGraphTask
	locker    sync.RWMutex
}

func (lw *loadGraphWorker) Init() {
	lw.loadTasks = make(map[int32]*LoadGraphTask)
}

func (lw *loadGraphWorker) GetLoadTask(taskID int32) *LoadGraphTask {
	lw.locker.Lock()
	defer lw.locker.Unlock()
	return lw.loadTasks[taskID]
}

func (lw *loadGraphWorker) InstallTask(taskInfo *structure.TaskInfo) *LoadGraphTask {
	lw.locker.Lock()
	defer lw.locker.Unlock()
	if _, ok := lw.loadTasks[taskInfo.ID]; ok {
		return lw.loadTasks[taskInfo.ID]
	}
	loadTask := LoadGraphTask{
		Task:      taskInfo,
		loadParts: make([]LoadPartition, 0),
		Parallel:  new(int32),
		LoadWg:    &sync.WaitGroup{},
		RecvWg:    new(common.SpinWaiter),
		Locker:    &sync.Mutex{},
	}
	loadTask.SendCount = make(map[string]*int32, len(loadTask.Task.Workers))
	loadTask.RecvCount = make(map[string]*int32, len(loadTask.Task.Workers))
	for _, worker := range loadTask.Task.Workers {
		loadTask.SendCount[worker.Name] = new(int32)
		loadTask.RecvCount[worker.Name] = new(int32)
	}
	lw.loadTasks[taskInfo.ID] = &loadTask
	// TODO: why not a pointer?
	return &loadTask
}

func (lw *loadGraphWorker) RunTask(info structure.TaskInfo) {
	if info.Type == LoadTypeLocal {

	} else if info.Type == LoadTypeHdfs {

	}
}

func (lw *loadGraphWorker) DeleteTask(taskID int32) {
	lw.locker.Lock()
	defer lw.locker.Unlock()
	delete(lw.loadTasks, taskID)
}
