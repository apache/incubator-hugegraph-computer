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

package compute

import (
	"fmt"
	"io"
	"sync"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/options"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type ComputerTask struct {
	Task             *structure.TaskInfo
	Algorithm        string
	Step             int32
	ComputeWorker    WorkerComputer
	ComputeMaster    MasterComputer
	ComputeValue     ComputerResult
	Statistics       [][]byte
	TpResult         computeTpResult
	Parallel         *int32
	StepWg           *sync.WaitGroup
	RecvWg           *common.SpinWaiter
	Locker           *sync.Mutex
	SettingOutDegree bool
	SettingOutEdges  bool
	SendCount        map[string]*int32
	RecvCount        map[string]*int32
}

func (ct *ComputerTask) FreeMemory() {
	ct.ComputeMaster = nil
	ct.ComputeWorker = nil
	ct.SendCount = nil
	ct.RecvCount = nil
}

type ComputerResult struct {
	Values []VertexValue
	Timer  *time.Timer
}

type computeTpResult struct {
	WorkerResult [][]byte
	Output       any
}

type VertexValue struct {
	ID    string
	Value string
}

var ComputerTaskManager = &computerTaskManager{}

type computerTaskManager struct {
	tasks  map[int32]*ComputerTask
	locker sync.Mutex
}

// type ComputerTaskSpace struct {
// 	tasks  map[int32]*ComputerTask
// 	locker sync.Mutex
// }

// func (ctm *computerTaskManager) AddSpace(spaceName string) error {
// 	ctm.Lock()
// 	defer ctm.Unlock()
// 	if ctm.ComputerTaskSpaceMap[spaceName] != nil {
// 		return fmt.Errorf("ComputerTaskManager space exists:%s", spaceName)
// 	}
// 	computerTaskSpace := &ComputerTaskSpace{}
// 	computerTaskSpace.Init()
// 	ctm.ComputerTaskSpaceMap[spaceName] = computerTaskSpace
// 	return nil
// }

func (ctm *computerTaskManager) Init() {
	ctm.tasks = make(map[int32]*ComputerTask)
}

// func (cts *ComputerTaskSpace) Init() {
// 	cts.tasks = make(map[int32]*ComputerTask)
// 	cts.locker = sync.Mutex{}
// }

func (ctm *computerTaskManager) MakeTask(taskInfo *structure.TaskInfo) *ComputerTask {
	ctm.locker.Lock()
	defer ctm.locker.Unlock()
	algo := ctm.ExtractAlgo(taskInfo.Params)
	ct := ComputerTask{
		Task:      taskInfo,
		Algorithm: algo,
		Step:      0,
		Parallel:  new(int32),
		StepWg:    &sync.WaitGroup{},
		RecvWg:    new(common.SpinWaiter),
		Locker:    &sync.Mutex{},
	}
	ctm.tasks[taskInfo.ID] = &ct
	return &ct
}

func (ctm *computerTaskManager) ExtractAlgo(params map[string]string) string {
	if params == nil {
		return ""
	}
	return options.GetString(params, "compute.algorithm")
}

func (ctm *computerTaskManager) GetTask(taskID int32) *ComputerTask {
	ctm.locker.Lock()
	defer ctm.locker.Unlock()
	return ctm.tasks[taskID]
}

func (ctm *computerTaskManager) GetComputeValues(taskID int32, cursor, limit int) ([]VertexValue, int32, error) {
	ctm.locker.Lock()
	defer ctm.locker.Unlock()
	task := ctm.tasks[taskID]
	if task == nil {
		return nil, 0, fmt.Errorf("task_id error:%v", taskID)
	}
	if len(task.ComputeValue.Values) == 0 {
		return nil, 0, fmt.Errorf("compute value not exist, task_id:%v", taskID)
	}
	if cursor == len(task.ComputeValue.Values) {
		return nil, 0, io.EOF
	} else if cursor < 0 || cursor > len(task.ComputeValue.Values) {
		return nil, 0, fmt.Errorf("cursor:%v must be >= 0 and <= %v", cursor, len(task.ComputeValue.Values))
	}
	start := cursor
	end := cursor + limit
	if end > len(task.ComputeValue.Values) {
		end = len(task.ComputeValue.Values)
	}
	computeValue := make([]VertexValue, end-start)
	copy(computeValue, task.ComputeValue.Values[start:end])
	//更新时间
	if task.ComputeValue.Timer != nil {
		task.ComputeValue.Timer.Stop()
	}
	ctm.initTimer(taskID)
	return computeValue, int32(end), nil
}

func (ctm *computerTaskManager) DeleteTask(taskID int32) {
	ctm.locker.Lock()
	defer ctm.locker.Unlock()
	ctm.deleteTask(taskID)
}

func (ctm *computerTaskManager) deleteTask(taskID int32) {
	delete(ctm.tasks, taskID)
}

func (ctm *computerTaskManager) InitTimer(taskID int32) {
	ctm.locker.Lock()
	defer ctm.locker.Unlock()
	ctm.initTimer(taskID)
}

func (ctm *computerTaskManager) initTimer(taskID int32) {
	ct := ctm.tasks[taskID]
	ct.ComputeValue.Timer = time.AfterFunc(10*time.Minute, func() {
		ct.ComputeValue.Values = nil
		ctm.DeleteTask(taskID)
		logrus.Infof("computer result deleted, task_id:%v", taskID)
	})
}

func (ctm *computerTaskManager) GetOltpResult(taskID int32) any {
	ctm.locker.Lock()
	defer ctm.locker.Unlock()
	task := ctm.tasks[taskID]
	if task == nil {
		return nil
	}
	return task.TpResult.Output
}
