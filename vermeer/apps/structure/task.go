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

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"vermeer/apps/common"
	storage "vermeer/apps/storage"

	"github.com/sirupsen/logrus"
)

type TaskWorker struct {
	Name  string    `json:"name,omitempty"`
	State TaskState `json:"state,omitempty"`
}

type TaskInfo struct {
	ID               int32
	State            TaskState
	CreateUser       string
	CreateType       string
	CreateTime       time.Time
	StartTime        time.Time
	UpdateTime       time.Time
	GraphName        string
	SpaceName        string
	Type             string
	Params           map[string]string
	Workers          []*TaskWorker
	ErrMessage       string
	wg               *sync.WaitGroup
	Action           int32
	StatisticsResult map[string]any
}

func (ti *TaskInfo) SetState(state TaskState) {
	ti.State = state
	for _, w := range ti.Workers {
		w.State = state
	}
	ti.UpdateTime = time.Now()
}

func (ti *TaskInfo) SetErrMsg(msg string) {
	ti.ErrMessage = msg
	ti.UpdateTime = time.Now()
}

func (ti *TaskInfo) SetErr(msg string) {
	ti.State = TaskStateError
	ti.ErrMessage = msg
	ti.UpdateTime = time.Now()
}

func (ti *TaskInfo) SetWorkerState(workerName string, state TaskState) {
	ti.UpdateTime = time.Now()
	for _, w := range ti.Workers {
		if w.Name == workerName {
			w.State = state
			break
		}
	}
}

func (ti *TaskInfo) CheckTaskState(state TaskState) bool {
	for _, w := range ti.Workers {
		if w.State != state {
			return false
		}
	}
	return true
}

func (ti *TaskInfo) GetWg() *sync.WaitGroup {
	return ti.wg
}

func (ti *TaskInfo) Equivalent(other *TaskInfo) bool {
	if other == nil {
		return false
	}

	return ti.GraphName == other.GraphName &&
		ti.SpaceName == other.SpaceName &&
		ti.Type == other.Type &&
		ti.CreateUser == other.CreateUser &&
		reflect.DeepEqual(ti.Params, other.Params)
}

// -----------------------TaskManager----------------------------------------

var TaskManager = &taskManager{}

type taskManager struct {
	MutexLocker
	tasks      map[int32]*TaskInfo
	spaceTasks map[string]map[int32]*TaskInfo
	idSeed     int32
	store      storage.Store
}

func (tm *taskManager) Init() {
	tm.tasks = make(map[int32]*TaskInfo)
	tm.spaceTasks = make(map[string]map[int32]*TaskInfo)
	tm.idSeed = 1
}

func (tm *taskManager) CreateTask(spaceName string, taskType string, taskID int32) (*TaskInfo, error) {
	if spaceName == "" {
		return nil, fmt.Errorf("the argument `spaceName` is empty")
	}
	if taskType == "" {
		return nil, fmt.Errorf("the argument `taskType` is empty")
	}

	defer tm.Unlock(tm.Lock())

	task := TaskInfo{}
	task.CreateTime = time.Now()
	task.State = TaskStateCreated
	task.Type = taskType
	task.wg = &sync.WaitGroup{}
	task.Workers = make([]*TaskWorker, 0)
	task.SpaceName = spaceName

	if taskID < 0 {
		task.ID = taskID
		return &task, nil
	} else if taskID == 0 {
		task.ID = tm.idSeed
		tm.idSeed++
	} else {
		task.ID = taskID
		tm.idSeed = taskID + 1
	}

	common.PrometheusMetrics.TaskCnt.WithLabelValues(taskType).Inc()
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(taskType).Inc()

	return &task, nil
}

func (tm *taskManager) appendToSpace(task *TaskInfo) {
	buf := tm.spaceTasks[task.SpaceName]
	if buf == nil {
		buf = make(map[int32]*TaskInfo)
		tm.spaceTasks[task.SpaceName] = buf
	}
	buf[task.ID] = task
}

func (tm *taskManager) deleteToSpace(task *TaskInfo) {
	buf := tm.spaceTasks[task.SpaceName]
	if buf == nil {
		return
	}
	delete(buf, task.ID)
}

// AddTask return false when the task exists.
func (tm *taskManager) AddTask(task *TaskInfo) (ok bool, err error) {
	if task == nil {
		return false, fmt.Errorf("the argument `task` is nil")
	}

	defer tm.Unlock(tm.Lock())

	if _, ok := tm.tasks[task.ID]; ok {
		logrus.Errorf("AddTask error, task exists: %d", task.ID)
		return false, nil
	}
	if tm.idSeed <= task.ID {
		tm.idSeed = task.ID + 1
	}
	if tm.IsFinished(task) {
		return false, nil
	}
	tm.tasks[task.ID] = task
	tm.appendToSpace(task)

	return true, nil
}
func (tm *taskManager) GetAllTasks(limit int) []*TaskInfo {
	defer tm.Unlock(tm.Lock())
	tasks := make([]*TaskInfo, 0, len(tm.tasks))

	var err error
	for taskID := tm.idSeed - 1; taskID > 0; taskID-- {
		if len(tasks) >= limit {
			break
		}
		task, ok := tm.tasks[taskID]
		if !ok && tm.store != nil {
			task, err = tm.readTask(taskID)
			if err != nil {
				logrus.Errorf("GetAllTask read task failed, taskID=%d, err=%v", taskID, err)
			}
		}
		if task != nil {
			tasks = append(tasks, task)
		}
	}
	return SortTaskDesc(tasks)
}

func (tm *taskManager) GetTasks(spaceName string, limit int) []*TaskInfo {
	defer tm.Unlock(tm.Lock())
	buf := tm.spaceTasks[spaceName]

	tasks := make([]*TaskInfo, 0, len(buf))

	var err error
	for taskID := tm.idSeed - 1; taskID > 0; taskID-- {
		if len(tasks) >= limit {
			break
		}
		task, ok := buf[taskID]
		if ok {
			tasks = append(tasks, task)
			continue
		}
		task, ok = tm.tasks[taskID]
		if !ok && tm.store != nil {
			task, err = tm.readTask(taskID)
			if err != nil {
				logrus.Errorf("GetAllTask read task failed, taskID=%d, err=%v", taskID, err)
			}
		}
		if task != nil && task.SpaceName == spaceName {
			tasks = append(tasks, task)
		}
	}
	return SortTaskDesc(tasks)
}

func (tm *taskManager) GetTaskByID(taskID int32) *TaskInfo {
	defer tm.Unlock(tm.Lock())
	task, ok := tm.tasks[taskID]
	if !ok {
		if tm.store != nil {
			// store 不为 nil，代表在master上查询
			taskInfo, err := tm.readTask(taskID)
			if err != nil {
				logrus.Errorf("GetTaskByID read task failed, taskID=%d, err=%v", taskID, err)
				return nil
			}
			return taskInfo
		}
		logrus.Errorf("get task ID:%v not exist", taskID)
		return nil
	}
	return task
}

func (tm *taskManager) GetAllRunningTasks() []*TaskInfo {
	defer tm.Unlock(tm.Lock())
	tasks := make([]*TaskInfo, 0, len(tm.tasks))

	for _, v := range tm.tasks {
		if tm.isRunningTask(v) {
			tasks = append(tasks, v)
		}
	}

	return SortTaskDesc(tasks)
}

func (tm *taskManager) GetAllWaitingTasks() []*TaskInfo {
	defer tm.Unlock(tm.Lock())
	tasks := make([]*TaskInfo, 0, len(tm.tasks))

	for _, v := range tm.tasks {
		if v.State == TaskStateWaiting {
			tasks = append(tasks, v)
		}
	}

	return SortTaskDesc(tasks)
}

func (tm *taskManager) GetTaskRunning(spaceName string) []*TaskInfo {
	defer tm.Unlock(tm.Lock())

	buf := tm.spaceTasks[spaceName]
	tasks := make([]*TaskInfo, 0, len(buf))

	for _, v := range buf {
		if tm.isRunningTask(v) {
			tasks = append(tasks, v)
		}
	}

	return SortTaskDesc(tasks)
}

func (tm *taskManager) isRunningTask(task *TaskInfo) bool {
	return task.State != TaskStateComplete &&
		task.State != TaskStateLoaded &&
		task.State != TaskStateCanceled &&
		task.State != TaskStateError &&
		task.State != TaskStateCreated &&
		task.State != TaskStateWaiting
}

func (tm *taskManager) IsFinished(task *TaskInfo) bool {
	return task.State == TaskStateComplete ||
		task.State == TaskStateLoaded ||
		task.State == TaskStateError ||
		task.State == TaskStateCanceled
}

func (tm *taskManager) FinishTask(taskID int32) error {
	defer tm.Unlock(tm.Lock())
	task, ok := tm.tasks[taskID]
	if !ok {
		logrus.Errorf("get task ID:%v not exist", taskID)
		return fmt.Errorf("get task ID:%v not exist", taskID)
	}
	if !tm.IsFinished(task) {
		logrus.Errorf("task not finished:%v", task.State)
		return fmt.Errorf("task not finished:%v", task.State)
	}
	tm.deleteToSpace(tm.tasks[taskID])
	delete(tm.tasks, taskID)
	return nil
}

func (tm *taskManager) GetTaskByGraph(spaceName, graphName string) []*TaskInfo {
	defer tm.Unlock(tm.Lock())
	tasks := make([]*TaskInfo, 0)
	if tm.spaceTasks[spaceName] == nil {
		return tasks
	}
	for _, taskInfo := range tm.spaceTasks[spaceName] {
		if taskInfo.GraphName == graphName {
			tasks = append(tasks, taskInfo)
		}
	}
	return SortTaskDesc(tasks)
}

func (tm *taskManager) DeleteTask(taskID int32) error {
	defer tm.Unlock(tm.Lock())
	if tm.tasks[taskID] == nil {
		return nil
	}
	tm.deleteToSpace(tm.tasks[taskID])
	delete(tm.tasks, taskID)
	return nil
}

func (tm *taskManager) InitStore() error {
	p, err := common.GetCurrentPath()
	if err != nil {
		logrus.Errorf("get current path error:%v", err)
		return err
	}
	dir := path.Join(p, "vermeer_data", "task_info")
	tm.store, err = storage.StoreMaker(storage.StoreOption{
		StoreName: storage.StoreTypePebble,
		Path:      dir,
		Fsync:     true,
	})
	if err != nil {
		logrus.Errorf("create store error:%v", err)
		return err
	}
	return nil
}

func (tm *taskManager) SaveTask(taskID int32) error {
	defer tm.Unlock(tm.Lock())
	task, ok := tm.tasks[taskID]

	if !ok {
		logrus.Infof("get task ID:%v not exist", taskID)
		return nil
	}

	return tm.doSaveTask(task)
}

func (tm *taskManager) ForceState(task *TaskInfo, state TaskState) bool {
	if task == nil {
		logrus.Error("TaskManager.ForceState: the argument `task` is nil")
		return false
	}
	if state == "" {
		logrus.Error("TaskManager.ForceState: the argument `state` is empty")
		return false
	}

	defer tm.Unlock(tm.Lock())

	task.SetState(state)

	if err := tm.doSaveTask(task); err != nil {
		logrus.Errorf("failed to save task '%d' state '%s', caused by: %v", task.ID, state, err)
		return false
	}

	return true
}

func (tm *taskManager) SetState(task *TaskInfo, state TaskState) error {
	if task == nil {
		return fmt.Errorf("the argument `task` is nil")
	}
	if state == "" {
		return fmt.Errorf("the argument `state` is empty")
	}

	defer tm.Unlock(tm.Lock())

	prevState := task.State
	preTime := task.UpdateTime

	task.SetState(state)

	if err := tm.doSaveTask(task); err != nil {
		task.SetState(prevState)
		task.UpdateTime = preTime
		logrus.Errorf("failed to save task '%d' state '%s', caused by: %v", task.ID, state, err)
		return err
	}

	return nil
}

func (tm *taskManager) SetError(task *TaskInfo, msg string) bool {
	if task == nil {
		logrus.Errorf("SaveError: the argument `task` is nil")
		return false
	}

	defer tm.Unlock(tm.Lock())

	task.SetState(TaskStateError)
	task.ErrMessage = msg

	if err := tm.doSaveTask(task); err != nil {
		logrus.Errorf("failed to save task '%d' error state , caused by: %v", task.ID, err)
		return false
	}

	return true
}

func (tm *taskManager) doSaveTask(task *TaskInfo) error {

	key := strconv.Itoa(int(task.ID))
	bytes, err := json.Marshal(task)

	if err != nil {
		return err
	}
	batch := tm.store.NewBatch()
	err = batch.Set([]byte(key), bytes)
	if err != nil {
		return err
	}
	err = batch.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (tm *taskManager) ReadTask(taskID int32) (*TaskInfo, error) {
	defer tm.Unlock(tm.Lock())
	return tm.readTask(taskID)
}

func (tm *taskManager) readTask(taskID int32) (*TaskInfo, error) {
	key := strconv.Itoa(int(taskID))
	bytes, err := tm.store.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	task := &TaskInfo{}
	err = json.Unmarshal(bytes, task)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (tm *taskManager) ReadAllTask() ([]*TaskInfo, error) {
	defer tm.Unlock(tm.Lock())
	return tm.readAllTask()
}

func (tm *taskManager) readAllTask() ([]*TaskInfo, error) {
	scan := tm.store.Scan()
	tasks := make([]*TaskInfo, 0)
	for kv := range scan {
		task := &TaskInfo{}
		err := json.Unmarshal(kv.Value, task)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

const (
	ActionDoNoting int32 = iota
	ActionCancelTask
	ActionPauseTask
	ActionResumeTask
)

func (tm *taskManager) SetAction(task *TaskInfo, action int32) error {
	if task == nil {
		return fmt.Errorf("the argument `task` is nil")
	}
	if action < 1 || action > 3 {
		return fmt.Errorf("invalid action type")
	}
	if task.Action == ActionCancelTask {
		return fmt.Errorf("the action is `cancel`, cannot changed")
	}
	defer tm.Unlock(tm.Lock())
	atomic.StoreInt32(&task.Action, action)
	return nil
}

func SortTaskDesc(tasks []*TaskInfo) []*TaskInfo {
	if tasks == nil {
		return nil
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID > tasks[j].ID
	})

	return tasks
}
