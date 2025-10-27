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
	"errors"
	"fmt"
	"strconv"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/master/schedules"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

/*
* @Description: ScheduleBl is the scheduler business logic.
* @Note: This is the main scheduler business logic.
 */
type ScheduleBl struct {
	structure.MutexLocker
	// resource management
	resourceManager *schedules.SchedulerResourceManager
	// algorithm management
	algorithmManager *schedules.SchedulerAlgorithmManager
	// task management
	taskManager *schedules.SchedulerTaskManager
	// cron management
	cronManager *schedules.SchedulerCronManager
	// start channel for tasks to be started
	startChan chan *structure.TaskInfo
	// configurations
	startChanSize  int
	tickerInterval int
	softSchedule   bool
}

/*
* @Description: Init initializes the ScheduleBl.
* @Note: This function will initialize the ScheduleBl.
 */
func (s *ScheduleBl) Init() {
	logrus.Info("Initializing ScheduleBl...")
	s.LoadConfig()
	startChan := make(chan *structure.TaskInfo, s.startChanSize)
	s.startChan = startChan

	s.resourceManager = &schedules.SchedulerResourceManager{}
	s.resourceManager.Init()
	s.taskManager = &schedules.SchedulerTaskManager{}
	s.taskManager.Init()
	s.algorithmManager = &schedules.SchedulerAlgorithmManager{}
	s.algorithmManager.Init()
	s.cronManager = &schedules.SchedulerCronManager{}
	s.cronManager.Init(s.QueueTaskFromTemplate)
	go s.startTicker()
	go s.waitingStartedTask()
}

/*
* @Description: LoadConfig loads the configuration from the common package.
* @Note: This function will load the configuration from the common package.
 */
func (s *ScheduleBl) LoadConfig() {
	// Load configuration from common package

	// startChanSize
	const defaultChanSizeConfig = "10"
	chanSize := common.GetConfigDefault("start_chan_size", defaultChanSizeConfig).(string)
	// Convert string to int
	chanSizeInt, err := strconv.Atoi(chanSize)
	if err != nil {
		logrus.Errorf("failed to convert start_chan_size to int: %v", err)
		logrus.Infof("using default start_chan_size: %s", defaultChanSizeConfig)
		chanSizeInt, _ = strconv.Atoi(defaultChanSizeConfig)
	}
	s.startChanSize = chanSizeInt

	// tickerInterval
	const defaultTickerInterval = "3"
	tickerInterval := common.GetConfigDefault("ticker_interval", defaultTickerInterval).(string)
	tickerIntervalInt, err := strconv.Atoi(tickerInterval)
	if err != nil {
		logrus.Errorf("failed to convert ticker_interval to int: %v", err)
		logrus.Infof("using default ticker_interval: %s", defaultTickerInterval)
		tickerIntervalInt, _ = strconv.Atoi(defaultTickerInterval)
	}
	s.tickerInterval = tickerIntervalInt

	// softSchedule
	softSchedule := common.GetConfigDefault("soft_schedule", "true").(string)
	if softSchedule == "true" {
		s.softSchedule = true
	} else {
		s.softSchedule = false
	}

	logrus.Infof("ScheduleBl configuration: startChanSize=%d, tickerInterval=%d, softSchedule=%v",
		s.startChanSize, s.tickerInterval, s.softSchedule)
}

/*
* @Description: startTicker starts the ticker.
* @Note: This function will start the ticker.
 */
func (s *ScheduleBl) startTicker() {
	// Create a ticker with the specified interval
	ticker := time.Tick(time.Duration(s.tickerInterval) * time.Second)

	for range ticker {
		logrus.Debug("Ticker ticked")
		s.TryScheduleNextTasks()
	}
}

// this make scheduler manager try to schedule next tasks
/*
* @Description: TryScheduleNextTasks tries to schedule the next tasks.
* @Note: This function will try to schedule the next tasks.
* @Param noLock
 */
func (s *ScheduleBl) TryScheduleNextTasks(noLock ...bool) {
	defer func() {
		if err := recover(); err != nil {
			logrus.Errorln("TryScheduleNextTasks() has been recovered:", err)
		}
	}()

	if err := s.tryScheduleInner(s.softSchedule, noLock...); err != nil {
		logrus.Errorf("do scheduling error:%v", err)
	}
}

// Main routine to schedule tasks
/*
* @Description: tryScheduleInner tries to schedule the next tasks.
* @Note: This function will try to schedule the next tasks.
* @Param softSchedule
* @Param noLock
 */
func (s *ScheduleBl) tryScheduleInner(softSchedule bool, noLock ...bool) error {
	// Implement logic to get the next task in the queue for the given space
	if !(len(noLock) > 0 && noLock[0]) {
		defer s.Unlock(s.Lock())
	}

	// step 1: make sure all tasks have alloc to a worker group
	// This is done by the TaskManager, which assigns a worker group to each task
	s.taskManager.RefreshTaskToWorkerGroupMap()

	// step 2: get available resources and tasks
	logrus.Debugf("scheduling next tasks, softSchedule: %v", softSchedule)
	idleWorkerGroups := s.resourceManager.GetIdleWorkerGroups()
	concurrentWorkerGroups := s.resourceManager.GetConcurrentWorkerGroups()
	allTasks := s.taskManager.GetAllTasksNotComplete()
	if len(allTasks) == 0 || (len(idleWorkerGroups) == 0 && len(concurrentWorkerGroups) == 0) {
		logrus.Debugf("no available tasks or workerGroups, allTasks: %d, workerGroups: %d/%d",
			len(allTasks), len(idleWorkerGroups), len(concurrentWorkerGroups))
		return nil
	}
	logrus.Debugf("all tasks: %d, workerGroups: %d/%d", len(allTasks), len(idleWorkerGroups), len(concurrentWorkerGroups))

	// TODO: NEED TO JUDGE IF THE TASK CAN CONCURRENTLY RUNNING
	// NOT only by user setting, but also by scheduler setting

	// step 3: return the task with the highest priority or small tasks which can be executed immediately
	taskToWorkerGroupMap := s.taskManager.GetTaskToWorkerGroupMap()
	nextTasks, err := s.algorithmManager.ScheduleNextTasks(allTasks, taskToWorkerGroupMap, idleWorkerGroups, concurrentWorkerGroups, softSchedule)
	if err != nil {
		logrus.Errorf("failed to schedule next tasks: %v", err)
		return err
	}
	logrus.Debugf("scheduled %d tasks", len(nextTasks))
	// step 4: send to start channel
	for _, task := range nextTasks {
		if task == nil {
			logrus.Warnf("received a nil task from algorithm manager")
			continue
		}
		if task.State != structure.TaskStateWaiting {
			logrus.Warnf("task '%d' is not in waiting state, current state: %s", task.ID, task.State)
			continue
		}
		logrus.Infof("scheduling task '%d' with type '%s' to start channel", task.ID, task.Type)
		select {
		case s.startChan <- task:
			logrus.Infof("task '%d' sent to start channel", task.ID)
		default:
			errMsg := fmt.Sprintf("start channel is full, cannot schedule task %d", task.ID)
			logrus.Errorf(errMsg)
			taskMgr.SetError(task, errMsg)
		}
	}

	return nil
}

// QueueTask Add the task to the inner queue.
// If the task exists, return false.
/*
* @Description: QueueTask queues the task.
* @Note: This function will queue the task.
* @Param taskInfo
* @Return bool, error
 */
func (s *ScheduleBl) QueueTask(taskInfo *structure.TaskInfo) (bool, error) {
	if taskInfo == nil {
		return false, errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.SpaceName == "" {
		return false, errors.New("the property `SpaceName` of taskInfo is empty")
	}

	defer s.Unlock(s.Lock())
	if err := taskMgr.SetState(taskInfo, structure.TaskStateWaiting); err != nil {
		return false, err
	}

	logrus.Debugf("queuing task %d with parameters: %+v", taskInfo.ID, taskInfo)

	// check dependency if exists
	if len(taskInfo.Preorders) > 0 {
		for _, depTaskID := range taskInfo.Preorders {
			depTask := taskMgr.GetTaskByID(depTaskID)
			if depTask == nil {
				err := errors.New("the dependency task with ID " + strconv.Itoa(int(depTaskID)) + " does not exist")
				logrus.Error(err)
				taskMgr.SetError(taskInfo, err.Error())
				return false, err
			}
		}
	}

	// Notice: Ensure successful invocation.
	// make sure all tasks have alloc to a worker group
	ok, err := s.taskManager.QueueTask(taskInfo)
	if err != nil {
		taskMgr.SetError(taskInfo, err.Error())
		return ok, err
	}

	if s.cronManager.CheckCronExpression(taskInfo.CronExpr) == nil {
		if err := s.cronManager.AddCronTask(taskInfo); err != nil {
			logrus.Errorf("failed to add cron task: %v", err)
			return false, err
		}
		logrus.Infof("added cron task for task '%d' with expression '%s'", taskInfo.ID, taskInfo.CronExpr)
	}

	return ok, nil
}

/*
* @Description: QueueTaskFromTemplate queues the task from the template.
* @Note: This function will queue the task from the template. This function is used by cron tasks.
* @Param template
* @Return int32, error
 */
func (s *ScheduleBl) QueueTaskFromTemplate(template *structure.TaskInfo) (int32, error) {
	if template == nil {
		return -1, errors.New("the argument `template` is nil")
	}

	bc := &baseCreator{}
	taskInfo, err := bc.CopyTaskInfo(template)
	if err != nil {
		logrus.Errorf("failed to copy task info from template, template ID: %d, caused by: %v", template.ID, err)
		return -1, err
	}
	bc.saveTaskInfo(taskInfo)

	ok, err := s.QueueTask(taskInfo)
	if err != nil || !ok {
		logrus.Errorf("failed to queue task from template, template ID: %d, caused by: %v", template.ID, err)
		return -1, err
	}

	logrus.Infof("queued task '%d' from template '%d'", taskInfo.ID, template.ID)

	return taskInfo.ID, nil
}

/*
* @Description: BatchQueueTask batches the task.
* @Note: This function will batch the task.
* @Param taskInfos
* @Return []bool, []error
 */
func (s *ScheduleBl) BatchQueueTask(taskInfos []*structure.TaskInfo) ([]bool, []error) {
	if len(taskInfos) == 0 {
		return []bool{}, []error{}
	}

	s.PauseDispatch()

	defer s.ResumeDispatch()
	// defer s.Unlock(s.Lock())

	errors := make([]error, len(taskInfos))
	oks := make([]bool, len(taskInfos))

	for _, taskInfo := range taskInfos {
		ok, err := s.QueueTask(taskInfo)
		if err != nil {
			logrus.Errorf("failed to queue task '%d': %v", taskInfo.ID, err)
		}
		errors = append(errors, err)
		oks = append(oks, ok)
	}

	return oks, errors
}

// ******** CloseCurrent ********
func (s *ScheduleBl) CloseCurrent(taskId int32, removeWorkerName ...string) error {
	defer s.Unlock(s.Lock())

	// trace tasks need these workers, check if these tasks are available
	s.taskManager.RemoveTask(taskId)
	// release the worker group
	s.resourceManager.ReleaseByTaskID(taskId)

	if len(removeWorkerName) > 0 {
		// stop the cron job if exists when need remove worker, otherwise the task is just closed normally
		s.cronManager.DeleteTask(taskId)
		// remove the worker from resource manager
		workerName := removeWorkerName[0]
		if workerName == "" {
			return errors.New("the argument `removeWorkerName` is empty")
		}
		logrus.Infof("removing worker '%s' from resource manager", workerName)
		s.ChangeWorkerStatus(workerName, schedules.WorkerOngoingStatusDeleted)
	}

	logrus.Infof("invoke dispatch when task '%d' is closed", taskId)
	s.TryScheduleNextTasks(true)
	return nil
}

// This will be called when a worker is offline.
// This will be called when a worker is online.
func (s *ScheduleBl) ChangeWorkerStatus(workerName string, status schedules.WorkerOngoingStatus) (bool, error) {
	defer s.Unlock(s.Lock())
	s.resourceManager.ChangeWorkerStatus(workerName, status)

	logrus.Infof("worker '%s' status changed to '%s'", workerName, status)
	// After changing the worker status, we may need to reschedule tasks
	s.TryScheduleNextTasks(true)

	return true, nil
}

// ******** START TASK ********
func (s *ScheduleBl) waitingStartedTask() {
	for taskInfo := range s.startChan {
		if taskInfo == nil {
			logrus.Warnf("received a nil task from startChan")
			continue
		}

		logrus.Infof("chan received task '%d' to start", taskInfo.ID)
		s.handleStartTask(taskInfo)
	}
}

// now, start task!
func (s *ScheduleBl) handleStartTask(taskInfo *structure.TaskInfo) {
	agent, status, err := s.resourceManager.GetAgentAndAssignTask(taskInfo)

	if err != nil {
		logrus.Errorf("apply agent error: %v", err)
		taskMgr.SetError(taskInfo, err.Error())
		return
	}

	switch status {
	case schedules.AgentStatusNoWorker:
		fallthrough
	case schedules.AgentStatusWorkerNotReady:
		logrus.Warnf("failed to apply an agent for task '%d', graph: %s/%s, status: %s",
			taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName, status)
		return
	}

	if agent == nil {
		logrus.Infof("no available agent for task '%d', graph: %s/%s, status: %s",
			taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName, status)
		return
	}

	logrus.Infof("got an agent '%s' for task '%d', graph: %s/%s",
		agent.GroupName(), taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName)

	go s.startWaitingTask(agent, taskInfo)
}

func (s *ScheduleBl) startWaitingTask(agent *schedules.Agent, taskInfo *structure.TaskInfo) {
	logrus.Infof("starting a task, id: %v, type: %v, graph: %v", taskInfo.ID, taskInfo.Type, taskInfo.GraphName)

	defer func() {
		if err := recover(); err != nil {
			logrus.Errorln("startWaitingTask() has been recovered:", err)
		}
	}()

	// TODO: Is here need a lock? TOCTTOU
	if taskInfo.State != structure.TaskStateWaiting {
		logrus.Errorf("task state is not in 'Waiting' state, taskID: %v", taskInfo)
		return
	}

	err := taskMgr.SetState(taskInfo, structure.TaskStateCreated)
	if err != nil {
		logrus.Errorf("set taskInfo to %s error:%v", structure.TaskStateCreated, err)
		return
	}

	taskStarter, err := NewTaskStarter(taskInfo, agent.GroupName())
	if err != nil {
		logrus.Errorf("failed to construct a TaskStarter with task type: %s, taskID: %d, caused by: %v", taskInfo.Type, taskInfo.ID, err)
		taskMgr.SetError(taskInfo, err.Error())
		return
	}

	taskInfo.StartTime = time.Now()
	err = taskStarter.StartTask()

	// only for test or debug, record the task start sequence
	if err := s.taskManager.AddTaskStartSequence(taskInfo.ID); err != nil {
		logrus.Errorf("failed to add task '%d' to start sequence: %v", taskInfo.ID, err)
	}

	if err != nil {
		logrus.Errorf("failed to start a task, type: %s, taskID: %d, caused by: %v", taskInfo.Type, taskInfo.ID, err)
		taskMgr.SetError(taskInfo, err.Error())
	}
}

// ********* CANCEL TASK ********
// handle cancel task
// need to cancel cron task
func (s *ScheduleBl) CancelTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	defer s.Unlock(s.Lock())

	isHeadTask := s.taskManager.IsTaskOngoing(taskInfo.ID)
	task := s.taskManager.RemoveTask(taskInfo.ID)
	s.cronManager.DeleteTask(taskInfo.ID)
	// err := s.taskManager.CancelTask(taskInfo)
	isInQueue := false
	if task != nil {
		logrus.Infof("removed task '%d' from space queue", taskInfo.ID)
		isInQueue = true
	}

	if isInQueue && !isHeadTask {
		if err := taskMgr.SetState(taskInfo, structure.TaskStateCanceled); err != nil {
			return err
		}

		logrus.Infof("set task '%d' to TaskStateCanceled", taskInfo.ID)
	} else {
		logrus.Infof("sending task '%d' to task canceler", taskInfo.ID)
		return s.handleCancelTask(taskInfo)
	}

	return nil
}

func (s *ScheduleBl) handleCancelTask(taskInfo *structure.TaskInfo) error {
	logrus.Infof("received task '%d' to cancel", taskInfo.ID)
	canceler, err := NewTaskCanceler(taskInfo)
	if err != nil {
		logrus.Errorf("failed to create new TaskCanceler err: %v", err)
		taskMgr.SetError(taskInfo, err.Error())
		return err
	}

	if err := canceler.CancelTask(); err != nil {
		logrus.Errorf("failed to cancel task '%d', caused by: %v", taskInfo.ID, err)
		taskMgr.SetError(taskInfo, err.Error())
		return err
	}

	// set worker state to idle or concurrent running
	s.resourceManager.ReleaseByTaskID(taskInfo.ID)

	return nil
}

func (s *ScheduleBl) CancelCronTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	s.cronManager.DeleteTask(taskInfo.ID)

	return nil
}

// ** Other Methods **

func (s *ScheduleBl) PeekSpaceTail(space string) *structure.TaskInfo {
	return s.taskManager.GetLastTask(space)
}

func (s *ScheduleBl) IsDispatchPaused() bool {
	// Implement logic to check if dispatching is paused
	return s.algorithmManager.IsDispatchPaused()
}

func (s *ScheduleBl) PauseDispatch() {
	// Implement logic to pause dispatching
	s.algorithmManager.PauseDispatch()
}

func (s *ScheduleBl) ResumeDispatch() {
	// Implement logic to resume dispatching
	s.algorithmManager.ResumeDispatch()
}

func (s *ScheduleBl) AllTasksInQueue() []*structure.TaskInfo {
	// Implement logic to get all tasks in the queue
	return s.taskManager.GetAllTasks()
}

func (s *ScheduleBl) TasksInQueue(space string) []*structure.TaskInfo {
	// Implement logic to get tasks in the queue for a specific space
	return s.taskManager.GetTasksInQueue(space)
}

func (s *ScheduleBl) TaskStartSequence(queryTasks []int32) []*structure.TaskInfo {
	// Only for debug or test, get task start sequence
	return s.taskManager.GetTaskStartSequence(queryTasks)
}
