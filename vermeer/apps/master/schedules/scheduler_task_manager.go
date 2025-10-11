package schedules

import (
	"errors"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

/*
* @Description: SchedulerTaskManager is the manager for the scheduler task.
* @Note: This is the manager for the scheduler task.
 */
type SchedulerTaskManager struct {
	structure.MutexLocker
	// This struct is responsible for managing tasks in the scheduling system.
	// A map from task ID to TaskInfo can be used to track tasks.
	allTaskMap   map[int32]*structure.TaskInfo
	allTaskQueue []*structure.TaskInfo
	// For debug or test, get task start sequence
	startTaskQueue []*structure.TaskInfo
	// onGoingTasks
	notCompleteTasks map[int32]*structure.TaskInfo
	// A map from task ID to worker group can be used to track which worker group is handling which task.
	taskToworkerGroupMap map[int32]string
}

/*
* @Description: Init initializes the SchedulerTaskManager.
* @Note: This function will initialize the SchedulerTaskManager.
* @Return *SchedulerTaskManager
 */
func (t *SchedulerTaskManager) Init() *SchedulerTaskManager {
	t.allTaskMap = make(map[int32]*structure.TaskInfo)
	t.notCompleteTasks = make(map[int32]*structure.TaskInfo)
	t.taskToworkerGroupMap = make(map[int32]string)
	return t
}

/*
* @Description: QueueTask queues the task.
* @Note: This function will queue the task.
* @Param taskInfo
* @Return bool, error
 */
func (t *SchedulerTaskManager) QueueTask(taskInfo *structure.TaskInfo) (bool, error) {
	if taskInfo == nil {
		return false, errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.SpaceName == "" {
		return false, errors.New("the property `SpaceName` of taskInfo is empty")
	}

	defer t.Unlock(t.Lock())

	// Add the task to the task map
	t.allTaskMap[taskInfo.ID] = taskInfo
	t.allTaskQueue = append(t.allTaskQueue, taskInfo)
	t.notCompleteTasks[taskInfo.ID] = taskInfo
	t.AssignGroup(taskInfo)
	return true, nil
}

/*
* @Description: RefreshTaskToWorkerGroupMap refreshes the task to worker group map.
* @Note: This function will refresh the task to worker group map.
 */
func (t *SchedulerTaskManager) RefreshTaskToWorkerGroupMap() {
	defer t.Unlock(t.Lock())

	for _, taskInfo := range t.GetAllTasksNotComplete() {
		if taskInfo == nil {
			continue
		}
		t.AssignGroup(taskInfo)
		t.taskToworkerGroupMap[taskInfo.ID] = workerMgr.ApplyGroup(taskInfo.SpaceName, taskInfo.GraphName)
	}
}

// Only for debug or test, get task start sequence
/*
* @Description: AddTaskStartSequence adds the task start sequence.
* @Note: This function will add the task start sequence.
* @Param taskID
* @Return error
 */
func (t *SchedulerTaskManager) AddTaskStartSequence(taskID int32) error {
	if _, exists := t.allTaskMap[taskID]; !exists {
		return errors.New("task not found")
	}
	t.startTaskQueue = append(t.startTaskQueue, t.allTaskMap[taskID])
	return nil
}

/*
* @Description: RemoveTask removes the task.
* @Note: This function will remove the task.
* @Param taskID
* @Return error
 */
func (t *SchedulerTaskManager) RemoveTask(taskID int32) error {
	if _, exists := t.allTaskMap[taskID]; !exists {
		return errors.New("task not found")
	}
	defer t.Unlock(t.Lock())
	delete(t.allTaskMap, taskID)
	// remove from queue
	for i, task := range t.allTaskQueue {
		if task.ID == taskID {
			t.allTaskQueue = append(t.allTaskQueue[:i], t.allTaskQueue[i+1:]...)
			break
		}
	}
	delete(t.taskToworkerGroupMap, taskID)
	delete(t.notCompleteTasks, taskID)
	return nil
}

/*
* @Description: MarkTaskComplete marks the task complete.
* @Note: This function will mark the task complete.
* @Param taskID
* @Return error
 */
func (t *SchedulerTaskManager) MarkTaskComplete(taskID int32) error {
	if _, exists := t.allTaskMap[taskID]; !exists {
		return errors.New("task not found")
	}
	defer t.Unlock(t.Lock())
	delete(t.notCompleteTasks, taskID)
	return nil
}

// update or create a task in the task map
/*
* @Description: AssignGroup assigns the group.
* @Note: This function will assign the group.
* @Param taskInfo
* @Return error
 */
func (t *SchedulerTaskManager) AssignGroup(taskInfo *structure.TaskInfo) error {
	group := workerMgr.ApplyGroup(taskInfo.SpaceName, taskInfo.GraphName)
	if group == "" {
		return errors.New("failed to assign group for task")
	}
	t.taskToworkerGroupMap[taskInfo.ID] = group
	return nil
}

/*
* @Description: GetTaskByID gets the task by ID.
* @Note: This function will get the task by ID.
* @Param taskID
* @Return *structure.TaskInfo, error
 */
func (t *SchedulerTaskManager) GetTaskByID(taskID int32) (*structure.TaskInfo, error) {
	task, exists := t.allTaskMap[taskID]
	if !exists {
		return nil, errors.New("task not found")
	}
	return task, nil
}

/*
* @Description: GetLastTask gets the last task.
* @Note: This function will get the last task.
* @Param spaceName
* @Return *structure.TaskInfo
 */
func (t *SchedulerTaskManager) GetLastTask(spaceName string) *structure.TaskInfo {
	// Implement logic to get the last task in the queue for the given space
	if len(t.allTaskQueue) == 0 {
		return nil
	}
	for i := len(t.allTaskQueue) - 1; i >= 0; i-- {
		if t.allTaskQueue[i].SpaceName == spaceName {
			return t.allTaskQueue[i]
		}
	}
	return nil
}

/*
* @Description: GetAllTasks gets all tasks.
* @Note: This function will get all tasks.
* @Return []*structure.TaskInfo
 */
func (t *SchedulerTaskManager) GetAllTasks() []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(t.allTaskMap))
	for _, task := range t.allTaskMap {
		tasks = append(tasks, task)
	}
	return tasks
}

func (t *SchedulerTaskManager) GetAllTasksNotComplete() []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(t.allTaskMap))
	for _, task := range t.notCompleteTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

func (t *SchedulerTaskManager) GetAllTasksWaitng() []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(t.allTaskMap))
	for _, task := range t.GetAllTasksNotComplete() {
		if task.State == structure.TaskStateWaiting {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (t *SchedulerTaskManager) GetTasksInQueue(space string) []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0)
	for _, task := range t.GetAllTasksNotComplete() {
		if task.SpaceName == space {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// Only for debug or test, get task start sequence
func (t *SchedulerTaskManager) GetTaskStartSequence(queryTasks []int32) []*structure.TaskInfo {
	if len(t.startTaskQueue) == 0 {
		return nil
	}
	if len(queryTasks) == 0 {
		return t.startTaskQueue
	}
	tasks := make([]*structure.TaskInfo, 0, len(queryTasks))
	taskSet := make(map[int32]struct{})
	for _, id := range queryTasks {
		taskSet[id] = struct{}{}
	}
	for _, task := range t.startTaskQueue {
		if _, exists := taskSet[task.ID]; exists {
			tasks = append(tasks, task)
		}
	}
	logrus.Infof("GetTaskStartSequence: return %d tasks", len(tasks))
	for _, task := range tasks {
		logrus.Debugf("TaskID: %d", task.ID)
	}
	return tasks
}

func (t *SchedulerTaskManager) GetTaskToWorkerGroupMap() map[int32]string {
	// Return a copy of the worker group map to avoid external modifications
	taskNotComplete := t.GetAllTasksNotComplete()
	groupMap := make(map[int32]string, len(taskNotComplete))
	for _, task := range taskNotComplete {
		if group, exists := t.taskToworkerGroupMap[task.ID]; exists {
			groupMap[task.ID] = group
		}
	}
	return groupMap
}

func (t *SchedulerTaskManager) IsTaskOngoing(taskID int32) bool {
	// Check if the task is currently ongoing
	task, exists := t.allTaskMap[taskID]
	if !exists {
		return false
	}
	return task.State == structure.TaskStateCreated
}
