package schedules

import (
	"slices"
	"sort"
	"strconv"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

/*
* @Description: SchedulerAlgorithm is the interface for the scheduler algorithm.
* @Note: This is the interface for the scheduler algorithm.
 */
type SchedulerAlgorithm interface {
	// Name returns the name of the SchedulerAlgorithm
	Name() string
	// Init initializes the SchedulerAlgorithm
	Init()
	// FilterNextTasks filters the next tasks to be scheduled based on the provided parameters
	FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error)
	// ScheduleNextTasks schedules the next tasks based on the filtered tasks
	ScheduleNextTasks(filteredTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error)
}

/*
* @Description: SchedulerAlgorithmManager is the manager for the scheduler algorithm.
* @Note: This is the manager for the scheduler algorithm.
 */
type SchedulerAlgorithmManager struct {
	filteredSchedulerAlgorithms  map[string]SchedulerAlgorithm
	scheduledSchedulerAlgorithms map[string]SchedulerAlgorithm
	dispatchPaused               bool
}

/*
* @Description: Init initializes the SchedulerAlgorithmManager.
* @Note: This function will initialize the SchedulerAlgorithmManager.
 */
// Need to put DependsSchedulerAlgorithm before WaitingSchedulerAlgorithm
func (am *SchedulerAlgorithmManager) Init() {
	am.filteredSchedulerAlgorithms = make(map[string]SchedulerAlgorithm)
	am.scheduledSchedulerAlgorithms = make(map[string]SchedulerAlgorithm)
	am.dispatchPaused = false
	// Register filter and schedule algorithms
	am.RegisterFilterAlgorithm(&DependsSchedulerAlgorithm{})
	am.RegisterFilterAlgorithm(&WaitingSchedulerAlgorithm{})
	// Register default SchedulerAlgorithms
	am.RegisterSchedulerAlgorithm(&PriorityElderSchedulerAlgorithm{})
}

/*
* @Description: RegisterSchedulerAlgorithm registers the scheduler algorithm.
* @Note: This function will register the scheduler algorithm.
* @Param schedulerAlgorithm
 */
func (am *SchedulerAlgorithmManager) RegisterSchedulerAlgorithm(schedulerAlgorithm SchedulerAlgorithm) {
	if schedulerAlgorithm == nil {
		return
	}
	name := schedulerAlgorithm.Name()
	if _, exists := am.scheduledSchedulerAlgorithms[name]; exists {
		return // SchedulerAlgorithm already registered
	}

	// only support one scheduling algorithm for now
	if len(am.scheduledSchedulerAlgorithms) > 0 {
		return // Only one scheduling algorithm can be registered
	}
	schedulerAlgorithm.Init()
	am.scheduledSchedulerAlgorithms[name] = schedulerAlgorithm
}

/*
* @Description: RegisterFilterAlgorithm registers the filter algorithm.
* @Note: This function will register the filter algorithm.
* @Param filterAlgorithm
 */
func (am *SchedulerAlgorithmManager) RegisterFilterAlgorithm(filterAlgorithm SchedulerAlgorithm) {
	if filterAlgorithm == nil {
		return
	}
	name := filterAlgorithm.Name()
	if _, exists := am.filteredSchedulerAlgorithms[name]; exists {
		return // SchedulerAlgorithm already registered
	}
	filterAlgorithm.Init()
	am.filteredSchedulerAlgorithms[name] = filterAlgorithm
}

/*
* @Description: IsDispatchPaused checks if the dispatch is paused.
* @Note: This function will check if the dispatch is paused.
* @Return bool
 */
func (am *SchedulerAlgorithmManager) IsDispatchPaused() bool {
	return am.dispatchPaused
}

/*
* @Description: PauseDispatch pauses the dispatch.
* @Note: This function will pause the dispatch.
 */
func (am *SchedulerAlgorithmManager) PauseDispatch() {
	am.dispatchPaused = true
}

/*
* @Description: ResumeDispatch resumes the dispatch.
* @Note: This function will resume the dispatch.
 */
func (am *SchedulerAlgorithmManager) ResumeDispatch() {
	am.dispatchPaused = false
}

/*
* @Description: ScheduleNextTasks schedules the next tasks.
* @Note: This function will schedule the next tasks.
* @Param allTasks
* @Param taskToWorkerGroupMap
* @Param idleWorkerGroups
* @Param concurrentWorkerGroups
* @Param softSchedule
* @Return []*structure.TaskInfo, error
 */
// For all tasks, filter and schedule them
// Only one scheduling algorithm is supported for now
func (am *SchedulerAlgorithmManager) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if am.dispatchPaused {
		return nil, nil // No tasks to schedule if dispatch is paused
	}

	filteredTasks := allTasks
	for _, algorithm := range am.filteredSchedulerAlgorithms {
		var err error
		filteredTasks, err = algorithm.FilterNextTasks(filteredTasks, taskToWorkerGroupMap, idleWorkerGroups, concurrentWorkerGroups, softSchedule)
		if err != nil {
			return nil, err
		}
	}
	if len(filteredTasks) == 0 {
		return nil, nil // No tasks to schedule after filtering
	}

	// only support one scheduling algorithm for now
	// get first algorithm
	for _, algorithm := range am.scheduledSchedulerAlgorithms {
		tasks, err := algorithm.ScheduleNextTasks(filteredTasks, taskToWorkerGroupMap, idleWorkerGroups, concurrentWorkerGroups, softSchedule)
		if err != nil {
			return nil, err
		}
		return tasks, nil // Return the scheduled tasks
	}

	return nil, nil // No tasks scheduled
}

type FIFOSchedulerAlgorithm struct{}

func (f *FIFOSchedulerAlgorithm) Name() string {
	return "FIFO"
}

func (f *FIFOSchedulerAlgorithm) Init() {
	// No specific initialization needed for FIFO
	logrus.Info("Initializing FIFOSchedulerAlgorithm")
}

func (f *FIFOSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	// just return the waiting tasks as is for FIFO
	return allTasks, nil
}

func (f *FIFOSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// For FIFO, we simply return the available tasks in the order they are provided
	for _, task := range allTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			// only support idle worker groups for now
			for _, idleGroup := range idleWorkerGroups {
				if group == idleGroup {
					logrus.Debugf("Task %d is assigned to worker group %s", task.ID, group)
					return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
				}
			}
		}
	}

	return nil, nil
}

type PrioritySchedulerAlgorithm struct{}

func (p *PrioritySchedulerAlgorithm) Name() string {
	return "Priority"
}

func (p *PrioritySchedulerAlgorithm) Init() {
	// No specific initialization needed for Priority
	logrus.Info("Initializing PrioritySchedulerAlgorithm")
}

func (p *PrioritySchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	// just return the waiting tasks as is for Priority
	return allTasks, nil
}

func (p *PrioritySchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// Sort tasks by priority (higher priority first)
	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].Priority > allTasks[j].Priority
	})

	for _, task := range allTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			// only support idle worker groups for now
			for _, idleGroup := range idleWorkerGroups {
				if group == idleGroup {
					logrus.Debugf("Task %d is assigned to worker group %s", task.ID, group)
					return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
				}
			}
		}
	}

	return nil, nil
}

type PriorityElderSchedulerAlgorithm struct {
	ageParam         int64
	priorityParam    int64
	resourceParam    int64
	randomValueParam int64
}

func (p *PriorityElderSchedulerAlgorithm) Name() string {
	return "PriorityElder"
}

func (p *PriorityElderSchedulerAlgorithm) Init() {
	logrus.Info("Initializing PriorityElderSchedulerAlgorithm")

	// Initialize parameters with default values
	defaultAgeParam := "1"
	defaultPriorityParam := "1"
	defaultResourceParam := "10000000000"
	defaultRandomValueParam := "1" // Placeholder for any random value logic

	// Load parameters from configuration
	ageParam := common.GetConfigDefault("priority_elder_age_param", defaultAgeParam).(string)
	priorityParam := common.GetConfigDefault("priority_elder_priority_param", defaultPriorityParam).(string)
	resourceParam := common.GetConfigDefault("priority_elder_resource_param", defaultResourceParam).(string)
	randomValueParam := common.GetConfigDefault("priority_elder_random_value_param", defaultRandomValueParam).(string)

	ageParamInt, err := strconv.Atoi(ageParam)
	if err != nil {
		logrus.Errorf("failed to convert priority_elder_age_param to int: %v", err)
		logrus.Infof("using default priority_elder_age_param: %s", defaultAgeParam)
		ageParamInt, _ = strconv.Atoi(defaultAgeParam)
	}
	p.ageParam = int64(ageParamInt)
	priorityParamInt, err := strconv.Atoi(priorityParam)
	if err != nil {
		logrus.Errorf("failed to convert priority_elder_priority_param to int: %v", err)
		logrus.Infof("using default priority_elder_priority_param: %s", defaultPriorityParam)
		priorityParamInt, _ = strconv.Atoi(defaultPriorityParam)
	}
	p.priorityParam = int64(priorityParamInt)
	resourceParamInt, err := strconv.Atoi(resourceParam)
	if err != nil {
		logrus.Errorf("failed to convert priority_elder_resource_param to int: %v", err)
		logrus.Infof("using default priority_elder_resource_param: %s", defaultResourceParam)
		resourceParamInt, _ = strconv.Atoi(defaultResourceParam)
	}
	p.resourceParam = int64(resourceParamInt)
	randomValueParamInt, err := strconv.Atoi(randomValueParam)
	if err != nil {
		logrus.Errorf("failed to convert priority_elder_random_value_param to int: %v", err)
		logrus.Infof("using default priority_elder_random_value_param: %s", defaultRandomValueParam)
		randomValueParamInt, _ = strconv.Atoi(defaultRandomValueParam)
	}
	p.randomValueParam = int64(randomValueParamInt)

	logrus.Infof("PriorityElderSchedulerAlgorithm initialized with parameters: ageParam=%d, priorityParam=%d, resourceParam=%d, randomValueParam=%d", p.ageParam, p.priorityParam, p.resourceParam, p.randomValueParam)
}

func (p *PriorityElderSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	// just return the waiting tasks as is for PriorityElder
	return allTasks, nil
}

func (p *PriorityElderSchedulerAlgorithm) CalculateTaskEmergency(task *structure.TaskInfo, taskToWorkerGroupMap map[int32]string, printValue bool) int64 {
	// step 0: get params
	ageParam := p.ageParam
	priorityParam := p.priorityParam
	resourceParam := p.resourceParam
	randomValueParam := p.randomValueParam
	// step 1: age
	ageCost := ageParam * time.Since(task.CreateTime).Milliseconds() / 1000 // in seconds
	// step 2: priority
	priorityCost := priorityParam * int64(task.Priority)
	// step 3: resource cost
	graph := structure.GraphManager.GetGraphByName(task.SpaceName, task.GraphName)
	resourceCost := int64(0)
	if graph == nil {
		resourceCost = resourceParam // if graph not found, use max resource cost
	} else {
		resourceCost = resourceParam / max(1, graph.VertexCount+graph.EdgeCount) // Avoid division by zero, ensure at least 1
	}
	// step 4: some random value
	randomValue := int64(randomValueParam) // Placeholder for any random value logic
	if printValue {
		logrus.Debugf("Task %d: Age Cost: %d, Priority Cost: %d, Resource Cost: %d, Random Value: %d", task.ID, ageCost, priorityCost, resourceCost, randomValue)
	}
	return ageCost + priorityCost + resourceCost + randomValue
}

func (p *PriorityElderSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// calculate emergency value for each task
	taskEmergencies := make(map[int32]int64)
	for _, task := range allTasks {
		taskEmergencies[task.ID] = p.CalculateTaskEmergency(task, taskToWorkerGroupMap, false)
	}

	// Sort tasks by priority (higher priority first)
	sort.Slice(allTasks, func(i, j int) bool {
		return taskEmergencies[allTasks[i].ID] > taskEmergencies[allTasks[j].ID]
	})

	for _, task := range allTasks {
		logrus.Debugf("Task %d: Emergency Value: %d", task.ID, taskEmergencies[task.ID])
	}

	for _, task := range allTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			for _, idleGroup := range idleWorkerGroups {
				if group == idleGroup {
					logrus.Debugf("Task %d is assigned to worker group %s", task.ID, group)
					return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
				}
			}
			// if allow concurrent running, check if the group is in concurrent worker groups
			if !task.Exclusive {
				for _, concurrentGroup := range concurrentWorkerGroups {
					if group == concurrentGroup {
						logrus.Debugf("Task %d is assigned to concurrent worker group %s", task.ID, group)
						return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
					}
				}
			}
		}
	}

	return nil, nil
}

type WaitingSchedulerAlgorithm struct{}

func (w *WaitingSchedulerAlgorithm) Name() string {
	return "Waiting"
}

func (w *WaitingSchedulerAlgorithm) Init() {
	// No specific initialization needed for Waiting
	logrus.Info("Initializing WaitingSchedulerAlgorithm")
}

func (w *WaitingSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	waitingTasks := make([]*structure.TaskInfo, 0)
	for _, task := range allTasks {
		if task.State == structure.TaskStateWaiting {
			waitingTasks = append(waitingTasks, task)
		}
	}
	return waitingTasks, nil
}

func (w *WaitingSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	waitingTasks, err := w.FilterNextTasks(allTasks, taskToWorkerGroupMap, idleWorkerGroups, concurrentWorkerGroups, softSchedule)
	if err != nil {
		return nil, err
	}
	if len(waitingTasks) == 0 {
		return nil, nil
	}
	for _, task := range waitingTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			// only support idle worker groups for now
			for _, idleGroup := range idleWorkerGroups {
				if group == idleGroup {
					logrus.Debugf("Task %d is assigned to worker group %s", task.ID, group)
					return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
				}
			}
		}
	}
	return nil, nil // No tasks scheduled
}

type DependsSchedulerAlgorithm struct{}

func (d *DependsSchedulerAlgorithm) Name() string {
	return "Depends"
}

func (d *DependsSchedulerAlgorithm) Init() {
	// No specific initialization needed for Depends
	logrus.Info("Initializing DependsSchedulerAlgorithm")
}

func (d *DependsSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].ID < allTasks[j].ID
	})

	taskIDs := make(map[int32]*structure.TaskInfo)
	for _, task := range allTasks {
		taskIDs[task.ID] = task
	}

	filteredTasks := make([]*structure.TaskInfo, 0)
	for _, task := range allTasks {
		depends := task.Preorders
		// Check if all dependencies are satisfied
		allDepsSatisfied := true
		for _, dep := range depends {
			if depTask, exists := taskIDs[dep]; exists && depTask.State != structure.TaskStateComplete {
				allDepsSatisfied = false
				break
			}
		}
		if allDepsSatisfied {
			if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
				filteredTasks = append(filteredTasks, task) // Add to filtered tasks if dependencies are satisfied
			}
		}
	}
	return filteredTasks, nil
}

func (d *DependsSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkerGroups []string, concurrentWorkerGroups []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].ID < allTasks[j].ID
	})

	allTaskIDs := make(map[int32]*structure.TaskInfo)
	for _, task := range allTasks {
		allTaskIDs[task.ID] = task
	}

	for _, task := range allTasks {
		depends := task.Preorders
		// Check if all dependencies are satisfied
		allDepsSatisfied := true
		for _, dep := range depends {
			if depTask, exists := allTaskIDs[dep]; exists && depTask.State != structure.TaskStateComplete {
				allDepsSatisfied = false
				break
			}
		}
		if allDepsSatisfied {
			if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
				// only support idle worker groups for now
				if slices.Contains(idleWorkerGroups, group) {
					logrus.Debugf("Task %d is assigned to worker group %s", task.ID, group)
					return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
				}
			}
		}
	}

	return nil, nil
}
