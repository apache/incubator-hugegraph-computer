package schedules

import (
	"errors"
	"vermeer/apps/structure"

	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

/*
* @Description: SchedulerCronManager is the manager for the scheduler cron.
* @Note: This is the manager for the scheduler cron.
 */
type SchedulerCronManager struct {
	cronTasks map[int32][]*structure.TaskInfo // cron expression to TaskInfo. Origin task ID to copied tasks
	crons     map[int32][]*cron.Cron          // cron expression to cron jobs
	// queueTemplateHandler is a function that handles the task queue
	queueTemplateHandler func(*structure.TaskInfo) (int32, error)
}

/*
* @Description: Init initializes the SchedulerCronManager.
* @Note: This function will initialize the SchedulerCronManager.
* @Param queueTemplateHandler
* @Return *SchedulerCronManager
 */
func (t *SchedulerCronManager) Init(queueTemplateHandler func(*structure.TaskInfo) (int32, error)) *SchedulerCronManager {
	t.cronTasks = make(map[int32][]*structure.TaskInfo)
	t.crons = make(map[int32][]*cron.Cron)
	t.queueTemplateHandler = queueTemplateHandler
	return t
}

/*
* @Description: CheckCronExpression checks the cron expression.
* @Note: This function will check the cron expression.
* @Param cronExpr
* @Return error
 */
func (t *SchedulerCronManager) CheckCronExpression(cronExpr string) error {
	if cronExpr == "" {
		return errors.New("cron expression is empty")
	}
	if _, err := cron.ParseStandard(cronExpr); err != nil {
		logrus.Errorf("Failed to parse cron expression: %v", err)
		return errors.New("invalid cron expression: " + err.Error())
	}
	return nil
}

/*
* @Description: AddCronTask adds the cron task.
* @Note: This function will add the cron task.
* @Param taskInfo
* @Return error
 */
func (t *SchedulerCronManager) AddCronTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.CronExpr == "" {
		return errors.New("the property `CronExpr` of taskInfo is empty")
	}

	// add to cron tasks
	cronJob := cron.New()
	_, err := cronJob.AddFunc(taskInfo.CronExpr, func() {
		if taskInfo == nil {
			return
		}

		// CREATE a new task from the original task, using taskbl, it is handled in queueTemplateHandler
		// copy a new taskInfo
		newID, err := t.queueTemplateHandler(taskInfo)
		if err != nil {
			logrus.Errorf("Failed to queue task %d in cron job: %v", taskInfo.ID, err)
			return
		}
		logrus.Infof("Successfully queued task %d from cron job", newID)
	})
	if err != nil {
		logrus.Errorf("Failed to add cron job for task %d: %v", taskInfo.ID, err)
		return err
	}
	t.cronTasks[taskInfo.ID] = append(t.cronTasks[taskInfo.ID], taskInfo)
	t.crons[taskInfo.ID] = append(t.crons[taskInfo.ID], cronJob)
	cronJob.Start()
	logrus.Infof("Added cron task for task ID %d with expression %s", taskInfo.ID, taskInfo.CronExpr)
	return nil
}

/*
* @Description: DeleteTask deletes the cron task.
* @Note: This function will delete the cron task.
* @Param taskID
* @Return error
 */
func (t *SchedulerCronManager) DeleteTask(taskID int32) error {
	if _, exists := t.cronTasks[taskID]; !exists {
		return errors.New("task not found in cron tasks")
	}

	for _, cronJob := range t.crons[taskID] {
		cronJob.Stop()
	}
	delete(t.cronTasks, taskID)
	delete(t.crons, taskID)
	logrus.Infof("Deleted cron task for task ID %d", taskID)
	return nil
}

/*
* @Description: DeleteTaskByGraph deletes the cron task by graph.
* @Note: This function will delete the cron task by graph.
* @Param spaceName
* @Param graphName
* @Return error
 */
func (t *SchedulerCronManager) DeleteTaskByGraph(spaceName, graphName string) error {
	if spaceName == "" || graphName == "" {
		return errors.New("the argument `spaceName` or `graphName` is empty")
	}

	var toDelete []int32
	for taskID, tasks := range t.cronTasks {
		for _, task := range tasks {
			if task.SpaceName == spaceName && task.GraphName == graphName {
				toDelete = append(toDelete, taskID)
				break
			}
		}
	}

	for _, taskID := range toDelete {
		if err := t.DeleteTask(taskID); err != nil {
			logrus.Errorf("Failed to delete cron task for task ID %d: %v", taskID, err)
			return err
		}
	}
	logrus.Infof("Deleted cron tasks for space %s and graph %s", spaceName, graphName)
	return nil
}
