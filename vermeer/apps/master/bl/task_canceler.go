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
	"context"
	"fmt"
	"sync"
	"time"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type TaskCanceler interface {
	CancelTask() error
}

func NewTaskCanceler(taskInfo *structure.TaskInfo) (TaskCanceler, error) {
	if taskInfo == nil {
		return nil, fmt.Errorf("the argument `taskInfo` should not be nil")
	}

	base := baseCanceler{task: taskInfo}
	switch taskInfo.Type {
	case structure.TaskTypeLoad:
		return &loadTaskCanceler{base}, nil
	case structure.TaskTypeCompute:
		return &computeTaskCanceler{base}, nil
	case structure.TaskTypeReload:
		return &loadTaskCanceler{base}, nil
	default:
		return nil, fmt.Errorf("taskType must be set to load/compute/reload, get taskType:%v", taskInfo.Type)
	}
}

type baseCanceler struct {
	TaskCanceler
	task *structure.TaskInfo
}

type loadTaskCanceler struct {
	baseCanceler
}
type computeTaskCanceler struct {
	baseCanceler
}

// TODO: to implement
// type reloadTaskCanceler struct {
// 	baseCanceler
// }

func (lc *loadTaskCanceler) CancelTask() error {
	if isContinue, err := lc.baseCanceler.doCancelTask(); !isContinue {
		return err
	}

	taskInfo := lc.task
	canceled := true

	for i := 0; loadGraphMgr.GetLoadTask(taskInfo.ID) == nil; i++ {
		if i > 9 {
			logrus.Warnf("Abandoned the cancellation 'load' task handled by worker, which was retried %d times, taskID: %d", i, taskInfo.ID)
			canceled = false
			break
		}
		time.Sleep(200 * time.Millisecond)
		logrus.Warnf("load task:%v not init", taskInfo.ID)
	}

	if canceled {
		loadTaskBl := LoadTaskBl{}
		loadTaskBl.Canceled(loadGraphMgr.GetLoadTask(taskInfo.ID))
	}

	return lc.setCancelDone()

}

func (cc *computeTaskCanceler) CancelTask() error {
	if isContinue, err := cc.baseCanceler.doCancelTask(); !isContinue {
		return err
	}

	taskInfo := cc.task
	canceled := true
	for i := 0; computerTaskMgr.GetTask(taskInfo.ID) == nil; i++ {
		if i > 9 {
			logrus.Errorf("Abandoned the cancellation 'compute' task handled by worker, which was retried %d times, taskID: %d", i, taskInfo.ID)
			canceled = false
			break
		}
		time.Sleep(1000 * time.Millisecond)
		logrus.Warnf("compute task:%v not init", taskInfo.ID)
	}
	if canceled {
		computeTaskBl := ComputeTaskBl{}
		computeTaskBl.Canceled(computerTaskMgr.GetTask(taskInfo.ID))
	}

	return cc.setCancelDone()
}

// CancelTask
func (bc *baseCanceler) doCancelTask() (isContinue bool, err error) {
	taskInfo := bc.task

	if taskInfo == nil {
		return false, fmt.Errorf("the argument `taskInfo` is nil")
	}

	switch taskInfo.State {
	case structure.TaskStateCanceled:
		fallthrough
	case structure.TaskStateError:
		fallthrough
	case structure.TaskStateComplete:
		fallthrough
	case structure.TaskStateLoaded:
		logrus.Warnf("received a cancellation request for a task '%d' in 'over' state '%s'", taskInfo.ID, taskInfo.State)
		return false, nil
	}

	workerNames, err := bc.getWorkerNames()
	if err != nil {
		return false, err
	}

	canceled := true
	//先发送给worker，让worker设置状态，进行退出的一些工作。
	wg := &sync.WaitGroup{}

	for _, workerName := range workerNames {
		wg.Add(1)
		go func(workerName string) {
			defer wg.Done()
			workerClient := workerMgr.GetWorker(workerName)
			_, err := workerClient.Session.ControlTask(context.Background(), &pb.ControlTaskReq{TaskID: taskInfo.ID, Action: structure.ActionCancelTask})
			if err != nil {
				logrus.Errorf("worker：%v cancel task error:%v", workerName, err)
				canceled = false
				return
			}
		}(workerName)
	}
	wg.Wait()

	if !canceled {
		return false, bc.setCancelDone()
	}

	return true, nil
}

func (bc *baseCanceler) setCancelDone() error {
	logrus.Infof("finally set task's '%d' state from '%s' to canceled", bc.task.ID, bc.task.State)

	if err := taskMgr.SetState(bc.task, structure.TaskStateCanceled); err != nil {
		return err
	}

	return nil
}

func (bc *baseCanceler) getWorkerNames() ([]string, error) {
	taskInfo := bc.task

	if taskInfo.Workers != nil && len(taskInfo.Workers) > 0 {
		workerNames := make([]string, 0, len(taskInfo.Workers))
		for _, worker := range taskInfo.Workers {
			workerNames = append(workerNames, worker.Name)
		}

		return workerNames, nil
	}

	graph := graphMgr.GetGraphByName(taskInfo.SpaceName, taskInfo.GraphName)

	if graph == nil {
		return nil, fmt.Errorf("failed to retrieve graph with name: %s/%s", taskInfo.SpaceName, taskInfo.GraphName)
	}

	switch bc.task.Type {
	case structure.TaskTypeCompute:
		return bc.getWorkerNamesViaGraph(graph)
	default:
		return bc.getWorkerNamesViaGroup(graph)
	}

}
func (bc *baseCanceler) getWorkerNamesViaGraph(graph *structure.VermeerGraph) ([]string, error) {
	if len(graph.Workers) == 0 {
		return nil, fmt.Errorf("no workers found for graph with name: %s/%s", bc.task.SpaceName, bc.task.GraphName)
	}

	workerNames := make([]string, 0)

	for _, w := range graph.Workers {
		workerNames = append(workerNames, w.Name)
	}

	return workerNames, nil
}

func (bc *baseCanceler) getWorkerNamesViaGroup(graph *structure.VermeerGraph) ([]string, error) {
	workerClients := workerMgr.GroupWorkers(graph.WorkerGroup)

	workerNames := make([]string, 0)

	for _, w := range workerClients {
		workerNames = append(workerNames, w.Name)
	}

	return workerNames, nil
}
