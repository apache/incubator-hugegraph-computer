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

package schedules

import (
	"sync"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

// TaskQueue 任务等待队列管理器，用于操作等待队列
type TaskQueue struct {
	queue         *common.Queue
	executingTask sync.Map
	locker        sync.Mutex
}

func (tqm *TaskQueue) Init() {
	tqm.queue = common.NewQueue()
	tqm.executingTask = sync.Map{}
}

func (tqm *TaskQueue) PushTask(task *structure.TaskInfo) {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()
	tqm.queue.Put(task)
}

func (tqm *TaskQueue) PollTask() *structure.TaskInfo {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()
	var taskInfo *structure.TaskInfo
	if tqm.queue.Head != nil {
		taskInfo = tqm.queue.Poll().(*structure.TaskInfo)
	}
	return taskInfo
}

func (tqm *TaskQueue) Peek() *structure.TaskInfo {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()
	head := tqm.queue.Peek()

	if head != nil {
		return head.(*structure.TaskInfo)
	}

	return nil
}

func (tqm *TaskQueue) PeekTail() *structure.TaskInfo {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()
	tail := tqm.queue.PeekTail()

	if tail != nil {
		return tail.(*structure.TaskInfo)
	}

	return nil
}

func (tqm *TaskQueue) IsEmpty() bool {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()
	return tqm.queue.Head == nil
}

// DeleteByTaskId 根据taskId删除任务队列中的元素
func (tqm *TaskQueue) DeleteByTaskId(taskId int32) *structure.TaskInfo {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()
	var res *structure.TaskInfo

	q := tqm.queue
	if q.Head == nil {
		logrus.Infof("taskQueue is empty,taskid:%v", taskId)
		return nil
	}
	n := q.Head
	if n.Data.(*structure.TaskInfo).ID == taskId {
		res = n.Data.(*structure.TaskInfo)
		if n.Next == nil {
			q.Head = nil
			q.Tail = nil
		} else {
			q.Head = n.Next
		}
		q.Size--
		return res
	}
	for n.Next != nil {
		nextQueueTask := n.Next.Data.(*structure.TaskInfo)
		if nextQueueTask.ID == taskId {
			res = nextQueueTask
			q.Size--
			n.Next = n.Next.Next
			break
		}
		n = n.Next
	}

	return res
}

// GetAllTaskInQueue 查询任务队列中待执行的任务
func (tqm *TaskQueue) GetAllTaskInQueue() []*structure.TaskInfo {
	tqm.locker.Lock()
	defer tqm.locker.Unlock()

	allTasks := make([]*structure.TaskInfo, 0)
	Node := tqm.queue.Head

	for Node != nil {
		//logrus.Infof(" tqm.TaskQueue.Head: %v", Node.Data.(*structure.TaskInfo))
		curr := Node.Data.(*structure.TaskInfo)
		allTasks = append(allTasks, curr)
		Node = Node.Next
	}

	return allTasks
}
