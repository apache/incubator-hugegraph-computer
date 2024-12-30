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
	"errors"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type SpaceQueue struct {
	structure.MutexLocker
	structure.Syncer
	spaceQueue map[string]*TaskQueue
	idQueue    map[int32]*TaskQueue
}

func (s *SpaceQueue) Init() *SpaceQueue {
	s.spaceQueue = make(map[string]*TaskQueue)
	s.idQueue = make(map[int32]*TaskQueue)

	return s
}

// PushTask Enqueue the task at the end of the inner queue.
func (s *SpaceQueue) PushTask(taskInfo *structure.TaskInfo) (bool, error) {
	if taskInfo == nil {
		return false, errors.New("the argument `taskInfo` is nil")
	}
	if taskInfo.SpaceName == "" {
		return false, errors.New("the property `SpaceName` of taskInfo is empty")
	}
	if taskInfo.State != structure.TaskStateWaiting {
		return false, errors.New("the property `State` of taskInfo must be `Waiting`")
	}

	defer s.Unlock(s.Lock())

	if _, ok := s.idQueue[taskInfo.ID]; ok {
		logrus.Warnf("the task had been existing in the queue, taskID: %d", taskInfo.ID)
		return false, nil
	}

	queue := s.spaceQueue[taskInfo.SpaceName]

	if queue == nil {
		queue = &TaskQueue{}
		queue.Init()
		s.spaceQueue[taskInfo.SpaceName] = queue
	}

	queue.PushTask(taskInfo)
	s.idQueue[taskInfo.ID] = queue

	return true, nil
}

func (s *SpaceQueue) RemoveTask(taskID int32) *structure.TaskInfo {
	defer s.Unlock(s.Lock())

	queue := s.idQueue[taskID]

	if queue == nil {
		return nil
	}

	task := queue.DeleteByTaskId(taskID)
	if task == nil {
		return nil
	}

	delete(s.idQueue, taskID)

	return task
}

func (s *SpaceQueue) PeekTailTask(space string) *structure.TaskInfo {
	defer s.Unlock(s.Lock())

	if queue := s.spaceQueue[space]; queue != nil {
		return queue.PeekTail()
	}

	return nil
}

func (s *SpaceQueue) SpaceTasks(space string) []*structure.TaskInfo {
	defer s.Unlock(s.Lock())
	if queue := s.spaceQueue[space]; queue != nil {
		return queue.GetAllTaskInQueue()
	}

	return make([]*structure.TaskInfo, 0)
}

func (s *SpaceQueue) AllTasks() []*structure.TaskInfo {
	defer s.Unlock(s.Lock())
	res := make([]*structure.TaskInfo, 0)

	for _, queue := range s.spaceQueue {
		res = append(res, queue.GetAllTaskInQueue()...)
	}

	return res
}

func (s *SpaceQueue) HeadTasks() map[string]*structure.TaskInfo {
	defer s.Unlock(s.Lock())

	//ok := false
	res := make(map[string]*structure.TaskInfo)

	//for !ok {
	for s, q := range s.spaceQueue {
	LOOP:
		if t := q.Peek(); t != nil {
			if t.State == structure.TaskStateWaiting {
				res[s] = t
			} else {
				q.PollTask()
				goto LOOP
			}
		}
	}
	return res
	//	}

}

func (s *SpaceQueue) IsHeadTask(taskID int32) bool {
	defer s.Unlock(s.Lock())

	queue := s.idQueue[taskID]
	if queue == nil {
		return false
	}
	head := queue.Peek()
	if head == nil {
		return false
	}
	if head.ID == taskID {
		return true
	}

	return false
}
