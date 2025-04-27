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

package workers

import (
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

var WorkerManager = &workerManager{}

var manager = WorkerManager

func SendMemLimit(worker *WorkerClient, memLimit *WorkerMemLimit) error {
	if worker == nil {
		return errors.New("the argument `worker` cannot be nil")
	}
	if memLimit == nil {
		return errors.New("the argument `memLimit` cannot be empty")
	}

	if _, err := doSendMemoryLimitReq(worker, toMemoryLimitReq(memLimit)); err != nil {
		return fmt.Errorf("failed to send memory limit req to worker %s: %w", worker.Name, err)
	}
	logrus.Infof("sent memory limit '%s' via group '%s' to worker %s@%s", memLimit.toString(), worker.Group, worker.Name, worker.GrpcPeer)

	return nil
}

func SendMemLimitGroup(workerGroup string, memLimit *WorkerMemLimit) (total, success int, err error) {
	if workerGroup == "" {
		return 0, 0, errors.New("the argument `workerGroup` cannot be empty")
	}
	if memLimit == nil {
		return 0, 0, errors.New("the argument `memLimit` cannot be empty")
	}

	workers := manager.GroupWorkers(workerGroup)
	if workers == nil {
		return 0, 0, fmt.Errorf("received a nil result of workers with group '%s' via `GroupWorkers()`", workerGroup)
	}

	total = len(workers)
	if total == 0 {
		return 0, 0, nil
	}

	for _, worker := range workers {
		if _, err := doSendMemoryLimitReq(worker, toMemoryLimitReq(memLimit)); err != nil {
			logrus.Errorf("failed to send memory limit '%s' for worker %s@%s: %v", memLimit.toString(), worker.Name, worker.GrpcPeer, err)
			continue
		}
		success++
	}

	logrus.Infof("sent memory limits '%s' to %d/%d workers of group '%s'", memLimit.toString(), success, total, workerGroup)

	return total, success, nil
}
