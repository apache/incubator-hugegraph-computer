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
	"sync"
	"vermeer/apps/master/workers"
	pb "vermeer/apps/protos"
)

type ServerManager struct {
	loadSevers      map[string]*LoadGraphServer
	computeSevers   map[string]*ComputeTaskServer
	superStepSevers map[string]*SuperStepServer
	locker          sync.Mutex
}

func (s *ServerManager) Init() {
	s.loadSevers = make(map[string]*LoadGraphServer)
	s.computeSevers = make(map[string]*ComputeTaskServer)
	s.superStepSevers = make(map[string]*SuperStepServer)
}

func (s *ServerManager) StartingLoadServer(workerName string, stream pb.Master_LoadGraphTaskServer) error {
	if workerName == "" {
		return errors.New("workerName is empty")
	}

	loadServer := &LoadGraphServer{streamServer: stream}
	s.locker.Lock()
	s.loadSevers[workerName] = loadServer
	worker := workerMgr.GetWorker(workerName)
	workerMgr.SetTaskStream(worker, workers.LoadTaskStream, stream.Context())
	s.locker.Unlock()
	defer func() {
		workerMgr.ClearTaskStream(worker, workers.LoadTaskStream, stream.Context())
		s.locker.Lock()
		defer s.locker.Unlock()
		if s.loadSevers[workerName] == loadServer {
			delete(s.loadSevers, workerName)
		}
	}()
	loadServer.RecvHandler(workerName)

	return nil
}

func (s *ServerManager) StartingComputeServer(workerName string, stream pb.Master_ComputeTaskServer) error {
	if workerName == "" {
		return errors.New("workerName is empty")
	}

	computeServer := &ComputeTaskServer{streamServer: stream}
	s.locker.Lock()
	s.computeSevers[workerName] = computeServer
	worker := workerMgr.GetWorker(workerName)
	workerMgr.SetTaskStream(worker, workers.ComputeTaskStream, stream.Context())
	s.locker.Unlock()
	defer func() {
		workerMgr.ClearTaskStream(worker, workers.ComputeTaskStream, stream.Context())
		s.locker.Lock()
		defer s.locker.Unlock()
		if s.computeSevers[workerName] == computeServer {
			delete(s.computeSevers, workerName)
		}
	}()
	computeServer.RecvHandler(workerName)

	return nil
}
func (s *ServerManager) PutSuperStepServer(workerName string, stream pb.Master_SuperStepServer) error {
	if workerName == "" {
		return errors.New("workerName is empty")
	}

	s.locker.Lock()
	defer s.locker.Unlock()

	delete(s.superStepSevers, workerName)
	superStepServer := &SuperStepServer{streamServer: stream}
	s.superStepSevers[workerName] = superStepServer
	// Keep dispatch installation and readiness publication in the same order.
	worker := workerMgr.GetWorker(workerName)
	workerMgr.SetTaskStream(worker, workers.SuperStepStream, stream.Context())

	return nil
}
func (s *ServerManager) LoadServer(workerName string) *LoadGraphServer {
	s.locker.Lock()
	defer s.locker.Unlock()
	// TODO: handle nil
	return s.loadSevers[workerName]
}

func (s *ServerManager) ComputeServer(workerName string) *ComputeTaskServer {
	s.locker.Lock()
	defer s.locker.Unlock()
	// TODO: handle nil
	return s.computeSevers[workerName]
}

func (s *ServerManager) SuperStepServer(workerName string) *SuperStepServer {
	s.locker.Lock()
	defer s.locker.Unlock()

	// TODO: handle nil
	return s.superStepSevers[workerName]
}
