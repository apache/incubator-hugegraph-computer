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
	"context"
	"errors"
	"fmt"
	"sort"
	pb "vermeer/apps/protos"
)

// assure must to done without any error
type assure func()

type WorkerMemLimit struct {
	MaxMem  uint32
	MinFree uint32
	GcRatio float32
}

func (wm *WorkerMemLimit) toString() string {
	return fmt.Sprintf("{MaxMem:%v, MinFree:%v, GcRatio:%v}", wm.MaxMem, wm.MinFree, wm.GcRatio)
}

func atomProcess(atom func() error, process []assure) (err error) {
	if err = atom(); err == nil {
		for _, p := range process {
			p()
		}
	}
	return err
}

func map2Workers(mappedWorkers map[string]*WorkerClient) []*WorkerClient {
	if mappedWorkers == nil {
		return make([]*WorkerClient, 0)
	}
	workers := make([]*WorkerClient, 0, len(mappedWorkers))
	for _, e := range mappedWorkers {
		e.State = e.Connection.GetState().String()
		workers = append(workers, e)
	}

	return workers
}

// SortWorkersAsc return the same instance passed as an argument, without creating a new one.
func SortWorkersAsc(workers []*WorkerClient) []*WorkerClient {
	if workers == nil {
		return nil
	}

	sort.Slice(workers, func(i, j int) bool {
		return workers[i].Id < workers[j].Id
	})

	return workers
}

func toMemoryLimitReq(limit *WorkerMemLimit) *pb.SetMemoryLimitReq {
	return &pb.SetMemoryLimitReq{
		MaxMemoryUsed:        limit.MaxMem,
		MinRemainMemory:      limit.MinFree,
		SoftMemoryLimitRatio: limit.GcRatio,
	}

}

func doSendMemoryLimitReq(worker *WorkerClient, req *pb.SetMemoryLimitReq) (resp *pb.SetMemoryLimitResp, err error) {
	if worker == nil || req == nil {
		return nil, nil
	}

	request := &pb.RuntimeActionReq{
		Request: &pb.RuntimeActionReq_MemoryLimitReq{
			MemoryLimitReq: req,
		},
	}

	if worker.Session == nil {
		return nil, fmt.Errorf("worker not connected, worker's name: %s", worker.Name)
	}

	var response *pb.RuntimeActionResp
	if response, err = worker.Session.RuntimeAction(context.Background(), request); err != nil {
		return nil, err
	}

	res := response.GetMemoryLimitResp()
	if res == nil {
		return nil, errors.New("the response of memory limit req is nil")
	}

	if res.Base != nil && res.Base.ErrorCode < 0 {
		return nil, errors.New(res.Base.Message)
	}

	return res, nil
}
