/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bl

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/master/workers"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type computeDispatchStream struct {
	grpc.ServerStream
	requests int
	sendErr  error
}

func (s *computeDispatchStream) Send(*pb.ComputeTaskResp) error {
	s.requests++
	return s.sendErr
}
func (s *computeDispatchStream) Recv() (*pb.ComputeTaskReq, error) { return nil, io.EOF }

type computeCancelClient struct {
	pb.WorkerClient
	task      *structure.TaskInfo
	calls     int
	notFound  bool
	cancelErr error
	invalid   bool
}

func (c *computeCancelClient) ControlTask(ctx context.Context, req *pb.ControlTaskReq,
	_ ...grpc.CallOption) (*pb.ControlTaskResp, error) {
	c.calls++
	if req.TaskID != c.task.ID || req.Action != structure.ActionCancelTask {
		c.invalid = true
		return nil, errors.New("wrong cancellation scope")
	}
	if c.task.State != structure.TaskStateError {
		c.invalid = true
		return nil, errors.New("task was not failed before cancellation")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second {
		c.invalid = true
		return nil, errors.New("cancellation has no bounded deadline")
	}
	if c.notFound && c.calls == 1 {
		return nil, status.Error(codes.NotFound, "not initialized")
	}
	return &pb.ControlTaskResp{}, c.cancelErr
}

// Reuse the registration regression's isolated stores rather than reopen Pebble fixtures.
func testPartialComputeDispatch(t *testing.T) {
	previousGraphs := common.PrometheusMetrics.GraphCnt
	common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_compute_dispatch_graphs"}, nil)
	t.Cleanup(func() { common.PrometheusMetrics.GraphCnt = previousGraphs })
	if err := graphMgr.InitStore(); err != nil {
		t.Fatal(err)
	}
	algorithmMgr.Init()
	algorithmMgr.Register(new(compute.AlgorithmMakerBase), "test")
	for _, test := range []struct {
		name                                                               string
		action                                                             pb.ComputeAction
		missingWorker, missingStream, sendFailure, cancelFailure, lateInit bool
	}{
		{name: "missing_worker", missingWorker: true},
		{name: "missing_stream", missingStream: true},
		{name: "send_failure", sendFailure: true},
		{name: "cleanup_failure", missingStream: true, cancelFailure: true},
		{name: "late_initialization", missingStream: true, lateInit: true},
		{name: "success"},
		{name: "out_edges", action: pb.ComputeAction_SettingOutEdges, sendFailure: true},
		{name: "out_degree", action: pb.ComputeAction_SettingOutDegree, sendFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ServerMgr.Init()
			defer ServerMgr.Init()
			task, err := taskMgr.CreateTask("compute-dispatch", structure.TaskTypeCompute, 0)
			if err != nil {
				t.Fatal(err)
			}
			task.GraphName = "graph"
			graph := graphMgr.CreateGraph(task.SpaceName, task.GraphName)
			clients := make([]*computeCancelClient, 3)
			streams := make([]*computeDispatchStream, 3)
			names := []string{"compute-first", "compute-second", "compute-third"}
			dispatchErr := errors.New("compute dispatch failed")
			for i, name := range names {
				graph.Workers = append(graph.Workers, &structure.GraphWorker{Name: name})
				task.Workers = append(task.Workers, &structure.TaskWorker{Name: name})
				clients[i] = &computeCancelClient{task: task}
				streams[i] = new(computeDispatchStream)
				workerMgr.RemoveWorker(name)
				if i == 2 && test.missingWorker {
					continue
				}
				_, err = workerMgr.AddWorker(&workers.WorkerClient{Name: name, Session: clients[i]})
				if err != nil {
					t.Fatal(err)
				}
				if i != 2 || !test.missingStream {
					ServerMgr.computeSevers[name] = &ComputeTaskServer{streamServer: streams[i]}
				}
			}
			if test.sendFailure {
				streams[2].sendErr = dispatchErr
			}
			if test.cancelFailure {
				clients[0].cancelErr = errors.New("cancel failed")
			}
			clients[0].notFound = test.lateInit
			ct := &compute.ComputerTask{Task: task, Algorithm: "base"}
			err = StartComputeTask(graph, ct, test.action)
			failed := test.missingWorker || test.missingStream || test.sendFailure
			if (err != nil) != failed {
				t.Fatalf("dispatch err=%v, want failure=%v", err, failed)
			}
			if test.sendFailure && !errors.Is(err, dispatchErr) {
				t.Fatalf("replaced original error: %v", err)
			}
			if failed && (task.State != structure.TaskStateError || task.ErrMessage != err.Error()) {
				t.Errorf("partial dispatch left master state=%s, error=%q; want Error and original dispatch error", task.State, task.ErrMessage)
			} else if failed {
				saved, readErr := taskMgr.ReadTask(task.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if task.State != structure.TaskStateError || saved.State != task.State ||
					task.ErrMessage != err.Error() || saved.ErrMessage != err.Error() {
					t.Fatal("dispatch failure must persist the original error")
				}
			}
			for i, c := range clients {
				if c.invalid {
					t.Errorf("worker %d received an invalid cancellation", i)
				}
				want := 0
				if failed && i < 2 {
					want = 1
				}
				if test.lateInit && i == 0 {
					want = 2
				}
				if c.calls != want {
					t.Errorf("worker %d cancellation calls=%d, want %d", i, c.calls, want)
				}
			}
			for i, s := range streams {
				want := 1
				if i == 2 && (test.missingWorker || test.missingStream) {
					want = 0
				}
				if s.requests != want {
					t.Errorf("worker %d sends=%d want=%d", i, s.requests, want)
				}
			}
		})
	}
}
