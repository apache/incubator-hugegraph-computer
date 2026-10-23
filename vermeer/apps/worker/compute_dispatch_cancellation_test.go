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

package worker

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type computeStatusClient struct {
	pb.MasterClient
	calls     int
	entered   chan struct{}
	release   chan struct{}
	reportErr error
}

func (c *computeStatusClient) ComputeTaskStatus(context.Context, *pb.ComputeTaskStatusReq,
	...grpc.CallOption) (*pb.ComputeTaskStatusResp, error) {
	c.calls++
	if c.entered != nil && c.calls == 1 {
		close(c.entered)
		<-c.release
	}
	return &pb.ComputeTaskStatusResp{}, c.reportErr
}

type cancelInitWorker struct {
	compute.WorkerComputerBase
	entered chan struct{}
	release chan struct{}
}

func (w *cancelInitWorker) Init() error {
	close(w.entered)
	<-w.release
	return nil
}

type cancelInitMaker struct {
	compute.AlgorithmMakerBase
	worker *cancelInitWorker
}

func (m *cancelInitMaker) CreateWorkerComputer() compute.WorkerComputer { return m.worker }

func TestComputeDispatchCancellation(t *testing.T) {
	previousOptions := options.AllOptionsMap
	previousGraphs := common.PrometheusMetrics.GraphCnt
	previousTasks := common.PrometheusMetrics.TaskCnt
	previousRunning := common.PrometheusMetrics.TaskRunningCnt
	previousMaster := ServiceWorker.MasterClient
	options.Init()
	options.AllOptionsMap["compute.algorithm"] = &options.Option{
		Name: "compute.algorithm", ValueType: options.OptionValueTypeString, Value: "base", Convert: options.ToString,
	}
	common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_compute_cancel_graphs"}, nil)
	common.PrometheusMetrics.TaskCnt = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_compute_cancel_tasks"}, []string{"type"})
	common.PrometheusMetrics.TaskRunningCnt = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_compute_cancel_running"}, []string{"type"})
	GraphMgr.Init()
	TaskMgr.Init()
	ComputeTaskMgr.Init()
	AlgorithmMgr.Init()
	t.Cleanup(func() {
		GraphMgr.Init()
		TaskMgr.Init()
		ComputeTaskMgr.Init()
		options.AllOptionsMap = previousOptions
		common.PrometheusMetrics.GraphCnt = previousGraphs
		common.PrometheusMetrics.TaskCnt = previousTasks
		common.PrometheusMetrics.TaskRunningCnt = previousRunning
		ServiceWorker.MasterClient = previousMaster
	})
	graph := GraphMgr.CreateGraph("compute-cancel", "graph")
	graph.Workers = []*structure.GraphWorker{{Name: "worker", IsSelf: true}}
	if err := GraphMgr.AddGraph(graph); err != nil {
		t.Fatal(err)
	}
	handler := new(PeerHandler)
	t.Run("before_task_creation", func(t *testing.T) {
		_, err := handler.ControlTask(context.Background(), &pb.ControlTaskReq{
			TaskID: 7001, Action: structure.ActionCancelTask})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("missing task cancellation must be retryable NotFound, got %v", err)
		}
	})
	for index := 0; index < 3; index++ {
		duringInit := index == 0
		name := "waiting_for_first_step"
		if duringInit {
			name = "during_initialization"
		} else if index == 2 {
			name = "status_error_after_cancel"
		}
		t.Run(name, func(t *testing.T) {
			taskID := int32(7002 + index)
			statusClient := new(computeStatusClient)
			if index == 2 {
				statusClient.entered = make(chan struct{})
				statusClient.release = make(chan struct{})
				statusClient.reportErr = errors.New("status report failed")
			}
			ServiceWorker.MasterClient = statusClient
			initWorker := &cancelInitWorker{entered: make(chan struct{}), release: make(chan struct{})}
			if duringInit {
				AlgorithmMgr.Register(&cancelInitMaker{worker: initWorker}, "test")
			} else {
				AlgorithmMgr.Register(new(compute.AlgorithmMakerBase), "test")
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				new(ComputeBl).StartCompute(taskID, graph.SpaceName, graph.Name, "base", map[string]string{"compute.parallel": "1"})
			}()
			if duringInit {
				select {
				case <-initWorker.entered:
				case <-time.After(time.Second):
					t.Fatal("worker did not reach initialization")
				}
			} else if index == 2 {
				select {
				case <-statusClient.entered:
				case <-time.After(time.Second):
					t.Fatal("status report was not reached")
				}
			} else {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("initialization did not finish")
				}
			}
			_, err := handler.ControlTask(context.Background(), &pb.ControlTaskReq{
				TaskID: taskID, Action: structure.ActionCancelTask})
			if err != nil {
				t.Fatal(err)
			}
			if duringInit {
				close(initWorker.release)
			} else if index == 2 {
				close(statusClient.release)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("canceled initialization did not finish")
			}
			ct := ComputeTaskMgr.GetTask(taskID)
			if ct == nil || ct.Task.State != structure.TaskStateCanceled {
				t.Fatalf("cancellation was not consumed: %+v", ct)
			}
			wantReports := 1
			if duringInit {
				wantReports = 0
			}
			if statusClient.calls != wantReports {
				t.Errorf("status reports=%d want=%d", statusClient.calls, wantReports)
			}
			// An already queued first-step message must not consume cancellation twice.
			new(ComputeBl).RunSuperStep(taskID, nil)
			if ct.Task.State != structure.TaskStateCanceled {
				t.Fatal("late first step resumed a canceled task")
			}
			if running := testutil.ToFloat64(common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(structure.TaskTypeCompute)); running != 0 {
				t.Fatalf("cancellation left running gauge=%v", running)
			}
		})
	}
	for index, phase := range []string{"initialization", "first_step", "status_error"} {
		t.Run("paused_"+phase, func(t *testing.T) {
			taskID := int32(7010 + index)
			statusClient := new(computeStatusClient)
			if phase == "status_error" {
				statusClient.entered = make(chan struct{})
				statusClient.release = make(chan struct{})
				statusClient.reportErr = errors.New("status report failed")
			}
			ServiceWorker.MasterClient = statusClient
			initWorker := &cancelInitWorker{entered: make(chan struct{}), release: make(chan struct{})}
			if phase == "initialization" {
				AlgorithmMgr.Register(&cancelInitMaker{worker: initWorker}, "test")
			} else {
				AlgorithmMgr.Register(new(compute.AlgorithmMakerBase), "test")
			}
			start := func() {
				new(ComputeBl).StartCompute(taskID, graph.SpaceName, graph.Name, "base",
					map[string]string{"compute.parallel": "1"})
			}
			done := make(chan struct{})
			var entered <-chan struct{}
			if phase == "initialization" {
				entered = initWorker.entered
			} else if phase == "status_error" {
				entered = statusClient.entered
			}
			if entered == nil {
				start()
			} else {
				go func() { defer close(done); start() }()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("compute did not reach the controlled initialization gate")
				}
			}
			if _, err := handler.ControlTask(context.Background(), &pb.ControlTaskReq{
				TaskID: taskID, Action: structure.ActionPauseTask}); err != nil {
				t.Fatal(err)
			}
			switch phase {
			case "initialization":
				close(initWorker.release)
			case "status_error":
				close(statusClient.release)
			default:
				go func() { defer close(done); new(ComputeBl).RunSuperStep(taskID, nil) }()
			}
			// Observe the actual pause wait before canceling, without changing its 10-second cadence.
			if !waitForComputePause() {
				t.Error("compute did not enter the pause wait")
			}
			canceled := make(chan error, 1)
			go func() {
				_, err := handler.ControlTask(context.Background(), &pb.ControlTaskReq{
					TaskID: taskID, Action: structure.ActionCancelTask})
				canceled <- err
			}()
			select {
			case err := <-canceled:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(time.Second):
				t.Error("cancel RPC blocked behind the paused computation")
				// Join the old-source negative control too, before restoring shared fixtures.
				select {
				case <-canceled:
				case <-time.After(11 * time.Second):
					t.Fatal("cancel RPC did not finish after the pause poll")
				}
			}
			select {
			case <-done:
			case <-time.After(11 * time.Second):
				t.Fatal("paused computation did not consume cancellation")
			}
			ct := ComputeTaskMgr.GetTask(taskID)
			if ct.Task.State != structure.TaskStateCanceled {
				t.Fatalf("paused computation ended in state %s", ct.Task.State)
			}
			wantReports := 1
			if phase == "initialization" {
				wantReports = 0
			}
			if statusClient.calls != wantReports {
				t.Errorf("status reports=%d want=%d", statusClient.calls, wantReports)
			}
			new(ComputeBl).RunSuperStep(taskID, nil)
			if ct.Task.State != structure.TaskStateCanceled {
				t.Error("late first step resumed a canceled paused task")
			}
			if running := testutil.ToFloat64(common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(structure.TaskTypeCompute)); running != 0 {
				t.Errorf("paused cancellation left running gauge=%v", running)
			}
		})
	}
}

func waitForComputePause() bool {
	stack := make([]byte, 128<<10)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		n := runtime.Stack(stack, true)
		if bytes.Contains(stack[:n], []byte("vermeer/apps/worker.(*ComputeBl).pauseAction(")) {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-tick.C:
		}
	}
}
