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
	"net"
	"testing"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/master/schedules"
	"vermeer/apps/master/workers"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"
	"vermeer/apps/version"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type registrationWindowWorker struct {
	pb.UnimplementedWorkerServer
	started chan struct{}
	release chan struct{}
}

func (w *registrationWindowWorker) RuntimeAction(context.Context, *pb.RuntimeActionReq) (*pb.RuntimeActionResp, error) {
	close(w.started)
	<-w.release
	return &pb.RuntimeActionResp{Response: &pb.RuntimeActionResp_MemoryLimitResp{
		MemoryLimitResp: &pb.SetMemoryLimitResp{},
	}}, nil
}

// This executes real SayHelloMaster while its ordinary memory-limit RPC is pending.
// No worker task stream can exist yet: the worker only opens those after hello returns.
func TestWorkerAvailabilityWaitsForRegistration(t *testing.T) {
	previousWorkers := common.PrometheusMetrics.WorkerCnt
	previousTasks := common.PrometheusMetrics.TaskCnt
	previousRunning := common.PrometheusMetrics.TaskRunningCnt
	t.Cleanup(func() {
		common.PrometheusMetrics.WorkerCnt = previousWorkers
		common.PrometheusMetrics.TaskCnt = previousTasks
		common.PrometheusMetrics.TaskRunningCnt = previousRunning
	})
	common.PrometheusMetrics.WorkerCnt = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_registration_workers"}, nil)
	common.PrometheusMetrics.TaskCnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_registration_tasks"}, []string{"type"})
	common.PrometheusMetrics.TaskRunningCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_registration_running"}, []string{"type"})
	workers.WorkerManager.Init()
	structure.SpaceManager.Init()
	graphMgr.Init()
	taskMgr.Init()
	if err := taskMgr.InitStore(); err != nil {
		t.Fatal(err)
	}
	ServerMgr.Init()
	Scheduler.resourceManager = &schedules.SchedulerResourceManager{}
	Scheduler.resourceManager.Init()
	Scheduler.taskManager = new(schedules.SchedulerTaskManager).Init()
	Scheduler.algorithmManager = &schedules.SchedulerAlgorithmManager{}
	Scheduler.algorithmManager.Init()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := &registrationWindowWorker{started: make(chan struct{}), release: make(chan struct{})}
	server := grpc.NewServer()
	pb.RegisterWorkerServer(server, w)
	go server.Serve(listener)
	defer server.Stop()
	returned := make(chan error, 1)
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: listener.Addr()})
	go func() {
		_, err := new(ServerHandler).SayHelloMaster(ctx, &pb.HelloMasterReq{
			WorkerPeer: listener.Addr().String(), Version: version.Version,
		})
		returned <- err
	}()
	select {
	case <-w.started:
	case <-time.After(3 * time.Second):
		t.Fatal("registration did not reach memory RPC")
	}
	defer func() {
		close(w.release)
		select {
		case err := <-returned:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("hello did not return")
		}
	}()
	if workers.WorkerManager.HasAvailableWorkers("current", nil) {
		t.Fatal("hello still pending: reverse dial must not advertise task readiness")
	}
	task, err := taskMgr.CreateTask("current", structure.TaskTypeLoad, 0)
	if err != nil {
		t.Fatal(err)
	}
	task.GraphName = "graph"
	task.CreateType = structure.TaskCreateSync
	if _, err := taskMgr.AddTask(task); err != nil {
		t.Fatal(err)
	}
	agent, status, _, err := new(schedules.Broker).Init().ApplyAgent(task)
	if err != nil || agent != nil || status != schedules.AgentStatusWorkerNotReady {
		t.Fatalf("scheduler admitted a worker before its task streams: %v %v", status, err)
	}
	registered := workers.WorkerManager.GetAllWorkers()
	if len(registered) != 1 {
		t.Fatal("wrong registration count")
	}
	worker := registered[0]
	t.Cleanup(worker.Close)
	if ServerMgr.LoadServer(worker.Name) != nil {
		t.Fatal("unexpected load stream")
	}
	err = sendLoadingReq(task, registered, func(*workers.WorkerClient) *LoadingTaskReq {
		return &LoadingTaskReq{}
	})
	if err == nil {
		t.Fatal("missing task transport must return an error")
	}
	if task.State != structure.TaskStateError || task.ErrMessage != err.Error() {
		t.Fatalf("failed synchronous dispatch left state=%s, message=%q", task.State, task.ErrMessage)
	}
	persisted, readErr := taskMgr.ReadTask(task.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if persisted.State != structure.TaskStateError || persisted.ErrMessage != task.ErrMessage {
		t.Fatalf("persisted failed task has state=%s, message=%q", persisted.State, persisted.ErrMessage)
	}
}
