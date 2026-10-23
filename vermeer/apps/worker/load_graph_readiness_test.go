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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vermeer/apps/common"
	"vermeer/apps/graphio"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

type blockingLoadStatusClient struct {
	pb.MasterClient
	started chan struct{}
	release <-chan struct{}
}

func (client *blockingLoadStatusClient) LoadTaskStatus(context.Context, *pb.LoadTaskStatusReq,
	...grpc.CallOption) (*pb.LoadTaskStatusResp, error) {
	close(client.started)
	<-client.release
	return &pb.LoadTaskStatusResp{}, nil
}

func receiveFixture(t *testing.T) *graphio.LoadGraphTask {
	t.Helper()
	GraphMgr.Init()
	LoadGraphMgr.Init()
	task := LoadGraphMgr.InstallTask(&structure.TaskInfo{
		ID: 5001, SpaceName: "readiness-test", GraphName: "graph",
		Workers: []*structure.TaskWorker{{Name: "worker"}},
	})
	t.Cleanup(func() { GraphMgr.Init(); LoadGraphMgr.DeleteTask(task.Task.ID) })
	return task
}

func TestLoadReceiversUseInitializationResult(t *testing.T) {
	task := receiveFixture(t)
	task.InitializationFailed = true
	task.LoadWg.Done()
	// Runtime task errors can change after initialization. Receivers must only
	// inspect the immutable result published by the initialization barrier.
	stopped, started := make(chan struct{}), make(chan struct{})
	var stop atomic.Bool
	go func() {
		defer close(stopped)
		task.Task.State = structure.TaskStateError
		close(started)
		for !stop.Load() {
			task.Task.State = structure.TaskStateError
		}
	}()
	<-started
	t.Cleanup(func() { stop.Store(true); <-stopped })
	receiver := &LoadGraphBl{}
	for i := 0; i < 100; i++ {
		receiver.RecvVertex(task.Task.ID, "worker", 0, false, 0, nil)
		receiver.GatherVertex(task.Task.ID, "worker", false, 0, nil)
		receiver.RecvEdge(task.Task.ID, "worker", 0, false, 0, nil)
	}
	if count := atomic.LoadInt32((*int32)(task.RecvWg)); count != 0 {
		t.Fatalf("failed initialization leaked %d registered receivers", count)
	}
}

func TestLoadReceiversBarrierSurvivesTaskCleanup(t *testing.T) {
	GraphMgr.Init()
	LoadGraphMgr.Init()
	TaskMgr.Init()
	previousOptions := options.AllOptionsMap
	options.Init()
	previousGraphs := common.PrometheusMetrics.GraphCnt
	common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_cleanup_graphs"}, nil)
	t.Cleanup(func() {
		GraphMgr.Init()
		LoadGraphMgr.DeleteTask(-5002)
		TaskMgr.Init()
		options.AllOptionsMap = previousOptions
		common.PrometheusMetrics.GraphCnt = previousGraphs
	})
	var task *graphio.LoadGraphTask
	var barrier *sync.WaitGroup
	options.AllOptionsMap["load.type"] = &options.Option{
		Convert: func(string) (interface{}, error) {
			task = LoadGraphMgr.GetLoadTask(-5002)
			barrier = task.LoadWg
			// Simulate cancellation cleanup while initialization is still active.
			task.FreeMemory()
			panic("initialization failed after cleanup")
		},
	}
	reported, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	previousMaster := ServiceWorker.MasterClient
	ServiceWorker.MasterClient = &blockingLoadStatusClient{started: reported, release: release}
	t.Cleanup(func() {
		close(release)
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Error("initialization did not return after status report resumed")
		}
		ServiceWorker.MasterClient = previousMaster
	})
	go func() {
		defer close(returned)
		(&LoadGraphBl{}).StartLoadGraph("readiness-test", -5002, "graph", nil,
			map[string]string{"load.type": "cleanup-test"})
	}()
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("initialization did not report its failure")
	}
	select {
	case <-returned:
		t.Fatal("status report must still be blocked")
	default:
	}
	if task == nil || barrier == nil || task.LoadWg != nil || !task.InitializationFailed {
		t.Fatal("initialization must publish failure after task cleanup")
	}
	released := make(chan struct{})
	go func() { barrier.Wait(); close(released) }()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("blocked status report left the original initialization barrier blocked")
	}
}

func TestRecvVertexRegistersBeforeInitialization(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			task := receiveFixture(t)
			previous := common.PrometheusMetrics.GraphCnt
			common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{Name: "test_receive_graphs"}, nil)
			t.Cleanup(func() { common.PrometheusMetrics.GraphCnt = previous })
			graph := GraphMgr.CreateGraph(task.Task.SpaceName, task.Task.GraphName)
			graph.MallocData(structure.GraphDataBackendOption{VertexDataBackend: structure.DataBackendInMem})
			if err := graph.SetOption(false, false, false); err != nil {
				t.Fatal(err)
			}
			if err := GraphMgr.AddGraph(graph); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			release := func() {
				once.Do(func() {
					task.InitializationFailed = failed
					task.LoadWg.Done()
				})
			}
			received := make(chan struct{})
			vertex := structure.Vertex{ID: "delayed-vertex"}
			data := make([]byte, vertex.PredictSize())
			if _, err := vertex.Marshal(data); err != nil {
				t.Fatal(err)
			}
			go func() {
				(&LoadGraphBl{}).RecvVertex(task.Task.ID, "worker", 1, false, 0, data)
				close(received)
			}()
			t.Cleanup(func() {
				release()
				select {
				case <-received:
				case <-time.After(time.Second):
					t.Error("receiver did not leave initialization wait")
				}
			})
			deadline := time.Now().Add(time.Second)
			for atomic.LoadInt32((*int32)(task.RecvWg)) == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if count := atomic.LoadInt32((*int32)(task.RecvWg)); count != 1 {
				t.Fatalf("receiver must register before waiting for initialization, got %d", count)
			}
			completion := make(chan struct{})
			go func() { task.RecvWg.Wait(); close(completion) }()
			// This is the same completion wait used by an ending vertex frame.
			select {
			case <-completion:
				t.Fatal("completion advanced past an initialization-blocked receiver")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			select {
			case <-received:
			case <-time.After(time.Second):
				t.Fatal("receiver did not complete")
			}
			select {
			case <-completion:
			case <-time.After(time.Second):
				t.Fatal("completion waiter leaked after receiver exit")
			}
			if atomic.LoadInt32((*int32)(task.RecvWg)) != 0 {
				t.Fatal("unbalanced receiver accounting")
			}
			if failed {
				if graph.Data.VertexCount != 0 {
					t.Fatal("failed initialization must not accept vertices")
				}
			} else if graph.Data.VertexCount != 1 || graph.Data.Vertex.GetVertex(0).ID != vertex.ID {
				t.Fatal("successful initialization must process the registered vertex")
			}
		})
	}
}
