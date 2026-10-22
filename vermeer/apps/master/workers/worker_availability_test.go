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
	"maps"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func readyWorkerConnection(t *testing.T) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient("passthrough:///availability-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	connection.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		state := connection.GetState()
		if state == connectivity.Ready {
			return connection
		}
		if !connection.WaitForStateChange(ctx, state) {
			t.Fatal("worker connection did not become ready")
		}
	}
}

func readyTaskStreams() [taskStreamCount]context.Context {
	return [taskStreamCount]context.Context{
		context.Background(), context.Background(), context.Background(),
	}
}

func TestWorkerAvailabilityRespectsSpaceAllocation(t *testing.T) {
	connection := readyWorkerConnection(t)
	cases := []struct {
		name      string
		group     string
		mappings  map[string]string
		graphs    []string
		available bool
	}{
		{"unallocated named group", "default", nil, nil, false},
		{"another space", "default", map[string]string{"other": "default"}, nil, false},
		{"space allocation", "default", map[string]string{"current": "default"}, nil, true},
		{"graph allocation", "default", map[string]string{"current graph": "default"}, []string{"graph"}, true},
		{"other graph", "default", map[string]string{"current-other graph": "default"}, []string{"graph"}, false},
		{"common worker", "$", nil, nil, true},
		{"other space with shared prefix", "default", map[string]string{"current graph": "default"}, nil, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manager := workerManager{
				workersByName: map[string]*WorkerClient{
					"worker": {Name: "worker", Group: test.group, Connection: connection,
						taskStreams: readyTaskStreams()},
				},
				groupMapper: test.mappings,
			}
			before := maps.Clone(manager.groupMapper)
			if got := manager.HasAvailableWorkers("current", test.graphs); got != test.available {
				t.Fatalf("availability = %v, want %v", got, test.available)
			}
			if !maps.Equal(manager.groupMapper, before) {
				t.Fatal("readiness changed allocations")
			}
		})
	}
}

func TestWorkerAvailabilityHandlesRegistrationAndDisconnect(t *testing.T) {
	connection := readyWorkerConnection(t)
	manager := workerManager{workersByName: map[string]*WorkerClient{}, groupMapper: map[string]string{}}
	if manager.HasAvailableWorkers("current", nil) {
		t.Fatal("missing worker must not be ready")
	}
	manager.workersByName["worker"] = &WorkerClient{Name: "worker", Group: "$"}
	if manager.HasAvailableWorkers("current", nil) {
		t.Fatal("unconnected worker must not be ready")
	}
	worker := manager.workersByName["worker"]
	manager.SetWorkerConnection(worker, connection)
	if manager.HasAvailableWorkers("current", nil) {
		t.Fatal("reverse dial alone must not advertise a task-ready worker")
	}
	for stream := LoadTaskStream; stream < taskStreamCount; stream++ {
		manager.SetTaskStream(worker, stream, context.Background())
		if stream < SuperStepStream && manager.HasAvailableWorkers("current", nil) {
			t.Fatal("partially registered task streams must not be ready")
		}
	}
	if !manager.HasAvailableWorkers("current", nil) {
		t.Fatal("registered ready worker must be available")
	}
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 1000; i++ {
			manager.HasAvailableWorkers("current", nil)
		}
	}()
	for i := 0; i < 1000; i++ {
		manager.SetWorkerConnection(worker, nil)
		manager.SetWorkerConnection(worker, connection)
	}
	_ = connection.Close()
	wait.Wait()
	if manager.HasAvailableWorkers("current", nil) {
		t.Fatal("disconnected worker must not be ready")
	}
}

func TestWorkerAvailabilityWaitsForSelectedGroup(t *testing.T) {
	connection := readyWorkerConnection(t)
	manager := workerManager{
		workersByName: map[string]*WorkerClient{
			"first":  {Name: "first", Group: "assigned", Connection: connection, taskStreams: readyTaskStreams()},
			"second": {Name: "second", Group: "assigned", taskStreams: readyTaskStreams()},
			"common": {Name: "common", Group: "$", Connection: connection, taskStreams: readyTaskStreams()},
		},
		groupMapper: map[string]string{"current": "assigned"},
	}
	if manager.HasAvailableWorkers("current", nil) {
		t.Fatal("common workers cannot override an explicit group with an unready member")
	}
	manager.SetWorkerConnection(manager.workersByName["second"], connection)
	if !manager.HasAvailableWorkers("current", nil) {
		t.Fatal("the selected group must become available when all members are ready")
	}
	delete(manager.workersByName, "first")
	delete(manager.workersByName, "second")
	if manager.HasAvailableWorkers("current", nil) {
		t.Fatal("an empty explicit group cannot fall back to common workers")
	}
	manager.workersByName["graph-worker"] = &WorkerClient{
		Name: "graph-worker", Group: "graph-group", Connection: connection, taskStreams: readyTaskStreams(),
	}
	manager.groupMapper[manager.toOwnerKey("current", "graph")] = "graph-group"
	if !manager.HasAvailableWorkers("current", []string{"graph"}) {
		t.Fatal("an existing graph's explicit ready group remains an available workload")
	}
}

func TestWorkerAvailabilityTracksTaskStreamCancellationAndReplacement(t *testing.T) {
	worker := &WorkerClient{Name: "worker", Group: "$", Connection: readyWorkerConnection(t),
		taskStreams: readyTaskStreams()}
	manager := workerManager{workersByName: map[string]*WorkerClient{"worker": worker}}
	previous, cancel := context.WithCancel(context.Background())
	manager.SetTaskStream(worker, LoadTaskStream, previous)
	if !manager.IsWorkerReady(worker) {
		t.Fatal("complete worker must be schedulable")
	}
	cancel()
	if manager.IsWorkerReady(worker) || manager.HasAvailableWorkers("current", nil) {
		t.Fatal("a closed task stream must disable scheduling and advertised availability")
	}
	replacement, closeReplacement := context.WithCancel(context.Background())
	defer closeReplacement()
	manager.SetTaskStream(worker, LoadTaskStream, replacement)
	manager.ClearTaskStream(worker, LoadTaskStream, previous)
	if !manager.IsWorkerReady(worker) {
		t.Fatal("old stream cleanup must not clear its replacement")
	}
	manager.ClearTaskStream(worker, LoadTaskStream, replacement)
	if manager.IsWorkerReady(worker) {
		t.Fatal("cleared task stream must not be schedulable")
	}
	manager.workersByName[worker.Name] = &WorkerClient{Name: worker.Name}
	manager.SetTaskStream(worker, LoadTaskStream, replacement)
	if manager.IsWorkerReady(worker) {
		t.Fatal("a removed worker instance must not be schedulable")
	}
}
