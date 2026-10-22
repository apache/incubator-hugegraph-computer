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

func TestWorkerAvailabilityRespectsSpaceAllocation(t *testing.T) {
	connection := readyWorkerConnection(t)
	cases := []struct {
		name      string
		group     string
		mappings  map[string]string
		available bool
	}{
		{"unallocated named group", "default", nil, false},
		{"another space", "default", map[string]string{"other": "default"}, false},
		{"space allocation", "default", map[string]string{"current": "default"}, true},
		{"graph allocation", "default", map[string]string{"current graph": "default"}, true},
		{"other graph", "default", map[string]string{"current-other graph": "default"}, false},
		{"common worker", "$", nil, true},
		{"ungrouped worker", "", nil, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manager := workerManager{
				workersByName: map[string]*WorkerClient{
					"worker": {Name: "worker", Group: test.group, Connection: connection},
				},
				groupMapper: test.mappings,
			}
			before := maps.Clone(manager.groupMapper)
			if got := manager.HasAvailableWorkers("current"); got != test.available {
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
	if manager.HasAvailableWorkers("current") {
		t.Fatal("missing worker must not be ready")
	}
	manager.workersByName["worker"] = &WorkerClient{Group: "$"}
	if manager.HasAvailableWorkers("current") {
		t.Fatal("unconnected worker must not be ready")
	}
	worker := manager.workersByName["worker"]
	manager.SetWorkerConnection(worker, connection)
	if !manager.HasAvailableWorkers("current") {
		t.Fatal("registered ready worker must be available")
	}
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 1000; i++ {
			manager.HasAvailableWorkers("current")
		}
	}()
	for i := 0; i < 1000; i++ {
		manager.SetWorkerConnection(worker, nil)
		manager.SetWorkerConnection(worker, connection)
	}
	_ = connection.Close()
	wait.Wait()
	if manager.HasAvailableWorkers("current") {
		t.Fatal("disconnected worker must not be ready")
	}
}
