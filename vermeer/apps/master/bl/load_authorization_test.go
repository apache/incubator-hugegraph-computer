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
	"testing"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/prometheus/client_golang/prometheus"
)

func isLoadPermissionError(err error) bool {
	return err != nil && err.Error() == "admin permission is required to load from HugeGraph"
}

func setLoadAuthorizationMode(t *testing.T, mode string) {
	t.Helper()
	previous := common.GetConfigDefault("auth", "none")
	common.SetConfig("auth", mode)
	t.Cleanup(func() { common.SetConfig("auth", previous) })
}

func TestHugegraphLoadAuthorization(t *testing.T) {
	setLoadAuthorizationMode(t, "token")
	previousGraphs := common.PrometheusMetrics.GraphCnt
	common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_load_authorization_graphs"}, nil)
	graphMgr.Init()
	taskMgr.Init()
	t.Cleanup(func() {
		graphMgr.Init()
		taskMgr.Init()
		common.PrometheusMetrics.GraphCnt = previousGraphs
	})
	graph := graphMgr.CreateGraph("own-space", "alias")
	graph.Params = map[string]string{"load.type": "hugegraph", "load.hugegraph_name": "source/graph/g"}
	if err := graphMgr.AddGraph(graph); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, user, taskType string
		params               map[string]string
		denied               bool
	}{
		{"user same-space source", "reader", "load", map[string]string{
			"load.type": "hugegraph", "load.hugegraph_name": "own-space/graph/g"}, true},
		{"user other-space source", "reader", "load", map[string]string{
			"load.type": "hugegraph", "load.hugegraph_name": "other-space/graph/g"}, true},
		{"caller params cannot set identity", "reader", "load", map[string]string{
			"load.type": "hugegraph", "create_user": "admin", "user": "admin"}, true},
		{"default user", structure.DefaultUserName, "load", map[string]string{"load.type": "hugegraph"}, true},
		{"admin load", "admin", "load", map[string]string{"load.type": "hugegraph"}, false},
		{"user explicit reload", "reader", "reload", map[string]string{"load.type": "hugegraph"}, true},
		{"user inherited reload", "reader", "reload", nil, true},
		{"admin inherited reload", "admin", "reload", nil, false},
		{"user CSV load", "reader", "load", map[string]string{"load.type": "csv"}, false},
		{"user CSV reload override", "reader", "reload", map[string]string{"load.type": "csv"}, false},
		{"user compute", "reader", "compute", map[string]string{"load.type": "hugegraph"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cred, err := structure.NewCredential(test.user, "own-space")
			if err != nil {
				t.Fatal(err)
			}
			creator, err := NewTaskCreator(cred, test.taskType)
			if err != nil {
				t.Fatal(err)
			}
			// This is the real creation preview used before duplicate checks, graph changes or persistence.
			task, err := creator.NewTaskInfo("alias", test.params, -1)
			if test.denied {
				if !isLoadPermissionError(err) || task != nil {
					t.Fatalf("unauthorized creation returned task=%v, err=%v", task, err)
				}
				// Both ordinary and batch admission must reject before graph/task side effects.
				for _, checked := range []bool{IsCheck, NoCheck} {
					if _, err := (&TaskBl{Cred: cred}).CreateTaskInfo(
						"alias", test.taskType, test.params, checked); !isLoadPermissionError(err) {
						t.Fatalf("checked=%v: unexpected admission error %v", checked, err)
					}
				}
			} else if err != nil || task.CreateUser != test.user || task.SpaceName != "own-space" {
				t.Fatalf("allowed creation lost authenticated identity: task=%v, err=%v", task, err)
			}

			// Restored and cron-copied tasks must be checked without relying on a live HTTP credential.
			restored := &structure.TaskInfo{CreateUser: test.user, SpaceName: "own-space",
				GraphName: "alias", Type: test.taskType, Params: test.params}
			starter, err := NewTaskStarter(restored, "$")
			if test.denied {
				if !isLoadPermissionError(err) || starter != nil {
					t.Fatalf("unauthorized execution returned starter=%v, err=%v", starter, err)
				}
			} else if err != nil || starter == nil {
				t.Fatalf("allowed execution rejected: %v", err)
			}
		})
	}
	if len(taskMgr.GetAllTasks(100)) != 0 || graphMgr.GetGraphByName("own-space", "alias") != graph {
		t.Fatal("rejected requests changed task or graph state")
	}
}

func TestHugegraphLoadAuthorizationRechecksReload(t *testing.T) {
	setLoadAuthorizationMode(t, "token")
	graphMgr.Init()
	t.Cleanup(graphMgr.Init)
	previousGraphs := common.PrometheusMetrics.GraphCnt
	common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_reload_authorization_graphs"}, nil)
	t.Cleanup(func() { common.PrometheusMetrics.GraphCnt = previousGraphs })
	graph := graphMgr.CreateGraph("own-space", "alias")
	graph.Params = map[string]string{"load.type": "csv"}
	if err := graphMgr.AddGraph(graph); err != nil {
		t.Fatal(err)
	}
	cred, _ := structure.NewCredential("reader", "own-space")
	creator, _ := NewTaskCreator(cred, structure.TaskTypeReload)
	task, err := creator.NewTaskInfo("alias", nil, -1)
	if err != nil {
		t.Fatal(err)
	}
	// A queued reload inherits the graph parameters at execution time.
	graph.Params = map[string]string{"load.type": "hugegraph"}
	if _, err := NewTaskStarter(task, "$"); !isLoadPermissionError(err) {
		t.Fatalf("queued reload inherited a privileged source without rejection: %v", err)
	}
}

func TestHugegraphLoadAuthorizationWithoutTokenAuth(t *testing.T) {
	setLoadAuthorizationMode(t, "none")
	taskMgr.Init()
	t.Cleanup(taskMgr.Init)
	creator, err := NewTaskCreator(structure.DefaultCredential(), structure.TaskTypeLoad)
	if err != nil {
		t.Fatal(err)
	}
	task, err := creator.NewTaskInfo("graph", map[string]string{"load.type": "hugegraph"}, -1)
	if err != nil {
		t.Fatalf("no-auth deployments must retain their existing trusted-caller behavior: %v", err)
	}
	if _, err := NewTaskStarter(task, "$"); err != nil {
		t.Fatal(err)
	}
	common.SetConfig("auth", "token")
	if _, err := NewTaskStarter(task, "$"); !isLoadPermissionError(err) {
		t.Fatalf("enabling token auth must reject an old non-admin load: %v", err)
	}
}
