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

package schedules

import (
	"fmt"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"

	"vermeer/apps/master/workers"
)

type AgentStatus string

const (
	AgentStatusOk             AgentStatus = "ok"
	AgentStatusError          AgentStatus = "error"
	AgentStatusPending        AgentStatus = "pending"
	AgentStatusNoWorker       AgentStatus = "no_worker"
	AgentStatusWorkerNotReady AgentStatus = "worker_not_ready"
	AgentStatusAgentBusy      AgentStatus = "agent_busy"
	AgentStatusWorkerBusy     AgentStatus = "worker_busy"
)

type Agent struct {
	group string
	tasks map[int32]*structure.TaskInfo
}

func (a *Agent) GroupName() string {
	return a.group
}

func (a *Agent) AssignTask(taskInfo *structure.TaskInfo) {
	a.tasks[taskInfo.ID] = taskInfo
}

// Singleton
type Broker struct {
	structure.MutexLocker
	structure.Syncer
	agents map[string]*Agent
}

func (b *Broker) Init() *Broker {
	b.agents = make(map[string]*Agent)
	return b
}

func (b *Broker) AllAgents() []*Agent {
	res := make([]*Agent, 0)
	for _, a := range b.agents {
		res = append(res, a)
	}

	return res
}

func (b *Broker) ApplyAgent(taskInfo *structure.TaskInfo, forceApply ...bool) (*Agent, AgentStatus, map[string]*workers.WorkerClient, error) {
	if taskInfo == nil {
		return nil, AgentStatusError, nil, fmt.Errorf("taskInfo is nil")
	}

	defer b.Unlock(b.Lock())

	agent, workers, err := b.getAgent(taskInfo)
	if err != nil {
		return nil, AgentStatusError, nil, err
	}

	if agent == nil {
		return nil, AgentStatusPending, nil, nil
	}

	if workers == nil || len(workers) == 0 {
		return nil, AgentStatusNoWorker, nil, nil
	}

	if !b.isWorkersReady(workers) {
		logrus.Warnf("the workers of agent '%s' are not ready", agent.GroupName())
		return nil, AgentStatusWorkerNotReady, nil, nil
	}

	if !(forceApply != nil && len(forceApply) > 0 && forceApply[0]) {
		if b.isAgentBusy(agent) {
			return nil, AgentStatusAgentBusy, nil, nil
		}

		if b.isWorkerBusy(workers, agent) {
			return nil, AgentStatusWorkerBusy, nil, nil
		}
	}

	agent.AssignTask(taskInfo)

	return agent, AgentStatusOk, workers, nil
}

// func (b *Broker) isAgentReady(taskInfo *structure.TaskInfo, agent *Agent) bool {

// 	switch taskInfo.Type {
// 	case structure.TaskTypeLoad:
// 		fallthrough
// 	case structure.TaskTypeReload:
// 		return b.isWorkersReady(agent)
// 	case structure.TaskTypeCompute:
// 		fallthrough
// 	default:
// 		return true
// 	}
// }

func (b *Broker) isWorkersReady(workers map[string]*workers.WorkerClient) bool {
	ok := false
	for _, w := range workers {
		if w.Connection == nil {
			ok = false
			break
		}

		if w.Connection.GetState().String() == "READY" {
			ok = true
		} else {
			ok = false
			break
		}
	}

	return ok
}

func (b *Broker) isAgentBusy(agent *Agent) bool {
	busy := false

	for id, t := range agent.tasks {
		switch t.State {
		case structure.TaskStateLoaded:
			fallthrough
		case structure.TaskStateError:
			fallthrough
		case structure.TaskStateComplete:
			fallthrough
		case structure.TaskStateCanceled:
			busy = busy || false
			delete(agent.tasks, id)
		default:
			busy = true
		}

	}

	return busy
}

func (b *Broker) isWorkerBusy(workers map[string]*workers.WorkerClient, agent *Agent) bool {
	for _, a := range b.agents {
		if a == agent {
			continue
		}

		if !b.isAgentBusy(a) {
			continue
		}

		for _, t := range a.tasks {
			for _, w := range t.Workers {
				if _, ok := workers[w.Name]; ok {
					return true
				}
			}
		}
	}

	return false
}

func (b *Broker) getAgent(taskInfo *structure.TaskInfo) (*Agent, map[string]*workers.WorkerClient, error) {
	switch taskInfo.Type {
	case structure.TaskTypeLoad:
		fallthrough
	case structure.TaskTypeReload:
		return b.getAgentFromWorker(taskInfo)
	case structure.TaskTypeCompute:
		return b.getAgentFromGraph(taskInfo)
	default:
		return nil, nil, fmt.Errorf("unsupported task type: %s", taskInfo.Type)
	}

}

func (b *Broker) getAgentFromGraph(taskInfo *structure.TaskInfo) (*Agent, map[string]*workers.WorkerClient, error) {
	graph := graphMgr.GetGraphByName(taskInfo.SpaceName, taskInfo.GraphName)
	if graph == nil {
		return nil, nil, fmt.Errorf("failed to retrieve graph with name: %s/%s", taskInfo.SpaceName, taskInfo.GraphName)
	}

	if len(graph.Workers) == 0 {
		return nil, nil, fmt.Errorf("no workers found for graph with name: %s/%s", taskInfo.SpaceName, taskInfo.GraphName)
	}

	switch graph.State {
	case structure.GraphStateCreated:
		fallthrough
	case structure.GraphStateError:
		return nil, nil, fmt.Errorf("the graph is in an improper state `%s` for a compute task, graph : %s/%s", graph.State, graph.SpaceName, taskInfo.GraphName)
	case structure.GraphStateInComplete:
		fallthrough
	case structure.GraphStateLoading:
		return nil, nil, nil // waiting for the next check
	}

	workers := make(map[string]*workers.WorkerClient)

	for _, w := range graph.Workers {
		wc := workerMgr.GetWorker(w.Name)
		if wc == nil {
			logrus.Warnf("the worker '%s' held by graph '%s/%s' can not found", w.Name, graph.SpaceName, graph.Name)
			return nil, nil, nil
		}
		workers[w.Name] = wc
	}

	return b.retrieveAgent(graph.WorkerGroup), workers, nil

}

func (b *Broker) getAgentFromWorker(taskInfo *structure.TaskInfo) (*Agent, map[string]*workers.WorkerClient, error) {
	group := workerMgr.ApplyGroup(taskInfo.SpaceName, taskInfo.GraphName)
	return b.retrieveAgent(group), workerMgr.GroupWorkerMap(group), nil
}

func (b *Broker) retrieveAgent(group string) *Agent {
	if group == "" {
		group = "$"
	}

	agent := b.agents[group]

	if agent == nil {
		agent = &Agent{
			group: group,
			tasks: make(map[int32]*structure.TaskInfo, 0),
		}
		b.agents[group] = agent
	}

	return agent
}
