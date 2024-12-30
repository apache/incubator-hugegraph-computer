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

package tasks

import (
	"fmt"
	"vermeer/apps/compute"
	"vermeer/apps/graphio"
	"vermeer/apps/options"
	"vermeer/apps/structure"

	. "vermeer/apps/master/workers"

	"github.com/sirupsen/logrus"
)

func MergeParams(target map[string]string, from map[string]string) {
	if target == nil || from == nil {
		return
	}

	for k, v := range from {
		target[k] = v
	}
}

func SetGraphProperties(graph *structure.VermeerGraph, params map[string]string) {
	if graph == nil || params == nil {
		return
	}
	graph.UseOutEdges = options.GetInt(params, "load.use_outedge") == 1
	graph.UseOutDegree = options.GetInt(params, "load.use_out_degree") == 1
	graph.UseProperty = options.GetInt(params, "load.use_property") == 1
	//有无向图功能时，无需out edges
	if options.GetInt(params, "load.use_undirected") == 1 {
		graph.UseOutEdges = true
	}
}

func ToGraphWorkers(workerClients []*WorkerClient) (workers []*structure.GraphWorker, workersName []string) {
	if workerClients == nil {
		return make([]*structure.GraphWorker, 0), make([]string, 0)
	}

	workers = make([]*structure.GraphWorker, 0, len(workerClients))
	workersName = make([]string, 0, len(workerClients))

	for _, w := range workerClients {
		gw := structure.GraphWorker{
			Name:        w.Name,
			VertexCount: 0,
		}
		workers = append(workers, &gw)
		workersName = append(workersName, w.Name)
	}

	return workers, workersName
}

// It's used to check the compatibility of a computer task with the graph and params
func CheckAlgoComputable(graph *structure.VermeerGraph, params map[string]string) error {
	if graph == nil {
		return fmt.Errorf("the argument `graph` is nil")
	}

	if params == nil {
		return fmt.Errorf("the argument `params` is nil")
	}

	algoName := computerTaskMgr.ExtractAlgo(params)
	if algoName == "" {
		return fmt.Errorf("failed to retrieve the algo name from argument: `params`")
	}

	// TODO: status->state, make sure that the enums for graph states are clear.
	// TODO: escape the check when creating the task
	if graph.State != structure.GraphStateLoaded && graph.State != structure.GraphStateOnDisk {
		return fmt.Errorf("graph status not correct %s/%s: %s", graph.SpaceName, graph.Name, graph.State)
	}

	maker := algorithmMgr.GetMaker(algoName)
	if maker == nil {
		return fmt.Errorf("algorithm not exists: %s", algoName)
	}
	dataNeeded := maker.DataNeeded()
	var algoProperty bool

	for _, dataName := range dataNeeded {
		switch dataName {
		//case algorithms.UseOutEdge:
		//	algoOutEdge = true
		//case algorithms.UseOutDegree:
		//	algoOutDegree = true
		case compute.UseProperty:
			algoProperty = true
		}
	}

	//if algoOutEdge && !graph.UseOutEdges {
	//	logrus.Warnf("algorithm %v : outedge missing", algoName)
	//	return fmt.Errorf("algorithm %v : outedge or undirected missing.\n", algoName)
	//}
	//if algoOutDegree && !graph.UseOutDegree {
	//	logrus.Warnf("algorithm %v : outdegree missing", algoName)
	//	return fmt.Errorf("algorithm %v : outdegree missing.\n", algoName)
	//}
	if algoProperty && !graph.UseProperty {
		logrus.Warnf("algorithm %v : property missing", algoName)
		return fmt.Errorf("algorithm %v : property missing.\n", algoName)
	}

	outputType := options.GetString(params, "output.type")
	if _, ok := graphio.LoadMakers[outputType]; !ok {
		return fmt.Errorf("no matched output.type:%s", outputType)
	}

	return nil
}
