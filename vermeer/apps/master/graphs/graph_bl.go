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

package graphs

import (
	"context"
	"fmt"
	"sort"
	"vermeer/apps/common"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type GraphBl struct {
	Cred *structure.Credential
}

// GetGraph
func (gb *GraphBl) GetGraph(graphName string) (graph *structure.VermeerGraph, err error) {
	g := graphMgr.GetGraphByName(gb.Cred.Space(), graphName)
	if g == nil {
		return nil, fmt.Errorf("graph %v/%v not exist", gb.Cred.Space(), graphName)
	}
	g.Status = g.State.Converter()
	return g, nil
}

var graphStateCanDelete = map[structure.GraphState]struct{}{
	structure.GraphStateCreated:    {},
	structure.GraphStateError:      {},
	structure.GraphStateLoaded:     {},
	structure.GraphStateOnDisk:     {},
	structure.GraphStateInComplete: {},
}

// DeleteGraph Sending a delete request to the workers, while removing the graph data file.
func (gb *GraphBl) DeleteGraph(graphName string) error {
	graph, err := gb.GetGraph(graphName)
	if err != nil {
		return err
	}

	if _, ok := graphStateCanDelete[graph.State]; !ok {
		return fmt.Errorf("graph cannot delete, status: %s", graph.State)
	}

	//if atomic.LoadInt32(&graph.UsingNum) != 0
	if !graph.AlwaysUsing() && graph.GetUsingNum() != 0 {
		return fmt.Errorf("graph is busy, using_num: %v, graph: %s/%s", graph.GetUsingNum(), graph.SpaceName, graph.Name)
	}

	for _, worker := range graph.Workers {
		workerClient := workerMgr.GetWorker(worker.Name)
		req := pb.DeleteGraphReq{
			SpaceName:  gb.Cred.Space(),
			GraphName:  graphName,
			DeleteFile: true,
		}
		if workerClient == nil {
			logrus.Errorf("graph '%v/%v worker' '%v' not exist", gb.Cred.Space(), graphName, worker.Name)
			continue
		}
		_, err := workerClient.Session.DeleteGraph(context.Background(), &req)
		if err != nil {
			logrus.Warnf("worker delete graph err: %s, %s", worker.Name, graphName)
		}
	}

	graphMgr.DeleteGraph(gb.Cred.Space(), graphName)
	graphMgr.DeleteInfo(gb.Cred.Space(), graphName)

	return nil
}

// ReleaseGraph Sending a delete request to the workers, while preserving the graph data file.
func (gb *GraphBl) ReleaseGraph(graphName string) error {
	graph, err := gb.GetGraph(graphName)

	if err != nil {
		return err
	}

	for _, graphWorker := range graph.Workers {

		workerClient, err := workerMgr.GetWorkerByName(graphWorker.Name)

		if err != nil {
			logrus.Warnf("worker release graph err: worker with name '%s' not exists, graph: %s/%s", graphWorker.Name, graph.SpaceName, graph.Name)
			continue
		}

		if !workerMgr.CheckWorkerAlive(workerClient.Name) {
			logrus.Infof("aborted to release graph through a worker with the name: '%s', which is not alive, graph: %s/%s",
				graphWorker.Name, graph.SpaceName, graph.Name)
			continue
		}

		req := pb.DeleteGraphReq{
			SpaceName:  graph.SpaceName,
			GraphName:  graph.Name,
			DeleteFile: false,
		}

		_, err = workerClient.Session.DeleteGraph(context.Background(), &req)

		if err != nil {
			logrus.Warnf("worker release graph err, worker: %s, graph: %s/%s, error: %v", graphWorker.Name, graph.SpaceName, graph.Name, err)
		}
	}

	if graph.OnDisk {
		graph.SetState(structure.GraphStateInComplete)
		//graphMgr.ForceState(graph, structure.GraphStateInComplete)
		//graph.UsingNum = 0
		graph.ResetUsingNum()
	} else {
		graphMgr.DeleteGraph(graph.SpaceName, graph.Name)
		graphMgr.DeleteInfo(graph.SpaceName, graph.Name)
	}

	return nil
}

// GetGraphs
func (gb *GraphBl) GetGraphs() ([]*structure.VermeerGraph, error) {
	var graphs []*structure.VermeerGraph
	if gb.Cred.IsAdmin() {
		graphs = graphMgr.GetAllGraphs()
	} else {
		graphs = graphMgr.GetGraphs(gb.Cred.Space())
	}
	for _, graph := range graphs {
		graph.Status = graph.State.Converter()
	}
	return graphs, nil
}

// GetSpaceGraphs returns all graphs in the space (admin only)
func (gb *GraphBl) GetSpaceGraphs(space string) ([]*structure.VermeerGraph, error) {
	var graphs []*structure.VermeerGraph

	if !gb.Cred.IsAdmin() {
		return nil, fmt.Errorf("admin access only")
	}

	graphs = graphMgr.GetGraphs(space)

	for _, graph := range graphs {
		graph.Status = graph.State.Converter()
	}
	return graphs, nil
}

// AppendGraph
// Create a new graph and add it to the graph manager if it does not exist, indicated by name.
// Always return a graph pointer when no error is raised.
// If the name exists, the pointer will point to the old one.
func (gb *GraphBl) AppendGraph(graphName string, params map[string]string) (graph *structure.VermeerGraph, exists bool, err error) {
	if graphName == "" {
		return nil, false, fmt.Errorf("invalid graph name")
	}

	defer graphMgr.UnSync(graphMgr.Sync())

	graph = graphMgr.GetGraphByName(gb.Cred.Space(), graphName)
	if graph != nil {
		exists = true
		goto SAVE
	}

	graph, err = graphMgr.AddSpaceGraph(gb.Cred.Space(), graphName)
	if err != nil {
		exists = false
		return
	}
	goto SAVE

SAVE:
	gb.setGraphProperties(graph, params)

	if err = graphMgr.SaveInfo(gb.Cred.Space(), graph.Name); err != nil {
		graphMgr.DeleteGraph(gb.Cred.Space(), graphName)
		return nil, exists, err
	}

	return
}

func (gb *GraphBl) AppendGraphObj(graph *structure.VermeerGraph, params map[string]string) (exists bool, err error) {
	if graph == nil {
		return false, fmt.Errorf("graphObj is nil")
	}

	if graph.SpaceName != gb.Cred.Space() {
		return false, fmt.Errorf("the space name of the graph does not match with the credentials: %s", gb.Cred.Space())
	}

	defer graphMgr.UnSync(graphMgr.Sync())

	if g := graphMgr.GetGraphByName(gb.Cred.Space(), graph.Name); g != nil {
		exists = true
		err = nil
		goto SAVE
	}

	err = graphMgr.AddGraph(graph)
	if err != nil {
		return false, err
	}

	exists = false
	err = nil
	goto SAVE

SAVE:
	gb.setGraphProperties(graph, params)
	if err = graphMgr.SaveInfo(gb.Cred.Space(), graph.Name); err != nil {
		graphMgr.DeleteGraph(gb.Cred.Space(), graph.Name)
		return false, err
	}

	return
}

// CreateGraph
// deprecated to see AppendGraph
func (gb *GraphBl) CreateGraph(graphName string, params map[string]string) (*structure.VermeerGraph, error) {
	if graphName == "" {
		return nil, fmt.Errorf("invalid graph name")
	}

	graph, err := graphMgr.AddSpaceGraph(gb.Cred.Space(), graphName)
	if err != nil {
		return nil, err
	}

	gb.setGraphProperties(graph, params)

	if err = graphMgr.SaveInfo(gb.Cred.Space(), graph.Name); err != nil {
		graphMgr.DeleteGraph(gb.Cred.Space(), graphName)
		return nil, err
	}

	return graph, nil
}

func (gb *GraphBl) setGraphProperties(graph *structure.VermeerGraph, params map[string]string) {
	if graph == nil {
		return
	}

	graph.UseOutEdges = options.GetInt(params, "load.use_outedge") == 1
	graph.UseOutDegree = options.GetInt(params, "load.use_out_degree") == 1
	graph.UseProperty = options.GetInt(params, "load.use_property") == 1
	//有无向图功能时，无需out edges
	if options.GetInt(params, "load.use_undirected") == 1 {
		graph.UseOutEdges = true
	}

	graph.BackendOption.VertexDataBackend = options.GetString(params, "load.vertex_backend")

	// set always using
	graph.SetAlwaysUsing(options.GetInt(params, "load.always_using") == 1)

	graph.Params = params
}

// GetEdges
func (gb *GraphBl) GetEdges(graphName, vertexId, direction string) (inEdges, outEdges []string, inEdgeProperty []EdgeProperty, err error) {
	graph, err := gb.GetGraph(graphName)
	if err != nil {
		return nil, nil, nil, err
	}

	if graph.State != structure.GraphStateLoaded {
		return nil, nil, nil, fmt.Errorf("graph get edge, status: %s", graph.State)
	}

	if vertexId == "" {
		return nil, nil, nil, fmt.Errorf("vertex_id not exist: %s", vertexId)
	}

	workerIdx := common.HashBKDR(vertexId) % len(graph.Workers)
	workerClient := workerMgr.GetWorker(graph.Workers[workerIdx].Name)
	getEdgesResp, err := workerClient.Session.GetEdges(context.Background(),
		&pb.GetEdgesReq{SpaceName: gb.Cred.Space(), GraphName: graphName, VertexId: vertexId, Direction: direction})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("graph get edge, error: %w", err)
	}

	inEdges = getEdgesResp.InEdges
	outEdges = getEdgesResp.OutEdges
	//resp.BothEdges = getEdgesResp.BothEdges
	inEdgeProperty = make([]EdgeProperty, 0, len(getEdgesResp.InEdgeProperty))
	for _, edgeProp := range getEdgesResp.InEdgeProperty {
		inEdgeProperty = append(inEdgeProperty, EdgeProperty{edgeProp.Edge, edgeProp.Property})
	}

	return inEdges, outEdges, inEdgeProperty, nil
}

// GetVertices
func (gb *GraphBl) GetVertices(graphName string, vertexIds []string) (vertices []Vertex, err error) {
	graph, err := gb.GetGraph(graphName)
	if err != nil {
		return nil, err
	}

	//校验图状态
	if graph.State != structure.GraphStateLoaded {
		return nil, fmt.Errorf("graph get vertex, status: %s", graph.State)
	}

	//根据顶点hash值取模，组装各个worker需要的顶点数组
	vertexArr := make([][]string, len(graph.Workers))
	for _, vertexId := range vertexIds {
		workerIdx := common.HashBKDR(vertexId) % len(graph.Workers)
		vertexArr[workerIdx] = append(vertexArr[workerIdx], vertexId)
	}

	//遍历各个worker，获取对应点信息
	ch := make(chan VerticesGoResponse)
	for i := 0; i < len(graph.Workers); i++ {
		if vertexArr[i] != nil {
			go func(workerName string, arr []string) {
				vr := VerticesGoResponse{}
				workerClient := workerMgr.GetWorker(workerName)
				getVertexResp, err := workerClient.Session.GetVertex(context.Background(),
					&pb.GetVertexReq{SpaceName: gb.Cred.Space(), GraphName: graphName, VertexId: arr})
				vr.err = err
				vr.Vertices = make([]Vertex, 0, len(getVertexResp.GetVerts()))
				for _, vertexInfo := range getVertexResp.GetVerts() {
					vr.Vertices = append(vr.Vertices, Vertex{
						ID:       vertexInfo.ID,
						Property: vertexInfo.Property,
					})
				}
				ch <- vr
			}(graph.Workers[i].Name, vertexArr[i])
		}
	}

	//聚合点信息
	for i := 0; i < len(graph.Workers); i++ {
		if vertexArr[i] != nil {
			r := <-ch
			vertices = append(vertices, r.Vertices...)
			if r.err != nil {
				err = r.err
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("graph get vertex, error: %w", err)
	}

	sort.Slice(vertices, func(i, j int) bool {
		return vertices[i].ID < vertices[j].ID
	})

	return vertices, nil
}

// SaveIdle
// Save idle graphs to disk and unload them from RAM.
// The `graphs` parameter serves as a whitelist of graph names that will be exempted.
func (gb *GraphBl) SaveIdle(graphs ...string) {
	var empty = struct{}{}
	gset := make(map[string]any, len(graphs))
	for _, e := range graphs {
		gset[gb.Cred.Space()+"/"+e] = empty
	}
	groups := make(map[string]struct{})
	for _, graphName := range graphs {
		graph := graphMgr.GetGraphByName(gb.Cred.Space(), graphName)
		if graph != nil {
			groups[graph.WorkerGroup] = struct{}{}
		}
	}
	for group := range groups {
		for _, graph := range graphMgr.GetGraphsLoadedByGroup(group) {
			if gset[graph.SpaceName+"/"+graph.Name] != nil {
				continue
			}
			if graph.IsUsing() {
				continue
			}
			_, success := GraphPersistenceTask.Operate(graph.SpaceName, graph.Name, Save)
			if !success {
				logrus.Errorf("autoSave space:%v garph %v failed", graph.SpaceName, graph.Name)
				//return false, fmt.Errorf("failed to execute autoSave with graph: %s/%s", graph.SpaceName, graph.Name)
			} else {
				graph.OnDisk = true
				//graph.SetState(structure.GraphStateOnDisk)
				graphMgr.ForceState(graph, structure.GraphStateOnDisk)
				logrus.Infof("autoSave space:%v graph:%v success", graph.SpaceName, graph.Name)
			}
		}
	}
}

func (gb *GraphBl) Merge(graph *structure.VermeerGraph, params map[string]string) {

}

func (gb *GraphBl) checkOp(operation string) error {
	if !gb.Cred.IsAdmin() {
		return fmt.Errorf("permission required for this operation: %s", operation)
	}
	return nil
}

// HideParams 隐藏参数
func HideParams(graph *structure.VermeerGraph) *structure.VermeerGraph {
	newGraph := structure.VermeerGraph{}
	newGraph = *graph
	newGraph.Params = nil

	return &newGraph
}
