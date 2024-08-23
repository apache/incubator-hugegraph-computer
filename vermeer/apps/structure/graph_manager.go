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

package structure

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"vermeer/apps/common"
	storage "vermeer/apps/storage"

	"github.com/sirupsen/logrus"
)

var GraphManager = &graphManager{}

type graphManager struct {
	MutexLocker
	Syncer
	graphSpaceMap map[string]*GraphMap
	store         storage.Store
	delimiter     string
}

type GraphMap struct {
	graphs map[string]*VermeerGraph
	sync.Mutex
}

func (gm *graphManager) Init() {
	gm.delimiter = ":"
	if gm.graphSpaceMap != nil {
		for _, graphMap := range gm.graphSpaceMap {
			for _, v := range graphMap.graphs {
				v.FreeMem()
			}
		}
	}
	gm.graphSpaceMap = make(map[string]*GraphMap)
}

// AddSpaceGraph Create a new Graph and add it to this manager.
func (gm *graphManager) AddSpaceGraph(spaceName string, graphName string) (*VermeerGraph, error) {
	if spaceName == "" {
		return nil, errors.New("invalid spaceName")
	}
	if graphName == "" {
		return nil, errors.New("invalid graphName")
	}
	g := gm.CreateGraph(spaceName, graphName)
	if err := gm.AddGraph(g); err != nil {
		return nil, err
	}
	return g, nil
}

func (gm *graphManager) AddGraph(g *VermeerGraph) error {
	defer gm.Unlock(gm.Lock())
	graphMap := gm.graphSpaceMap[g.SpaceName]

	if graphMap == nil {
		graphMap = &GraphMap{graphs: make(map[string]*VermeerGraph)}
		gm.graphSpaceMap[g.SpaceName] = graphMap
	}

	if _, ok := graphMap.graphs[g.Name]; ok {
		return fmt.Errorf("graph name exists: %s/%s", g.SpaceName, g.Name)
	}
	common.PrometheusMetrics.GraphCnt.WithLabelValues().Inc()
	graphMap.graphs[g.Name] = g
	return nil
}

func (gm *graphManager) CreateGraph(spaceName string, graphName string) *VermeerGraph {
	g := VermeerGraph{}
	g.Init()
	g.Name = graphName
	g.SpaceName = spaceName
	return &g
}

func (gm *graphManager) GetAllGraphs() []*VermeerGraph {
	graphs := make([]*VermeerGraph, 0, 16)
	for _, space := range gm.graphSpaceMap {
		for _, g := range space.graphs {
			graphs = append(graphs, g)
		}
	}
	return graphs
}

func (gm *graphManager) GetGraphs(spaceName string) []*VermeerGraph {
	graphs := make([]*VermeerGraph, 0)
	if gm.graphSpaceMap[spaceName] == nil {
		return graphs
	}
	gm.graphSpaceMap[spaceName].Lock()
	defer gm.graphSpaceMap[spaceName].Unlock()

	if gm.graphSpaceMap[spaceName] == nil {
		return graphs
	}
	for _, v := range gm.graphSpaceMap[spaceName].graphs {
		graphs = append(graphs, v)
	}
	return graphs
}

func (gm *graphManager) GetGraphsLoaded(spaceName string) []*VermeerGraph {
	graphs := make([]*VermeerGraph, 0)
	if gm.graphSpaceMap[spaceName] == nil {
		return graphs
	}
	gm.graphSpaceMap[spaceName].Lock()
	defer gm.graphSpaceMap[spaceName].Unlock()

	for _, v := range gm.graphSpaceMap[spaceName].graphs {
		if v.State == GraphStateLoaded {
			graphs = append(graphs, v)
		}
	}
	return graphs
}

func (gm *graphManager) GetGraphsLoadedByGroup(workerGroup string) []*VermeerGraph {
	graphs := make([]*VermeerGraph, 0)
	for _, space := range gm.graphSpaceMap {
		if space == nil {
			continue
		}
		space.Lock()
		for _, g := range space.graphs {
			if g.State == GraphStateLoaded && g.WorkerGroup == workerGroup {
				graphs = append(graphs, g)
			}
		}
		space.Unlock()
	}
	return graphs
}

func (gm *graphManager) GetGraphByName(spaceName string, graphName string) *VermeerGraph {
	graphMap := gm.graphSpaceMap[spaceName]
	if graphMap == nil {
		return nil
	}
	graphMap.Lock()
	defer graphMap.Unlock()
	if graph, ok := graphMap.graphs[graphName]; ok {
		return graph
	}
	return nil
}

func (gm *graphManager) DeleteGraph(spaceName string, graphName string) {
	graphMap := gm.graphSpaceMap[spaceName]
	if graphMap == nil {
		return
	}

	graphMap.Lock()
	defer graphMap.Unlock()

	if graph, ok := graphMap.graphs[graphName]; ok {
		graph.FreeMem()
		delete(graphMap.graphs, graphName)
		common.PrometheusMetrics.GraphCnt.WithLabelValues().Dec()
		if graph.State == GraphStateLoaded {
			common.PrometheusMetrics.GraphLoadedCnt.WithLabelValues().Dec()
		}
		common.PrometheusMetrics.VertexCnt.DeleteLabelValues(graphName)
		common.PrometheusMetrics.EdgeCnt.DeleteLabelValues(graphName)
		logrus.Infof("graph deleted: %s", graphName)
	} else {
		logrus.Errorf("graph is not exists:%v", graphName)
	}
}

func (gm *graphManager) DeleteGraphFile(spaceName string, graphName string) {
	if gm.graphSpaceMap[spaceName] == nil {
		return
	}
	gm.graphSpaceMap[spaceName].Lock()
	defer gm.graphSpaceMap[spaceName].Unlock()

	p, err := common.GetCurrentPath()
	if err != nil {
		logrus.Errorf("DeleteGraph get current path error:%v", err)
	}

	dir := path.Join(p, "vermeer_data", "graph_data", spaceName, graphName)
	if !common.IsFileOrDirExist(dir) {
		logrus.Errorf("graph dir not exist,maybe graph has been deleted")
	}
	err = os.RemoveAll(dir)
	if err != nil {
		logrus.Errorf("DeleteGraph remove data error:%v", err)
	}
}

func (gm *graphManager) GetGraphsByWorker(workerName string) []*VermeerGraph {
	graphs := make([]*VermeerGraph, 0)
	for _, graphMap := range gm.graphSpaceMap {
		for _, graph := range graphMap.graphs {
			for _, worker := range graph.Workers {
				if worker.Name == workerName {
					graphs = append(graphs, graph)
					break
				}
			}
		}
	}
	return graphs
}

// SaveGraph 图数据落盘并释放内存
func (gm *graphManager) SaveGraph(spaceName string, graphName string, workerName string) error {
	gm.graphSpaceMap[spaceName].Lock()
	defer gm.graphSpaceMap[spaceName].Unlock()
	if graph, ok := gm.graphSpaceMap[spaceName].graphs[graphName]; ok {
		err := graph.Save(workerName)
		if err != nil {
			return err
		}
		graph.OnDisk = true
		graph.State = GraphStateOnDisk
		graph.FreeMem()
		gm.locker.Lock()
		delete(gm.graphSpaceMap[spaceName].graphs, graphName)
		gm.locker.Unlock()

	} else {
		logrus.Errorf("graph is not exists:%v", graphName)
		return errors.New("graph is not exists")
	}
	return nil
}

// WriteDisk 图数据落盘，但不删除内存中的图
func (gm *graphManager) WriteDisk(spaceName string, graphName string, workerName string) error {
	gm.graphSpaceMap[spaceName].Lock()
	defer gm.graphSpaceMap[spaceName].Unlock()
	if graph, ok := gm.graphSpaceMap[spaceName].graphs[graphName]; ok {
		err := graph.Save(workerName)
		if err != nil {
			return err
		}
		graph.OnDisk = true
	} else {
		logrus.Errorf("graph is not exists:%v", graphName)
		return errors.New("graph is not exists")
	}
	return nil
}

// ReadGraph 从磁盘中读取图数据并恢复
func (gm *graphManager) ReadGraph(spaceName string, graphName string, workerName string) error {
	gm.locker.Lock()
	if _, ok := gm.graphSpaceMap[spaceName]; !ok {
		gm.graphSpaceMap[spaceName] = &GraphMap{graphs: make(map[string]*VermeerGraph)}
	}
	gm.locker.Unlock()
	gm.graphSpaceMap[spaceName].Lock()
	defer gm.graphSpaceMap[spaceName].Unlock()
	if _, ok := gm.graphSpaceMap[spaceName].graphs[graphName]; ok {
		logrus.Errorf("graph %v exists, no need read", graphName)
		return fmt.Errorf("graph %v exists, no need read", graphName)
	}
	graph := new(VermeerGraph)
	graph.SpaceName = spaceName
	graph.Name = graphName
	_, err := graph.SetDataDir(spaceName, graphName, workerName)
	if err != nil {
		logrus.Errorf("graph %v set data dir error,err:%v", graphName, err)
		return fmt.Errorf("graph %v set data dir error,err:%w", graphName, err)
	}
	err = graph.Read(workerName)
	if err != nil {
		logrus.Errorf("graph %v read error,err:%v", graphName, err.Error())
		return fmt.Errorf("graph %v load error,err:%v", graphName, err.Error())
	}

	graph.State = GraphStateLoaded
	err = gm.AddGraph(graph)
	if err != nil {
		logrus.Errorf("add graph %v error,err:%v", graphName, err.Error())
		return fmt.Errorf("add graph %v error,err:%v", graphName, err.Error())
	}

	return nil
}

// InitStore master保存图信息 初始化DB
func (gm *graphManager) InitStore() error {
	p, err := common.GetCurrentPath()
	if err != nil {
		logrus.Errorf("get current path error:%v", err)
		return err
	}
	dir := path.Join(p, "vermeer_data", "graph_info")
	gm.store, err = storage.StoreMaker(storage.StoreOption{
		StoreName: storage.StoreTypePebble,
		Path:      dir,
		Fsync:     true,
	})
	if err != nil {
		return err
	}
	return nil
}

// SaveInfo master保存图信息 存储单个图信息
func (gm *graphManager) SaveInfo(spaceName string, graphName string) error {
	gm.locker.Lock()
	defer gm.locker.Unlock()

	spaceMap := gm.graphSpaceMap[spaceName]
	if spaceMap == nil {
		return fmt.Errorf("failed to save graph info because space '%s' does not exist", spaceName)
	}

	graph := spaceMap.graphs[graphName]

	if graph == nil {
		return fmt.Errorf("failed to save graph info because graph '%s/%s' does not exist", spaceName, graphName)
	}

	return gm.doSaveInfo(graph)
}

func (gm *graphManager) ForceState(graph *VermeerGraph, state GraphState) bool {
	if graph == nil {
		logrus.Error("GraphManager.ForceState: the argument `graph` is nil")
		return false
	}
	if state == "" {
		logrus.Error("GraphManager.ForceState: the argument `state` is empty")
		return false
	}

	defer gm.Unlock(gm.Lock())

	graph.SetState(state)

	if err := gm.doSaveInfo(graph); err != nil {
		logrus.Errorf("failed to save graph '%s/%s' state '%s', caused by: %v", graph.SpaceName, graph.Name, state, err)
		return false
	}

	return true
}

func (gm *graphManager) SetState(graph *VermeerGraph, state GraphState) error {
	if graph == nil {
		return fmt.Errorf("the argument `graph` is nil")
	}
	if state == "" {
		return fmt.Errorf("the argument `state` is empty")
	}

	defer gm.Unlock(gm.Lock())

	prevState := graph.State
	prevTime := graph.UpdateTime

	graph.SetState(state)

	if err := gm.doSaveInfo(graph); err != nil {
		graph.State = prevState
		graph.UpdateTime = prevTime

		logrus.Errorf("failed to save graph '%s/%s' state '%s', caused by: %v", graph.SpaceName, graph.Name, state, err)

		return err
	}

	return nil
}

func (gm *graphManager) SetError(graph *VermeerGraph) bool {
	if graph == nil {
		logrus.Errorf("GraphManager.SetError: the argument `graph` is nil")
		return false
	}

	defer gm.Unlock(gm.Lock())

	graph.SetState(GraphStateError)

	if err := gm.doSaveInfo(graph); err != nil {
		logrus.Errorf("failed to save graph '%s/%s' error state, caused by: %v", graph.SpaceName, graph.Name, err)
		return false
	}

	return true
}

func (gm *graphManager) SaveWorkerGroup(graph *VermeerGraph, workerGroup string) error {
	if graph == nil {
		return errors.New("the argument `graph` is nil")
	}
	if workerGroup == "" {
		return errors.New("the argument `workerGroup` is empty")
	}

	gm.locker.Lock()
	defer gm.locker.Unlock()

	prevGroup := graph.WorkerGroup
	graph.WorkerGroup = workerGroup

	if err := gm.doSaveInfo(graph); err != nil {
		graph.WorkerGroup = prevGroup
		return err
	}

	return nil
}

func (gm *graphManager) doSaveInfo(graph *VermeerGraph) error {
	bytes, err := json.Marshal(graph)
	if err != nil {
		return err
	}

	batch := gm.store.NewBatch()
	err = batch.Set([]byte(graph.SpaceName+gm.delimiter+graph.Name), bytes)
	if err != nil {
		return err
	}

	err = batch.Commit()
	if err != nil {
		return err
	}

	logrus.Infof("save graph info successfully: %v/%v", graph.SpaceName, graph.Name)

	return nil
}

// ReadInfo master从DB中恢复指定图信息
func (gm *graphManager) ReadInfo(spaceName string, graphName string) (*VermeerGraph, error) {
	gm.locker.Lock()
	defer gm.locker.Unlock()
	graphBytes, err := gm.store.Get([]byte(spaceName + gm.delimiter + graphName))
	if err != nil {
		return nil, err
	}
	graph := &VermeerGraph{}
	err = json.Unmarshal(graphBytes, graph)
	if err != nil {
		return nil, err
	}
	logrus.Infof("load graph info success:%v:%v", spaceName, graphName)
	return graph, nil
}

// ReadAllInfo master从DB中恢复所有图空间名与图名
func (gm *graphManager) ReadAllInfo() ([]*VermeerGraph, error) {
	gm.locker.Lock()
	defer gm.locker.Unlock()
	graphs := make([]*VermeerGraph, 0)
	scan := gm.store.Scan()
	for kv := range scan {
		graph := &VermeerGraph{}
		err := json.Unmarshal(kv.Value, graph)
		if err != nil {
			return nil, err
		}
		graphs = append(graphs, graph)
	}
	return graphs, nil
}

// DeleteInfo master从DB中删除指定图信息
func (gm *graphManager) DeleteInfo(spaceName string, graphName string) {
	gm.locker.Lock()
	defer gm.locker.Unlock()
	batch := gm.store.NewBatch()
	err := batch.Delete([]byte(spaceName + gm.delimiter + graphName))
	if err != nil {
		logrus.Errorf("delete space %v graph %v info error:%v", spaceName, graphName, err)
	}
	err = batch.Commit()
	if err != nil {
		logrus.Errorf("commit delete space %v graph %v info error:%v", spaceName, graphName, err)
	}
}
