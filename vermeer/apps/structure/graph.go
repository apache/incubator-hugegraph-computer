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
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"vermeer/apps/common"

	"github.com/sirupsen/logrus"
)

type GraphWorker struct {
	Name          string
	VertexCount   uint32
	VertIdStart   uint32
	EdgeCount     int64
	IsSelf        bool
	ScatterOffset uint32
}

type GraphPersistenceInfo struct {
	WorkerName string `json:"worker_name,omitempty"`
	Status     string `json:"status,omitempty"`
	Cost       string `json:"cost,omitempty"`
	ErrorInfo  string `json:"error,omitempty"`
}

type VermeerGraph struct {
	Syncer
	Name          string                 `json:"name,omitempty"`
	SpaceName     string                 `json:"space_name,omitempty"`
	Status        GraphStatus            `json:"status,omitempty"`
	State         GraphState             `json:"state,omitempty"`
	CreateTime    time.Time              `json:"create_time,omitempty"`
	UpdateTime    time.Time              `json:"update_time,omitempty"`
	VertexCount   int64                  `json:"vertex_count,omitempty"`
	EdgeCount     int64                  `json:"edge_count,omitempty"`
	Workers       []*GraphWorker         `json:"workers,omitempty"`
	WorkerGroup   string                 `json:"worker_group,omitempty"`
	Data          *GraphData             `json:"-"`
	Locker        sync.Mutex             `json:"-"`
	UsingState    GraphUsingState        `json:"using_state,omitempty"`
	UseOutEdges   bool                   `json:"use_out_edges,omitempty"`
	UseProperty   bool                   `json:"use_property,omitempty"`
	UseOutDegree  bool                   `json:"use_out_degree,omitempty"`
	OnDisk        bool                   `json:"on_disk,omitempty"`
	Params        map[string]string      `json:"params,omitempty"`
	DataDir       string                 `json:"data_dir,omitempty"`
	BackendOption GraphDataBackendOption `json:"backend_option,omitempty"`
	//UseUndirected bool           `json:"use_undirected,omitempty"`
}

func (vg *VermeerGraph) Init() {
	vg.Locker = sync.Mutex{}
	vg.CreateTime = time.Now()
	vg.UpdateTime = time.Now()
	vg.SetState(GraphStateCreated)
}

func (vg *VermeerGraph) SetState(state GraphState) {
	vg.State = state
	vg.UpdateTime = time.Now()
}

func (vg *VermeerGraph) MallocData(option GraphDataBackendOption) {
	vg.Data = &GraphData{}
	vg.BackendOption = option
	vg.Data.MallocData(option)
}
func (vg *VermeerGraph) SetOption(useOutEdges bool, useOutDegree bool, useProperty bool) {
	vg.UseOutEdges = useOutEdges
	vg.UseOutDegree = useOutDegree
	vg.UseProperty = useProperty
	vg.Data.SetOption(GraphDataOption{
		graphName:    vg.Name,
		spaceName:    vg.SpaceName,
		dataDir:      vg.DataDir,
		firstInit:    true,
		useOutEdges:  useOutEdges,
		useOutDegree: useOutDegree,
	})
}
func (vg *VermeerGraph) SetWorkerVertexCount(workerName string, count uint32, start uint32) {
	for i := range vg.Workers {
		if vg.Workers[i].Name == workerName {
			vg.Workers[i].VertexCount = count
			vg.Workers[i].VertIdStart = start
			vg.Workers[i].ScatterOffset = start
			return
		}
	}
	logrus.Warnf("SetWorkerVertexCount no worker: %s", workerName)
}

func (vg *VermeerGraph) SetWorkerEdgeCount(workerName string, count int64) {
	for i := range vg.Workers {
		if vg.Workers[i].Name == workerName {
			vg.Workers[i].EdgeCount = count
			return
		}
	}
	logrus.Warnf("SetWorkerEdgeCount no worker: %s", workerName)
}

func (vg *VermeerGraph) Statics() {
	vertSum := int64(0)
	edgeSum := int64(0)
	for i := range vg.Workers {
		vertSum += int64(vg.Workers[i].VertexCount)
		edgeSum += vg.Workers[i].EdgeCount
	}
	vg.VertexCount = vertSum
	vg.EdgeCount = edgeSum
	common.PrometheusMetrics.VertexCnt.WithLabelValues(vg.Name).Set(float64(vg.VertexCount))
	common.PrometheusMetrics.EdgeCnt.WithLabelValues(vg.Name).Set(float64(vg.EdgeCount))
}

func (vg *VermeerGraph) GetGraphWorker(workName string) *GraphWorker {
	for i := range vg.Workers {
		if vg.Workers[i].Name == workName {
			return vg.Workers[i]
		}
	}
	return nil
}

func (vg *VermeerGraph) GetSelfWorker() *GraphWorker {
	for i := range vg.Workers {
		if vg.Workers[i].IsSelf {
			return vg.Workers[i]
		}
	}
	return nil
}

func (vg *VermeerGraph) GetSelfIndex() int {
	for i := range vg.Workers {
		if vg.Workers[i].IsSelf {
			return i
		}
	}
	return -1
}

func (vg *VermeerGraph) DispatchVertexId() {
	sum := uint32(0)
	for i := range vg.Workers {
		vg.Workers[i].VertIdStart = sum
		sum += vg.Workers[i].VertexCount
	}
}

func (vg *VermeerGraph) VertexScattered() bool {
	for _, gw := range vg.Workers {
		if gw.ScatterOffset != gw.VertIdStart+gw.VertexCount {
			return false
		}
	}
	return true
}

func (vg *VermeerGraph) RecastVertex() {
	bTime := time.Now()

	vg.VertexCount = vg.Data.RecastCount(vg.Workers)
	vg.Data.Vertex.RecastVertex(vg.VertexCount, vg.Data.VertIDStart, vg.Workers)
	if vg.UseProperty && len(vg.Workers) > 1 {
		vg.Data.VertexProperty.Recast(vg.VertexCount, vg.Data.VertIDStart, vg.Data.VertexPropertySchema)
	}
	//vg.VertexCount = vg.Data.RecastVertex(vg.Workers, vg.UseProperty)
	logrus.Infof("recast vertex vSize: %d, cost: %v", vg.VertexCount, time.Since(bTime))
}

func (vg *VermeerGraph) BuildEdge(edgeNums int) {
	bTime := time.Now()
	vg.Data.Edges.BuildEdge(edgeNums, vg.Data.VertexCount)
	if vg.UseProperty {
		vg.Data.InEdgesProperty.Init(vg.Data.InEdgesPropertySchema, vg.Data.VertexCount)
	}
	logrus.Infof("build edge OK, cost: %v", time.Since(bTime))
}

func (vg *VermeerGraph) BuildTotalVertex() {
	bTime := time.Now()
	vg.Data.Vertex.BuildVertexMap()
	if vg.UseOutDegree {
		vg.Data.Edges.BuildOutDegree(vg.Data.Vertex.TotalVertexCount())
	}
	logrus.Infof("BuildTotalVertex vertex count: %d, cost: %v", vg.Data.Vertex.TotalVertexCount(), time.Since(bTime))
}

func (vg *VermeerGraph) OptimizeMemory() {
	bTime := time.Now()
	vg.Data.Edges.OptimizeEdgesMemory()
	if vg.UseProperty {
		vg.Data.InEdgesProperty.OptimizeMemory()
	}
	logrus.Infof("OptimizeEdgesMemory verts: %d, cost: %v", vg.Data.VertexCount, time.Since(bTime))
}

func (vg *VermeerGraph) SetDataDir(spaceName, graphName, workerName string) (string, error) {
	p, err := common.GetCurrentPath()
	if err != nil {
		return "", err
	}
	vg.DataDir = path.Join(p, "vermeer_data", "graph_data", spaceName, graphName, workerName)
	return vg.DataDir, nil
}

func (vg *VermeerGraph) Save(workerName string) error {
	graphMetaFile := path.Join(vg.DataDir, "graph_meta")
	dataDir := path.Join(vg.DataDir, "data")
	if common.IsFileOrDirExist(graphMetaFile) {

		b, e := readFromFile(graphMetaFile)
		if e != nil {
			return e
		}

		graph := new(VermeerGraph)
		e = json.Unmarshal(b, graph)
		if e != nil {
			return e
		}

		if vg.CreateTime.Unix() > graph.CreateTime.Unix() {
			e = vg.Data.Remove(dataDir)
			if e != nil {
				return e
			}
			e = vg.Data.Save(dataDir)
			if e != nil {
				return e
			}
		}

		if vg.UpdateTime.Unix() > graph.UpdateTime.Unix() {
			e = vg.SaveMeta(vg.DataDir)
			if e != nil {
				return e
			}
		}
		if vg.UseOutEdges && !graph.UseOutEdges || vg.UseOutDegree && !graph.UseOutDegree {
			e = vg.SaveMeta(vg.DataDir)
			if e != nil {
				return e
			}

			e = vg.Data.Remove(dataDir)
			if e != nil {
				return e
			}

			e = vg.Data.Save(dataDir)
			if e != nil {
				return e
			}
		}

		return nil
	}

	if !common.IsFileOrDirExist(graphMetaFile) {

		e := os.MkdirAll(vg.DataDir, os.ModePerm)
		if e != nil {
			return e
		}

		e = vg.SaveMeta(vg.DataDir)
		if e != nil {
			return e
		}

		e = vg.Data.Save(dataDir)
		if e != nil {
			return e
		}
	}

	return nil
}

func (vg *VermeerGraph) SaveMeta(dir string) error {
	fileName := path.Join(dir, "graph_meta")
	if common.IsFileOrDirExist(fileName) {
		err := os.Remove(fileName)
		if err != nil {
			return err
		}
	}
	b, err := json.Marshal(vg)
	if err != nil {
		return err
	}
	err = writeToFile(fileName, b)
	if err != nil {
		return err
	}
	return nil
}

func (vg *VermeerGraph) Read(workerName string) error {
	if !common.IsFileOrDirExist(vg.DataDir) {
		return errors.New("graph dir not exist,maybe graph not be saved")
	}

	fileName := path.Join(vg.DataDir, "graph_meta")
	logrus.Debugf(fileName)
	b, err := readFromFile(fileName)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, vg)
	if err != nil {
		return err
	}
	data := new(GraphData)
	data.MallocData(vg.BackendOption)
	data.SetOption(GraphDataOption{
		graphName:    vg.Name,
		spaceName:    vg.SpaceName,
		dataDir:      vg.DataDir,
		firstInit:    false,
		useOutEdges:  vg.UseOutEdges,
		useOutDegree: vg.UseOutDegree,
	})
	err = data.Load(vg.DataDir)
	if err != nil {
		return err
	}
	vg.Data = data
	vg.Locker = sync.Mutex{}

	return nil
}

type GraphUsingState struct {
	usingNum    int32 `json:"-"`
	UsingNum    int32 `json:"using_num,omitempty"` // Caution: only for json output
	AlwaysUsing bool  `json:"always_using,omitempty"`
}

func (vg *VermeerGraph) IsUsing() bool {
	return vg.GetUsingNum() > 0 || vg.UsingState.AlwaysUsing
}
func (vg *VermeerGraph) AlwaysUsing() bool {
	return vg.UsingState.AlwaysUsing
}

func (vg *VermeerGraph) SetAlwaysUsing(always bool) {
	vg.Locker.Lock()
	defer vg.Locker.Unlock()
	vg.UsingState.AlwaysUsing = always
}

func (vg *VermeerGraph) GetUsingNum() int32 {
	return atomic.LoadInt32(&vg.UsingState.usingNum)
}
func (vg *VermeerGraph) AddUsingNum() {
	atomic.AddInt32(&vg.UsingState.usingNum, 1)
	vg.UsingState.UsingNum = vg.UsingState.usingNum
}
func (vg *VermeerGraph) SubUsingNum() {
	atomic.AddInt32(&vg.UsingState.usingNum, -1)
	vg.UsingState.UsingNum = vg.UsingState.usingNum
}
func (vg *VermeerGraph) ResetUsingNum() {
	atomic.StoreInt32(&vg.UsingState.usingNum, 0)
	vg.UsingState.UsingNum = 0
}

func (vg *VermeerGraph) Remove() {
	if strings.Contains(vg.DataDir, path.Join("vermeer_data", "graph_data")) {
		err := os.RemoveAll(vg.DataDir)
		if err != nil {
			logrus.Errorf("remove graph data dir failed, err: %v", err)
		}
	}
}

func (vg *VermeerGraph) FreeMem() {
	if vg.Data != nil {
		vg.Data.FreeMem()
	}
}
