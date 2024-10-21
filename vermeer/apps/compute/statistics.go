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

package compute

import (
	"encoding/json"
	"math"
	"time"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type StatisticsType string

const (
	StatisticsTypeCount      StatisticsType = "count"
	StatisticsTypeModularity StatisticsType = "modularity"
	StatisticsTypeTopK       StatisticsType = "top_k"
	StatisticsTypeIgnore     StatisticsType = "ignore"
	// sketch statistics
	StatisticsTypeSketchDegree StatisticsType = "sketch_degree"
	StatisticsTypeSketchDepth  StatisticsType = "sketch_depth"
	StatisticsTypeSketchCount  StatisticsType = "sketch_count"
)

func StatisticsWorkerMaker(statisticsType StatisticsType) StatisticsWorker {
	switch statisticsType {
	case StatisticsTypeIgnore:
		return &StatisticsWorkerBase{}
	case StatisticsTypeCount:
		return &StatisticsCountWorker{}
	case StatisticsTypeModularity:
		return &StatisticsModularityWorker{}
	case StatisticsTypeTopK:
		return &StatisticsTopKWorker{}
	case StatisticsTypeSketchDegree:
		return &StatisticsSketchDegreeWorker{}
	case StatisticsTypeSketchDepth:
		return &StatisticsSketchDepthWorker{}
	case StatisticsTypeSketchCount:
		return &StatisticsSketchCountWorker{}
	}
	return nil
}

func StatisticsMasterMaker(statisticsType StatisticsType) StatisticsMaster {
	switch statisticsType {
	case StatisticsTypeIgnore:
		return &StatisticsMasterBase{}
	case StatisticsTypeCount:
		return &StatisticsCountMaster{}
	case StatisticsTypeModularity:
		return &StatisticsModularityMaster{}
	case StatisticsTypeTopK:
		return &StatisticsTopKMaster{}
	case StatisticsTypeSketchDegree:
		return &StatisticsSketchDegreeMaster{}
	case StatisticsTypeSketchDepth:
		return &StatisticsSketchDepthMaster{}
	case StatisticsTypeSketchCount:
		return &StatisticsSketchCountMaster{}
	}
	return nil
}

type StatisticsWorker interface {
	Init(params map[string]string)
	Collect(vertexID uint32, value serialize.MarshalAble)
	NeedCollectAll() bool
	CollectAll(vertexID uint32, value serialize.MarshalAble)
	Output() []byte
	MakeContext() *SWContext
}

type StatisticsMaster interface {
	Init(params map[string]string)
	Aggregate(data []byte)
	Output() map[string]any
	MakeContext() *SMContext
}

type SWContext struct {
	Graph *structure.GraphData
}

type StatisticsWorkerBase struct {
	sContext *SWContext
}

func (b *StatisticsWorkerBase) Init(params map[string]string) {
}
func (b *StatisticsWorkerBase) Collect(vertexID uint32, value serialize.MarshalAble) {
}
func (b *StatisticsWorkerBase) CollectAll(vertexID uint32, value serialize.MarshalAble) {
}
func (b *StatisticsWorkerBase) NeedCollectAll() bool {
	return false
}
func (b *StatisticsWorkerBase) Output() []byte {
	return nil
}
func (b *StatisticsWorkerBase) MakeContext() *SWContext {
	b.sContext = &SWContext{}
	return b.sContext
}

type SMContext struct {
	Graph *structure.VermeerGraph
}
type StatisticsMasterBase struct {
	sContext *SMContext
}

func (b *StatisticsMasterBase) Init(params map[string]string) {
}
func (b *StatisticsMasterBase) Aggregate(data []byte) {
}
func (b *StatisticsMasterBase) Output() map[string]any {
	return nil
}
func (b *StatisticsMasterBase) MakeContext() *SMContext {
	b.sContext = &SMContext{}
	return b.sContext
}

type StatisticsCountWorker struct {
	StatisticsWorkerBase
	count map[string]struct{}
}

func (b *StatisticsCountWorker) Init(params map[string]string) {
	_ = params
	b.count = make(map[string]struct{})
}

func (b *StatisticsCountWorker) Collect(vertexID uint32, value serialize.MarshalAble) {
	_ = vertexID
	b.count[value.ToString()] = struct{}{}
}

func (b *StatisticsCountWorker) Output() []byte {
	bytes, err := json.Marshal((*b).count)
	if err != nil {
		logrus.Errorf("json marshal error:%v", err)
		return nil
	}
	return bytes
}

type StatisticsCountMaster struct {
	StatisticsMasterBase
	count map[string]struct{}
}

func (b *StatisticsCountMaster) Init(params map[string]string) {
	_ = params
	b.count = make(map[string]struct{})
}

func (b *StatisticsCountMaster) Aggregate(data []byte) {
	d := make(map[string]struct{})
	err := json.Unmarshal(data, &d)
	if err != nil {
		logrus.Errorf("json unmarshal error:%v", err)
		return
	}
	for key := range d {
		b.count[key] = struct{}{}
	}
}

func (b *StatisticsCountMaster) Output() map[string]any {
	return map[string]any{"count": len(b.count)}
}

// todo

type community struct {
	//社区的总度数
	SigmaTot int64 `json:"sigma_tot"`
	//本社区内部的连接度权重之和
	KIn int64 `json:"k_in"`
}

type StatisticsModularityWorker struct {
	StatisticsWorkerBase
	communities map[string]*community
	totalResult []string
}

func (b *StatisticsModularityWorker) Init(params map[string]string) {
	b.communities = make(map[string]*community)
	b.totalResult = make([]string, b.sContext.Graph.Vertex.TotalVertexCount())
}

func (b *StatisticsModularityWorker) Collect(vertexID uint32, value serialize.MarshalAble) {
	vID := vertexID - b.sContext.Graph.VertIDStart
	if b.communities[value.ToString()] == nil {
		b.communities[value.ToString()] = &community{}
	}

	var count int64
	//if b.sContext.Graph.BothEdges != nil {
	//	count = int64(len(b.sContext.Graph.InEdges[vID]) + len(b.sContext.Graph.OutEdges[vID]))
	//} else
	if b.sContext.Graph.Edges.UseOutEdges() {
		count = int64(len(b.sContext.Graph.Edges.GetInEdges(vID)) + len(b.sContext.Graph.Edges.GetOutEdges(vID)))
	}
	b.communities[value.ToString()].SigmaTot += count
}

func (b *StatisticsModularityWorker) NeedCollectAll() bool {
	return true
}
func (b *StatisticsModularityWorker) CollectAll(vertexID uint32, value serialize.MarshalAble) {
	b.totalResult[vertexID] = value.ToString()
}

func (b *StatisticsModularityWorker) findSameComm(commID string, edges ...[]serialize.SUint32) int64 {
	var KIn int64
	for _, edge := range edges {
		for _, edgeID := range edge {
			if b.totalResult[edgeID] == commID {
				KIn++
			}
		}
	}
	return KIn
}

func (b *StatisticsModularityWorker) Output() []byte {
	for i := uint32(0); i < b.sContext.Graph.VertexCount; i++ {
		longVertexID := i + b.sContext.Graph.VertIDStart
		var KIn int64
		//if b.sContext.Graph.BothEdges != nil {
		//	KIn = b.findSameComm(b.totalResult[longVertexID], b.sContext.Graph.BothEdges[i])
		//} else
		if b.sContext.Graph.Edges.UseOutEdges() {
			KIn = b.findSameComm(b.totalResult[longVertexID], b.sContext.Graph.Edges.GetInEdges(i), b.sContext.Graph.Edges.GetOutEdges(i))
		}
		if b.communities[b.totalResult[longVertexID]] == nil {
			b.communities[b.totalResult[longVertexID]] = &community{}
		}
		b.communities[b.totalResult[longVertexID]].KIn += KIn
	}
	bytes, err := json.Marshal(b.communities)
	if err != nil {
		logrus.Errorf("json marshal error:%v", err)
		return nil
	}
	//logrus.Infof("%s", bytes)
	return bytes
}

type StatisticsModularityMaster struct {
	StatisticsMasterBase
	communities map[string]*community
}

func (b *StatisticsModularityMaster) Init(params map[string]string) {
	_ = params
	b.communities = make(map[string]*community)
}

func (b *StatisticsModularityMaster) Aggregate(data []byte) {
	//logrus.Infof("%s", data)
	comm := make(map[string]community)
	err := json.Unmarshal(data, &comm)
	if err != nil {
		logrus.Errorf("json unmarshal error:%v", err)
		return
	}
	for s, c := range comm {
		if b.communities[s] == nil {
			b.communities[s] = &community{}
		}
		b.communities[s].SigmaTot += c.SigmaTot
		b.communities[s].KIn += c.KIn
	}
}

func (b *StatisticsModularityMaster) Output() map[string]any {
	var modularity float64
	edgeNums := 2 * b.sContext.Graph.EdgeCount
	for _, c := range b.communities {
		modularity += float64(c.KIn)/float64(edgeNums) - float64(c.SigmaTot)/float64(edgeNums)*float64(c.SigmaTot)/float64(edgeNums)
	}
	return map[string]any{"modularity": modularity}
}

// todo: not implemented
type direction string

const (
	Asc  direction = "asc"
	Desc direction = "desc"
)

type StatisticsTopKWorker struct {
	StatisticsWorkerBase
}

type StatisticsTopKMaster struct {
	StatisticsMasterBase
}

type StatisticsSketchDegreeWorker struct {
	StatisticsWorkerBase
	degrees     map[int32]int64
	superVertex map[string]int32
}

func (sd *StatisticsSketchDegreeWorker) Init(params map[string]string) {
	_ = params
	sd.degrees = make(map[int32]int64, 100)
	sd.superVertex = make(map[string]int32, 100)
}
func (sd *StatisticsSketchDegreeWorker) Collect(vertexID uint32, value serialize.MarshalAble) {
	v, ok := value.(*serialize.SUint32)
	if !ok {
		logrus.Errorf("value is not serialize.SUint32")
	}
	vs := *v
	sd.degrees[int32(vs)]++
	if float64(vs) > float64(sd.sContext.Graph.EdgeCount)*0.01 {
		sd.superVertex[sd.sContext.Graph.Vertex.GetVertex(vertexID).ID] = int32(vs)
	}
}

func (sd *StatisticsSketchDegreeWorker) Output() []byte {
	result := make([][]byte, 2)
	degreeBytes, err := json.Marshal(sd.degrees)
	if err != nil {
		logrus.Errorf("json marshal error:%v", err)
		return nil
	}
	result[0] = degreeBytes
	superVertexBytes, err := json.Marshal(sd.superVertex)
	if err != nil {
		logrus.Errorf("json marshal error:%v", err)
		return nil
	}
	result[1] = superVertexBytes

	bytes, err := json.Marshal(result)
	if err != nil {
		logrus.Errorf("json marshal error:%v", err)
		return nil
	}
	return bytes
}

type StatisticsSketchDegreeMaster struct {
	StatisticsMasterBase
	degreeAvg   float64
	dispersions map[int32]float64
	minDegree   int32
	maxDegree   int32
	degrees     map[int32]int64
	afsUri      string
	afsFilePath string
	superVertex []map[string]int32
	edgeCount   int64
}

func (sd *StatisticsSketchDegreeMaster) Init(params map[string]string) {
	sd.afsUri = params["output.afs_uri"]
	sd.afsFilePath = params["output.file_path"]
	sd.minDegree = math.MaxInt32
	sd.maxDegree = math.MinInt32
	sd.degrees = make(map[int32]int64)
	sd.dispersions = make(map[int32]float64)
	sd.degreeAvg += float64(sd.sContext.Graph.EdgeCount) / float64(sd.sContext.Graph.VertexCount)
	sd.edgeCount = sd.sContext.Graph.EdgeCount
	sd.superVertex = make([]map[string]int32, 4)
	for i := range sd.superVertex {
		sd.superVertex[i] = make(map[string]int32)
	}
}
func (sd *StatisticsSketchDegreeMaster) Aggregate(data []byte) {
	result := make([][]byte, 2)
	err := json.Unmarshal(data, &result)
	if err != nil {
		logrus.Errorf("json unmarshal error:%v", err)
	}
	superVertex := make(map[string]int32)
	degrees := make(map[int32]int64)
	err = json.Unmarshal(result[0], &degrees)
	if err != nil {
		logrus.Errorf("json unmarshal error:%v", err)
	}
	err = json.Unmarshal(result[1], &superVertex)
	if err != nil {
		logrus.Errorf("json unmarshal error:%v", err)
	}

	for k, v := range degrees {
		if k < sd.minDegree {
			sd.minDegree = k
		}
		if k > sd.maxDegree {
			sd.maxDegree = k
		}
		sd.degrees[k] += v
		if _, ok := sd.dispersions[k]; !ok {
			sd.dispersions[k] = (float64(v) - sd.degreeAvg) * (float64(v) - sd.degreeAvg)
		}
	}
	for k, v := range superVertex {
		if float64(v) >= float64(sd.edgeCount)*0.01 && float64(v) < float64(sd.edgeCount)*0.03 {
			sd.superVertex[0][k] = v
		} else if float64(v) >= float64(sd.edgeCount)*0.03 && float64(v) < float64(sd.edgeCount)*0.05 {
			sd.superVertex[1][k] = v
		} else if float64(v) >= float64(sd.edgeCount)*0.05 && float64(v) < float64(sd.edgeCount)*0.1 {
			sd.superVertex[2][k] = v
		} else if float64(v) >= float64(sd.edgeCount)*0.1 {
			sd.superVertex[3][k] = v
		}
	}
}

func (sd *StatisticsSketchDegreeMaster) Output() map[string]any {
	result := make(map[string]any)
	result["afs_uri"] = sd.afsUri
	result["afs_file"] = sd.afsFilePath
	result["sketch_time"] = time.Now().Format(time.DateTime)
	result["vertex_count"] = sd.sContext.Graph.VertexCount
	result["edge_count"] = sd.sContext.Graph.EdgeCount
	result["degree_avg"] = sd.degreeAvg
	result["degree_min"] = sd.minDegree
	result["degree_max"] = sd.maxDegree
	result["isolated"] = sd.degrees[0]
	degreePercent := (sd.maxDegree-sd.minDegree)/100 + 1
	degrees := make([]int64, 100)
	for k, v := range sd.degrees {
		degrees[(k-sd.minDegree)/degreePercent] += v
	}
	result["degree"] = degrees
	dispersion := float64(0)
	for _, v := range sd.dispersions {
		dispersion += v
	}
	result["dispersion"] = math.Sqrt(dispersion)

	result["super_vertex"] = []map[string]int32{{}, {}, {}, {}}
	result["super_vertex_count"] = []int{0, 0, 0, 0}
	if sd.edgeCount > 100000 {
		result["super_vertex"] = sd.superVertex
		result["super_vertex_count"] = []int{len(sd.superVertex[0]), len(sd.superVertex[1]), len(sd.superVertex[2]), len(sd.superVertex[3])}
	}

	return result
}

type StatisticsSketchDepthWorker struct {
	StatisticsWorkerBase
	maxDepth serialize.SUint32
}

func (sd *StatisticsSketchDepthWorker) Init(params map[string]string) {
	_ = params
	sd.maxDepth = 0
}

func (sd *StatisticsSketchDepthWorker) Collect(vertexID uint32, value serialize.MarshalAble) {
	_ = vertexID
	v, ok := value.(*serialize.SUint32)
	if !ok {
		logrus.Errorf("value is not serialize.SUint32")
	}
	vs := *v
	if vs > sd.maxDepth {
		sd.maxDepth = vs
	}
}

func (sd *StatisticsSketchDepthWorker) Output() []byte {
	bytes := make([]byte, sd.maxDepth.PredictSize()+16)
	_, err := sd.maxDepth.Marshal(bytes)
	if err != nil {
		logrus.Errorf("marshal error:%v", err)
	}
	return bytes
}

type StatisticsSketchDepthMaster struct {
	StatisticsMasterBase
	maxDepth    serialize.SUint32
	afsUri      string
	afsFilePath string
}

func (sd *StatisticsSketchDepthMaster) Init(params map[string]string) {
	_ = params
	sd.maxDepth = 0
	sd.afsUri = params["output.afs_uri"]
	sd.afsFilePath = params["output.file_path"]
}

func (sd *StatisticsSketchDepthMaster) Aggregate(data []byte) {
	maxDepth := serialize.SUint32(0)
	_, err := maxDepth.Unmarshal(data)
	if err != nil {
		logrus.Errorf("unmarshal error:%v", err)
	}
	if maxDepth > sd.maxDepth {
		sd.maxDepth = maxDepth
	}
}

func (sd *StatisticsSketchDepthMaster) Output() map[string]any {
	return map[string]any{
		"depth_max":   sd.maxDepth,
		"afs_uri":     sd.afsUri,
		"afs_file":    sd.afsFilePath,
		"sketch_time": time.Now().Format(time.DateTime),
	}
}

type StatisticsSketchCountWorker struct {
	StatisticsWorkerBase
	count map[string]int64
}

func (sc *StatisticsSketchCountWorker) Init(params map[string]string) {
	_ = params
	sc.count = make(map[string]int64)
}
func (sc *StatisticsSketchCountWorker) Collect(vertexID uint32, value serialize.MarshalAble) {
	_ = vertexID
	sc.count[value.ToString()]++
}

func (sc *StatisticsSketchCountWorker) Output() []byte {
	bytes, err := json.Marshal((*sc).count)
	if err != nil {
		logrus.Errorf("json marshal error:%v", err)
		return nil
	}
	return bytes
}

type StatisticsSketchCountMaster struct {
	StatisticsMasterBase
	afsUri      string
	afsFilePath string
	count       map[string]int64
}

func (sc *StatisticsSketchCountMaster) Init(params map[string]string) {
	sc.afsUri = params["output.afs_uri"]
	sc.afsFilePath = params["output.file_path"]
	sc.count = make(map[string]int64)
}
func (sc *StatisticsSketchCountMaster) Aggregate(data []byte) {
	d := make(map[string]int64)
	err := json.Unmarshal(data, &d)
	if err != nil {
		logrus.Errorf("json unmarshal error:%v", err)
		return
	}
	for key, count := range d {
		sc.count[key] += count
	}
}

func (sc *StatisticsSketchCountMaster) Output() map[string]any {
	result := make(map[string]any)
	result["afs_uri"] = sc.afsUri
	result["afs_file"] = sc.afsFilePath
	result["sketch_time"] = time.Now().Format(time.DateTime)
	minCount := int64(math.MaxInt64)
	maxCount := int64(math.MinInt64)
	bigCommunity := 0
	nodeCount := (sc.sContext.Graph.VertexCount / 100)
	for _, v := range sc.count {
		if minCount > v {
			minCount = v
		}
		if maxCount < v {
			maxCount = v
		}
		if v > nodeCount && v > 100 {
			bigCommunity++
		}
	}
	result["count"] = len(sc.count)
	result["min_count"] = minCount
	result["max_count"] = maxCount
	communityPercent := (maxCount-minCount)/100 + 1
	community := make([]int64, 100)
	for _, v := range sc.count {
		community[(v-minCount)/communityPercent] += v
	}
	result["community"] = community
	result["big_community"] = bigCommunity
	return result
}
