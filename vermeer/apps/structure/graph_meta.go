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
	"strconv"
	"sync"
	"time"
	"vermeer/apps/common"

	"github.com/sirupsen/logrus"

	"vermeer/apps/serialize"
)

type GraphMeta struct {
	Name        string              `json:"name,omitempty"`
	VertIDStart uint32              `json:"vert_id_start,omitempty"`
	VertexCount uint32              `json:"vertex_count,omitempty"`
	TotalVertex serialize.PartsMeta `json:"total_vertex,omitempty"`
	//BothEdges       serialize.PartsMeta `json:"both_edges,omitempty"`
	InEdges         serialize.PartsMeta `json:"in_edges,omitempty"`
	OutEdges        serialize.PartsMeta `json:"out_edges,omitempty"`
	OutDegree       serialize.PartsMeta `json:"out_degree,omitempty"`
	VertexLongIDMap serialize.PartsMeta `json:"vertex_long_id_map,omitempty"`

	VertexPropertySchema  serialize.PartsMeta            `json:"vertex_property_schema,omitempty"`
	VertexProperty        map[string]serialize.PartsMeta `json:"vertex_property,omitempty"`
	InEdgesPropertySchema serialize.PartsMeta            `json:"in_edges_property_schema,omitempty"`
	InEdgesProperty       map[string]serialize.PartsMeta `json:"in_edges_property,omitempty"`
}

func SerializeToFile(m serialize.MarshalAble, size int, fileName string, group *sync.WaitGroup) {
	defer func() {
		group.Done()
		if r := recover(); r != nil {
			logrus.Errorf("SerializeToFile recover panic:%v, stack message: %s",
				r, common.GetCurrentGoroutineStack())
		}
	}()

	t := time.Now()
	buffer := make([]byte, size)
	_, err := m.Marshal(buffer)

	if err != nil {
		logrus.Errorf("SerializeToFile error,fileName=%v,err=%v", fileName, err)
	}

	err = writeToFile(fileName, buffer)
	if err != nil {
		logrus.Errorf("SerializeToFile error,fileName=%v,err=%v", fileName, err)
	}
	logrus.Debugf("SerializeToFile cost=%v,fileName=%v", time.Since(t), fileName)

}

func writeToFile(fileName string, buffer []byte) error {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0660))
	if err != nil {
		return err
	}
	defer f.Close()

	writer := serialize.NewWriter(f)
	_, err = writer.Write(buffer)
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func DeserializeFromFile(m serialize.MarshalAble, fileName string, group *sync.WaitGroup) {
	defer func() {
		group.Done()
		if r := recover(); r != nil {
			logrus.Errorf("DeserializeFromFile recover panic:%v, stack message: %s",
				r, common.GetCurrentGoroutineStack())
		}
	}()
	t := time.Now()
	buffer, err := readFromFile(fileName)
	if err != nil {
		logrus.Errorf("DeserializeFromFile error fileName=%v,error=%v", fileName, err)
	}
	_, err = m.Unmarshal(buffer)
	if err != nil {
		logrus.Errorf("DeserializeFromFile Unmarshal error, fileName=%v,error=%v", fileName, err)
	}
	logrus.Debugf("DeserializeFromFile cost=%v,fileName=%s", time.Since(t), fileName)

}

func readFromFile(fileName string) ([]byte, error) {
	f, err := os.Open(fileName)
	if err != nil {
		logrus.Errorf("open file err: %v", err)
		return nil, err
	}
	reader := serialize.NewReader(f)

	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		logrus.Errorf("stat file err: %v", err)
		return nil, err
	}
	fs := fi.Size()
	buffer := make([]byte, fs)
	_, err = reader.Read(buffer)
	if err != nil {
		logrus.Errorf("read file err: %v", err)
		return nil, err
	}
	return buffer, nil

}

type SerializeTask struct {
	m        serialize.MarshalAble
	size     int
	fileName string
}

const FileSizeLimit1 = 100 * 1024 * 1024
const FileSizeLimit2 = 20 * 1024 * 1024

func (gd *GraphData) Save(dir string) error {

	if !common.IsFileOrDirExist(dir) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	var graphMeta GraphMeta
	graphMeta.Name = gd.graphName
	graphMeta.VertIDStart = gd.VertIDStart
	graphMeta.VertexCount = gd.VertexCount

	wg := sync.WaitGroup{}

	if gd.VertexProperty != nil {
		wg.Add(1)
		go gd.VertexProperty.save(&graphMeta, dir, &wg)
		//saveVertexProperty(gd, &graphMeta, FileSizeLimit1, dir, &wg)
	}

	if gd.InEdgesProperty != nil {
		wg.Add(1)
		go gd.InEdgesProperty.save(&graphMeta, dir, &wg)
		//go saveInEdgesProperty(gd, &graphMeta, FileSizeLimit1, dir, &wg)
	}

	if gd.Vertex != nil {
		gd.Vertex.save(&graphMeta, dir, &wg)
	}

	if gd.Edges != nil {
		gd.Edges.save(&graphMeta, dir, &wg)
		//go saveInEdges(gd, &graphMeta, FileSizeLimit1, dir, &wg)
	}

	if gd.VertexPropertySchema.HgPSchema != nil || gd.VertexPropertySchema.Schema != nil {
		wg.Add(1)
		go gd.VertexPropertySchema.saveVertexPropertySchema(&graphMeta, dir, &wg)
		//go saveVertexPropertySchema(gd, &graphMeta, dir, &wg)
	}

	if gd.InEdgesPropertySchema.HgPSchema != nil || gd.InEdgesPropertySchema.Schema != nil {

		wg.Add(1)
		go gd.InEdgesPropertySchema.saveInEdgesPropertySchema(&graphMeta, dir, &wg)
		//go saveInEdgesPropertySchema(gd, &graphMeta, dir, &wg)

	}

	wg.Wait()
	mb, err := json.Marshal(graphMeta)
	if err != nil {
		return err
	}
	err = writeToFile(path.Join(dir, "data_meta"), mb)
	if err != nil {
		return nil
	}
	return nil
}

func (ps *PropertySchema) saveInEdgesPropertySchema(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	inEdgesPropertySchemaFile := "in_edges_property_schema"
	wg.Add(1)
	go SerializeToFile(ps, ps.PredictSize(), path.Join(dir, inEdgesPropertySchemaFile), wg)

	var meta serialize.PartsMeta
	meta.FilePrefix = inEdgesPropertySchemaFile
	meta.PartNum = 1
	graphMeta.InEdgesPropertySchema = meta
}

func (ps *PropertySchema) saveVertexPropertySchema(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	vertexPropertySchemaFile := "vertex_property_schema"
	wg.Add(1)
	go SerializeToFile(ps, ps.PredictSize(), path.Join(dir, vertexPropertySchemaFile), wg)
	var meta serialize.PartsMeta
	meta.FilePrefix = vertexPropertySchemaFile
	meta.PartNum = 1
	graphMeta.VertexPropertySchema = meta

}
func (em *EdgeMem) save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup) {
	if em.InEdges != nil {
		wg.Add(1)
		go em.saveInEdges(graphMeta, FileSizeLimit1, dir, wg)
	}

	if em.OutEdges != nil {
		wg.Add(1)
		go em.saveOutEdges(graphMeta, FileSizeLimit1, dir, wg)
	}

	if em.OutDegree != nil {
		wg.Add(1)
		go em.saveOutDegree(graphMeta, FileSizeLimit1, dir, wg)
	}
}

func (em *EdgeMem) saveOutDegree(graphMeta *GraphMeta, fileLimit int, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	outDegree := serialize.SliceUint32(em.OutDegree)
	tt := time.Now()
	parts := outDegree.Partition(fileLimit)
	logrus.Debugf("Serialize OutDegree  Partition cost=%v", time.Since(tt))

	filePrefix := "out_degree_"
	for id, one := range parts {
		part := outDegree[one.Start:one.End]
		wg.Add(1)
		go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)

	}
	var meta serialize.PartsMeta
	meta.FilePrefix = filePrefix
	meta.PartNum = len(parts)
	graphMeta.OutDegree = meta
}

func (em *EdgeMem) saveOutEdges(graphMeta *GraphMeta, fileLimit int, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	tt := time.Now()
	parts := em.OutEdges.Partition(fileLimit)
	logrus.Debugf("Serialize OutEdges  Partition cost=%v", time.Since(tt))

	filePrefix := "out_edges_"
	for id, one := range parts {
		part := em.OutEdges[one.Start:one.End]
		wg.Add(1)
		go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)

	}
	var meta serialize.PartsMeta
	meta.FilePrefix = filePrefix
	meta.PartNum = len(parts)
	graphMeta.OutEdges = meta
}

func (em *EdgeMem) saveInEdges(graphMeta *GraphMeta, fileLimit int, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	tt := time.Now()
	parts := em.InEdges.Partition(fileLimit)
	logrus.Debugf("Serialize InEdges  Partition cost=%v", time.Since(tt))

	filePrefix := "in_edges_"
	for id, one := range parts {
		part := em.InEdges[one.Start:one.End]
		wg.Add(1)
		go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
	}
	var meta serialize.PartsMeta
	meta.FilePrefix = filePrefix
	meta.PartNum = len(parts)
	graphMeta.InEdges = meta
}

func (vm *VertexMem) save(graphMeta *GraphMeta, dir string, wg *sync.WaitGroup) {
	if vm.VertexLongIDMap != nil {
		wg.Add(1)
		go vm.saveVertexLongIDMap(graphMeta, FileSizeLimit2, dir, wg)
	}

	if vm.TotalVertex != nil {
		wg.Add(1)
		go vm.saveTotalVertex(graphMeta, FileSizeLimit1, dir, wg)
	}
}

func (vm *VertexMem) saveTotalVertex(graphMeta *GraphMeta, fileLimit int, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	totalVertex := SliceVertex(vm.TotalVertex)
	tt := time.Now()
	parts := totalVertex.Partition(fileLimit)
	logrus.Debugf("Serialize TotalVertex  Partition cost=%v", time.Since(tt))

	filePrefix := "total_vertex_"
	for id, one := range parts {
		part := totalVertex[one.Start:one.End]
		wg.Add(1)
		go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)

	}
	var meta serialize.PartsMeta
	meta.FilePrefix = filePrefix
	meta.PartNum = len(parts)
	graphMeta.TotalVertex = meta
}

func (vm *VertexMem) saveVertexLongIDMap(graphMeta *GraphMeta, fileLimit int, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	length := len(vm.VertexLongIDMap)
	t1 := time.Now()
	s := make([]serialize.KVpair, length)
	index := 0
	for k, v := range vm.VertexLongIDMap {
		entry := serialize.KVpair{K: k, V: v}
		s[index] = entry
		index++
	}
	logrus.Debugf("Serialize vertexLongIDMap map to slice cost=%v,s length=%v", time.Since(t1), len(s))

	filePrefix := "vertex_long_id_map_"
	vertexLongIDSlice := serialize.SliceKVpair(s)
	parts := vertexLongIDSlice.Partition(fileLimit)
	logrus.Debugf("Serialize vertexLongIDMap parts=%v", parts)
	for id, one := range parts {
		part := vertexLongIDSlice[one.Start:one.End]
		wg.Add(1)
		go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
	}

	var meta serialize.PartsMeta
	meta.FilePrefix = filePrefix
	meta.PartNum = len(parts)
	graphMeta.VertexLongIDMap = meta
}

func (vp *VertexProperties) save(meta *GraphMeta, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.Now()
	var vertexPropertyMeta = make(map[string]serialize.PartsMeta)
	fileLimit := FileSizeLimit1
	for k, v := range *vp {
		var partMeta serialize.PartsMeta

		filePrefix := "vertex_property_" + k + "_"
		partMeta.FilePrefix = filePrefix

		switch v.VType {
		case ValueTypeInt32:
			{
				values := serialize.SliceInt32(v.Values.([]serialize.SInt32))
				tt := time.Now()
				parts := values.Partition(fileLimit)
				logrus.Debugf("Serialize VertexProperty ValueTypeInt32 Partition cost=%v", time.Since(tt))

				partMeta.PartNum = len(parts)
				partMeta.ValueType = uint16(ValueTypeInt32)

				for id, one := range parts {
					part := values[one.Start:one.End]
					wg.Add(1)
					go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
				}

			}

		case ValueTypeFloat32:
			{
				values := serialize.SliceFloat32(v.Values.([]serialize.SFloat32))
				tt := time.Now()
				parts := values.Partition(fileLimit)
				logrus.Debugf("Serialize VertexProperty ValueTypeFloat32 Partition cost=%v", time.Since(tt))

				partMeta.PartNum = len(parts)
				partMeta.ValueType = uint16(ValueTypeFloat32)

				for id, one := range parts {
					part := values[one.Start:one.End]
					wg.Add(1)
					go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
				}

			}
		case ValueTypeString:
			{
				values := serialize.SliceString(v.Values.([]serialize.SString))
				tt := time.Now()
				parts := values.Partition(fileLimit)
				logrus.Debugf("Serialize VertexProperty ValueTypeString Partition cost=%v", time.Since(tt))

				partMeta.PartNum = len(parts)
				partMeta.ValueType = uint16(ValueTypeString)

				for id, one := range parts {
					part := values[one.Start:one.End]
					wg.Add(1)
					go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
				}
			}
		}
		vertexPropertyMeta[k] = partMeta
	}
	meta.VertexProperty = vertexPropertyMeta

	logrus.Debugf("Serialize serializeVertexProperty2  cost=%v", time.Since(t))

}

func (ep *EdgeProperties) save(meta *GraphMeta, dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.Now()
	var inEdgesPropertyMeta = make(map[string]serialize.PartsMeta)
	fileLimit := FileSizeLimit1
	for k, v := range *ep {
		var partMeta serialize.PartsMeta
		filePrefix := "in_edges_property_" + k + "_"
		partMeta.FilePrefix = filePrefix

		switch v.VType {
		case ValueTypeInt32:
			{
				values := serialize.TwoDimSliceInt32(v.Values.([][]serialize.SInt32))
				tt := time.Now()
				parts := values.Partition(fileLimit)
				logrus.Debugf("Serialize InEdgesProperty ValueTypeInt32 Partition cost=%v", time.Since(tt))

				partMeta.PartNum = len(parts)
				partMeta.ValueType = uint16(ValueTypeInt32)

				for id, one := range parts {
					part := values[one.Start:one.End]
					wg.Add(1)
					go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
				}

			}

		case ValueTypeFloat32:
			{
				values := serialize.TwoDimSliceFloat32(v.Values.([][]serialize.SFloat32))
				tt := time.Now()
				parts := values.Partition(fileLimit)
				logrus.Debugf("Serialize InEdgesProperty ValueTypeFloat32 Partition cost=%v", time.Since(tt))
				partMeta.PartNum = len(parts)
				partMeta.ValueType = uint16(ValueTypeFloat32)

				for id, one := range parts {
					part := values[one.Start:one.End]
					wg.Add(1)
					go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
				}

			}
		case ValueTypeString:
			{
				values := serialize.TwoDimSliceString(v.Values.([][]serialize.SString))
				tt := time.Now()
				parts := values.Partition(fileLimit)
				logrus.Debugf("Serialize InEdgesProperty ValueTypeString Partition cost=%v", time.Since(tt))

				partMeta.PartNum = len(parts)
				partMeta.ValueType = uint16(ValueTypeString)

				for id, one := range parts {
					part := values[one.Start:one.End]
					wg.Add(1)
					go SerializeToFile(&part, one.Size, path.Join(dir, filePrefix)+strconv.Itoa(id), wg)
				}
			}
		}
		inEdgesPropertyMeta[k] = partMeta
	}
	meta.InEdgesProperty = inEdgesPropertyMeta
	logrus.Debugf("Serialize serializeInEdgesProperty2   cost=%v", time.Since(t))

}

func (gd *GraphData) Load(dir string) error {

	if len(gd.graphName) == 0 {
		return errors.New("need db name")
	}

	dataDir := path.Join(dir, "data")

	metaFile := path.Join(dataDir, "data_meta")
	buffer, err := readFromFile(metaFile)
	if err != nil {
		return err
	}

	var meta GraphMeta
	err = json.Unmarshal(buffer, &meta)
	if err != nil {
		return err
	}

	gd.graphName = meta.Name
	gd.VertIDStart = meta.VertIDStart
	gd.VertexCount = meta.VertexCount

	wg := sync.WaitGroup{}

	gd.Vertex.load(meta, dataDir, &wg)
	gd.Edges.load(meta, dataDir, &wg)
	if meta.VertexPropertySchema.PartNum != 0 {
		wg.Add(1)
		go gd.VertexPropertySchema.load(&wg, meta.VertexPropertySchema.FilePrefix, dataDir)
	}

	if meta.InEdgesPropertySchema.PartNum != 0 {
		wg.Add(1)
		go gd.InEdgesPropertySchema.load(&wg, meta.InEdgesPropertySchema.FilePrefix, dataDir)
	}

	if len(meta.VertexProperty) != 0 {
		wg.Add(1)
		go gd.VertexProperty.load(&wg, meta, dataDir)
	}

	if len(meta.InEdgesProperty) != 0 {
		wg.Add(1)
		go gd.InEdgesProperty.load(&wg, meta, dataDir)
	}

	wg.Wait()

	return nil
}

func (ps *PropertySchema) load(wg *sync.WaitGroup, filePrefix string, dir string) {
	defer wg.Done()
	w := sync.WaitGroup{}
	fileName := path.Join(dir, filePrefix)
	w.Add(1)
	go DeserializeFromFile(ps, fileName, &w)
	w.Wait()
}

func (em *EdgeMem) load(meta GraphMeta, dataDir string, wg *sync.WaitGroup) {
	if meta.InEdges.PartNum != 0 {
		wg.Add(1)
		go em.loadInEdges(wg, meta, dataDir)
	}

	if meta.OutEdges.PartNum != 0 {
		wg.Add(1)
		go em.loadOutEdges(wg, meta, dataDir)
	}

	if meta.OutDegree.PartNum != 0 {
		wg.Add(1)
		go em.loadOutDegree(wg, meta, dataDir)
	}
}

func (em *EdgeMem) loadOutDegree(wg *sync.WaitGroup, meta GraphMeta, dir string) {
	tt := time.Now()
	defer wg.Done()
	w := sync.WaitGroup{}
	partsNum := meta.OutDegree.PartNum
	p := make([]serialize.SliceUint32, partsNum)
	for i := 0; i < partsNum; i++ {
		fileName := path.Join(dir, meta.OutDegree.FilePrefix) + strconv.Itoa(i)
		w.Add(1)
		go DeserializeFromFile(&p[i], fileName, &w)
	}
	w.Wait()
	ttt := time.Now()
	length := 0
	for _, s := range p {
		length += len(s)
	}
	outDegree := make([]serialize.SUint32, length)
	offset := 0
	for _, s := range p {
		n := copy(outDegree[offset:], s)
		offset += n
	}
	em.OutDegree = outDegree
	logrus.Debugf("Deserialize outDegree wait cost=%v", time.Since(ttt))
	logrus.Debugf("Deserialize OutDegree cost=%v", time.Since(tt))
}

func (em *EdgeMem) loadOutEdges(wg *sync.WaitGroup, meta GraphMeta, dir string) {
	tt := time.Now()
	defer wg.Done()
	w := sync.WaitGroup{}
	partsNum := meta.OutEdges.PartNum
	p := make([]serialize.TwoDimSliceUint32, partsNum)
	for i := 0; i < partsNum; i++ {
		fileName := path.Join(dir, meta.OutEdges.FilePrefix) + strconv.Itoa(i)
		w.Add(1)
		go DeserializeFromFile(&p[i], fileName, &w)
	}
	w.Wait()
	ttt := time.Now()

	length := 0
	for _, s := range p {
		length += len(s)
	}
	outEdges := make([]serialize.SliceUint32, length)
	offset := 0
	for _, s := range p {
		n := copy(outEdges[offset:], s)
		offset += n
	}
	em.OutEdges = outEdges
	logrus.Debugf("Deserialize outEdges wait cost=%v", time.Since(ttt))
	logrus.Debugf("Deserialize OutEdges cost=%v", time.Since(tt))
}

func (em *EdgeMem) loadInEdges(wg *sync.WaitGroup, meta GraphMeta, dir string) {
	tt := time.Now()
	defer wg.Done()
	w := sync.WaitGroup{}
	partsNum := meta.InEdges.PartNum
	p := make([]serialize.TwoDimSliceUint32, partsNum)
	for i := 0; i < partsNum; i++ {
		fileName := path.Join(dir, meta.InEdges.FilePrefix) + strconv.Itoa(i)
		w.Add(1)
		go DeserializeFromFile(&p[i], fileName, &w)
	}
	w.Wait()
	ttt := time.Now()
	length := 0
	for _, s := range p {
		length += len(s)
	}
	inEdges := make([]serialize.SliceUint32, length)
	offset := 0
	for _, s := range p {
		n := copy(inEdges[offset:], s)
		offset += n
	}
	em.InEdges = inEdges
	logrus.Debugf("Deserialize InEdges wait cost=%v", time.Since(ttt))
	logrus.Debugf("Deserialize InEdges cost=%v", time.Since(tt))
}

func (vm *VertexMem) load(meta GraphMeta, dataDir string, wg *sync.WaitGroup) {
	if meta.VertexLongIDMap.PartNum != 0 {
		wg.Add(1)
		go vm.loadVertexLongIDMap(wg, meta, dataDir)
	}

	if meta.TotalVertex.PartNum != 0 {
		wg.Add(1)
		go vm.loadTotalVertex(wg, meta, dataDir)
	}
}

func (vm *VertexMem) loadTotalVertex(wg *sync.WaitGroup, meta GraphMeta, dir string) {
	tt := time.Now()
	defer wg.Done()
	w := sync.WaitGroup{}
	partsNum := meta.TotalVertex.PartNum
	p := make([]SliceVertex, partsNum)
	for i := 0; i < partsNum; i++ {
		fileName := path.Join(dir, meta.TotalVertex.FilePrefix) + strconv.Itoa(i)
		w.Add(1)
		go DeserializeFromFile(&p[i], fileName, &w)
	}
	w.Wait()

	length := 0
	for _, s := range p {
		length += len(s)
	}
	totalVertex := make([]Vertex, length)
	offset := 0
	for _, s := range p {
		n := copy(totalVertex[offset:], s)
		offset += n
	}
	vm.TotalVertex = totalVertex
	logrus.Debugf("Deserialize TotalVertex cost=%v", time.Since(tt))
}

func (vm *VertexMem) loadVertexLongIDMap(wg *sync.WaitGroup, meta GraphMeta, dir string) {
	defer wg.Done()
	tt := time.Now()
	w := sync.WaitGroup{}
	partsNum := meta.VertexLongIDMap.PartNum
	p := make([]serialize.SliceKVpair, partsNum)
	for i := 0; i < partsNum; i++ {
		fileName := path.Join(dir, meta.VertexLongIDMap.FilePrefix) + strconv.Itoa(i)
		w.Add(1)
		go DeserializeFromFile(&p[i], fileName, &w)
	}
	w.Wait()

	length := 0
	for _, s := range p {
		length += len(s)
	}
	t1 := time.Now()
	vertexLongIDMap := make(map[string]uint32, length)
	for _, s := range p {
		for _, v := range s {
			vertexLongIDMap[v.K] = v.V
		}
	}
	logrus.Debugf("Deserialize VertexLongIDMap build map cost=%v", time.Since(t1))

	vm.VertexLongIDMap = vertexLongIDMap
	logrus.Debugf("Deserialize VertexLongIDMap cost=%v", time.Since(tt))

}

func (vp *VertexProperties) load(wg *sync.WaitGroup, meta GraphMeta, dir string) {

	defer wg.Done()
	tt := time.Now()
	vertexProperty := make(map[string]*VValues)
	ww := sync.WaitGroup{}
	rwLock := sync.RWMutex{}

	for k, v := range meta.VertexProperty {
		ww.Add(1)
		go func(wg *sync.WaitGroup, kk string, vv serialize.PartsMeta) {
			defer wg.Done()
			vValues := new(VValues)
			vValues.VType = ValueType(vv.ValueType)
			switch vValues.VType {
			case ValueTypeInt32:
				{
					partsNum := vv.PartNum
					p := make([]serialize.SliceInt32, partsNum)
					w := sync.WaitGroup{}
					for i := 0; i < partsNum; i++ {
						fileName := path.Join(dir, vv.FilePrefix) + strconv.Itoa(i)
						w.Add(1)
						go DeserializeFromFile(&p[i], fileName, &w)
					}
					w.Wait()

					length := 0
					for _, s := range p {
						length += len(s)
					}
					values := make([]serialize.SInt32, length)
					offset := 0
					for _, s := range p {
						n := copy(values[offset:], s)
						offset += n
					}

					vValues.Values = values

				}
			case ValueTypeFloat32:
				{
					partsNum := vv.PartNum
					p := make([]serialize.SliceFloat32, partsNum)
					w := sync.WaitGroup{}
					for i := 0; i < partsNum; i++ {
						fileName := path.Join(dir, vv.FilePrefix) + strconv.Itoa(i)
						w.Add(1)
						go DeserializeFromFile(&p[i], fileName, &w)
					}
					w.Wait()

					length := 0
					for _, s := range p {
						length += len(s)
					}
					values := make([]serialize.SFloat32, length)
					offset := 0
					for _, s := range p {
						n := copy(values[offset:], s)
						offset += n
					}

					vValues.Values = values

				}
			case ValueTypeString:
				{
					partsNum := vv.PartNum
					p := make([]serialize.SliceString, partsNum)
					w := sync.WaitGroup{}
					for i := 0; i < partsNum; i++ {
						fileName := path.Join(dir, vv.FilePrefix) + strconv.Itoa(i)
						w.Add(1)
						go DeserializeFromFile(&p[i], fileName, &w)
					}
					w.Wait()

					length := 0
					for _, s := range p {
						length += len(s)
					}
					values := make([]serialize.SString, length)
					offset := 0
					for _, s := range p {
						n := copy(values[offset:], s)
						offset += n
					}
					vValues.Values = values
				}
			}
			rwLock.Lock()
			vertexProperty[kk] = vValues
			rwLock.Unlock()
		}(&ww, k, v)

	}
	ww.Wait()
	*vp = vertexProperty
	logrus.Debugf("Deserialize VertexProperty cost=%v", time.Since(tt))

}

func (ep *EdgeProperties) load(wg *sync.WaitGroup, meta GraphMeta, dir string) {

	defer wg.Done()
	tt := time.Now()
	inEdgeProperty := make(map[string]*VValues)
	ww := sync.WaitGroup{}
	rwLock := sync.RWMutex{}

	for k, v := range meta.InEdgesProperty {
		ww.Add(1)
		go func(wg *sync.WaitGroup, kk string, vv serialize.PartsMeta) {
			defer wg.Done()
			vValues := new(VValues)
			vValues.VType = ValueType(vv.ValueType)
			switch vValues.VType {
			case ValueTypeInt32:
				{
					partsNum := vv.PartNum
					p := make([]serialize.TwoDimSliceInt32, partsNum)
					w := sync.WaitGroup{}
					for i := 0; i < partsNum; i++ {
						fileName := path.Join(dir, vv.FilePrefix) + strconv.Itoa(i)
						w.Add(1)
						go DeserializeFromFile(&p[i], fileName, &w)
					}
					w.Wait()

					length := 0
					for _, s := range p {
						length += len(s)
					}
					values := make([][]serialize.SInt32, length)
					offset := 0
					for _, s := range p {
						n := copy(values[offset:], s)
						offset += n
					}

					vValues.Values = values

				}
			case ValueTypeFloat32:
				{
					partsNum := vv.PartNum
					p := make([]serialize.TwoDimSliceFloat32, partsNum)
					w := sync.WaitGroup{}
					for i := 0; i < partsNum; i++ {
						fileName := path.Join(dir, vv.FilePrefix) + strconv.Itoa(i)
						w.Add(1)
						go DeserializeFromFile(&p[i], fileName, &w)
					}
					w.Wait()

					length := 0
					for _, s := range p {
						length += len(s)
					}
					values := make([][]serialize.SFloat32, length)
					offset := 0
					for _, s := range p {
						n := copy(values[offset:], s)
						offset += n
					}

					vValues.Values = values

				}
			case ValueTypeString:
				{
					partsNum := vv.PartNum
					p := make([]serialize.TwoDimSliceString, partsNum)
					w := sync.WaitGroup{}
					for i := 0; i < partsNum; i++ {
						fileName := path.Join(dir, vv.FilePrefix) + strconv.Itoa(i)
						w.Add(1)
						go DeserializeFromFile(&p[i], fileName, &w)
					}
					w.Wait()

					length := 0
					for _, s := range p {
						length += len(s)
					}
					values := make([][]serialize.SString, length)
					offset := 0
					for _, s := range p {
						n := copy(values[offset:], s)
						offset += n
					}
					vValues.Values = values
				}
			}
			rwLock.Lock()
			inEdgeProperty[kk] = vValues
			rwLock.Unlock()
		}(&ww, k, v)

	}
	ww.Wait()
	*ep = inEdgeProperty
	logrus.Debugf("Deserialize InEdgesProperty cost=%v", time.Since(tt))

}

func (gd *GraphData) Remove(dir string) error {
	err := os.RemoveAll(dir)
	return err
}
