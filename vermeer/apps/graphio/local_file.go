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

package graphio

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"vermeer/apps/common"
	"vermeer/apps/options"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

func init() {
	LoadMakers[LoadTypeLocal] = &LocalMaker{}
}

type LocalMaker struct{}

func (a *LocalMaker) CreateGraphLoader() GraphLoader {
	return &LocalLoader{}
}

func (a *LocalMaker) CreateGraphWriter() GraphWriter {
	return &LocalWriter{}
}

func (a *LocalMaker) MakeTasks(params map[string]string, taskID int32) ([]LoadPartition, error) {
	partID := int32(1)
	loadParts := make([]LoadPartition, 0)
	fileMap := options.GetMapString(params, "load.vertex_files")
	for ip, files := range fileMap {
		bIdx := strings.LastIndex(files, "[")
		eIdx := strings.LastIndex(files, "]")
		if bIdx < 0 || eIdx < 0 {
			part := LoadPartition{}
			part.Init(partID, taskID, LoadPartTypeVertex)
			part.IpAddr = ip
			part.Params = make(map[string]string)
			part.Params["load.file_path"] = files
			loadParts = append(loadParts, part)
			partID += 1
			continue
		}
		ss := strings.Split(files[bIdx+1:eIdx], ",")
		if len(ss) != 2 {
			s := fmt.Sprintf("MakeTask LoadTypeLocal parse file error: %s", files)
			logrus.Errorf(s)
			return nil, errors.New(s)
		}
		s, err := strconv.Atoi(ss[0])
		if err != nil {
			s := fmt.Sprintf("MakeTask LoadTypeLocal parse file error: %s, %s", files, err)
			logrus.Errorf(s)
			return nil, errors.New(s)
		}
		e, err := strconv.Atoi(ss[1])
		if err != nil {
			s := fmt.Sprintf("MakeTask LoadTypeLocal parse file error: %s, %s", files, err)
			logrus.Errorf(s)
			return nil, errors.New(s)
		}
		logrus.Debugf("MakeTask LoadTypeLocal parse file: %s, s:%d, e:%d", files, s, e)
		for i := s; i <= e; i++ {
			part := LoadPartition{}
			part.Init(partID, taskID, LoadPartTypeVertex)
			part.IpAddr = ip
			part.Params = make(map[string]string)
			part.Params["load.file_path"] = files[:bIdx] + common.ItoaPad(i, len(ss[1]))
			loadParts = append(loadParts, part)
			partID += 1
		}
	}

	fileMap = options.GetMapString(params, "load.edge_files")
	for ip, files := range fileMap {
		bIdx := strings.LastIndex(files, "[")
		eIdx := strings.LastIndex(files, "]")
		if bIdx < 0 || eIdx < 0 {
			part := LoadPartition{}
			part.Init(partID, taskID, LoadPartTypeEdge)
			part.IpAddr = ip
			part.Params = make(map[string]string)
			part.Params["load.file_path"] = files
			loadParts = append(loadParts, part)
			partID += 1
			continue
		}
		ss := strings.Split(files[bIdx+1:eIdx], ",")
		if len(ss) != 2 {
			s := fmt.Sprintf("MakeTask LoadTypeLocal parse file error: %s", files)
			logrus.Errorf(s)
			return nil, errors.New(s)
		}
		s, err := strconv.Atoi(ss[0])
		if err != nil {
			s := fmt.Sprintf("MakeTask LoadTypeLocal parse file error: %s", files)
			logrus.Errorf(s)
			return nil, errors.New(s)
		}
		e, err := strconv.Atoi(ss[1])
		if err != nil {
			s := fmt.Sprintf("MakeTask LoadTypeLocal parse file error: %s", files)
			logrus.Errorf(s)
			return nil, errors.New(s)
		}
		for i := s; i <= e; i++ {
			part := LoadPartition{}
			part.Init(partID, taskID, LoadPartTypeEdge)
			part.IpAddr = ip
			part.Params = make(map[string]string)
			part.Params["load.file_path"] = files[:bIdx] + common.ItoaPad(i, len(ss[1]))
			loadParts = append(loadParts, part)
			partID += 1
		}
	}
	return loadParts, nil
}

type LocalLoader struct {
	filePath    string
	delimiter   string
	count       int
	useProperty bool
	file        *os.File
	reader      *bufio.Reader
	schema      structure.PropertySchema
}

func (ll *LocalLoader) Init(params map[string]string, schema structure.PropertySchema) error {
	ll.filePath = options.GetString(params, "load.file_path")
	ll.useProperty = false
	if options.GetInt(params, "load.use_property") == 1 {
		ll.useProperty = true
		ll.schema = schema
	}
	logrus.Infof("local loader open: %s", ll.filePath)
	var err error
	ll.file, err = os.Open(ll.filePath)
	if err != nil {
		logrus.Errorf("open file err: %s\n", err)
		return err
	}
	ll.reader = bufio.NewReaderSize(ll.file, 1*1024*1024)
	ll.delimiter = options.GetString(params, "load.delimiter")
	return nil
}

func (ll *LocalLoader) ReadVertex(vertex *structure.Vertex, property *structure.PropertyValue) error {
	line, err := ll.reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	ss := strings.Split(line, ll.delimiter)
	vertex.ID = ss[0]
	if ll.useProperty && len(ss) > 1 {
		propStr := strings.TrimSpace(line[len(ss[0]):])
		property.LoadFromString(propStr, ll.schema)
	}
	ll.count += 1
	return nil
}

func (ll *LocalLoader) ReadEdge(edge *structure.Edge, property *structure.PropertyValue) error {
	line, err := ll.reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	ss := strings.Split(line, ll.delimiter)
	if len(ss) < 2 {
		logrus.Errorf("read edge format error %v", ss)
		return fmt.Errorf("read edge format error")
	}
	edge.Source = ss[0]
	edge.Target = ss[1]
	if ll.useProperty && len(ss) > 2 {
		var ps string
		for i := 2; i < len(ss); i++ {
			ps += ss[i]
		}
		propStr := strings.Clone(strings.TrimSpace(ps))
		property.LoadFromString(propStr, ll.schema)
	}
	ll.count += 1
	return nil
}

func (ll *LocalLoader) Name() string {
	return ll.filePath
}

func (ll *LocalLoader) ReadCount() int {
	return ll.count
}

func (ll *LocalLoader) Close() {
	err := ll.file.Close()
	if err != nil {
		logrus.Errorf("local loader close error: %s", err)
	}
}

type LocalWriter struct {
	filePath  string
	file      *os.File
	writer    *bufio.Writer
	delimiter string
	count     int
}

func (lw *LocalWriter) Init(info WriterInitInfo) error {
	lw.delimiter = options.GetString(info.Params, "output.delimiter")
	switch info.Mode {
	case WriteModeVertexValue:
		lw.filePath = options.GetString(info.Params, "output.file_path")
		zeroPad := 1
		for info.MaxID /= 10; info.MaxID > 0; info.MaxID /= 10 {
			zeroPad++
		}
		lw.filePath += "_" + common.ItoaPad(info.PartID, zeroPad)
	case WriteModeStatistics:
		lw.filePath = options.GetString(info.Params, "output.statistics_file_path")
	}
	//find dir path
	dirPath := filepath.Dir(lw.filePath)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		logrus.Errorf("local writer mkdir error: %s", err)
		return err
	}
	lw.file, err = os.OpenFile(lw.filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		logrus.Errorf("local writer open file error: %s", err)
		return err
	}
	lw.writer = bufio.NewWriterSize(lw.file, 1*1024*1024)
	return nil
}

func (lw *LocalWriter) WriteVertex(vertexValue WriteVertexValue) {
	builder := strings.Builder{}
	valueString := vertexValue.Value.ToString()
	builder.Grow(len(vertexValue.VertexID) + len(valueString) + 2)
	builder.WriteString(vertexValue.VertexID)
	builder.WriteString(lw.delimiter)
	builder.WriteString(valueString)
	builder.WriteString("\n")
	_, _ = lw.writer.WriteString(builder.String())
}

func (lw *LocalWriter) WriteStatistics(statistics map[string]any) error {
	bytes, err := json.Marshal(statistics)
	if err != nil {
		return err
	}
	_, err = lw.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (lw *LocalWriter) WriteCount() int {
	return lw.count
}

func (lw *LocalWriter) Close() {
	err := lw.writer.Flush()
	if err != nil {
		logrus.Errorf("local writer flush error: %s", err)
	}
	err = lw.file.Close()
	if err != nil {
		logrus.Errorf("local writer close error: %s", err)
	}
}
