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
	"fmt"
	"os/user"
	"path/filepath"
	"strings"
	"vermeer/apps/common"
	"vermeer/apps/options"
	"vermeer/apps/structure"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	krb "github.com/jcmturner/gokrb5/v8/client"
	krbCfg "github.com/jcmturner/gokrb5/v8/config"
	krbKeytab "github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/sirupsen/logrus"
)

const useKrbYes = 1

func init() {
	LoadMakers[LoadTypeHdfs] = &HdfsMaker{}
}

type HdfsMaker struct{}

func (a *HdfsMaker) CreateGraphLoader() GraphLoader {
	return &HdfsLoader{}
}

func (a *HdfsMaker) CreateGraphWriter() GraphWriter {
	return &HdfsWriter{}
}

func (a *HdfsMaker) MakeTasks(params map[string]string, taskID int32) ([]LoadPartition, error) {
	partID := int32(1)
	loadParts := make([]LoadPartition, 0)

	namenode := options.GetString(params, "load.hdfs_namenode")
	hdfsConfPath := options.GetString(params, "load.hdfs_conf_path")
	krbRealm := options.GetString(params, "load.krb_realm")
	krbName := options.GetString(params, "load.krb_name")
	keytabPath := options.GetString(params, "load.krb_keytab_path")
	krbConfPath := options.GetString(params, "load.krb_conf_path")
	useKrb := options.GetInt(params, "load.hdfs_use_krb")
	client, err := GetHdfsClient(namenode, hdfsConfPath, krbRealm, keytabPath, krbConfPath, krbName, useKrb)
	if err != nil {
		return nil, err
	}
	files := options.GetString(params, "load.vertex_files")

	dir := filepath.Dir(files)
	readDir, err := client.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, info := range readDir {
		newPath := filepath.Join(dir, info.Name())
		match, err := filepath.Match(files, newPath)
		if err != nil {
			return nil, err
		}
		if match {
			part := LoadPartition{}
			part.Init(partID, taskID, LoadPartTypeVertex)
			part.Params = make(map[string]string)
			part.Params["load.file_path"] = newPath
			loadParts = append(loadParts, part)
			partID += 1
		}
	}

	files = options.GetString(params, "load.edge_files")
	dir = filepath.Dir(files)
	readDir, err = client.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, info := range readDir {
		newPath := filepath.Join(dir, info.Name())
		match, err := filepath.Match(files, newPath)
		if err != nil {
			return nil, err
		}
		if match {
			part := LoadPartition{}
			part.Init(partID, taskID, LoadPartTypeEdge)
			part.Params = make(map[string]string)
			part.Params["load.file_path"] = newPath
			loadParts = append(loadParts, part)
			partID += 1
		}
	}

	for i := range loadParts {
		loadParts[i].Params["load.hdfs_namenode"] = params["load.hdfs_namenode"]
		loadParts[i].Params["load.hdfs_conf_path"] = params["load.hdfs_conf_path"]
		loadParts[i].Params["load.krb_realm"] = params["load.krb_realm"]
		loadParts[i].Params["load.krb_name"] = params["load.krb_name"]
		loadParts[i].Params["load.krb_keytab_path"] = params["load.krb_keytab_path"]
		loadParts[i].Params["load.krb_conf_path"] = params["load.krb_conf_path"]
		loadParts[i].Params["load.hdfs_use_krb"] = params["load.hdfs_use_krb"]
	}
	return loadParts, nil
}

type HdfsLoader struct {
	reader      *bufio.Reader
	file        *hdfs.FileReader
	client      *hdfs.Client
	schema      structure.PropertySchema
	filename    string
	delimiter   string
	count       int
	useProperty bool
}

func (hl *HdfsLoader) Init(params map[string]string, schema structure.PropertySchema) error {
	hl.useProperty = false
	if options.GetInt(params, "load.use_property") == 1 {
		hl.useProperty = true
		hl.schema = schema
	}
	namenode := options.GetString(params, "load.hdfs_namenode")
	hdfsConfPath := options.GetString(params, "load.hdfs_conf_path")
	krbRealm := options.GetString(params, "load.krb_realm")
	krbName := options.GetString(params, "load.krb_name")
	keytabPath := options.GetString(params, "load.krb_keytab_path")
	krbConfPath := options.GetString(params, "load.krb_conf_path")
	useKrb := options.GetInt(params, "load.hdfs_use_krb")
	client, err := GetHdfsClient(namenode, hdfsConfPath, krbRealm, keytabPath, krbConfPath, krbName, useKrb)
	if err != nil {
		return err
	}
	hl.client = client
	hl.filename = options.GetString(params, "load.file_path")
	hl.file, err = hl.client.Open(hl.filename)
	if err != nil {
		return err
	}
	hl.reader = bufio.NewReader(hl.file)
	hl.delimiter = options.GetString(params, "load.delimiter")
	return nil
}

func (hl *HdfsLoader) ReadVertex(vertex *structure.Vertex, property *structure.PropertyValue) error {
	line, err := hl.reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	ss := strings.Split(line, hl.delimiter)
	vertex.ID = ss[0]
	if hl.useProperty && len(ss) > 1 {
		propStr := strings.TrimSpace(line[len(ss[0]):])
		property.LoadFromString(propStr, hl.schema)
	}
	hl.count += 1
	return nil
}

func (hl *HdfsLoader) ReadEdge(edge *structure.Edge, property *structure.PropertyValue) error {
	line, err := hl.reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	ss := strings.Split(line, hl.delimiter)
	if len(ss) < 2 {
		return fmt.Errorf("read edge format error")
	}
	edge.Source = ss[0]
	edge.Target = ss[1]
	if hl.useProperty && len(ss) > 2 {
		var ps string
		for i := 2; i < len(ss); i++ {
			ps += ss[i]
		}
		propStr := strings.TrimSpace(ps)
		property.LoadFromString(propStr, hl.schema)
	}
	hl.count += 1
	return nil
}
func (hl *HdfsLoader) Name() string {
	return hl.filename
}
func (hl *HdfsLoader) ReadCount() int {
	return hl.count
}

func (hl *HdfsLoader) Close() {
	err := hl.file.Close()
	if err != nil {
		logrus.Errorf("hdfs loader file close error: %s", err)
	}
	err = hl.client.Close()
	if err != nil {
		logrus.Errorf("hdfs loader client close error: %s", err)
	}
}

type HdfsWriter struct {
	file      *hdfs.FileWriter
	writer    *bufio.Writer
	client    *hdfs.Client
	count     int
	delimiter string
	filePath  string
}

func (hw *HdfsWriter) Init(info WriterInitInfo) error {
	hw.delimiter = options.GetString(info.Params, "output.delimiter")
	switch info.Mode {
	case WriteModeVertexValue:
		hw.filePath = options.GetString(info.Params, "output.file_path")
		zeroPad := 1
		for info.MaxID /= 10; info.MaxID > 0; info.MaxID /= 10 {
			zeroPad++
		}
		hw.filePath += "_" + common.ItoaPad(info.PartID, zeroPad)
	case WriteModeStatistics:
		hw.filePath = options.GetString(info.Params, "output.statistics_file_path")
	}
	namenode := options.GetString(info.Params, "output.hdfs_namenode")
	hdfsConfPath := options.GetString(info.Params, "output.hdfs_conf_path")
	krbRealm := options.GetString(info.Params, "output.krb_realm")
	krbName := options.GetString(info.Params, "output.krb_name")
	keytabPath := options.GetString(info.Params, "output.krb_keytab_path")
	krbConfPath := options.GetString(info.Params, "output.krb_conf_path")
	useKrb := options.GetInt(info.Params, "output.hdfs_use_krb")
	client, err := GetHdfsClient(namenode, hdfsConfPath, krbRealm, keytabPath, krbConfPath, krbName, useKrb)
	if err != nil {
		return err
	}
	hw.client = client
	//find dir path
	dirPath := filepath.Dir(hw.filePath)
	err = hw.client.MkdirAll(dirPath, 0755)
	if err != nil {
		logrus.Errorf("hdfs writer mkdir error: %s", err)
		return err
	}
	//清空已有同名文件实现覆盖写
	_ = hw.client.Remove(hw.filePath)
	hw.file, err = hw.client.CreateFile(hw.filePath, 3, 128*1024*1024, 0666)
	if err != nil {
		logrus.Errorf("hdfs writer open file error: %s", err)
		return err
	}
	hw.writer = bufio.NewWriterSize(hw.file, 1*1024*1024)
	return nil
}

func (hw *HdfsWriter) WriteVertex(vertexValue WriteVertexValue) {
	builder := strings.Builder{}
	valueString := vertexValue.Value.ToString()
	builder.Grow(len(vertexValue.VertexID) + len(valueString) + 2)
	builder.WriteString(vertexValue.VertexID)
	builder.WriteString(hw.delimiter)
	builder.WriteString(valueString)
	builder.WriteString("\n")
	_, _ = hw.writer.WriteString(builder.String())
}

func (hw *HdfsWriter) WriteCount() int {
	return hw.count
}

func (hw *HdfsWriter) WriteStatistics(statistics map[string]any) error {
	bytes, err := json.Marshal(statistics)
	if err != nil {
		return err
	}
	_, err = hw.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (hw *HdfsWriter) Close() {
	err := hw.writer.Flush()
	if err != nil {
		logrus.Errorf("hdfs writer flush error: %s", err)
	}
	err = hw.file.Close()
	if err != nil {
		logrus.Errorf("hdfs writer file close error: %s", err)
	}
	err = hw.client.Close()
	if err != nil {
		logrus.Errorf("hdfs writer client close error: %s", err)
	}
}

func GetHdfsClient(namenode, hdfsConfPath, realm, keytabPath, krbConfPath, krbName string, useKrb int) (*hdfs.Client, error) {
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, err
	}

	if hdfsConfPath != "" {
		conf, err = hadoopconf.Load(hdfsConfPath)
		if err != nil {
			return nil, err
		}
	}

	option := hdfs.ClientOptionsFromConf(conf)
	if namenode != "" {
		option.Addresses = strings.Split(namenode, ",")
	}

	u, err := user.Current()
	if err != nil {
		return nil, err
	}
	option.User = u.Username
	if useKrb == useKrbYes {
		c, err := getKrb(krbName, realm, keytabPath, krbConfPath)
		if err != nil {
			return nil, err
		}
		option.KerberosClient = c
		option.KerberosServicePrincipleName = krbName
	}

	client, err := hdfs.NewClient(option)
	if err != nil {
		return nil, err
	}

	return client, nil

}

func getKrb(username, realm, ktPath, krbConfPath string) (*krb.Client, error) {
	krb5conf, err := krbCfg.Load(krbConfPath)
	if err != nil {
		return nil, err
	}
	kt, err := krbKeytab.Load(ktPath)
	if err != nil {
		return nil, err
	}
	client := krb.NewWithKeytab(username, realm, kt, krb5conf)
	return client, nil
}
