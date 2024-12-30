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

package options

import (
	"encoding/json"
	"strconv"

	"github.com/sirupsen/logrus"
)

const (
	OptionValueTypeInt    = "int"
	OptionValueTypeFloat  = "float"
	OptionValueTypeString = "string"
)

type Option struct {
	Name      string
	ValueType string
	Value     interface{}
	Convert   func(string) (interface{}, error)
}

func ToInt(value string) (interface{}, error) {
	return strconv.Atoi(value)
}

func ToFloat(value string) (interface{}, error) {
	return strconv.ParseFloat(value, 64)
}

func ToString(value string) (interface{}, error) {
	return value, nil
}

var AllOptions = []*Option{
	// load params
	{"load.parallel", OptionValueTypeInt, 1, ToInt},
	{"load.type", OptionValueTypeString, "", ToString},
	{"load.vertex_files", OptionValueTypeString, "", ToString},
	{"load.edge_files", OptionValueTypeString, "", ToString},
	{"load.edges_per_vertex", OptionValueTypeInt, 10, ToInt},
	{"load.file_path", OptionValueTypeString, "", ToString},
	{"load.use_property", OptionValueTypeInt, 0, ToInt},
	{"load.vertex_property", OptionValueTypeString, "", ToString},
	{"load.edge_property", OptionValueTypeString, "", ToString},
	{"load.use_outedge", OptionValueTypeInt, 0, ToInt},
	{"load.use_out_degree", OptionValueTypeInt, 0, ToInt},
	{"load.use_undirected", OptionValueTypeInt, 0, ToInt},
	{"load.delimiter", OptionValueTypeString, " ", ToString},
	{"load.hdfs_conf_path", OptionValueTypeString, "", ToString},
	{"load.hdfs_namenode", OptionValueTypeString, "", ToString},
	{"load.hdfs_use_krb", OptionValueTypeInt, 0, ToInt},
	{"load.krb_name", OptionValueTypeString, "", ToString},
	{"load.krb_realm", OptionValueTypeString, "", ToString},
	{"load.krb_conf_path", OptionValueTypeString, "", ToString},
	{"load.krb_keytab_path", OptionValueTypeString, "", ToString},
	{"load.hg_pd_peers", OptionValueTypeString, "[]", ToString},
	{"load.hg_partitions", OptionValueTypeString, "[]", ToString},
	{"load.hugegraph_name", OptionValueTypeString, "", ToString},
	{"load.hugegraph_username", OptionValueTypeString, "", ToString},
	{"load.hugegraph_password", OptionValueTypeString, "", ToString},
	{"load.hugegraph_vertex_condition", OptionValueTypeString, "", ToString},
	{"load.hugegraph_edge_condition", OptionValueTypeString, "", ToString},
	{"load.hugestore_batch_timeout", OptionValueTypeInt, 300, ToInt},
	{"load.hugestore_batchsize", OptionValueTypeInt, 100000, ToInt},
	{"load.always_using", OptionValueTypeInt, 0, ToInt},
	{"load.vertex_backend", OptionValueTypeString, "db", ToString},

	// output params
	{"output.parallel", OptionValueTypeInt, 1, ToInt},
	{"output.delimiter", OptionValueTypeString, ",", ToString},
	{"output.file_path", OptionValueTypeString, "./data/result", ToString},
	{"output.type", OptionValueTypeString, "local", ToString},
	{"output.hdfs_conf_path", OptionValueTypeString, "", ToString},
	{"output.hdfs_namenode", OptionValueTypeString, "", ToString},
	{"output.hdfs_use_krb", OptionValueTypeInt, 0, ToInt},
	{"output.krb_name", OptionValueTypeString, "", ToString},
	{"output.krb_realm", OptionValueTypeString, "", ToString},
	{"output.krb_conf_path", OptionValueTypeString, "", ToString},
	{"output.krb_keytab_path", OptionValueTypeString, "", ToString},
	{"output.hg_pd_peers", OptionValueTypeString, "", ToString},
	{"output.hugegraph_name", OptionValueTypeString, "", ToString},
	{"output.hugegraph_username", OptionValueTypeString, "", ToString},
	{"output.hugegraph_password", OptionValueTypeString, "", ToString},
	{"output.hugegraph_property", OptionValueTypeString, "", ToString},
	{"output.need_query", OptionValueTypeInt, 0, ToInt},
	{"output.need_statistics", OptionValueTypeInt, 0, ToInt},
	{"output.statistics_file_path", OptionValueTypeString, "./data/statistics.json", ToString},
	{"output.statistics_mode", OptionValueTypeString, "", ToString},
	{"output.hugegraph_write_type", OptionValueTypeString, "OLAP_COMMON", ToString},
	{"output.filter_expr", OptionValueTypeString, "", ToString},
	{"output.filter_properties", OptionValueTypeString, "[]", ToString},

	// compute params
	{"compute.max_step", OptionValueTypeInt, 10, ToInt},
	{"compute.parallel", OptionValueTypeInt, 1, ToInt},
	{"filter.vertex_expr", OptionValueTypeString, "", ToString},
	{"filter.edge_expr", OptionValueTypeString, "", ToString},
	{"filter.vertex_properties", OptionValueTypeString, "[]", ToString},
	{"filter.edge_properties", OptionValueTypeString, "[]", ToString},
	{"pagerank.damping", OptionValueTypeFloat, 0.85, ToFloat},
	{"pagerank.diff_threshold", OptionValueTypeFloat, 0.00001, ToFloat},
	{"kout.source", OptionValueTypeString, "1", ToString},
	{"kout.direction", OptionValueTypeString, "both", ToString},
	{"degree.direction", OptionValueTypeString, "out", ToString},
	{"closeness_centrality.sample_rate", OptionValueTypeFloat, 1.0, ToFloat},
	{"closeness_centrality.wf_improved", OptionValueTypeInt, 1, ToInt},
	{"betweenness_centrality.sample_rate", OptionValueTypeFloat, 1.0, ToFloat},
	{"betweenness_centrality.use_endpoint", OptionValueTypeInt, 0, ToInt},
	{"sssp.source", OptionValueTypeString, "0", ToString},
	{"kcore.degree_k", OptionValueTypeInt, 3, ToInt},
	{"louvain.threshold", OptionValueTypeFloat, 0.0000001, ToFloat},
	{"louvain.resolution", OptionValueTypeFloat, 1.0, ToFloat},
	{"louvain.edge_weight_property", OptionValueTypeString, "", ToString},
	{"louvain.step", OptionValueTypeInt, 10, ToInt},
	{"jaccard.source", OptionValueTypeString, "", ToString},
	{"ppr.source", OptionValueTypeString, "0", ToString},
	{"ppr.damping", OptionValueTypeFloat, 0.85, ToFloat},
	{"ppr.diff_threshold", OptionValueTypeFloat, 0.00001, ToFloat},
	{"cycle.max_length", OptionValueTypeInt, 5, ToInt},
	{"cycle.min_length", OptionValueTypeInt, 0, ToInt},
	{"cycle.max_cycles", OptionValueTypeInt, 10, ToInt},
	{"cycle.mode", OptionValueTypeString, "all", ToString},
	{"pagerank.edge_weight_property", OptionValueTypeString, "", ToString},
	{"lpa.vertex_weight_property", OptionValueTypeString, "", ToString},
	{"lpa.compare_option", OptionValueTypeString, "id", ToString},
	{"lpa.update_method", OptionValueTypeString, "sync", ToString},
	{"slpa.k", OptionValueTypeInt, 10, ToInt},
	{"slpa.select_method", OptionValueTypeString, "max", ToString},
}

var AllOptionsMap map[string]*Option

func Init() {
	AllOptionsMap = make(map[string]*Option, len(AllOptions))
	for _, v := range AllOptions {
		AllOptionsMap[v.Name] = v
	}
}

func Get(params map[string]string, key string) interface{} {
	v, pok := params[key]
	if v == "" {
		pok = false
	}
	o, ook := AllOptionsMap[key]

	if pok && ook {
		r, err := o.Convert(v)
		if err != nil {
			logrus.Errorf("conver error: %s: %s", key, v)
			return o.Value
		}
		return r
	}

	if pok && !ook {
		return v
	}

	if !pok && ook {
		return o.Value
	}

	logrus.Errorf("option not exists")
	return nil
}

func GetInt(params map[string]string, key string) int {
	v, ok := Get(params, key).(int)
	if !ok {
		logrus.Errorf("get option error: %s", key)
		return 0
	}
	return v
}

func GetFloat(params map[string]string, key string) float64 {
	v, ok := Get(params, key).(float64)
	if !ok {
		logrus.Errorf("get option error: %s", key)
		return 0
	}
	return v
}

func GetString(params map[string]string, key string) string {
	v, ok := Get(params, key).(string)
	if !ok {
		logrus.Errorf("get option error: %s", key)
		return ""
	}
	return v
}

func GetMapString(params map[string]string, key string) map[string]string {
	result := make(map[string]string, 0)
	v, ok := Get(params, key).(string)
	if !ok {
		logrus.Errorf("get option error: %s", key)
		return result
	}
	err := json.Unmarshal([]byte(v), &result)
	if err != nil {
		logrus.Errorf("get option Unmarshal error: %s", key)
		return result
	}
	return result
}

func GetSliceString(params map[string]string, key string) []string {
	result := make([]string, 0)
	v, ok := Get(params, key).(string)
	if !ok {
		logrus.Errorf("get option error: %s", key)
		return result
	}
	err := json.Unmarshal([]byte(v), &result)
	if err != nil {
		logrus.Errorf("get option Unmarshal error: %s", key)
		return result
	}
	return result
}

func GetSliceInt(params map[string]string, key string) []int {
	result := make([]int, 0)
	v, ok := Get(params, key).(string)
	if !ok {
		logrus.Errorf("get option error: %s", key)
		return result
	}
	err := json.Unmarshal([]byte(v), &result)
	if err != nil {
		logrus.Errorf("get option Unmarshal error: %s", key)
		return result
	}
	return result
}

type VermeerOptions struct {
	optionsMap map[string]*Option
	params     map[string]string
}

func (vo *VermeerOptions) Init() {
	vo.optionsMap = make(map[string]*Option, len(AllOptions))
	for _, v := range AllOptions {
		vo.optionsMap[v.Name] = v
	}
}

func (vo *VermeerOptions) ParseFromMap(params map[string]string) error {
	for k, v := range params {
		o, ok := vo.optionsMap[k]
		if ok {
			ov, err := o.Convert(v)
			if err != nil {
				logrus.Errorf("option parse error, k: %s, v:%s, %s", k, v, err)
			}
			o.Value = ov
		}
	}
	vo.params = params
	return nil
}

func (vo *VermeerOptions) Get(name string) interface{} {
	return vo.optionsMap[name].Value
}

func (vo *VermeerOptions) GetInt(name string) int {
	v := vo.optionsMap[name]
	return v.Value.(int)
}

func (vo *VermeerOptions) GetFloat(name string) float64 {
	v := vo.optionsMap[name]
	return v.Value.(float64)
}

func (vo *VermeerOptions) GetString(name string) string {
	v := vo.optionsMap[name]
	return v.Value.(string)
}

func (vo *VermeerOptions) GetParam(name string) string {
	return vo.params[name]
}
