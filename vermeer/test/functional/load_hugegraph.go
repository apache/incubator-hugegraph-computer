//go:build vermeer_test

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

package functional

type LoadTaskHugeGraph struct {
	LoadTaskTestBase
}

func (lt *LoadTaskHugeGraph) TaskLoadBody() map[string]string {

	return map[string]string{
		"load.parallel":           "100",
		"load.type":               "local",
		"load.use_property":       "0",
		"load.use_outedge":        "1",
		"load.use_out_degree":     "1",
		"load.use_undirected":     "0",
		"load.hg_pd_peers":        "[\"10.81.116.78:8886\"]",
		"load.hugegraph_name":     "DEFAULT/hugegraph/g",
		"load.hugegraph_username": "admin",
		"load.hugegraph_password": "admin",
		"load.vertex_property":    "",
		"load.edge_property":      "",
	}
}
