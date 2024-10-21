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

import (
	"math/rand"
)

type LoadTaskLocal struct {
	LoadTaskTestBase
}

func (lt *LoadTaskLocal) TaskLoadBody() map[string]string {
	vertexBackends := []string{"db", "mem"}

	return map[string]string{
		"load.parallel":     "100",
		"load.type":         "local",
		"load.use_property": "0",
		//"load.use_outedge":    "1",
		//"load.use_out_degree": "1",
		//"load.use_undirected": "0",
		"load.delimiter":      " ",
		"load.vertex_files":   "{\"127.0.0.1\":\"" + "test/case/vertex/vertex_[0,29]" + "\"}",
		"load.edge_files":     "{\"127.0.0.1\":\"" + "test/case/edge/edge_[0,29]" + "\"}",
		"load.vertex_backend": vertexBackends[rand.Intn(len(vertexBackends))],
	}
}
