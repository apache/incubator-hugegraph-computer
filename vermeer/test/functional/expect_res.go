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
	"encoding/json"
	"io"
	"os"
)

type ExpectRes struct {
	VertexCount int64               `json:"vertex_count"`
	EdgeCount   int64               `json:"edge_count"`
	InEdges     map[string][]string `json:"in_edges"`
	OutEdges    map[string][]string `json:"out_edges"`
}

func GetExpectRes(expectResPath string) (*ExpectRes, error) {
	file, err := os.Open(expectResPath)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	expect, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	expectJson := &ExpectRes{}
	err = json.Unmarshal(expect, &expectJson)
	if err != nil {
		return nil, err
	}
	return expectJson, nil
}
