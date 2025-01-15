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
	"fmt"
	"testing"
	"vermeer/client"
)

const (
	OutputTypeInt = iota
	OutputTypeFloat
	OutputTypeString
)

type ComputeTask interface {
	Init(graphName string, algoName string, expectRes *ExpectRes, waitSecond int,
		masterHttp *client.VermeerClient, t *testing.T, healthCheck *HealthCheck)
	TaskComputeBody() map[string]string
	SendComputeReqAsync(params map[string]string)
	SendComputeReqSync(params map[string]string)
	LoadComputeRes() ([]interface{}, error)
	CheckRes()
	CheckGetComputeValue()
}

func MakeComputeTask(computeType string) (ComputeTask, error) {
	//todo: 加入新的算法名称
	//需要设置结果的数据类型和校验的误差范围
	//float类型的数据需要设置误差范围，int类型和string类型不需要
	switch computeType {
	case "pagerank":
		pagerankTest := &PagerankTest{}
		pagerankTest.outputType = OutputTypeFloat
		pagerankTest.errorRange = 0.01
		return pagerankTest, nil
	case "wcc":
		wccTest := &WccTest{}
		wccTest.outputType = OutputTypeInt
		return wccTest, nil
	case "degree_out":
		degreeTest := &DegreeTest{direction: "out"}
		degreeTest.outputType = OutputTypeInt
		return degreeTest, nil
	case "degree_in":
		degreeTest := &DegreeTest{direction: "in"}
		degreeTest.outputType = OutputTypeInt
		return degreeTest, nil
	case "degree_both":
		degreeTest := &DegreeTest{direction: "both"}
		degreeTest.outputType = OutputTypeInt
		return degreeTest, nil
	case "triangle_count":
		triangleCount := &TriangleCountTest{}
		triangleCount.outputType = OutputTypeInt
		return triangleCount, nil
	case "sssp":
		ssspTest := &SsspTest{}
		ssspTest.outputType = OutputTypeInt
		return ssspTest, nil
	case "closeness_centrality":
		closenessCentralityTest := &ClosenessCentralityTest{}
		closenessCentralityTest.outputType = OutputTypeFloat
		closenessCentralityTest.errorRange = 0.25
		return closenessCentralityTest, nil
	case "betweenness_centrality":
		betweennessCentralityTest := &BetweennessCentralityTest{}
		betweennessCentralityTest.outputType = OutputTypeFloat
		betweennessCentralityTest.errorRange = 0.45
		return betweennessCentralityTest, nil
	case "lpa":
		lpaTest := &LpaTest{}
		lpaTest.outputType = OutputTypeInt
		return lpaTest, nil
	case "kcore":
		kcoreTest := &KcoreTest{}
		kcoreTest.outputType = OutputTypeInt
		return kcoreTest, nil
	case "louvain":
		louvainTest := &LouvainTest{}
		louvainTest.outputType = OutputTypeInt
		return louvainTest, nil
	//case "kout_all":
	//	koutAllTest := &KoutAllTest{}
	//	koutAllTest.outputType = OutputTypeInt
	//	return koutAllTest
	case "jaccard":
		jaccardTest := &JaccardTest{}
		jaccardTest.outputType = OutputTypeFloat
		jaccardTest.errorRange = 0.001
		return jaccardTest, nil
	case "ppr":
		pprTest := &PprTest{}
		pprTest.outputType = OutputTypeFloat
		pprTest.errorRange = 0.01
		return pprTest, nil
	case "clustering_coefficient":
		clusteringTest := &ClusteringCoefficientTest{}
		clusteringTest.outputType = OutputTypeFloat
		clusteringTest.errorRange = 0.001
		return clusteringTest, nil
	case "scc":
		sccTest := &SccTest{}
		sccTest.outputType = OutputTypeInt
		return sccTest, nil
	}
	return nil, fmt.Errorf("no matched compute type: %s", computeType)
}
