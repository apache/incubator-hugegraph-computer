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
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/stretchr/testify/require"
)

type ClosenessCentralityTest struct {
	ComputeTaskBase
}

func (cct *ClosenessCentralityTest) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return map[string]string{
		"compute.algorithm":                "closeness_centrality",
		"compute.max_step":                 "50",
		"compute.parallel":                 "100",
		"closeness_centrality.wf_improved": "1",
		"closeness_centrality.sample_rate": "0.05",
		"output.parallel":                  "10",
		"output.file_path":                 "./data/" + cct.algoName,
		"output.type":                      "local",
	}
}

func (cct *ClosenessCentralityTest) CheckRes() {
	computeRes, err := cct.LoadComputeRes()
	require.NoError(cct.t, err)

	type cc struct {
		id    string
		value float64
	}
	ccS := make([]cc, len(computeRes))
	for i := range computeRes {
		ccS[i] = struct {
			id    string
			value float64
		}{id: strconv.Itoa(i), value: computeRes[i].(float64)}
	}
	sort.Slice(ccS, func(i, j int) bool {
		return ccS[i].value > ccS[j].value
	})

	computeTop100 := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		computeTop100[ccS[i].id] = struct{}{}
	}

	expectRes, err := cct.loadExpectRes("test/case/expect_" + cct.algoName)
	require.NoError(cct.t, err)

	count := 0
	for i := range expectRes {
		if _, ok := computeTop100[expectRes[i]]; ok {
			count++
		}
	}
	fmt.Printf("closeness_centrality top100 intersection get:%v\n", float64(count)/100)
	require.GreaterOrEqual(cct.t, float64(count), 100*(1-cct.errorRange),
		"%v compute res top100 intersection not correct, expect>=%v,get:%v",
		cct.algoName, 1-cct.errorRange, float64(count)/100)
}

func (cct *ClosenessCentralityTest) loadExpectRes(filepath string) ([]string, error) {
	res := make([]string, 100)
	count := 0
	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		res[count] = strings.TrimSpace(scanner.Text())
		count++
	}
	require.Equal(cct.t, count, 100)
	return res, nil
}
