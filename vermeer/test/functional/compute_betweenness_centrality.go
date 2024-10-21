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

type BetweennessCentralityTest struct {
	ComputeTaskBase
}

func (bct *BetweennessCentralityTest) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return map[string]string{
		"compute.algorithm":                   "betweenness_centrality",
		"compute.max_step":                    "50",
		"compute.parallel":                    "100",
		"betweenness_centrality.use_endpoint": "0",
		"betweenness_centrality.sample_rate":  "0.05",
		"output.parallel":                     "10",
		"output.file_path":                    "./data/" + bct.algoName,
		"output.type":                         "local",
	}
}

func (bct *BetweennessCentralityTest) CheckRes() {
	computeRes, err := bct.LoadComputeRes()
	require.NoError(bct.t, err)
	type bc struct {
		id    string
		value float64
	}
	bcS := make([]bc, len(computeRes))
	for i := range computeRes {
		bcS[i] = struct {
			id    string
			value float64
		}{id: strconv.Itoa(i), value: computeRes[i].(float64)}
	}
	sort.Slice(bcS, func(i, j int) bool {
		return bcS[i].value > bcS[j].value
	})

	computeTop100 := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		computeTop100[bcS[i].id] = struct{}{}
	}

	expectRes, err := bct.loadExpectRes("test/case/expect_" + bct.algoName)
	require.NoError(bct.t, err)
	count := 0
	for i := range expectRes {
		if _, ok := computeTop100[expectRes[i]]; ok {
			count++
		}
	}
	fmt.Printf("betweenness_centrality top100 intersection get:%v\n", float64(count)/100)
	require.GreaterOrEqual(bct.t, float64(count), 100*(1-bct.errorRange))
}

func (bct *BetweennessCentralityTest) loadExpectRes(filepath string) ([]string, error) {
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
	require.Equal(bct.t, count, 100)
	return res, nil
}
