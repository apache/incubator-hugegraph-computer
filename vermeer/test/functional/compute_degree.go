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
	"strconv"
	"strings"

	"github.com/stretchr/testify/require"
)

var (
	expectDegreeResFile = "test/case/expect_degree"
)

type DegreeTest struct {
	ComputeTaskBase
	direction string
}

func (dt *DegreeTest) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return map[string]string{
		"compute.algorithm": "degree",
		"compute.parallel":  "100",
		"compute.max_step":  "1",
		"degree.direction":  dt.direction,
		"output.file_path":  "./data/" + dt.algoName,
		"output.type":       "local",
		"output.parallel":   "10",
	}
}

func (dt *DegreeTest) CheckRes() {

	//解析输出结果，校验结果正确性。
	degreeRes, count, err := dt.loadComputeRes()
	require.NoError(dt.t, err)
	require.Equal(dt.t, int(dt.expectRes.VertexCount), count)

	expectDegree, err := dt.loadExpectRes()
	require.NoError(dt.t, err)
	check, err := dt.compare(expectDegree, degreeRes)
	require.NoError(dt.t, err)
	require.Equal(dt.t, true, check)
}

func (dt *DegreeTest) compare(expect []int, computeRes []int) (bool, error) {
	if len(expect) != len(computeRes) {
		return false, fmt.Errorf("length not equal")
	}
	for i := range expect {
		if computeRes[i] != expect[i] {
			return false, nil
		}
	}
	return true, nil
}

func (dt *DegreeTest) loadComputeRes() ([]int, int, error) {
	dir, err := os.ReadDir("data/")
	if err != nil {
		return nil, 0, err
	}
	res := make([]int, 4847571)
	count := 0
	for _, file := range dir {
		if !strings.HasPrefix(file.Name(), "degree_"+dt.direction) {
			continue
		}
		f, err := os.Open("data/" + file.Name())
		if err != nil {
			return nil, 0, err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			count++
			ss := strings.Split(scanner.Text(), ",")
			vertex, err := strconv.Atoi(ss[0])
			if err != nil {
				return nil, 0, err
			}
			res[vertex], err = strconv.Atoi(ss[1])
			if err != nil {
				return nil, 0, err
			}
		}
	}
	return res, count, nil
}

func (dt *DegreeTest) loadExpectRes() ([]int, error) {
	str2int := map[string]int{
		"out":  0,
		"in":   1,
		"both": 2,
	}
	col := str2int[dt.direction]
	res := make([]int, 4847571)
	file, err := os.Open(expectDegreeResFile)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	idx := 0
	for scanner.Scan() {
		ss := strings.Split(scanner.Text(), "\t")

		res[idx], err = strconv.Atoi(ss[col])
		if err != nil {
			return nil, err
		}
		idx++
	}
	if idx != 4847571 {
		return nil, fmt.Errorf("expect length not 4847571")
	}
	return res, nil
}
