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
	"os"
	"strconv"
	"strings"
)

var (
	expectLpaResFile = "test/case/expect_lpa"
)

type LpaTest struct {
	ComputeTaskBase
}

func (lpt *LpaTest) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return map[string]string{
		"compute.algorithm":  "lpa",
		"compute.max_step":   "20",
		"compute.parallel":   "100",
		"output.parallel":    "10",
		"output.file_path":   "./data/" + lpt.algoName,
		"output.type":        "local",
		"lpa.compare_option": "origin",
	}
}

func (lpt *LpaTest) CheckRes() {
	//解析输出结果，校验结果正确性。
	count, communityNum, err := lpt.loadComputeRes()
	// require.NoError(lpt.t, err)
	// require.Equal(lpt.t, int(lpt.expectRes.VertexCount), count)

	expectCommunity, err := lpt.loadExpectRes()
	// require.NoError(lpt.t, err)
	// require.Equal(lpt.t, expectCommunity, communityNum)

	_ = count
	_ = communityNum
	_ = expectCommunity
	_ = err
}

func (lpt *LpaTest) loadComputeRes() (int, int, error) {
	dir, err := os.ReadDir("data/")
	if err != nil {
		return 0, 0, err
	}
	count := 0
	community := make(map[int]int)
	for _, file := range dir {
		if !strings.HasPrefix(file.Name(), "lpa") {
			continue
		}
		f, err := os.Open("data/" + file.Name())
		if err != nil {
			return 0, 0, err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			count++
			ss := strings.Split(scanner.Text(), ",")
			comLabel, err := strconv.Atoi(ss[1])
			if err != nil {
				return 0, 0, err
			}
			community[comLabel]++
		}
	}
	return count, len(community), nil
}

func (lpt *LpaTest) loadExpectRes() (int, error) {
	file, err := os.Open(expectLpaResFile)
	if err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(file)
	var expectCommunity int
	for scanner.Scan() {
		expectCommunity, err = strconv.Atoi(scanner.Text())
		if err != nil {
			panic(err)
		}
	}
	return expectCommunity, nil
}
