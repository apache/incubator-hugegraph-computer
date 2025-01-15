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

type JaccardTest struct {
	ComputeTaskBase
}

func (jt *JaccardTest) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return map[string]string{
		"compute.algorithm": "jaccard",
		"jaccard.source":    "10009",
		"compute.max_step":  "2",
		"compute.parallel":  "100",
		"output.parallel":   "10",
		"output.delimiter":  ",",
		"output.file_path":  "./data/" + jt.algoName,
		"output.type":       "local",
	}
}
