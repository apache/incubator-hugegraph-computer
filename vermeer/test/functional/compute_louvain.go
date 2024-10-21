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

type LouvainTest struct {
	ComputeTaskBase
}

func (lt *LouvainTest) TaskComputeBody() map[string]string {
	//获取compute任务的body
	return map[string]string{
		"compute.algorithm":      "louvain",
		"louvain.step":           "10",
		"compute.max_step":       "1000",
		"compute.parallel":       "100",
		"output.file_path":       "./data/" + lt.algoName,
		"output.type":            "local",
		"output.parallel":        "10",
		"output.need_statistics": "1",
		// "output.statistics_file_path": "./data/statistics_louvain.json",
	}
}

func (lt *LouvainTest) CheckRes() {
	// file, err := os.Open("./data/statistics_louvain.json")
	// require.NoError(lt.t, err)
	// bytes, err := io.ReadAll(file)
	// require.NoError(lt.t, err)
	// mod := make(map[string]any)
	// err = json.Unmarshal(bytes, &mod)
	// require.NoError(lt.t, err)
	// require.GreaterOrEqual(lt.t, mod["modularity_in_louvain"], 0.65)
}
