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

package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"vermeer/apps/auth"

	"github.com/sirupsen/logrus"
)

type VermeerClient struct {
	client   *http.Client
	httpAddr string
	useAuth  bool
	token    string
}
type AuthOptions struct {
	User            string
	Space           string
	AuthTokenFactor string
	Client          string
}

func (vc *VermeerClient) Init(httpAddr string, client *http.Client) {
	if client == nil {
		client = http.DefaultClient
	}
	vc.client = client
	vc.httpAddr = httpAddr
}

func (vc *VermeerClient) SetAuth(options AuthOptions) error {
	vc.useAuth = true
	factory := &auth.TokenFactory{Factor: options.AuthTokenFactor}
	token := factory.NewToken(options.User, options.Space, options.Client)
	factory.Sign(token)
	b, err := json.Marshal(token)
	if err != nil {
		return err
	}
	vc.token = auth.ToBase58(string(b))
	return nil
}

func (vc *VermeerClient) SetToken(token string) {
	vc.useAuth = true
	vc.token = token
}

func (vc *VermeerClient) CreateGraph(request GraphCreateRequest) (bool, error) {
	reader, err := Request2Reader(request)
	if err != nil {
		return false, err
	}
	resp, err := vc.post(vc.httpAddr+"/graphs/create", reader)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("response:%s", string(respByte))
	}
	return true, nil
}

func (vc *VermeerClient) GetGraphs() (*GraphsResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/graphs")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	graphResp := &GraphsResponse{}
	err = ParseResponse2Any(resp, graphResp)
	if err != nil {
		return nil, err
	}
	return graphResp, err
}

func (vc *VermeerClient) GetGraph(graphName string) (*GraphResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/graphs/" + graphName)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	graphResp := &GraphResponse{}
	err = ParseResponse2Any(resp, graphResp)
	if err != nil {
		return nil, err
	}
	return graphResp, err
}

func (vc *VermeerClient) GetWorkers() (*WorkersResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/workers")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	workersResp := &WorkersResponse{}
	err = ParseResponse2Any(resp, workersResp)
	if err != nil {
		return nil, err
	}
	return workersResp, err
}

func (vc *VermeerClient) AllocGroupGraph(graphName string, groupName string) (bool, error) {
	reader, err := Request2Reader(struct{}{})
	if err != nil {
		return false, err
	}
	resp, err := vc.post(vc.httpAddr+"/admin/workers/alloc/"+groupName+"/$DEFAULT/"+graphName, reader)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("response:%s", string(respByte))
	}
	return true, nil
}

func (vc *VermeerClient) GetMaster() (*MasterResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/master")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	masterResp := &MasterResponse{}
	err = ParseResponse2Any(resp, masterResp)
	if err != nil {
		return nil, err
	}
	return masterResp, err
}

func (vc *VermeerClient) DeleteGraph(graphName string) (bool, error) {
	resp, err := vc.delete(vc.httpAddr + "/graphs/" + graphName)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("response:%s", string(respByte))
	}
	return true, nil
}

func (vc *VermeerClient) createTask(url string, request TaskCreateRequest) (*TaskResponse, error) {
	reader, err := Request2Reader(request)
	if err != nil {
		return nil, err
	}
	resp, err := vc.post(url, reader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	taskResp := &TaskResponse{}
	err = ParseResponse2Any(resp, taskResp)
	if err != nil {
		return nil, err
	}
	return taskResp, err
}

func (vc *VermeerClient) CreateTaskAsync(request TaskCreateRequest) (*TaskResponse, error) {
	taskResponse, err := vc.createTask(vc.httpAddr+"/tasks/create", request)
	if err != nil {
		return nil, err
	}
	return taskResponse, err
}

func (vc *VermeerClient) CreateTaskSync(request TaskCreateRequest) (*TaskResponse, error) {
	taskResponse, err := vc.createTask(vc.httpAddr+"/tasks/create/sync", request)
	if err != nil {
		return nil, err
	}
	return taskResponse, err
}

func (vc *VermeerClient) CreateTaskBatch(request TaskCreateBatchRequest) (*TaskBatchCreateResponse, error) {
	reader, err := Request2Reader(request)
	if err != nil {
		return nil, err
	}
	resp, err := vc.post(vc.httpAddr+"/tasks/create/batch", reader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	taskResp := &TaskBatchCreateResponse{}
	err = ParseResponse2Any(resp, taskResp)
	if err != nil {
		return nil, err
	}
	return taskResp, err
}

func (vc *VermeerClient) GetTasks() (*TasksResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/tasks")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	tasksResp := &TasksResponse{}
	err = ParseResponse2Any(resp, tasksResp)
	if err != nil {
		return nil, err
	}
	return tasksResp, err
}

func (vc *VermeerClient) GetTask(taskID int) (*TaskResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/task/" + strconv.Itoa(taskID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	taskResp := &TaskResponse{}
	err = ParseResponse2Any(resp, taskResp)
	if err != nil {
		return nil, err
	}
	return taskResp, err
}

func (vc *VermeerClient) GetTaskStartSequence(queryTasks []int32) (*TaskStartSequenceResp, error) {
	reader, err := Request2Reader(TaskStartSequenceRequest{QueryTasks: queryTasks})
	if err != nil {
		return nil, err
	}
	resp, err := vc.post(vc.httpAddr+"/tasks/start_sequence", reader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	taskResp := &TaskStartSequenceResp{}
	err = ParseResponse2Any(resp, taskResp)
	if err != nil {
		return nil, err
	}
	return taskResp, err
}

func (vc *VermeerClient) GetEdges(graphName string, vertexID string, direction string) (*EdgesResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/graphs/" + graphName + "/edges?vertex_id=" + vertexID + "&direction=" + direction)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	edgesResp := &EdgesResponse{}
	err = ParseResponse2Any(resp, edgesResp)
	if err != nil {
		return nil, err
	}
	return edgesResp, err

}

func (vc *VermeerClient) GetVertices(graphName string, request VerticesRequest) (*VerticesResponse, error) {
	reader, err := Request2Reader(request)
	if err != nil {
		return nil, err
	}
	resp, err := vc.post(vc.httpAddr+"/graphs/"+graphName+"/vertices", reader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	verticesResp := &VerticesResponse{}
	err = ParseResponse2Any(resp, verticesResp)
	if err != nil {
		return nil, err
	}
	return verticesResp, err
}

func (vc *VermeerClient) GetComputeValue(taskID int, cursor int, limit int) (*ComputeValueResponse, error) {
	resp, err := vc.get(vc.httpAddr + "/tasks/value/" + strconv.Itoa(taskID) + "?cursor=" +
		strconv.Itoa(cursor) + "&limit=" + strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("response:%s", string(respByte))
	}
	computeValueResp := &ComputeValueResponse{}
	err = ParseResponse2Any(resp, computeValueResp)
	if err != nil {
		return nil, err
	}
	return computeValueResp, err
}

func (vc *VermeerClient) GetTaskCancel(taskID int) (bool, error) {
	resp, err := vc.get(vc.httpAddr + "/task/cancel/" + strconv.Itoa(taskID))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("response:%s", string(respByte))
	}
	return true, nil
}

func (vc *VermeerClient) HealthCheck() (bool, error) {
	resp, err := vc.get(vc.httpAddr + "/healthcheck")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respByte, err := ParseResponse2Byte(resp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("response:%s", string(respByte))
	}
	return true, nil
}

func (vc *VermeerClient) post(url string, requestBody io.Reader) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodPost, url, requestBody)
	if err != nil {
		return nil, err
	}

	response, err := vc.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (vc *VermeerClient) get(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	response, err := vc.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (vc *VermeerClient) put(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		return nil, err
	}

	response, err := vc.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (vc *VermeerClient) delete(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	response, err := vc.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (vc *VermeerClient) sendRequest(request *http.Request) (*http.Response, error) {
	vc.setAuth(request)
	response, err := vc.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		logrus.Warnf("response.Status Get:%v", response.Status)
	}
	return response, nil
}

func (vc *VermeerClient) setAuth(request *http.Request) {
	if vc.useAuth {
		request.Header.Set("Authorization", vc.token)
	}
}

func ParseResponse2Map(response *http.Response) (map[string]any, error) {
	responseBodyByte, err := ParseResponse2Byte(response)
	if err != nil {
		return nil, err
	}
	responseBody := make(map[string]any)
	err = json.Unmarshal(responseBodyByte, &responseBody)
	if err != nil {
		return nil, err
	}
	err = response.Body.Close()
	if err != nil {
		return nil, err
	}
	return responseBody, nil
}

func ParseResponse2Any(response *http.Response, anyStruct any) error {
	responseBodyByte, err := ParseResponse2Byte(response)
	if err != nil {
		return err
	}
	err = json.Unmarshal(responseBodyByte, anyStruct)
	if err != nil {
		return err
	}
	return nil
}

func ParseResponse2Byte(response *http.Response) ([]byte, error) {
	responseBodyByte, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return responseBodyByte, nil
}
