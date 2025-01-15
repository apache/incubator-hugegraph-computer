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
	"bytes"
	"encoding/json"
	"io"
	"time"
)

type BaseResponse struct {
	ErrCode int32  `json:"errcode"`
	Message string `json:"message,omitempty"`
}
type TasksResponse struct {
	BaseResponse
	Tasks []TaskInfo `json:"tasks,omitempty"`
}

type TaskResponse struct {
	BaseResponse
	Task TaskInfo `json:"task,omitempty"`
}

type TaskInfo struct {
	ID         int32             `json:"id,omitempty"`
	Status     string            `json:"status,omitempty"`
	CreateType string            `json:"create_type,omitempty"`
	CreateTime time.Time         `json:"create_time,omitempty"`
	UpdateTime time.Time         `json:"update_time,omitempty"`
	GraphName  string            `json:"graph_name,omitempty"`
	Type       string            `json:"task_type,omitempty"`
	Params     map[string]string `json:"params,omitempty"`
	Workers    []TaskWorker      `json:"workers,omitempty"`
}

type TaskWorker struct {
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

type ComputeValueResponse struct {
	BaseResponse
	Vertices []VertexValue `json:"vertices,omitempty"`
	Cursor   int32         `json:"cursor,omitempty"`
}

type VertexValue struct {
	ID    string
	Value string
}

type GraphsResponse struct {
	BaseResponse
	Graphs []VermeerGraph `json:"graphs,omitempty"`
}

type GraphResponse struct {
	BaseResponse
	Graph VermeerGraph `json:"graph,omitempty"`
}

type VermeerGraph struct {
	Name          string        `json:"name,omitempty"`
	SpaceName     string        `json:"space_name,omitempty"`
	Status        string        `json:"status,omitempty"`
	CreateTime    time.Time     `json:"create_time,omitempty"`
	UpdateTime    time.Time     `json:"update_time,omitempty"`
	VertexCount   int64         `json:"vertex_count,omitempty"`
	EdgeCount     int64         `json:"edge_count,omitempty"`
	Workers       []GraphWorker `json:"workers,omitempty"`
	UsingNum      int32         `json:"using_num,omitempty"`
	UseOutEdges   bool          `json:"use_out_edges,omitempty"`
	UseProperty   bool          `json:"use_property,omitempty"`
	UseOutDegree  bool          `json:"use_out_degree,omitempty"`
	UseUndirected bool          `json:"use_undirected,omitempty"`
	OnDisk        bool          `json:"on_disk,omitempty"`
}

type GraphWorker struct {
	Name          string
	VertexCount   uint32
	VertIDStart   uint32
	EdgeCount     int64
	ScatterOffset uint32
}

type WorkersResponse struct {
	BaseResponse
	Workers []Worker `json:"workers"`
}

type Worker struct {
	ID         int32     `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	GrpcPeer   string    `json:"grpc_peer,omitempty"`
	IPAddr     string    `json:"ip_addr,omitempty"`
	State      string    `json:"state,omitempty"`
	Version    string    `json:"version,omitempty"`
	LaunchTime time.Time `json:"launch_time,omitempty"`
}

type MasterInfo struct {
	GrpcPeer   string    `json:"grpc_peer,omitempty"`
	IPAddr     string    `json:"ip_addr,omitempty"`
	DebugMod   string    `json:"debug_mod,omitempty"`
	Version    string    `json:"version,omitempty"`
	LaunchTime time.Time `json:"launch_time,omitempty"`
}

type MasterResponse struct {
	Master MasterInfo `json:"master"`
}

type EdgesResponse struct {
	BaseResponse
	InEdges  []string `json:"in_edges"`
	OutEdges []string `json:"out_edges"`
	//BothEdges      []string       `json:"both_edges"`
	InEdgeProperty []edgeProperty `json:"in_edge_property,omitempty"`
}

type edgeProperty struct {
	Edge     string            `json:"edge"`
	Property map[string]string `json:"property"`
}

type VerticesRequest struct {
	VerticesIds []string `json:"vertices"`
}

type VerticesResponse struct {
	BaseResponse
	Vertices []vertex `json:"vertices"`
}

type vertex struct {
	ID       string            `json:"id"`
	Property map[string]string `json:"property"`
}

type TaskCreateRequest struct {
	TaskType  string            `json:"task_type"`
	GraphName string            `json:"graph"`
	Params    map[string]string `json:"params"`
}

type GraphCreateRequest struct {
	Name string `json:"name,omitempty"`
}

func Request2Reader(request any) (io.Reader, error) {
	bodyByte, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(bodyByte), err
}
