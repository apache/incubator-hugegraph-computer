/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package structure

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"vermeer/apps/common"
	"vermeer/apps/options"
	pd "vermeer/apps/protos/hugegraph-pd-grpc"
)

type schemaCredentialPD struct {
	pd.UnimplementedPDServer
	pd.UnimplementedDiscoveryServiceServer
	serverURL string
}

func (s *schemaCredentialPD) GetMembers(context.Context, *pd.GetMembersRequest) (*pd.GetMembersResponse, error) {
	return &pd.GetMembersResponse{}, nil
}

func (s *schemaCredentialPD) GetNodes(context.Context, *pd.Query) (*pd.NodeInfos, error) {
	return &pd.NodeInfos{Info: []*pd.NodeInfo{{
		Address: s.serverURL, Labels: map[string]string{"GRAPHSPACE": "DEFAULT"},
	}}}, nil
}

func TestHugegraphSchemaDeploymentCredentials(t *testing.T) {
	previousOptions := options.AllOptionsMap
	options.Init()
	t.Cleanup(func() { options.AllOptionsMap = previousOptions })
	var authorized atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "schema-reader" || password != "deployment-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/graphspaces/DEFAULT/graphs/fresh-graph/schema" || r.URL.Query().Get("format") != "json" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		authorized.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vertexlabels":[],"edgelabels":[],"propertykeys":[]}`))
	}))
	defer server.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	discovery := grpc.NewServer()
	fixture := &schemaCredentialPD{serverURL: server.URL}
	pd.RegisterPDServer(discovery, fixture)
	pd.RegisterDiscoveryServiceServer(discovery, fixture)
	go func() { _ = discovery.Serve(listener) }()
	defer discovery.Stop()
	credentials := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(credentials, []byte(`{"username":"schema-reader","password":"deployment-secret"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", credentials)
	t.Setenv("VERMEER_PD_PEERS", `["`+listener.Addr().String()+`"]`)
	params := map[string]string{
		"load.hg_pd_peers":    `["` + listener.Addr().String() + `"]`,
		"load.hugegraph_name": "DEFAULT/fresh-graph/g", "load.use_property": "1",
		"load.hugegraph_username": "wrong-task-user", "load.hugegraph_password": "wrong-task-password",
	}
	original := make(map[string]string)
	for k, v := range params {
		original[k] = v
	}
	vertex, edge, err := GetSchemaFromHugegraph(params)
	if err != nil {
		t.Fatal(err)
	}
	if vertex.HgPSchema == nil || edge.HgPSchema == nil {
		t.Fatal("authenticated Schema response must initialize vertex and edge schemas")
	}
	if authorized.Load() != 2 {
		t.Fatalf("expected authenticated discovery probe and Schema read, got %d", authorized.Load())
	}
	if !reflect.DeepEqual(params, original) {
		t.Fatal("deployment credentials must not be written into task params")
	}
}

func TestHugegraphSchemaRejectsInvalidCredentialsBeforeDiscovery(t *testing.T) {
	t.Setenv("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing.json"))
	// There is no PD endpoint: credential validation must fail before discovery.
	if _, _, err := GetSchemaFromHugegraph(map[string]string{}); err == nil || !strings.Contains(err.Error(), "read HugeGraph credentials file") {
		t.Fatalf("expected credential error before discovery, got %v", err)
	}
}

func TestHugegraphSchemaRequiresDeploymentPD(t *testing.T) {
	previousOptions := options.AllOptionsMap
	options.Init()
	t.Cleanup(func() { options.AllOptionsMap = previousOptions })
	previousPeers := common.GetConfigDefault("pd_peers", "")
	common.SetConfig("pd_peers", "")
	t.Cleanup(func() { common.SetConfig("pd_peers", previousPeers) })
	t.Setenv("VERMEER_PD_PEERS", "")
	credentials := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(credentials, []byte(`{"username":"fixture-reader","password":"fixture-password"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", credentials)
	_, _, err := GetSchemaFromHugegraph(map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "configure pd_peers") {
		t.Fatal("deployment Schema credentials require deployment PD configuration before discovery")
	}
}
