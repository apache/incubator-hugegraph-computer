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

package services

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

func TestWorkerAvailabilityUsesAuthenticatedSpace(t *testing.T) {
	credential, err := structure.NewCredential("space-user", "own-space")
	if err != nil {
		t.Fatal(err)
	}
	previousGraphs := common.PrometheusMetrics.GraphCnt
	common.PrometheusMetrics.GraphCnt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_availability_graphs"}, nil)
	GraphMgr.Init()
	t.Cleanup(func() { GraphMgr.Init(); common.PrometheusMetrics.GraphCnt = previousGraphs })
	for _, space := range []string{"own-space", "another-space"} {
		if err := GraphMgr.AddGraph(GraphMgr.CreateGraph(space, space+"-graph")); err != nil {
			t.Fatal(err)
		}
	}
	router := gin.New()
	var checked string
	router.GET("/workers/availability", func(ctx *gin.Context) {
		ctx.Set(structure.CredentialKey, credential)
		writeWorkerAvailability(ctx, func(space string, graphs []string) bool {
			checked = space
			return space == "own-space" && len(graphs) == 1 && graphs[0] == "own-space-graph"
		})
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/workers/availability?space=another-space", nil))
	if checked != "own-space" {
		t.Fatalf("checked untrusted space %q", checked)
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body["available"] != true {
		t.Fatalf("response must contain only readiness: %v", body)
	}
}
