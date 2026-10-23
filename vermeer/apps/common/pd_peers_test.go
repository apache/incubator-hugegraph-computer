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

package common

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestPDPeersOverrideWarning(t *testing.T) {
	isolatePDPeerConfig(t)
	logger := logrus.StandardLogger()
	previousHooks := logger.ReplaceHooks(make(logrus.LevelHooks))
	previousLevel := logger.GetLevel()
	logger.SetLevel(logrus.WarnLevel)
	hook := logtest.NewLocal(logger)
	t.Cleanup(func() { logger.ReplaceHooks(previousHooks); logger.SetLevel(previousLevel) })
	configured := []string{"deployment-a:8686", "deployment-b:8686"}
	t.Setenv("VERMEER_PD_PEERS", `["deployment-a:8686","deployment-b:8686"]`)
	for _, peers := range [][]string{nil, {configured[1], configured[0]}, {configured[0]}} {
		if _, err := resolvePDPeers(peers); err != nil {
			t.Fatal(err)
		}
	}
	if err := requireTrustedPD(configured[0]); err != nil {
		t.Fatal(err)
	}
	if len(hook.AllEntries()) != 0 {
		t.Fatal("configured task peers and internal trust checks must remain quiet")
	}
	taskPeers := []string{"sensitive-task-a:8686", "sensitive-task-b:8686"}
	peers, err := resolvePDPeers(taskPeers)
	if err != nil || !reflect.DeepEqual(peers, configured) {
		t.Fatal("warning must preserve deployment precedence")
	}
	entries := hook.AllEntries()
	if len(entries) != 1 || entries[0].Level != logrus.WarnLevel {
		t.Fatalf("expected one override warning, got %d entries", len(entries))
	}
	entry, err := entries[0].String()
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range append(taskPeers, configured...) {
		if strings.Contains(entry, target) {
			t.Fatal("override warning exposed a target")
		}
	}
}

func isolatePDPeerConfig(t *testing.T) {
	t.Helper()
	for _, key := range []string{"pd_peers", "pd_password_file", "hugegraph_credentials_file"} {
		old := GetConfigDefault(key, "")
		SetConfig(key, "")
		t.Cleanup(func() { SetConfig(key, old) })
	}
	for _, key := range []string{"VERMEER_PD_PEERS", "VERMEER_PD_PASSWORD_FILE", "VERMEER_HUGEGRAPH_CREDENTIALS_FILE"} {
		t.Setenv(key, "")
	}
}

func TestPDPeersDeploymentPrecedence(t *testing.T) {
	isolatePDPeerConfig(t)
	taskPeers := []string{"task-pd:8686"}
	peers, err := resolvePDPeers(taskPeers)
	if err != nil || !reflect.DeepEqual(peers, taskPeers) {
		t.Fatal("unconfigured deployments must retain legacy task discovery")
	}
	SetConfig("pd_peers", `["configured-pd:8686"]`)
	peers, err = resolvePDPeers(taskPeers)
	if err != nil || !reflect.DeepEqual(peers, []string{"configured-pd:8686"}) {
		t.Fatal("deployment peers must replace task peers")
	}
	t.Setenv("VERMEER_PD_PEERS", `["environment-pd:8686"]`)
	peers, err = resolvePDPeers(taskPeers)
	if err != nil || !reflect.DeepEqual(peers, []string{"environment-pd:8686"}) {
		t.Fatal("environment peers must override configuration and task peers")
	}
}

func TestPDPeersDeploymentCredentialsRequireConfig(t *testing.T) {
	for _, key := range []string{"pd_password_file", "hugegraph_credentials_file",
		"VERMEER_PD_PASSWORD_FILE", "VERMEER_HUGEGRAPH_CREDENTIALS_FILE"} {
		t.Run(key, func(t *testing.T) {
			isolatePDPeerConfig(t)
			missing := filepath.Join(t.TempDir(), "not-created")
			if strings.HasPrefix(key, "VERMEER_") {
				t.Setenv(key, missing)
			} else {
				SetConfig(key, missing)
			}
			// No endpoint and no credential file exist: validation must precede IO.
			if _, err := FindValidPD(nil); err == nil || !strings.Contains(err.Error(), "configure pd_peers") {
				t.Fatalf("expected deployment peer validation before discovery, got %v", err)
			}
			if err := requireTrustedPD("task-pd:8686"); err == nil {
				t.Fatal("Server discovery must require deployment PD configuration too")
			}
		})
	}
}

func TestPDPeersInvalidDeploymentFailsClosed(t *testing.T) {
	isolatePDPeerConfig(t)
	SetConfig("pd_peers", `["configured-pd:8686"]`)
	for _, value := range []string{`null`, `[]`, `not-json`, `[""]`, `[null]`,
		`["task-pd"]`, `["task-pd:0"]`, `["task-pd:65536"]`, `["task-pd:8686",""]`} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("VERMEER_PD_PEERS", value)
			if _, err := FindValidPD([]string{"task-pd:8686"}); err == nil {
				t.Fatal("invalid deployment peers must not fall back to config or task peers")
			}
		})
	}
}

func TestPDPeersConstrainServerDiscovery(t *testing.T) {
	isolatePDPeerConfig(t)
	t.Setenv("VERMEER_PD_PEERS", `["configured-pd:8686"]`)
	if err := requireTrustedPD("configured-pd:8686"); err != nil {
		t.Fatal(err)
	}
	if _, err := FindServerAddr("task-pd:8686", "DEFAULT/graph/g", "fixture-user", "fixture-password"); err == nil || !strings.Contains(err.Error(), "outside configured pd_peers") {
		t.Fatal("Server discovery must reject an unconfigured PD before dialing")
	}
}

func TestHugegraphSchemaDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer source.Close()
	if addr := testServerIsValid([]string{source.URL}, "DEFAULT", "graph", "fixture-user", "fixture-password"); addr != "" {
		t.Fatal("a redirect must not count as a discovered Schema Server")
	}
	if _, err := getHugegraphSchema(source.URL, "fixture-user", "fixture-password"); err == nil {
		t.Fatal("a Schema redirect must be reported as an error")
	}
	if redirected.Load() != 0 {
		t.Fatal("Schema discovery and reads must stay at the original Server")
	}
}
