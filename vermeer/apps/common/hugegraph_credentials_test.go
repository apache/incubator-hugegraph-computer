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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHugegraphCredentialsFilePrecedence(t *testing.T) {
	old := GetConfigDefault("hugegraph_credentials_file", "")
	t.Cleanup(func() { SetConfig("hugegraph_credentials_file", old) })
	configured := filepath.Join(t.TempDir(), "configured.json")
	override := filepath.Join(t.TempDir(), "override.json")
	for file, data := range map[string]string{
		configured: `{"username":"config-user","password":"config-secret"}`,
		override:   `{"username":"environment-user","password":"environment-secret"}`,
	} {
		if err := os.WriteFile(file, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	SetConfig("hugegraph_credentials_file", configured)
	t.Setenv("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", override)
	username, password, err := ResolveHugegraphCredentials("task-user", "task-secret")
	if err != nil || username != "environment-user" || password != "environment-secret" {
		t.Fatal("environment file must override config and task credentials")
	}
	t.Setenv("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", "")
	username, password, err = ResolveHugegraphCredentials("task-user", "task-secret")
	if err != nil || username != "config-user" || password != "config-secret" {
		t.Fatal("configuration file must override task credentials")
	}
	SetConfig("hugegraph_credentials_file", "")
	username, password, err = ResolveHugegraphCredentials("task-user", "task-secret")
	if err != nil || username != "task-user" || password != "task-secret" {
		t.Fatal("unconfigured deployments must retain legacy task credentials")
	}
	username, password, err = ResolveHugegraphCredentials("", "")
	if err != nil || username != "" || password != "" {
		t.Fatal("unconfigured anonymous deployments must remain usable")
	}
}

func TestHugegraphCredentialsFileFailsClosed(t *testing.T) {
	old := GetConfigDefault("hugegraph_credentials_file", "")
	t.Cleanup(func() { SetConfig("hugegraph_credentials_file", old) })
	fallback := filepath.Join(t.TempDir(), "valid-fallback.json")
	if err := os.WriteFile(fallback, []byte(`{"username":"config-user","password":"config-password"}`), 0600); err != nil {
		t.Fatal(err)
	}
	SetConfig("hugegraph_credentials_file", fallback)
	for _, test := range []struct{ name, data string }{
		{"missing", ""}, {"empty", ""}, {"malformed", `{"username":"secret-user","password":"secret-password"`},
		{"null", `null`}, {"username-missing", `{"password":"secret-password"}`},
		{"username-empty", `{"username":"","password":"secret-password"}`},
		{"password-missing", `{"username":"secret-user"}`},
		{"password-empty", `{"username":"secret-user","password":""}`},
		{"wrong-type", `{"username":"secret-user","password":123}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "credentials.json")
			if test.name != "missing" {
				if err := os.WriteFile(file, []byte(test.data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", file)
			username, password, err := ResolveHugegraphCredentials("task-user", "task-secret")
			if err == nil || username != "" || password != "" {
				t.Fatal("invalid configured credentials must not fall back to task credentials")
			}
			for _, secret := range []string{"secret-user", "secret-password", "task-secret"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("credential contents must not appear in errors")
				}
			}
		})
	}
}
