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
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestPDAuthorityPasswordFile(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "pd-password")
	if err := os.WriteFile(passwordFile, []byte("test:shared-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VERMEER_PD_PASSWORD_FILE", passwordFile)
	authority := &PDAuthority{}
	ctx, err := authority.SetAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	values := md.Get(CredentialKey)
	if len(values) != 1 {
		t.Fatal("missing PD credential")
	}
	decoded, err := base64.StdEncoding.DecodeString(values[0])
	if err != nil || string(decoded) != "vermeer:test:shared-secret" {
		t.Fatal("PD credential must use the configured shared secret")
	}
}

func TestPDAuthorityRejectsUnavailablePasswordFile(t *testing.T) {
	for _, missing := range []bool{true, false} {
		file := filepath.Join(t.TempDir(), "password")
		if !missing {
			if err := os.WriteFile(file, nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		t.Setenv("VERMEER_PD_PASSWORD_FILE", file)
		if _, err := (&PDAuthority{}).SetAuthority(context.Background()); err == nil {
			t.Fatal("must not fall back to legacy credentials for an invalid configured file")
		}
	}
}
