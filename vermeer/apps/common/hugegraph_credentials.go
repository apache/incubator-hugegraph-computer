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
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// ResolveHugegraphCredentials keeps deployment credentials out of persisted task parameters.
// Without a configured file, legacy callers retain their task-provided credentials.
func ResolveHugegraphCredentials(username, password string) (string, string, error) {
	file := deploymentConfig("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", "hugegraph_credentials_file")
	if file == "" {
		return username, password, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", "", fmt.Errorf("read HugeGraph credentials file: %w", err)
	}
	var credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if json.Unmarshal(data, &credentials) != nil || credentials.Username == "" || credentials.Password == "" {
		// Do not include decoder errors or input bytes in task errors or logs.
		return "", "", errors.New("HugeGraph credentials file must contain a nonempty username and password")
	}
	return credentials.Username, credentials.Password, nil
}
