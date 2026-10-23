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
	"net"
	"os"
	"slices"
	"strconv"

	"github.com/sirupsen/logrus"
)

func deploymentConfig(environment, key string) string {
	if value := os.Getenv(environment); value != "" {
		return value
	}
	value, _ := GetConfigDefault(key, "").(string)
	return value
}

// Deployment discovery takes precedence over task parameters on both Master and Worker.
func resolvePDPeers(taskPeers []string) ([]string, error) {
	configured := deploymentConfig("VERMEER_PD_PEERS", "pd_peers")
	if configured == "" {
		if deploymentConfig("VERMEER_PD_PASSWORD_FILE", "pd_password_file") != "" ||
			deploymentConfig("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", "hugegraph_credentials_file") != "" {
			return nil, errors.New("configure pd_peers when using deployment credentials")
		}
		return taskPeers, nil
	}
	var peers []string
	if json.Unmarshal([]byte(configured), &peers) != nil || len(peers) == 0 {
		return nil, errors.New("pd_peers must be a nonempty JSON array of host:port addresses")
	}
	for _, peer := range peers {
		host, port, err := net.SplitHostPort(peer)
		number, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || number < 1 || number > 65535 {
			return nil, errors.New("pd_peers must contain valid host:port addresses")
		}
	}
	for _, peer := range taskPeers {
		if !slices.Contains(peers, peer) {
			logrus.Warn("configured pd_peers override task PD peers")
			break
		}
	}
	return peers, nil
}

func requireTrustedPD(peer string) error {
	peers, err := resolvePDPeers([]string{peer})
	if err != nil {
		return err
	}
	if !slices.Contains(peers, peer) {
		return errors.New("PD address is outside configured pd_peers")
	}
	return nil
}
