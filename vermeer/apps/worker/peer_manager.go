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

package worker

import (
	"sync"
	"vermeer/apps/common"
	pb "vermeer/apps/protos"

	"github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var PeerMgr PeerManager

type PeerClient struct {
	Id                   int32
	Name                 string
	GrpcPeer             string
	Self                 bool
	ScatterHandler       ScatterHandler
	LoadActionHandler    LoadActionHandler
	StepEndHandler       StepEndHandler
	SettingActionHandler SettingActionHandler
	peerConn             *grpc.ClientConn
	peerSession          pb.WorkerClient
}

type PeerManager struct {
	peersByName map[string]*PeerClient
	locker      sync.Mutex
}

func (pm *PeerManager) Init() {
	pm.locker = sync.Mutex{}
	pm.peersByName = make(map[string]*PeerClient)
}

func (pm *PeerManager) AddPeer(name string, id int32, grpcPeer string) {
	pm.locker.Lock()
	defer pm.locker.Unlock()
	peer := PeerClient{
		Id:       id,
		Name:     name,
		GrpcPeer: grpcPeer,
		Self:     name == ServiceWorker.WorkerName,
	}
	pm.peersByName[name] = &peer
	common.PrometheusMetrics.WorkerCnt.WithLabelValues().Inc()
}

func (pm *PeerManager) RemovePeer(name string) {
	pm.locker.Lock()
	defer pm.locker.Unlock()
	_, ok := pm.peersByName[name]
	if !ok {
		return
	}
	logrus.Infof("removed peer:%v", name)
	delete(pm.peersByName, name)
	common.PrometheusMetrics.WorkerCnt.WithLabelValues().Dec()
}

func (pm *PeerManager) GetPeer(name string) *PeerClient {
	pm.locker.Lock()
	defer pm.locker.Unlock()
	return pm.peersByName[name]
}

func (pm *PeerManager) GetAllWorkers() []*PeerClient {
	pm.locker.Lock()
	defer pm.locker.Unlock()
	peers := make([]*PeerClient, 0)
	for _, v := range pm.peersByName {
		peers = append(peers, v)
	}
	return peers
}

func (pm *PeerManager) CheckPeerAlive(name string) bool {
	pm.locker.Lock()
	defer pm.locker.Unlock()
	peer, ok := pm.peersByName[name]
	if !ok || peer.peerConn == nil || peer.peerConn.GetState() == connectivity.Idle {
		return false
	}
	return true
}
