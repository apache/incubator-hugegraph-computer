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
	"net"
	"os"
	"os/signal"
	"syscall"
	"vermeer/apps/common"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func Main() {
	var sen *common.Sentinel
	httpPeer := common.GetConfig("http_peer").(string)
	debugMode := common.GetConfig("debug_mode").(string)
	go func() {
		sen = new(common.Sentinel)
		sen.Init(httpPeer, debugMode)
		sen.Register(MakeRouter())
		sen.Run()
	}()
	logrus.Infof("worker http service start %s on peer: %s...", debugMode, httpPeer)

	grpcPeer := common.GetConfig("grpc_peer").(string)
	tcpListener, err := net.Listen("tcp", grpcPeer)
	if err != nil {
		logrus.Fatalf("failed to listen: %s", err)
	}
	// create grpc server
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1*1024*1024*1024),
		grpc.MaxSendMsgSize(1*1024*1024*1024),
	)
	//register reflection
	reflection.Register(grpcServer)
	GrpcRegister(grpcServer)
	go func() {
		if err := grpcServer.Serve(tcpListener); err != nil {
			logrus.Fatalf("faild to server: %s", err)
		}
	}()
	logrus.Infof("worker grpc service start on peer: %s...", grpcPeer)

	ServiceWorker = Service{}
	err = ServiceWorker.Init()
	if err != nil {
		logrus.Fatalf("worker service init error: %s", err)
	}
	ServiceWorker.Run()

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logrus.Infof("Shutdown Server ...")
	if sen != nil {
		cancel := sen.Shutdown()
		defer cancel()
	}

	grpcServer.Stop()
}
