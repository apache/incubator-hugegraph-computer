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

package master

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"vermeer/apps/auth"
	"vermeer/apps/common"
	"vermeer/apps/master/services"
	"vermeer/apps/version"
	"vermeer/asset"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var empty = common.Empty{}

func Main() {
	var sen *common.Sentinel
	httpPeer := common.GetConfig("http_peer").(string)
	debugMode := common.GetConfig("debug_mode").(string)
	authType := common.GetConfigDefault("auth", "none").(string)
	services.InfoMaster.IpAddr = httpPeer
	services.InfoMaster.DebugMod = debugMode
	services.InfoMaster.Version = version.Version

	sen = new(common.Sentinel)
	sen.Init(httpPeer, debugMode)

	switch authType {
	case "token":
		sen.StaticFS("ui", asset.Assets)
		auth.Init()
		services.SetRouters(sen, auth.TokenFilter)
		services.SetAdminRouters(sen, auth.TokenFilter, auth.AdminFilter)
		services.SetUI(sen)
		logrus.Info("token-auth was activated")
	default:
		services.SetAdminRouters(sen, auth.NoneAuthFilter)
		services.SetRouters(sen, auth.NoneAuthFilter)
		logrus.Warn("No authentication was activated.")
	}

	//ServiceMaster = services.Service{}
	err := services.ServiceMaster.Init()
	if err != nil {
		logrus.Fatalf("master service init error: %s", err)
	}
	grpcPeer := common.GetConfig("grpc_peer").(string)
	services.InfoMaster.GrpcPeer = grpcPeer
	tcpListener, err := net.Listen("tcp", grpcPeer)
	if err != nil {
		logrus.Fatalf("failed to listen: %s", err)
	}
	serverOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(4 * 1024 * 1024 * 1024),
		grpc.MaxSendMsgSize(4 * 1024 * 1024 * 1024)}

	// create grpc server
	grpcServer := grpc.NewServer(serverOptions...)
	//register reflection
	reflection.Register(grpcServer)
	// register grpc handler
	services.GrpcRegister(grpcServer)

	go sen.Run()
	logrus.Infof("master http service start %s on peer: %s...", debugMode, httpPeer)

	go services.ServiceMaster.Run()

	go func() {
		if err := grpcServer.Serve(tcpListener); err != nil {
			logrus.Fatalf("faild to server: %s", err)
		}
	}()
	logrus.Infof("master grpc service start on peer: %s...", grpcPeer)

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
