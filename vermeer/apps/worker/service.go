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
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"vermeer/algorithms"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/graphio"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"
	"vermeer/apps/version"
)

var ServiceWorker Service

var GraphMgr = structure.GraphManager
var TaskMgr = structure.TaskManager
var LoadGraphMgr = graphio.LoadGraphWorker
var ComputeTaskMgr = compute.ComputerTaskManager
var AlgorithmMgr = compute.AlgorithmManager
var SpaceMgr = structure.SpaceManager

type Service struct {
	masterConn         *grpc.ClientConn
	MasterClient       pb.MasterClient
	LoadGraphHandler   *LoadGraphTaskHandler
	ComputeTaskHandler *ComputeTaskHandler
	SuperStepHandler   *SuperStepHandler
	locker             *sync.Mutex
	WorkerName         string
	WorkerId           int32
}

func (s *Service) Init() error {
	var err error
	//node, err := snowflake.NewNode(rand.Int63n(1023))
	//if err != nil {
	//	logrus.Errorf("new snowflake error: %s", err)
	//}
	//s.WorkerName = node.Generate().String()
	s.ComputeTaskHandler = &ComputeTaskHandler{}
	s.LoadGraphHandler = &LoadGraphTaskHandler{}
	s.SuperStepHandler = &SuperStepHandler{}
	masterPeer := common.GetConfig("master_peer").(string)
	grpcPeer := common.GetConfig("grpc_peer").(string)
	// init peer manager
	PeerMgr.Init()

	dialOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallSendMsgSize(4*1024*1024*1024),
		grpc.MaxCallRecvMsgSize(4*1024*1024*1024))

	s.masterConn, err = grpc.Dial(
		masterPeer, grpc.WithTransportCredentials(insecure.NewCredentials()), dialOptions, grpc.WithBlock(), grpc.WithIdleTimeout(0))
	if err != nil {
		logrus.Errorf("connect master error: %s", err)
		return err
	}
	s.MasterClient = pb.NewMasterClient(s.masterConn)
	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	resp, err := s.MasterClient.SayHelloMaster(
		ctx, &pb.HelloMasterReq{WorkerPeer: grpcPeer, Version: version.Version})
	if err != nil {
		logrus.Errorf("say hello master error: %s", err)
		return err
	}
	s.WorkerId = resp.GetWorkerId()
	s.WorkerName = resp.GetWorkerName()
	logrus.Infof("Hello World, I am worker %s", s.WorkerName)
	PeerMgr.AddPeer(s.WorkerName, s.WorkerId, grpcPeer)
	md := metadata.New(map[string]string{"worker_name": s.WorkerName})
	for _, w := range resp.GetWorkers() {
		PeerMgr.AddPeer(w.Name, w.Id, w.GrpcPeer)
		peerClient := PeerMgr.GetPeer(w.Name)
		peerClient.peerConn, err = grpc.Dial(
			w.GrpcPeer, grpc.WithTransportCredentials(insecure.NewCredentials()), dialOptions, grpc.WithBlock(), grpc.WithIdleTimeout(0))
		if err != nil {
			logrus.Fatalf("connect to peer %s error: %s", w.GrpcPeer, err)
			return err
		}
		peerClient.peerSession = pb.NewWorkerClient(peerClient.peerConn)
		ctx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := peerClient.peerSession.SayHelloPeer(
			ctx, &pb.HelloPeerReq{SourceName: s.WorkerName, TargetName: w.Name, WorkerPeer: grpcPeer})
		if err != nil {
			logrus.Fatalf("say hello peer %s error: %s", w.GrpcPeer, err)
		}

		// install scatter
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream, err := peerClient.peerSession.Scatter(ctx)
		if err != nil {
			logrus.Fatalf("create scatter stream peer: %s error: %s", w.GrpcPeer, err)
		}
		peerClient.ScatterHandler.SetClient(stream)

		// install load action
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream2, err := peerClient.peerSession.LoadAction(ctx)
		if err != nil {
			logrus.Fatalf("create load action stream peer: %s error: %s", w.GrpcPeer, err)
		}
		peerClient.LoadActionHandler.SetClient(stream2)

		// install step end
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream3, err := peerClient.peerSession.StepEnd(ctx)
		if err != nil {
			logrus.Fatalf("create step end stream peer: %s error: %s", w.GrpcPeer, err)
		}
		peerClient.StepEndHandler.SetClient(stream3)

		// install setting action
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream4, err := peerClient.peerSession.SettingAction(ctx)
		if err != nil {
			logrus.Fatalf("create step end stream peer: %s error: %s", w.GrpcPeer, err)
		}
		peerClient.SettingActionHandler.SetClient(stream4)

		logrus.Infof("say hello peer %s, ok, %s", w.GrpcPeer, resp.GetStatus())
		cancel2()
	}
	logrus.Infof("say hello master ok, worker id: %d", s.WorkerId)

	ctx = metadata.NewOutgoingContext(context.Background(), md)
	s.LoadGraphHandler.grpcStream, err = s.MasterClient.LoadGraphTask(ctx)
	if err != nil {
		logrus.Errorf("create load graph task stream error: %s", err)
		return err
	}
	logrus.Infof("create load graph task stream ok")

	ctx = metadata.NewOutgoingContext(context.Background(), md)
	s.ComputeTaskHandler.grpcStream, err = s.MasterClient.ComputeTask(ctx)
	if err != nil {
		logrus.Errorf("create load graph task stream error: %s", err)
		return err
	}
	logrus.Infof("create compute task stream ok")

	ctx = metadata.NewOutgoingContext(context.Background(), md)
	stepReq := pb.SuperStepReq{}
	s.SuperStepHandler.grpcStream, err = s.MasterClient.SuperStep(ctx, &stepReq)
	if err != nil {
		logrus.Errorf("create super step stream error: %s", err)
		return err
	}
	logrus.Infof("create super step stream ok")

	SpaceMgr.Init()
	GraphMgr.Init()
	TaskMgr.Init()
	LoadGraphMgr.Init()
	ComputeTaskMgr.Init()
	AlgorithmMgr.Init()
	for _, maker := range algorithms.Algorithms {
		AlgorithmMgr.Register(maker, "built-in")
	}
	AlgorithmMgr.LoadPlugins()
	s.locker = &sync.Mutex{}
	// for _, space := range resp.Spaces {
	// 	err := createSpaceIfAbsent(space)
	// 	if err != nil {
	// 		logrus.Errorf("create space error:%v", err)
	// 		return err
	// 	}
	// }
	return nil
}

func (s *Service) Run() {
	//与master的连接
	go s.LoadGraphHandler.HandleLoadGraphTask()
	go s.ComputeTaskHandler.HandleComputeTask()
	go s.SuperStepHandler.HandleSuperStep()

	//与peer worker的连接
	for _, w := range PeerMgr.GetAllWorkers() {
		if w.ScatterHandler.mode == HandlerModeClient {
			go w.ScatterHandler.RecvHandler(w.Name)
		}
		if w.LoadActionHandler.mode == HandlerModeClient {
			go w.LoadActionHandler.RecvHandler(w.Name)
		}
		if w.StepEndHandler.mode == HandlerModeClient {
			go w.StepEndHandler.RecvHandler(w.Name)
		}
		if w.SettingActionHandler.mode == HandlerModeClient {
			go w.SettingActionHandler.RecvHandler(w.Name)
		}
	}
}

func (s *Service) Close() {
	err := s.masterConn.Close()
	if err != nil {
		logrus.Errorf("master connection close error: %s", err)
	}
	logrus.Infof("master connection closed.")
}

func (s *Service) CheckMasterAlive() bool {
	if s.masterConn == nil || s.masterConn.GetState() != connectivity.Ready {
		return false
	}
	return true
}

func (s *Service) ReconnectMaster() {
	s.locker.Lock()
	defer s.locker.Unlock()
	if s.CheckMasterAlive() {
		logrus.Infof("master is alive:%v", s.masterConn.GetState())
		return
	}
	logrus.Infof("try to reconnect master")
	//初始化
	SpaceMgr.Init()
	GraphMgr.Init()
	TaskMgr.Init()
	LoadGraphMgr.Init()
	ComputeTaskMgr.Init()
	PeerMgr.Init()
	//common.PrometheusMetrics.TaskCnt.Reset()
	common.PrometheusMetrics.TaskRunningCnt.Reset()
	//common.PrometheusMetrics.GraphCnt.Reset()
	//common.PrometheusMetrics.GraphLoadedCnt.Reset()
	masterPeer := common.GetConfig("master_peer").(string)
	grpcPeer := common.GetConfig("grpc_peer").(string)
	//连接master
	dialOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallSendMsgSize(4*1024*1024*1024),
		grpc.MaxCallRecvMsgSize(4*1024*1024*1024),
	)
	var err error
	s.masterConn, err = grpc.Dial(
		masterPeer, grpc.WithTransportCredentials(insecure.NewCredentials()), dialOptions, grpc.WithBlock(), grpc.WithIdleTimeout(0))
	if err != nil {
		logrus.Errorf("connect master error: %s", err)
		return
	}
	s.MasterClient = pb.NewMasterClient(s.masterConn)
	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	resp, err := s.MasterClient.SayHelloMaster(
		ctx, &pb.HelloMasterReq{WorkerPeer: grpcPeer, Version: version.Version})
	if err != nil {
		logrus.Infof("reconnect say hello master error:%v", err)
		s.masterConn = nil
		return
	}
	s.WorkerId = resp.GetWorkerId()
	s.WorkerName = resp.GetWorkerName()
	logrus.Infof("Reconnect, I am worker %s", s.WorkerName)
	PeerMgr.AddPeer(s.WorkerName, s.WorkerId, grpcPeer)
	md := metadata.New(map[string]string{"worker_name": s.WorkerName})
	for _, workerInfo := range resp.GetWorkers() {
		//对已建立grpc连接的peer忽略。建立未知peer的grpc连接，并启动handler
		peer := PeerMgr.GetPeer(workerInfo.GetName())
		if peer != nil {
			continue
		}
		PeerMgr.AddPeer(workerInfo.Name, workerInfo.Id, workerInfo.GrpcPeer)
		peerClient := PeerMgr.GetPeer(workerInfo.Name)
		peerClient.peerConn, err = grpc.Dial(
			workerInfo.GrpcPeer, grpc.WithTransportCredentials(insecure.NewCredentials()), dialOptions, grpc.WithBlock(), grpc.WithIdleTimeout(0))
		if err != nil {
			logrus.Fatalf("connect to peer %s error: %s", workerInfo.GrpcPeer, err)
			return
		}
		peerClient.peerSession = pb.NewWorkerClient(peerClient.peerConn)
		ctx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := peerClient.peerSession.SayHelloPeer(
			ctx, &pb.HelloPeerReq{SourceName: s.WorkerName, TargetName: workerInfo.Name, WorkerPeer: grpcPeer})
		if err != nil {
			logrus.Fatalf("say hello peer %s error: %s", workerInfo.GrpcPeer, err)
		}

		// install scatter
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream, err := peerClient.peerSession.Scatter(ctx)
		if err != nil {
			logrus.Fatalf("create scatter stream peer: %s error: %s", workerInfo.GrpcPeer, err)
		}
		peerClient.ScatterHandler.SetClient(stream)

		// install load action
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream2, err := peerClient.peerSession.LoadAction(ctx)
		if err != nil {
			logrus.Fatalf("create load action stream peer: %s error: %s", workerInfo.GrpcPeer, err)
		}
		peerClient.LoadActionHandler.SetClient(stream2)

		// install step end
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream3, err := peerClient.peerSession.StepEnd(ctx)
		if err != nil {
			logrus.Fatalf("create step end stream peer: %s error: %s", workerInfo.GrpcPeer, err)
		}
		peerClient.StepEndHandler.SetClient(stream3)

		// install setting action
		ctx = metadata.NewOutgoingContext(context.Background(), md)
		stream4, err := peerClient.peerSession.SettingAction(ctx)
		if err != nil {
			logrus.Fatalf("create step end stream peer: %s error: %s", workerInfo.GrpcPeer, err)
		}
		peerClient.SettingActionHandler.SetClient(stream4)

		logrus.Infof("say hello peer %s, ok, %s", workerInfo.GrpcPeer, resp.GetStatus())
		cancel2()
		if peerClient.ScatterHandler.mode == HandlerModeClient {
			go peerClient.ScatterHandler.RecvHandler(peerClient.Name)
		}
		if peerClient.LoadActionHandler.mode == HandlerModeClient {
			go peerClient.LoadActionHandler.RecvHandler(peerClient.Name)
		}
		if peerClient.StepEndHandler.mode == HandlerModeClient {
			go peerClient.StepEndHandler.RecvHandler(peerClient.Name)
		}
		if peerClient.SettingActionHandler.mode == HandlerModeClient {
			go peerClient.SettingActionHandler.RecvHandler(peerClient.Name)
		}
	}

	ctx = metadata.NewOutgoingContext(context.Background(), md)
	tempLoadGraphGrpcStream, err := s.MasterClient.LoadGraphTask(ctx)
	if err != nil {
		logrus.Errorf("reconnect create load graph task stream error: %s", err)
		return
	}
	s.LoadGraphHandler.grpcStream = tempLoadGraphGrpcStream
	logrus.Infof("reconnect create load graph task stream ok")

	ctx = metadata.NewOutgoingContext(context.Background(), md)
	tempComputeGrpcStream, err := s.MasterClient.ComputeTask(ctx)
	if err != nil {
		logrus.Errorf("reconnect create load graph task stream error: %s", err)
		return
	}
	s.ComputeTaskHandler.grpcStream = tempComputeGrpcStream
	logrus.Infof("reconnect create compute task stream ok")

	ctx = metadata.NewOutgoingContext(context.Background(), md)
	stepReq := pb.SuperStepReq{}
	tempSuperStepGrpcStream, err := s.MasterClient.SuperStep(ctx, &stepReq)
	if err != nil {
		logrus.Errorf("reconnect create super step stream error: %s", err)
		return
	}
	s.SuperStepHandler.grpcStream = tempSuperStepGrpcStream
	logrus.Infof("reconnect create super step stream ok")
}
