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
	"fmt"
	"strings"
	"sync"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/graphio"
	pb "vermeer/apps/protos"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type PeerHandler struct {
	pb.UnimplementedWorkerServer
}

func (ph *PeerHandler) SayHelloPeer(ctx context.Context, req *pb.HelloPeerReq) (*pb.HelloPeerResp, error) {
	p, _ := peer.FromContext(ctx)
	logrus.Infof("peer say hello name: %s, client: %s", req.GetSourceName(), p.Addr.String())
	semi := strings.LastIndex(p.Addr.String(), ":")
	if semi < 0 {
		err := fmt.Errorf("worker grpc peer ip error: %s", req)
		logrus.Errorf(err.Error())
		return &pb.HelloPeerResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}
	ip := p.Addr.String()[:semi]

	semi = strings.LastIndex(req.GetWorkerPeer(), ":")
	if semi < 0 {
		err := fmt.Errorf("worker grpc peer port error: %s", req.WorkerPeer)
		logrus.Errorf(err.Error())
		return &pb.HelloPeerResp{Base: &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}}, err
	}
	port := req.GetWorkerPeer()[semi+1:]
	if ServiceWorker.WorkerName != req.TargetName {
		err := fmt.Errorf("worker grpc peer target error: %s", req.TargetName)
		logrus.Errorf(err.Error())
		return &pb.HelloPeerResp{Base: &pb.BaseResponse{ErrorCode: -2, Message: err.Error()}}, err
	}

	PeerMgr.AddPeer(req.SourceName, req.Id, ip+":"+port)
	return &pb.HelloPeerResp{}, nil
}

func (ph *PeerHandler) Scatter(stream pb.Worker_ScatterServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	PeerMgr.GetPeer(name).ScatterHandler.SetServer(stream)
	PeerMgr.GetPeer(name).ScatterHandler.RecvHandler(name)
	return nil
}

func (ph *PeerHandler) LoadAction(stream pb.Worker_LoadActionServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	PeerMgr.GetPeer(name).LoadActionHandler.SetServer(stream)
	PeerMgr.GetPeer(name).LoadActionHandler.RecvHandler(name)
	return nil
}

func (ph *PeerHandler) StepEnd(stream pb.Worker_StepEndServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	PeerMgr.GetPeer(name).StepEndHandler.SetServer(stream)
	PeerMgr.GetPeer(name).StepEndHandler.RecvHandler(name)
	return nil
}

func (ph *PeerHandler) SettingAction(stream pb.Worker_SettingActionServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	name := md.Get("worker_name")[0]
	PeerMgr.GetPeer(name).SettingActionHandler.SetServer(stream)
	PeerMgr.GetPeer(name).SettingActionHandler.RecvHandler(name)
	return nil
}

func (ph *PeerHandler) DeleteGraph(ctx context.Context, req *pb.DeleteGraphReq) (*pb.DeleteGraphResp, error) {
	_ = ctx
	if req.DeleteFile {
		GraphMgr.DeleteGraphFile(req.GetSpaceName(), req.GraphName)
	}
	GraphMgr.DeleteGraph(req.GetSpaceName(), req.GraphName)
	return &pb.DeleteGraphResp{}, nil
}

func (ph *PeerHandler) GetEdges(ctx context.Context, req *pb.GetEdgesReq) (*pb.GetEdgesResp, error) {
	_ = ctx
	eb := EdgesBl{}
	inEdges, outEdges, edgeProperty, err := eb.GetEdges(req.GetSpaceName(), req.GraphName, req.VertexId, req.Direction)
	if err != nil {
		return &pb.GetEdgesResp{}, err
	}
	return &pb.GetEdgesResp{InEdges: inEdges, OutEdges: outEdges, InEdgeProperty: edgeProperty}, nil
}

func (ph *PeerHandler) GetVertex(ctx context.Context, req *pb.GetVertexReq) (*pb.GetVertexResp, error) {
	_, _ = ctx, req

	graph := GraphMgr.GetGraphByName(req.GetSpaceName(), req.GetGraphName())
	vertexIds := req.GetVertexId()
	if graph != nil && len(vertexIds) > 0 {
		resp := &pb.GetVertexResp{}
		resp.Verts = make([]*pb.VertexInfo, 0, len(vertexIds))
		for i := 0; i < len(vertexIds); i++ {
			resp.Verts = append(resp.Verts, &pb.VertexInfo{})
			shortId, ok := graph.Data.Vertex.GetVertexIndex(vertexIds[i])
			if !ok {
				logrus.Errorf("vertexId %s not found", vertexIds[i])
				continue
			}

			if shortId < graph.Data.VertIDStart ||
				shortId >= graph.Data.VertIDStart+graph.Data.VertexCount {
				logrus.Errorf("vertexId %s not in range", vertexIds[i])
				continue
			}
			resp.Verts[i].ID = vertexIds[i]
			if graph.UseProperty {
				properties := make(map[string]string)
				for _, ps := range graph.Data.VertexPropertySchema.Schema {
					properties[ps.PropKey] = graph.Data.VertexProperty.GetValue(ps.PropKey, shortId).ToString()
				}
				resp.Verts[i].Property = properties
			}
		}
		return resp, nil
	}
	return nil, nil
}

// ControlTask 控制任务
func (ph *PeerHandler) ControlTask(ctx context.Context, req *pb.ControlTaskReq) (*pb.ControlTaskResp, error) {
	_ = ctx
	task := TaskMgr.GetTaskByID(req.GetTaskID())
	if task == nil {
		return &pb.ControlTaskResp{}, nil
	}
	err := TaskMgr.SetAction(task, req.Action)
	if err != nil {
		return &pb.ControlTaskResp{}, err
	}
	//SELECT:
	//	for {
	//		switch task.State {
	//		case structure.TaskStateLoaded, structure.TaskStateComplete:
	//			break SELECT
	//		case structure.TaskStateCanceled, structure.TaskStateError:
	//			break SELECT
	//		case structure.TaskStateCanceling:
	//			time.Sleep(100 * time.Millisecond)
	//		default:
	//			task.SetState(structure.TaskStateCanceling)
	//		}
	//	}

	return &pb.ControlTaskResp{}, nil
}

func (ph *PeerHandler) SaveGraph(ctx context.Context, req *pb.GraphPersistenceReq) (*pb.GraphPersistenceResp, error) {
	_ = ctx

	logrus.Infof("SaveGraph %v", req.GraphName)
	err := GraphMgr.SaveGraph(req.GetSpaceName(), req.GraphName, ServiceWorker.WorkerName)
	res := new(pb.GraphPersistenceResp)
	if err != nil {
		logrus.Infof("SaveGraph %v error,err=%v", req.GraphName, err.Error())
		res.Base = &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}
	}
	return res, err
}

func (ph *PeerHandler) ReadGraph(ctx context.Context, req *pb.GraphPersistenceReq) (*pb.GraphPersistenceResp, error) {
	_ = ctx
	graphName := req.GetGraphName()
	res := new(pb.GraphPersistenceResp)
	logrus.Infof("ReadGraph %v", req.GraphName)
	err := GraphMgr.ReadGraph(req.GetSpaceName(), graphName, ServiceWorker.WorkerName)
	if err != nil {
		logrus.Infof("ReadGraph %v error,err=%v", req.GraphName, err.Error())
		res.Base = &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}
		return res, err
	}
	return res, nil
}

func (ph *PeerHandler) WriteDisk(ctx context.Context, req *pb.GraphPersistenceReq) (*pb.GraphPersistenceResp, error) {
	_ = ctx
	graphName := req.GetGraphName()
	res := new(pb.GraphPersistenceResp)
	logrus.Infof("WriteDisk %v", req.GraphName)
	err := GraphMgr.WriteDisk(req.SpaceName, graphName, ServiceWorker.WorkerName)
	if err != nil {
		logrus.Infof("WriteDisk %v error,err=%v", req.GraphName, err.Error())
		res.Base = &pb.BaseResponse{ErrorCode: -1, Message: err.Error()}
		return res, err
	}
	return res, nil
}

func (ph *PeerHandler) GetWorkerStatInfo(ctx context.Context, req *pb.WorkerStatInfoReq) (*pb.WorkerStatInfoResp, error) {
	_ = ctx
	_ = req

	resp := new(pb.WorkerStatInfoResp)
	mp, err := common.MachineMemUsedPercent()
	if err != nil {
		resp.Base.ErrorCode = -1
		resp.Base.Message = err.Error()
	}
	resp.MemMachineUsedPercent = mp

	pp, err := common.ProcessMemUsedPercent()
	if err != nil {
		resp.Base.ErrorCode = -1
		resp.Base.Message = err.Error()
	}
	resp.MemProcessUsedPercent = pp

	//logrus.Infof("GetWorkerStatInfo mem:%v", common.PrintMemUsage())
	//logrus.Infof("GetWorkerStatInfo res:%v", resp)

	return resp, nil

}

func (ph *PeerHandler) RuntimeAction(ctx context.Context, req *pb.RuntimeActionReq) (*pb.RuntimeActionResp, error) {
	_ = ctx
	var resp *pb.RuntimeActionResp
	switch req.GetRequest().(type) {
	case *pb.RuntimeActionReq_HostInfoReq:
		hostInfo, err := RuntimeSpv.HostInfo()
		if err != nil {
			logrus.Errorf("get host info error:%v", err)
			return nil, err
		}
		resp = &pb.RuntimeActionResp{
			Response: &pb.RuntimeActionResp_HostInfoResp{
				HostInfoResp: &pb.GetHostInfoResp{
					TotalMemory:     uint32(hostInfo.TotalMemory / (1024 * 1024)),
					AvailableMemory: uint32(hostInfo.AvailableMemory / (1024 * 1024)),
					CPUCount:        uint32(hostInfo.CPUs),
				},
			},
		}
	case *pb.RuntimeActionReq_MemoryLimitReq:
		RuntimeSpv.SetMemoryLimit(
			uint64(req.GetMemoryLimitReq().MaxMemoryUsed)*1024*1024,
			uint64(req.GetMemoryLimitReq().MinRemainMemory)*1024*1024,
			req.GetMemoryLimitReq().SoftMemoryLimitRatio,
		)
		resp = &pb.RuntimeActionResp{Response: &pb.RuntimeActionResp_MemoryLimitResp{MemoryLimitResp: &pb.SetMemoryLimitResp{}}}
	case *pb.RuntimeActionReq_CPULimitReq:
		RuntimeSpv.SetMaxCPU(int(req.GetCPULimitReq().MaxCPU))
		resp = &pb.RuntimeActionResp{Response: &pb.RuntimeActionResp_CPULimitResp{CPULimitResp: &pb.SetCPULimitResp{}}}
	}

	return resp, nil
}

type LoadGraphTaskHandler struct {
	grpcStream pb.Master_LoadGraphTaskClient
}

func (rh *LoadGraphTaskHandler) HandleLoadGraphTask() {
	for {
		resp, err := rh.grpcStream.Recv()
		if err != nil {
			logrus.Errorf("recv load graph task error: %s", err)
			time.Sleep(3 * time.Second)
			ServiceWorker.ReconnectMaster()
			continue
		}
		loadBl := LoadGraphBl{}
		if resp.Step == pb.LoadStep_Vertex {
			loadBl.StartLoadGraph(resp.SpaceName, resp.TaskId, resp.GraphName, resp.Workers, resp.Params)
		} else if resp.Step == pb.LoadStep_ScatterVertex {
			loadBl.ScatterVertex(resp.TaskId)
		} else if resp.Step == pb.LoadStep_Edge {
			loadBl.LoadEdge(resp.TaskId)
		} else if resp.Step == pb.LoadStep_Complete {
			loadBl.LoadComplete(resp.TaskId)
		} else if resp.Step == pb.LoadStep_OutDegree {
			loadBl.ScatterOutDegree(resp.TaskId)
		} else if resp.Step == pb.LoadStep_Error {
			loadBl.GetStatusError(resp.TaskId)
		}
	}
}

func (rh *LoadGraphTaskHandler) LoadComplete(taskId int32, partId int32) error {
	err := rh.grpcStream.Send(&pb.LoadGraphTaskReq{
		TaskId:     taskId,
		PartId:     partId,
		WorkerName: ServiceWorker.WorkerName,
		State:      graphio.LoadPartStatusDone,
	})
	return err
}

type ComputeTaskHandler struct {
	grpcStream pb.Master_ComputeTaskClient
}

func (rh *ComputeTaskHandler) HandleComputeTask() {
	for {
		resp, err := rh.grpcStream.Recv()
		if err != nil {
			logrus.Errorf("recv compute task error: %s", err)
			time.Sleep(3 * time.Second)
			ServiceWorker.ReconnectMaster()
			continue
		}
		cb := ComputeBl{}
		sb := SettingBl{}
		switch resp.Action {
		case pb.ComputeAction_Compute:
			cb.StartCompute(resp.TaskId, resp.SpaceName, resp.GraphName, resp.Algorithm, resp.Params)
		case pb.ComputeAction_SettingOutEdges:
			sb.StartSettingOutEdges(resp.TaskId, resp.SpaceName, resp.GraphName, resp.Params)
		case pb.ComputeAction_SettingOutDegree:
			sb.StartSettingOutDegree(resp.TaskId, resp.SpaceName, resp.GraphName, resp.Params)
		}
	}
}

func (rh *ComputeTaskHandler) ComputeComplete(taskId int32) error {
	err := rh.grpcStream.Send(&pb.ComputeTaskReq{
		TaskId:     taskId,
		WorkerName: ServiceWorker.WorkerName,
	})
	return err
}

type SuperStepHandler struct {
	grpcStream pb.Master_SuperStepClient
}

func (rh *SuperStepHandler) HandleSuperStep() {
	for {
		resp, err := rh.grpcStream.Recv()
		if err != nil {
			logrus.Errorf("recv super step error: %s", err)
			time.Sleep(3 * time.Second)
			ServiceWorker.ReconnectMaster()
			continue
		}
		cb := ComputeBl{}
		if resp.Output {
			go cb.RunOutput(resp.TaskId)
		} else {
			go cb.RunSuperStep(resp.TaskId, resp.ComputeValues)
		}
	}
}

//func (rh *SuperStepHandler) SuperStepDone(taskId int32, status string) error {
//	err := rh.grpcStream.Send(&pb.SuperStepReq{
//		TaskId:     taskId,
//		State:     status,
//		WorkerName: ServiceWorker.WorkerName,
//	})
//	return err
//}

const (
	HandlerModeClient = byte(1)
	HandlerModeServer = byte(2)
)

type LoadActionHandler struct {
	mode       byte
	grpcServer pb.Worker_LoadActionServer
	grpcClient pb.Worker_LoadActionClient
	locker     sync.Mutex
}

func (dh *LoadActionHandler) SetServer(stream pb.Worker_LoadActionServer) {
	dh.mode = HandlerModeServer
	dh.grpcServer = stream
}

func (dh *LoadActionHandler) SetClient(client pb.Worker_LoadActionClient) {
	dh.mode = HandlerModeClient
	dh.grpcClient = client
}

func (dh *LoadActionHandler) RecvHandler(name string) {
	logrus.Infof("load action recv handler setup: %s", name)
	dh.locker = sync.Mutex{}
	loadBl := LoadGraphBl{}
	if dh.mode == HandlerModeServer {
		for {
			resp, err := dh.grpcServer.Recv()
			if err != nil {
				logrus.Errorf("recv load action error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			//logrus.Infof("LoadActionHandler server recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.LoadAction_LoadVertex {
				go loadBl.RecvVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadScatter {
				go loadBl.GatherVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadEdge {
				go loadBl.RecvEdge(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadOutDegree {
				go loadBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Data)
			}
		}
	} else if dh.mode == HandlerModeClient {
		for {
			resp, err := dh.grpcClient.Recv()
			if err != nil {
				logrus.Errorf("recv load action error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			//logrus.Infof("LoadActionHandler client recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.LoadAction_LoadVertex {
				go loadBl.RecvVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadScatter {
				go loadBl.GatherVertex(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadEdge {
				go loadBl.RecvEdge(
					resp.TaskId,
					resp.WorkerName,
					resp.Count,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.LoadAction_LoadOutDegree {
				go loadBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Data)
			}
		}
	}
}

func (dh *LoadActionHandler) LoadAction(taskId int32, action pb.LoadAction, count int32, end bool, endNum int32, data []byte) {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	if dh.mode == HandlerModeServer {
		req := pb.LoadActionResp{
			TaskId:     taskId,
			Data:       data,
			Count:      count,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler server send action: %v", action)
	} else if dh.mode == HandlerModeClient {
		req := pb.LoadActionReq{
			TaskId:     taskId,
			Data:       data,
			Count:      count,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler client send action: %v", action)
	}
}

type ScatterHandler struct {
	mode       byte
	grpcServer pb.Worker_ScatterServer
	grpcClient pb.Worker_ScatterClient
	locker     sync.Mutex
}

func (sh *ScatterHandler) SetServer(stream pb.Worker_ScatterServer) {
	sh.mode = HandlerModeServer
	sh.grpcServer = stream
}

func (sh *ScatterHandler) SetClient(client pb.Worker_ScatterClient) {
	sh.mode = HandlerModeClient
	sh.grpcClient = client
}

func (sh *ScatterHandler) RecvHandler(name string) {
	logrus.Infof("scatter recv handler setup: %s", name)
	sh.locker = sync.Mutex{}
	if sh.mode == HandlerModeServer {
		for {
			resp, err := sh.grpcServer.Recv()
			if err != nil {
				logrus.Errorf("recv scatter error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			cb := ComputeBl{}
			go cb.RecvScatter(resp.TaskId, resp.Data, resp.End, resp.SIdx)
		}
	} else if sh.mode == HandlerModeClient {
		for {
			resp, err := sh.grpcClient.Recv()
			if err != nil {
				logrus.Errorf("recv scatter error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			cb := ComputeBl{}
			go cb.RecvScatter(resp.TaskId, resp.Data, resp.End, resp.SIdx)
		}
	}
}

func (sh *ScatterHandler) SendScatter(taskId int32, step int32, count int32, end bool, sIdx int32, data []byte) {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	if sh.mode == HandlerModeServer {
		req := pb.ScatterResp{
			TaskId:     taskId,
			Step:       step,
			Data:       data,
			Count:      count,
			End:        end,
			SIdx:       sIdx,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	} else if sh.mode == HandlerModeClient {
		req := pb.ScatterReq{
			TaskId:     taskId,
			Step:       step,
			Data:       data,
			Count:      count,
			End:        end,
			SIdx:       sIdx,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	}
}

type StepEndHandler struct {
	mode       byte
	grpcServer pb.Worker_StepEndServer
	grpcClient pb.Worker_StepEndClient
	locker     sync.Mutex
}

func (sh *StepEndHandler) SetServer(stream pb.Worker_StepEndServer) {
	sh.mode = HandlerModeServer
	sh.grpcServer = stream
}

func (sh *StepEndHandler) SetClient(client pb.Worker_StepEndClient) {
	sh.mode = HandlerModeClient
	sh.grpcClient = client
}

func (sh *StepEndHandler) RecvHandler(name string) {
	logrus.Infof("step end recv handler setup: %s", name)
	sh.locker = sync.Mutex{}
	if sh.mode == HandlerModeServer {
		for {
			resp, err := sh.grpcServer.Recv()
			if err != nil {
				logrus.Errorf("recv step end error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			cb := ComputeBl{}
			go cb.StepEnd(resp.TaskId, resp.WorkerName)
		}
	} else if sh.mode == HandlerModeClient {
		for {
			resp, err := sh.grpcClient.Recv()
			if err != nil {
				logrus.Errorf("recv step end error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			cb := ComputeBl{}
			go cb.StepEnd(resp.TaskId, resp.WorkerName)
		}
	}
}

func (sh *StepEndHandler) SendStepEnd(taskId int32) {
	sh.locker.Lock()
	defer sh.locker.Unlock()
	if sh.mode == HandlerModeServer {
		req := pb.StepEndResp{
			TaskId:     taskId,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	} else if sh.mode == HandlerModeClient {
		req := pb.StepEndReq{
			TaskId:     taskId,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := sh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send scatter error: %s", err)
		}
	}
}

type SettingActionHandler struct {
	mode       byte
	grpcServer pb.Worker_SettingActionServer
	grpcClient pb.Worker_SettingActionClient
	locker     sync.Mutex
}

func (dh *SettingActionHandler) SetServer(stream pb.Worker_SettingActionServer) {
	dh.mode = HandlerModeServer
	dh.grpcServer = stream
}

func (dh *SettingActionHandler) SetClient(client pb.Worker_SettingActionClient) {
	dh.mode = HandlerModeClient
	dh.grpcClient = client
}

func (dh *SettingActionHandler) RecvHandler(name string) {
	logrus.Infof("load action recv handler setup: %s", name)
	dh.locker = sync.Mutex{}
	settingBl := SettingBl{}
	if dh.mode == HandlerModeServer {
		for {
			resp, err := dh.grpcServer.Recv()
			if err != nil {
				logrus.Errorf("recv setting action error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			//logrus.Infof("LoadActionHandler server recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.SettingAction_SetOutEdges {
				go settingBl.GatherOutEdges(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.SettingAction_SetOutDegree {
				go settingBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			}
		}
	} else if dh.mode == HandlerModeClient {
		for {
			resp, err := dh.grpcClient.Recv()
			if err != nil {
				logrus.Errorf("recv setting action error: %s", err)
				time.Sleep(2 * time.Second)
				if !PeerMgr.CheckPeerAlive(name) {
					PeerMgr.RemovePeer(name)
					break
				}
				continue
			}
			//logrus.Infof("LoadActionHandler client recv action: %v, end: %v", resp.Action, resp.End)
			if resp.Action == pb.SettingAction_SetOutEdges {
				go settingBl.GatherOutEdges(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			} else if resp.Action == pb.SettingAction_SetOutDegree {
				go settingBl.GatherOutDegree(
					resp.TaskId,
					resp.WorkerName,
					resp.End,
					resp.Num,
					resp.Data)
			}
		}
	}
}

func (dh *SettingActionHandler) SettingAction(taskId int32, action pb.SettingAction, end bool, endNum int32, data []byte) {
	dh.locker.Lock()
	defer dh.locker.Unlock()
	if dh.mode == HandlerModeServer {
		req := pb.SettingActionResp{
			TaskId:     taskId,
			Data:       data,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcServer.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler server send action: %v", action)
	} else if dh.mode == HandlerModeClient {
		req := pb.SettingActionReq{
			TaskId:     taskId,
			Data:       data,
			Action:     action,
			End:        end,
			Num:        endNum,
			WorkerName: ServiceWorker.WorkerName,
		}
		err := dh.grpcClient.Send(&req)
		if err != nil {
			logrus.Errorf("send do action error: %s", err)
		}
		//logrus.Infof("LoadActionHandler client send action: %v", action)
	}
}
