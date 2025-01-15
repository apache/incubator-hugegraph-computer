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
	"sync"
	"sync/atomic"
	"time"
	"vermeer/apps/buffer"
	"vermeer/apps/common"
	"vermeer/apps/compute"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type SettingBl struct {
}

// StartSettingOutEdges 启动设置图的出边任务
func (sb *SettingBl) StartSettingOutEdges(taskID int32, spaceName string, graphName string, params map[string]string) {

	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		logrus.Errorf("graph not eixist: %s", graphName)
		sb.SetStatusError(taskID, fmt.Sprintf("graph not eixist: %s", graphName))
		return
	}
	graph.UseOutEdges = true
	graph.Data.Edges.Init(graph.UseOutEdges, graph.UseOutDegree)

	graph.Data.Edges.BuildOutEdges(options.GetInt(graph.Params, "load.edges_per_vertex"), graph.Data.VertexCount)

	var err error
	ct := ComputeTaskMgr.GetTask(taskID)
	if ct == nil {
		bl := ComputeBl{}
		ct, err = bl.initComputeTask(taskID, spaceName, graphName, params)
		if err != nil {
			logrus.Errorf("init compute task error:%v", err)
			sb.SetStatusError(taskID, fmt.Sprintf("init compute task error:%v", err))
			return
		}
	}

	ct.StepWg.Add(1)
	defer ct.StepWg.Done()

	ct.SendCount = make(map[string]*int32, len(graph.Workers))
	ct.RecvCount = make(map[string]*int32, len(graph.Workers))
	for _, worker := range graph.Workers {
		ct.SendCount[worker.Name] = new(int32)
		ct.RecvCount[worker.Name] = new(int32)
	}

	go sb.ScatterOutEdges(taskID, spaceName, graphName)
}

// StartSettingOutDegree 启动设置图的出度
//
// 参数：
// - taskID: int32，任务ID
// - spaceName: string，图所在的命名空间
// - graphName: string，图名
// - params: map[string]string，参数
//
// 返回值：无
func (sb *SettingBl) StartSettingOutDegree(taskID int32, spaceName string, graphName string, params map[string]string) {
	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		logrus.Errorf("graph not eixist: %s", graphName)
		sb.SetStatusError(taskID, fmt.Sprintf("graph not eixist: %s", graphName))
		return
	}

	graph.UseOutDegree = true
	graph.Data.Edges.Init(graph.UseOutEdges, graph.UseOutDegree)

	graph.Data.Edges.BuildOutDegree(graph.Data.Vertex.TotalVertexCount())

	var err error
	ct := ComputeTaskMgr.GetTask(taskID)
	if ct == nil {
		bl := ComputeBl{}
		ct, err = bl.initComputeTask(taskID, spaceName, graphName, params)
		if err != nil {
			logrus.Errorf("init compute task error:%v", err)
			sb.SetStatusError(taskID, fmt.Sprintf("init compute task error:%v", err))
			return
		}
	}

	ct.StepWg.Add(1)
	defer ct.StepWg.Done()

	ct.SendCount = make(map[string]*int32, len(graph.Workers))
	ct.RecvCount = make(map[string]*int32, len(graph.Workers))
	for _, worker := range graph.Workers {
		ct.SendCount[worker.Name] = new(int32)
		ct.RecvCount[worker.Name] = new(int32)
	}

	go sb.ScatterOutDegree(taskID, spaceName, graphName)

}

func (sb *SettingBl) ScatterOutEdges(taskID int32, spaceName string, graphName string) {
	defer func() {
		if r := recover(); r != nil {
			sb.SetStatusError(taskID, fmt.Sprintf("ScatterOutEdges panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("ScatterOutEdges panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	logrus.Infof("start scatter out edges, task_id:%v, space:%v, graph:%v", taskID, spaceName, graphName)
	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	computeTask := ComputeTaskMgr.GetTask(taskID)
	workerCount := len(graph.Workers)
	peers := make([]*PeerClient, 0, workerCount)
	for _, worker := range graph.Workers {
		peers = append(peers, PeerMgr.GetPeer(worker.Name))
	}
	if !sb.CheckAction(computeTask) {
		return
	}
	parallel := *computeTask.Parallel
	partCnt := int32(graph.Data.VertexCount)/parallel + 1
	wg := sync.WaitGroup{}
	for i := int32(0); i < parallel; i++ {
		wg.Add(1)
		go func(pID int32) {
			defer func() {
				if r := recover(); r != nil {
					sb.SetStatusError(taskID, fmt.Sprintf("ScatterOutEdges panic in goroutine recover panic:%v, stack message: %s",
						r, common.GetCurrentGoroutineStack()))
					logrus.Errorf("ScatterOutEdges panic recover in goroutine taskID:%v, pId:%v panic:%v, stack message: %s",
						taskID, pID, r, common.GetCurrentGoroutineStack())
				}
			}()
			defer wg.Done()
			sendBuffers := make([]buffer.EncodeBuffer, 0, workerCount)
			for range graph.Workers {
				buf := buffer.EncodeBuffer{}
				buf.Init(BufferSize)
				sendBuffers = append(sendBuffers, buf)
			}
			bIdx := uint32(partCnt * pID)
			eIdx := bIdx + uint32(partCnt)
			if eIdx > graph.Data.VertexCount {
				eIdx = graph.Data.VertexCount
			}
			edge := structure.IntEdge{}
			for vertID := bIdx; vertID < eIdx; vertID++ {

				edge.Target = vertID + graph.Data.VertIDStart
				inEdges := graph.Data.Edges.GetInEdges(vertID)
				for _, source := range inEdges {
					edge.Source = uint32(source)
					sendWorkerIDx := -1
					for workerIDx, worker := range graph.Workers {
						if edge.Source >= worker.VertIdStart && edge.Source < worker.VertIdStart+worker.VertexCount {
							sendWorkerIDx = workerIDx
							break
						}
					}
					_ = sendBuffers[sendWorkerIDx].Marshal(&edge)
					if sendBuffers[sendWorkerIDx].Full() {
						atomic.AddInt32(computeTask.SendCount[peers[sendWorkerIDx].Name], 1)
						if peers[sendWorkerIDx].Self {
							sb.GatherOutEdges(
								computeTask.Task.ID,
								peers[sendWorkerIDx].Name,
								false,
								atomic.LoadInt32(computeTask.SendCount[peers[sendWorkerIDx].Name]),
								sendBuffers[sendWorkerIDx].PayLoad())
						} else {
							peers[sendWorkerIDx].SettingActionHandler.SettingAction(
								computeTask.Task.ID,
								pb.SettingAction_SetOutEdges,
								false,
								atomic.LoadInt32(computeTask.SendCount[peers[sendWorkerIDx].Name]),
								sendBuffers[sendWorkerIDx].PayLoad())
						}
						sendBuffers[sendWorkerIDx].Reset()
					}
				}
			}
			for i, peer := range peers {
				atomic.AddInt32(computeTask.SendCount[peers[i].Name], 1)
				if peer.Self {
					sb.GatherOutEdges(
						computeTask.Task.ID,
						peer.Name,
						false,
						atomic.LoadInt32(computeTask.SendCount[peers[i].Name]),
						sendBuffers[i].PayLoad())
				} else {
					peer.SettingActionHandler.SettingAction(
						computeTask.Task.ID,
						pb.SettingAction_SetOutEdges,
						false,
						atomic.LoadInt32(computeTask.SendCount[peers[i].Name]),
						sendBuffers[i].PayLoad())
				}
				sendBuffers[i].Reset()
			}

		}(i)
	}
	wg.Wait()
	for i := range peers {
		atomic.AddInt32(computeTask.SendCount[peers[i].Name], 1)
		if peers[i].Self {
			sb.GatherOutEdges(
				computeTask.Task.ID,
				peers[i].Name,
				true,
				atomic.LoadInt32(computeTask.SendCount[peers[i].Name]),
				[]byte{})
		} else {
			peers[i].SettingActionHandler.SettingAction(
				computeTask.Task.ID,
				pb.SettingAction_SetOutEdges,
				true,
				atomic.LoadInt32(computeTask.SendCount[peers[i].Name]),
				[]byte{})
		}
	}
	for s := range computeTask.SendCount {
		*computeTask.SendCount[s] = 0
	}
}

func (sb *SettingBl) GatherOutEdges(taskID int32, worker string, end bool, endNum int32, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			sb.SetStatusError(taskID, fmt.Sprintf("GatherOutEdges panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("GatherOutEdges panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	computeTask := ComputeTaskMgr.GetTask(taskID)
	for i := 0; i < 100 && computeTask == nil; i++ {
		//wait 100ms if computeTask not init.
		logrus.Warnf("GatherOutEdges task id:%v is not available, wait 100ms", taskID)
		time.Sleep(100 * time.Millisecond)
		computeTask = ComputeTaskMgr.GetTask(taskID)
	}
	if !sb.CheckAction(computeTask) {
		return
	}

	computeTask.StepWg.Wait()
	computeTask.RecvWg.Add(1)

	logrus.Debugf("recv edge end: %v,endNum:%v, worker: %s", end, endNum, worker)
	graph := GraphMgr.GetGraphByName(computeTask.Task.SpaceName, computeTask.Task.GraphName)
	e := structure.IntEdge{}

	for i := 0; i < len(data); {
		n, err := e.Unmarshal(data[i:])
		if err != nil {
			sb.SetStatusError(taskID, fmt.Sprintf("load graph read edge error: %s", err))
			logrus.Errorf("load graph read edge error: %s", err)
			break
		}
		i += n

		outIdx := e.Source - graph.Data.VertIDStart
		graph.Data.Edges.AppendOutEdge(outIdx, serialize.SUint32(e.Target))
	}

	//graph.Locker.Unlock()
	computeTask.RecvWg.Done()
	atomic.AddInt32(computeTask.RecvCount[worker], 1)

	if end {
		// wait for all messages are processed
		computeTask.RecvWg.Wait()
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(computeTask.RecvCount[worker]) >= endNum {
				break
			}
			logrus.Warnf("There are still buffer left to be processed. From worker:%v", worker)
			logrus.Debugf("recv count:%v ,end num:%v ", *computeTask.RecvCount[worker], endNum)
			time.Sleep(100 * time.Millisecond)
		}

		tarStatus := structure.TaskStateSettingOutEdgesOK

		var allWorkerComplete bool
		computeTask.Locker.Lock()
		computeTask.Task.SetWorkerState(worker, tarStatus)
		allWorkerComplete = computeTask.Task.CheckTaskState(tarStatus)
		computeTask.Locker.Unlock()
		if allWorkerComplete {
			graph.Data.Edges.OptimizeOutEdgesMemory()
			computeTask.Task.SetState(tarStatus)
			req := pb.SettingGraphReq{
				WorkerName: ServiceWorker.WorkerName,
				TaskId:     taskID,
				State:      string(tarStatus),
			}
			_, err := ServiceWorker.MasterClient.SettingGraph(context.Background(), &req)
			if err != nil {
				logrus.Errorf("RecvEdge send load task status error: %s", err)
			}
			for s := range computeTask.RecvCount {
				*computeTask.RecvCount[s] = 0
			}
		}
	}
}

func (sb *SettingBl) ScatterOutDegree(taskID int32, spaceName string, graphName string) {
	defer func() {
		if r := recover(); r != nil {
			sb.SetStatusError(taskID, fmt.Sprintf("ScatterOutDegree panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("ScatterOutDegree panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	logrus.Infof("start scatter out degree, task_id:%v, space:%v, graph:%v", taskID, spaceName, graphName)

	graph := GraphMgr.GetGraphByName(spaceName, graphName)

	workerCount := len(graph.Workers)
	peers := make([]*PeerClient, 0, workerCount)
	for _, worker := range graph.Workers {
		peers = append(peers, PeerMgr.GetPeer(worker.Name))
	}

	outDegree := make([]serialize.SUint32, graph.Data.Vertex.TotalVertexCount())
	for i := uint32(0); i < graph.Data.VertexCount; i++ {
		inEdges := graph.Data.Edges.GetInEdges(i)
		for _, source := range inEdges {
			outDegree[source]++
		}
	}

	computeTask := ComputeTaskMgr.GetTask(taskID)
	parallel := *computeTask.Parallel
	partCnt := int32(graph.Data.Vertex.TotalVertexCount())/parallel + 1
	wg := sync.WaitGroup{}
	for i := int32(0); i < parallel; i++ {
		wg.Add(1)
		go func(pID int32) {
			defer func() {
				if r := recover(); r != nil {
					sb.SetStatusError(taskID, fmt.Sprintf("ScatterOutEdges panic in goroutine recover panic:%v, stack message: %s",
						r, common.GetCurrentGoroutineStack()))
					logrus.Errorf("ScatterOutEdges panic recover in goroutine taskID:%v, pId:%v panic:%v, stack message: %s",
						taskID, pID, r, common.GetCurrentGoroutineStack())
				}
			}()
			defer wg.Done()
			sendBuffer := buffer.EncodeBuffer{}
			sendBuffer.Init(BufferSize)
			bIdx := uint32(partCnt * pID)
			eIdx := bIdx + uint32(partCnt)
			if eIdx > graph.Data.Vertex.TotalVertexCount() {
				eIdx = graph.Data.Vertex.TotalVertexCount()
			}
			vOffset := serialize.SUint32(bIdx)
			if len(peers) > 0 {
				_ = sendBuffer.Marshal(&vOffset)
				for i := bIdx; i < eIdx; i++ {
					outDegree := outDegree[i]
					//logrus.Debugf("vertex:%v outDegree:%v ", graph.Data.Vertex.GetVertex(i).Id, outDegree)
					_ = sendBuffer.Marshal(&outDegree)
					if sendBuffer.Full() {
						for _, peer := range peers {
							atomic.AddInt32(computeTask.SendCount[peer.Name], 1)
							if peer.Self {
								sb.GatherOutDegree(
									taskID,
									peer.Name,
									false,
									0,
									sendBuffer.PayLoad())
							} else {
								peer.SettingActionHandler.SettingAction(
									computeTask.Task.ID,
									pb.SettingAction_SetOutDegree,
									false,
									0,
									sendBuffer.PayLoad())
							}
						}
						sendBuffer.Reset()
						vOffset = serialize.SUint32(i + 1)
						_ = sendBuffer.Marshal(&vOffset)
					}
				}
				for _, peer := range peers {
					atomic.AddInt32(computeTask.SendCount[peer.Name], 1)
					if peer.Self {
						sb.GatherOutDegree(
							taskID,
							peer.Name,
							false,
							0,
							sendBuffer.PayLoad())
					} else {
						peer.SettingActionHandler.SettingAction(
							computeTask.Task.ID,
							pb.SettingAction_SetOutDegree,
							false,
							0,
							sendBuffer.PayLoad())
					}
				}
				sendBuffer.Reset()
			}
		}(i)
	}
	wg.Wait()
	for _, peer := range peers {
		atomic.AddInt32(computeTask.SendCount[peer.Name], 1)
		if peer.Self {
			sb.GatherOutDegree(
				taskID,
				peer.Name,
				true,
				atomic.LoadInt32(computeTask.SendCount[peer.Name]),
				[]byte{})
		} else {
			peer.SettingActionHandler.SettingAction(
				computeTask.Task.ID,
				pb.SettingAction_SetOutDegree,
				true,
				atomic.LoadInt32(computeTask.SendCount[peer.Name]),
				[]byte{})
		}
	}
}

func (sb *SettingBl) GatherOutDegree(taskID int32, workerName string, end bool, endNum int32, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			sb.SetStatusError(taskID, fmt.Sprintf("GatherOutEdges panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("GatherOutEdges panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	logrus.Debugf("gather out degree worker:%v, end；%v, endnum:%v", workerName, end, endNum)
	computeTask := ComputeTaskMgr.GetTask(taskID)
	for i := 0; i < 100 && computeTask == nil; i++ {
		//wait 100ms if computeTask not init.
		logrus.Warnf("GatherOutEdges task id:%v is not available, wait 100ms", taskID)
		time.Sleep(100 * time.Millisecond)
		computeTask = ComputeTaskMgr.GetTask(taskID)
	}

	if !sb.CheckAction(computeTask) {
		return
	}
	//defer sb.CheckAction(computeTask)

	computeTask.StepWg.Wait()
	computeTask.RecvWg.Add(1)

	i := 0
	vOffSet := serialize.SUint32(0)
	if len(data) >= 4 {
		n, _ := vOffSet.Unmarshal(data)
		i += n
		var outDegree serialize.SUint32
		graph := GraphMgr.GetGraphByName(computeTask.Task.SpaceName, computeTask.Task.GraphName)
		for i < len(data) {
			n, err := outDegree.Unmarshal(data[i:])
			//n, err := graph.Data.OutDegree[int(vOffSet)].Unmarshal(data[i:])
			if err != nil {
				sb.SetStatusError(taskID, fmt.Sprintf("setting graph gather outdegree error: %sb", err))
				logrus.Errorf("setting graph gather outdegree error: %sb", err)
				break
			}
			graph.Data.Edges.AddOutDegree(uint32(vOffSet), uint32(outDegree))
			i += n
			vOffSet += 1
		}
	}

	computeTask.RecvWg.Done()
	atomic.AddInt32(computeTask.RecvCount[workerName], 1)

	if end {
		// wait for all messages are processed
		computeTask.RecvWg.Wait()
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(computeTask.RecvCount[workerName]) >= endNum {
				break
			}
			logrus.Warnf("There are still buffer left to be processed. From worker:%v", workerName)
			logrus.Debugf("recv count:%v ,end num:%v ", *computeTask.RecvCount[workerName], endNum)
			time.Sleep(100 * time.Millisecond)
		}

		var allWorkerComplete bool
		computeTask.Locker.Lock()
		state := structure.TaskStateSettingOutDegreeOK
		computeTask.Task.SetWorkerState(workerName, state)
		allWorkerComplete = computeTask.Task.CheckTaskState(state)
		computeTask.Locker.Unlock()
		if allWorkerComplete {
			//loadTask.Task.SetWorkerState(workerName, structure.TaskStateLoaded)
			//if loadTask.Task.CheckTaskState(structure.TaskStateLoaded) {
			computeTask.Task.SetState(state)
			req := pb.SettingGraphReq{
				WorkerName: ServiceWorker.WorkerName,
				TaskId:     taskID,
				State:      string(state),
			}
			ctx := context.Background()
			_, err := ServiceWorker.MasterClient.SettingGraph(ctx, &req)
			if err != nil {
				logrus.Errorf("LoadTaskStatus error: %sb", err)
			}
		}
	}
}

func (sb *SettingBl) SetStatusError(taskId int32, msg string) {
	computeTask := ComputeTaskMgr.GetTask(taskId)
	if computeTask == nil {
		return
	}
	computeTask.Task.State = structure.TaskStateError
	logrus.Errorf("compute task error: %d", taskId)
	req := pb.SettingGraphReq{
		WorkerName: ServiceWorker.WorkerName,
		TaskId:     taskId,
		State:      string(structure.TaskStateError),
		ErrorMsg:   msg,
	}
	_, err := ServiceWorker.MasterClient.SettingGraph(context.Background(), &req)
	if err != nil {
		logrus.Errorf("LoadTaskStatus error: %s", err)
	}
	time.AfterFunc(1*time.Minute, func() { computeTask.FreeMemory() })
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
}

func (sb *SettingBl) CheckAction(computeTask *compute.ComputerTask) (isContinue bool) {
	if computeTask == nil {
		return true
	}
	switch atomic.LoadInt32(&computeTask.Task.Action) {
	case structure.ActionDoNoting:

	case structure.ActionCancelTask:
		sb.cancelAction(computeTask)
		return false
	case structure.ActionPauseTask:
		return sb.pauseAction(computeTask)
	default:
		logrus.Errorf("unknown action %d", computeTask.Task.Action)
	}

	return true
}

func (sb *SettingBl) endTask(taskID int32) {
	computeTask := ComputeTaskMgr.GetTask(taskID)
	if computeTask != nil {
		computeTask.FreeMemory()
		ComputeTaskMgr.DeleteTask(computeTask.Task.ID)
	}
}

func (sb *SettingBl) cancelAction(computeTask *compute.ComputerTask) {
	computeTask.Task.SetState(structure.TaskStateCanceled)
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
	time.AfterFunc(1*time.Minute, func() { sb.endTask(computeTask.Task.ID) })
}

func (sb *SettingBl) pauseAction(computeTask *compute.ComputerTask) bool {
	task := computeTask.Task
	for {
		switch atomic.LoadInt32(&task.Action) {
		case structure.ActionCancelTask:
			sb.cancelAction(computeTask)
			return false
		case structure.ActionResumeTask:
			return true
		default:
			time.Sleep(10 * time.Second)
		}
	}
}
