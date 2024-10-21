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
*/package worker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"vermeer/apps/buffer"
	"vermeer/apps/common"
	"vermeer/apps/graphio"
	"vermeer/apps/options"
	pb "vermeer/apps/protos"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

const BufferSize = 2 * 1024 * 1024

type LoadGraphBl struct {
}

func (lb *LoadGraphBl) StartLoadGraph(spaceName string, taskId int32, graphName string, workers []string, params map[string]string) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("StartLoadGraph panic recover panic:%v,  stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("StartLoadGraph panic recover taskID:%v, panic:%v,  stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()

	graph := GraphMgr.CreateGraph(spaceName, graphName)
	graph.SetState(structure.GraphStateLoading)
	err := GraphMgr.AddGraph(graph)
	if err != nil {
		logrus.Errorf("add graph error: %s", err)
	}
	graph.Workers = make([]*structure.GraphWorker, 0, len(workers))
	task, err := TaskMgr.CreateTask(spaceName, structure.TaskTypeLoad, taskId)
	if err != nil {
		logrus.Errorf("create task error: %s", err)
	}
	for _, wn := range workers {
		gw := structure.GraphWorker{
			Name:        wn,
			VertexCount: 0,
		}
		if wn == ServiceWorker.WorkerName {
			gw.IsSelf = true
		}
		graph.Workers = append(graph.Workers, &gw)

		tw := structure.TaskWorker{
			Name: wn,
		}
		task.Workers = append(task.Workers, &tw)
	}

	task.GraphName = graphName
	task.Params = params
	TaskMgr.AddTask(task)
	logrus.Infof("create load task: %d, graph: %s", taskId, graphName)
	loadTask := LoadGraphMgr.InstallTask(task)
	// recv vertex wait until init done
	loadTask.LoadWg.Add(1)
	ctx := context.Background()
	parallel := options.GetInt(params, "load.parallel")
	if parallel <= 0 {
		logrus.Infof("load.parallel value must be larger than 0, get: %v, set to defalut value :1", parallel)
		parallel = 1
	}
	*loadTask.Parallel = int32(parallel)
	loadTask.LoadType = options.GetString(params, "load.type")

	_, err = graph.SetDataDir(spaceName, graphName, ServiceWorker.WorkerName)
	if err != nil {
		logrus.Errorf("set data dir error: %s", err)
		lb.SetStatusError(taskId, fmt.Sprintf("set data dir error: %s", err))
	}
	// remove history data
	graph.Remove()

	backendOption := structure.GraphDataBackendOption{
		VertexDataBackend: options.GetString(params, "load.vertex_backend"),
	}
	graph.MallocData(backendOption)

	//Determines if outEdges are required and sets graph.UseOutEdges.
	var useOutEdges, useOutDegree, useProperty bool
	useOutEdges = options.GetInt(loadTask.Task.Params, "load.use_outedge") == 1
	useOutDegree = options.GetInt(loadTask.Task.Params, "load.use_out_degree") == 1
	//graph.UseUndirected = options.GetInt(params, "load.use_undirected") == 1
	////有无向图功能时，无需out edges
	if options.GetInt(params, "load.use_undirected") == 1 {
		graph.UseOutEdges = true
	}
	useProperty = options.GetInt(loadTask.Task.Params, "load.use_property") == 1
	if loadTask.LoadType == graphio.LoadTypeHugegraph {
		useProperty = true
		graph.Data.VertexPropertySchema, graph.Data.InEdgesPropertySchema, err = structure.GetSchemaFromHugegraph(params)
		//logrus.Infof(" hugegraph vertex schema %v", graph.Data.VertexPropertySchema)
		//logrus.Infof(" hugegraph edge schema %v", graph.Data.InEdgesPropertySchema)
		if err != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("load schema from hugegraph error:%v", err))
			logrus.Errorf("load schema from hugegraph error:%v", err)
			return
		}

	} else if useProperty {
		useProperty = true
		graph.Data.VertexPropertySchema.Init(options.GetMapString(params, "load.vertex_property"))
		graph.Data.InEdgesPropertySchema.Init(options.GetMapString(params, "load.edge_property"))
	}
	graph.SetOption(useOutEdges, useOutDegree, useProperty)
	if graph.UseProperty {
		graph.Data.VertexProperty.Init(graph.Data.VertexPropertySchema)
	}

	loadTask.LoadWg.Done()
	for i := 0; i < parallel; i++ {
		valueCtx := context.WithValue(ctx, "worker_id", i)
		go lb.RunLoadVertex(valueCtx, loadTask)
	}
}

func (lb *LoadGraphBl) OnVertexLoaded(taskId int32) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("OnVertexLoaded panic recover panic:%v,  stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("OnVertexLoaded panic recover taskID:%v, panic:%v,  stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskId)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	ctx := context.Background()
	graph := GraphMgr.GetGraphByName(LoadGraphMgr.GetLoadTask(taskId).Task.SpaceName,
		LoadGraphMgr.GetLoadTask(taskId).Task.GraphName)
	graph.VertexCount = int64(graph.Data.VertexCount)
	graph.SetWorkerVertexCount(ServiceWorker.WorkerName, graph.Data.VertexCount, 0)
	graph.BuildEdge(options.GetInt(loadTask.Task.Params, "load.edges_per_vertex"))
	req := pb.WorkerVertexCountReq{
		TaskId:     taskId,
		WorkerName: ServiceWorker.WorkerName,
		Count:      atomic.LoadUint32(&graph.Data.VertexCount),
	}
	common.PrometheusMetrics.VertexCnt.WithLabelValues(graph.Name).Set(float64(graph.Data.VertexCount))
	_, err := ServiceWorker.MasterClient.WorkVertexCount(ctx, &req)
	if err != nil {
		lb.SetStatusError(taskId, fmt.Sprintf("WorkVertexCount error: %s", err))
		logrus.Errorf("WorkVertexCount error: %s", err)
		return
	}
}

func (lb *LoadGraphBl) ScatterVertex(taskID int32) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskID, fmt.Sprintf("ScatterVertex panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("ScatterVertex panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskID)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)

	ctx := context.Background()
	req := pb.GetGraphWorkersReq{
		GraphName: loadTask.Task.GraphName,
		SpaceName: loadTask.Task.SpaceName,
	}
	resp, err := ServiceWorker.MasterClient.GetGraphWorkers(ctx, &req)
	if err != nil {
		lb.SetStatusError(taskID, fmt.Sprintf("GetGraphWorkers error: %s", err))
		logrus.Errorf("GetGraphWorkers error: %s", err)
		return
	}

	for _, w := range resp.Workers {
		graph.SetWorkerVertexCount(w.Name, w.VertexCount, w.VertIdStart)
	}

	workerCount := len(loadTask.Task.Workers)

	graph.RecastVertex()

	loadTask.LoadWg.Done()
	peers := make([]*PeerClient, 0, workerCount-1)
	for _, wn := range loadTask.Task.Workers {
		if wn.Name == ServiceWorker.WorkerName {
			lb.GatherVertex(
				loadTask.Task.ID,
				wn.Name,
				true,
				1,
				[]byte{})
			continue
		}
		peers = append(peers, PeerMgr.GetPeer(wn.Name))
	}

	// skip if only on worker
	if len(peers) == 0 {
		return
	}

	// sendBuffer := buffer.EncodeBuffer{}
	// sendBuffer.Init(BufferSize)

	localGw := graph.GetGraphWorker(ServiceWorker.WorkerName)
	parallel := options.GetInt(loadTask.Task.Params, "load.parallel")
	if parallel <= 0 {
		logrus.Infof("load.parallel value must be larger than 0, get: %v, set to defalut value :1", parallel)
		parallel = 1
	} else if parallel > 10 {
		parallel = 10
	}

	sendBuffers := make([]buffer.EncodeBuffer, parallel)
	for i := range sendBuffers {
		sendBuffers[i] = buffer.EncodeBuffer{}
		sendBuffers[i].Init(BufferSize)
	}

	partCnt := int(localGw.VertexCount)/parallel + 1
	wg := &sync.WaitGroup{}
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(pID int) {
			defer func() {
				if r := recover(); r != nil {
					lb.SetStatusError(taskID, fmt.Sprintf("ScatterVertex panic recover panic:%v, stack message: %s",
						r, common.GetCurrentGoroutineStack()))
					logrus.Errorf("ScatterVertex panic recover taskID:%v, pId:%v panic:%v, stack message: %s",
						taskID, pID, r, common.GetCurrentGoroutineStack())
				}
			}()
			defer wg.Done()
			bIdx := uint32(partCnt*pID) + localGw.VertIdStart
			eIdx := bIdx + uint32(partCnt)
			if eIdx > localGw.VertIdStart+localGw.VertexCount {
				eIdx = localGw.VertIdStart + localGw.VertexCount
			}
			vOffSet := serialize.SUint32(bIdx)
			_ = sendBuffers[pID].Marshal(&vOffSet)
			for j := bIdx; j < eIdx; j++ {
				vertex := graph.Data.Vertex.GetVertex(j)
				_ = sendBuffers[pID].Marshal(&vertex)
				if graph.UseProperty {
					for _, k := range graph.Data.VertexPropertySchema.Schema {
						value := graph.Data.VertexProperty.GetValue(k.PropKey, j)
						_ = sendBuffers[pID].Marshal(value)
					}
				}
				if sendBuffers[pID].Full() {
					count := int32(sendBuffers[pID].ObjCount())
					if graph.UseProperty {
						count = int32(sendBuffers[pID].ObjCount() / (len(graph.Data.VertexPropertySchema.Schema) + 1))
					}
					for _, peer := range peers {
						atomic.AddInt32(loadTask.SendCount[peer.Name], 1)
						peer.LoadActionHandler.LoadAction(
							loadTask.Task.ID,
							pb.LoadAction_LoadScatter,
							count,
							false,
							0,
							sendBuffers[pID].PayLoad())
					}
					sendBuffers[pID].Reset()
					vOffSet = serialize.SUint32(j + 1)
					_ = sendBuffers[pID].Marshal(&vOffSet)
				}
			}
			count := int32(sendBuffers[pID].ObjCount())
			if graph.UseProperty {
				count = int32(sendBuffers[pID].ObjCount() / (len(graph.Data.VertexPropertySchema.Schema) + 1))
			}
			for _, peer := range peers {
				atomic.AddInt32(loadTask.SendCount[peer.Name], 1)
				peer.LoadActionHandler.LoadAction(
					loadTask.Task.ID,
					pb.LoadAction_LoadScatter,
					count,
					false,
					0,
					sendBuffers[pID].PayLoad())
			}
			sendBuffers[pID].Reset()
		}(i)
	}
	wg.Wait()
	// vOffSet := serialize.SUint32(localGw.VertIdStart)
	// _ = sendBuffer.Marshal(&vOffSet)
	// for i := localGw.VertIdStart; i < localGw.VertIdStart+localGw.VertexCount; i++ {
	// 	vertex := graph.Data.Vertex.GetVertex(i)
	// 	_ = sendBuffer.Marshal(&vertex)
	// 	if graph.UseProperty {
	// 		for _, k := range graph.Data.VertexPropertySchema.Schema {
	// 			value := graph.Data.VertexProperty.GetValue(k.PropKey, i)
	// 			_ = sendBuffer.Marshal(value)
	// 		}
	// 	}
	// 	if sendBuffer.Full() {
	// 		count := int32(sendBuffer.ObjCount())
	// 		if graph.UseProperty {
	// 			count = int32(sendBuffer.ObjCount() / (len(graph.Data.VertexPropertySchema.Schema) + 1))
	// 		}
	// 		for _, peer := range peers {
	// 			peer.LoadActionHandler.LoadAction(
	// 				loadTask.Task.ID,
	// 				pb.LoadAction_LoadScatter,
	// 				count,
	// 				false,
	// 				0,
	// 				sendBuffer.PayLoad())
	// 		}
	// 		sendBuffer.Reset()
	// 		vOffSet = serialize.SUint32(i + 1)
	// 		_ = sendBuffer.Marshal(&vOffSet)
	// 	}
	// }
	// count := int32(sendBuffer.ObjCount())
	// if graph.UseProperty {
	// 	count = int32(sendBuffer.ObjCount() / (len(graph.Data.VertexPropertySchema.Schema) + 1))
	// }
	for _, peer := range peers {
		atomic.AddInt32(loadTask.SendCount[peer.Name], 1)
		peer.LoadActionHandler.LoadAction(
			loadTask.Task.ID,
			pb.LoadAction_LoadScatter,
			0,
			true,
			atomic.LoadInt32(loadTask.SendCount[peer.Name]),
			[]byte{})
	}
	// sendBuffer.Reset()
	for s := range loadTask.SendCount {
		*loadTask.SendCount[s] = 0
	}
}

func (lb *LoadGraphBl) GatherVertex(taskID int32, workerName string, end bool, endNum int32, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskID, fmt.Sprintf("GatherVertex panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("GatherVertex panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	logrus.Debugf("gather vertex worker:%v, end:%v", workerName, end)
	loadTask := LoadGraphMgr.GetLoadTask(taskID)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	loadTask.LoadWg.Wait()
	loadTask.RecvWg.Add(1)
	//gw := graph.GetGraphWorker(workerName)

	i := 0
	vOffSet := serialize.SUint32(0)
	if len(data) >= 4 {
		n, _ := vOffSet.Unmarshal(data)
		i += n
	}

	vOffSetStart := vOffSet
	vertexList := make([]structure.Vertex, 0, 1000)
	for i < len(data) {
		var vertex structure.Vertex
		n, err := vertex.Unmarshal(data[i:])
		if err != nil {
			lb.SetStatusError(taskID, fmt.Sprintf("load graph read vertex error: %s", err))
			logrus.Errorf("load graph read vertex error: %s", err)
			break
		}
		vertexList = append(vertexList, vertex)
		// graph.Data.Vertex.SetVertex(uint32(vOffSet), vertex)
		//n, err := graph.Data.TotalVertex[int(vOffSet)].Unmarshal(data[i:])
		//if err != nil {
		//	lb.SetStatusError(taskId, fmt.Sprintf("load graph read vertex error: %s", err))
		//	logrus.Errorf("load graph read vertex error: %s", err)
		//	break
		//}
		i += n
		if graph.UseProperty {
			var value serialize.MarshalAble
			for _, k := range graph.Data.VertexPropertySchema.Schema {
				switch k.VType {
				case structure.ValueTypeInt32:
					var sInt32 serialize.SInt32
					n, err = sInt32.Unmarshal(data[i:])
					value = &sInt32
				case structure.ValueTypeFloat32:
					var sFloat32 serialize.SFloat32
					n, err = sFloat32.Unmarshal(data[i:])
					value = &sFloat32
				case structure.ValueTypeString:
					var sString serialize.SString
					n, err = sString.Unmarshal(data[i:])
					value = &sString
				}
				if err != nil {
					lb.SetStatusError(taskID, fmt.Sprintf("GatherVertex vertex property error: %s", err))
					logrus.Errorf("GatherVertex vertex property error: %s", err)
					break
				}
				graph.Data.VertexProperty.SetValue(k.PropKey, uint32(vOffSet), value)
				i += n
			}
		}

		vOffSet += 1
	}
	graph.Data.Vertex.SetVertices(uint32(vOffSetStart), vertexList...)
	//logrus.Infof("GatherVertex offset: %d, worker: %s, end: %v", vOffSet, workerName, end)
	loadTask.RecvWg.Done()
	atomic.AddInt32(loadTask.RecvCount[workerName], 1)

	if end {
		// wait for all messages are processed
		loadTask.RecvWg.Wait()
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(loadTask.RecvCount[workerName]) >= endNum {
				break
			}
			logrus.Warnf("There are still buffer left to be processed. From worker:%v", workerName)
			logrus.Debugf("recv count:%v ,end num:%v ", *loadTask.RecvCount[workerName], endNum)
			time.Sleep(100 * time.Millisecond)
		}
		var allWorkerComplete bool
		loadTask.Locker.Lock()
		loadTask.Task.SetWorkerState(workerName, structure.TaskStateLoadScatterOK)
		allWorkerComplete = loadTask.Task.CheckTaskState(structure.TaskStateLoadScatterOK)
		loadTask.Locker.Unlock()
		if allWorkerComplete {
			logrus.Infof("Gather vertex complete, task:%v ", taskID)
			//loadTask.Task.SetWorkerState(workerName, structure.TaskStateLoadScatterOK)
			//if loadTask.Task.CheckTaskState(structure.TaskStateLoadScatterOK) {
			loadTask.Task.SetState(structure.TaskStateLoadScatterOK)
			loadTask.LoadWg.Add(1)
			graph.BuildTotalVertex()
			req := pb.LoadTaskStatusReq{
				WorkerName: ServiceWorker.WorkerName,
				TaskId:     taskID,
				State:      string(structure.TaskStateLoadScatterOK),
			}
			ctx := context.Background()
			_, err := ServiceWorker.MasterClient.LoadTaskStatus(ctx, &req)
			if err != nil {
				logrus.Errorf("LoadTaskStatus error: %s", err)
			}
			for s := range loadTask.RecvCount {
				*loadTask.RecvCount[s] = 0
			}
		}
	}
}

func (lb *LoadGraphBl) LoadEdge(taskId int32) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("LoadEdge panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("LoadEdge panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskId)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	ctx := context.Background()
	parallel := options.GetInt(loadTask.Task.Params, "load.parallel")
	if parallel <= 0 {
		logrus.Infof("load.parallel value must be larger than 0, get: %v, set to defalut value :1", parallel)
		parallel = 1
	}
	*loadTask.Parallel = int32(parallel)
	loadTask.LoadType = options.GetString(loadTask.Task.Params, "load.type")
	loadTask.LoadWg.Done()
	logrus.Infof("load edge parallel: %d, type: %s", parallel, loadTask.LoadType)
	for i := 0; i < parallel; i++ {
		valueCtx := context.WithValue(ctx, "worker_id", i)
		go lb.RunLoadEdge(valueCtx, loadTask)
	}
}

func (lb *LoadGraphBl) RunLoadVertex(ctx context.Context, loadTask *graphio.LoadGraphTask) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(loadTask.Task.ID, fmt.Sprintf("RunLoadVertex panic recover panic:%v, stack message: %s",
				r, common.GetCurrentGoroutineStack()))
			logrus.Errorf("RunLoadVertex panic recover taskID:%v, panic:%v, stack message: %s",
				loadTask.Task.ID, r, common.GetCurrentGoroutineStack())
		}
	}()
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	workerCount := len(loadTask.Task.Workers)

	sendBuffers := make([]buffer.EncodeBuffer, 0, workerCount)
	peers := make([]*PeerClient, 0, workerCount)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	for _, v := range loadTask.Task.Workers {
		peers = append(peers, PeerMgr.GetPeer(v.Name))
		buf := buffer.EncodeBuffer{}
		buf.Init(BufferSize)
		sendBuffers = append(sendBuffers, buf)
	}

	vertex := structure.Vertex{}
	property := structure.PropertyValue{}
	property.Init(graph.Data.VertexPropertySchema)
	for {
		if !lb.CheckAction(loadTask) {
			return
		}
		reqCtx := context.Background()
		req := pb.FetchLoadPartReq{}
		req.TaskId = loadTask.Task.ID
		req.WorkerName = ServiceWorker.WorkerName
		resp, err := ServiceWorker.MasterClient.FetchLoadPart(reqCtx, &req)
		if err != nil {
			logrus.Errorf("RunLoadVertex fetch partition error: %s", err)
			break
		}
		if resp.PartId == 0 {
			logrus.Infof("RunLoadVertex fetch part eof, worker: %d", ctx.Value("worker_id"))
			break
		}

		loader := graphio.MakeLoader(loadTask.LoadType)
		err = loader.Init(resp.Params, graph.Data.VertexPropertySchema)
		if err != nil {
			lb.SetStatusError(loadTask.Task.ID, fmt.Sprintf("graph loader init error: %s", err))
			logrus.Errorf("graph loader init error: %s", err)
			loader.Close()
			return
		}
		logrus.Infof("start read part: %s", loader.Name())
		for {
			err = loader.ReadVertex(&vertex, &property)
			if err != nil {
				if err == io.EOF {
					logrus.Infof("read part eof: %s, count: %d", loader.Name(), loader.ReadCount())
					break
				}
				lb.SetStatusError(loadTask.Task.ID, fmt.Sprintf("read vertex error: %s", err))
				logrus.Errorf("read vertex error: %s", err)
				loader.Close()
				return
			}
			workerIdx := common.HashBKDR(vertex.ID) % workerCount
			_ = sendBuffers[workerIdx].Marshal(&vertex)
			if graph.UseProperty {
				_ = sendBuffers[workerIdx].Marshal(&property)
			}
			if sendBuffers[workerIdx].Full() {
				atomic.AddInt32(loadTask.SendCount[peers[workerIdx].Name], 1)
				count := int32(sendBuffers[workerIdx].ObjCount())
				if graph.UseProperty {
					count = int32(sendBuffers[workerIdx].ObjCount() / 2)
				}
				if peers[workerIdx].Self {
					lb.RecvVertex(
						loadTask.Task.ID,
						peers[workerIdx].Name,
						count,
						false,
						0,
						sendBuffers[workerIdx].PayLoad())
				} else {
					peers[workerIdx].LoadActionHandler.LoadAction(
						loadTask.Task.ID,
						pb.LoadAction_LoadVertex,
						count,
						false,
						0,
						sendBuffers[workerIdx].PayLoad())
				}

				sendBuffers[workerIdx].Reset()
			}
		}
		loader.Close()
	}

	loadTask.Locker.Lock()
	atomic.AddInt32(loadTask.Parallel, -1)
	end := atomic.LoadInt32(loadTask.Parallel) == 0
	for i := range sendBuffers {
		atomic.AddInt32(loadTask.SendCount[peers[i].Name], 1)
	}
	loadTask.Locker.Unlock()

	for i := range sendBuffers {
		count := int32(sendBuffers[i].ObjCount())
		if graph.UseProperty {
			count = int32(sendBuffers[i].ObjCount() / 2)
		}
		if peers[i].Self {
			lb.RecvVertex(
				loadTask.Task.ID,
				peers[i].Name,
				count,
				end,
				atomic.LoadInt32(loadTask.SendCount[peers[i].Name]),
				sendBuffers[i].PayLoad())
		} else {
			peers[i].LoadActionHandler.LoadAction(
				loadTask.Task.ID,
				pb.LoadAction_LoadVertex,
				count,
				end,
				atomic.LoadInt32(loadTask.SendCount[peers[i].Name]),
				sendBuffers[i].PayLoad())
		}
		sendBuffers[i].Reset()
	}
	if end {
		for s := range loadTask.SendCount {
			*loadTask.SendCount[s] = 0
		}
	}
}

func (lb *LoadGraphBl) RunLoadEdge(ctx context.Context, loadTask *graphio.LoadGraphTask) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(loadTask.Task.ID, fmt.Sprintf("RunLoadEdge panic recover panic:%v, stack message: %s",
				r, common.GetCurrentGoroutineStack()))
			logrus.Errorf("RunLoadEdge panic recover taskID:%v, panic:%v, stack message: %s",
				loadTask.Task.ID, r, common.GetCurrentGoroutineStack())
		}
	}()
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	workerCount := len(loadTask.Task.Workers)

	sendBuffers := make([]buffer.EncodeBuffer, 0, workerCount)
	peers := make([]*PeerClient, 0, workerCount)
	for _, v := range loadTask.Task.Workers {
		peers = append(peers, PeerMgr.GetPeer(v.Name))
		buf := buffer.EncodeBuffer{}
		buf.Init(BufferSize)
		sendBuffers = append(sendBuffers, buf)
	}
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	edge := structure.Edge{}
	intEdge := structure.IntEdge{}
	property := structure.PropertyValue{}
	property.Init(graph.Data.InEdgesPropertySchema)
	for {
		if !lb.CheckAction(loadTask) {
			return
		}
		reqCtx := context.Background()
		req := pb.FetchLoadPartReq{}
		req.TaskId = loadTask.Task.ID
		req.WorkerName = ServiceWorker.WorkerName
		resp, err := ServiceWorker.MasterClient.FetchLoadPart(reqCtx, &req)
		if err != nil {
			logrus.Errorf("RunLoadEdge fetch partition error: %s", err)
			break
		}
		if resp.PartId == 0 {
			logrus.Infof("RunLoadEdge fetch part eof, worker: %d", ctx.Value("worker_id"))
			break
		}

		loader := graphio.MakeLoader(loadTask.LoadType)
		err = loader.Init(resp.Params, graph.Data.InEdgesPropertySchema)
		if err != nil {
			lb.SetStatusError(loadTask.Task.ID, fmt.Sprintf("graph loader init error: %s", err))
			logrus.Errorf("graph loader init error: %s", err)
			loader.Close()
			return
		}
		logrus.Infof("start read part: %s", loader.Name())
		for {
			if !lb.CheckAction(loadTask) {
				return
			}
			err = loader.ReadEdge(&edge, &property)
			if err != nil {
				if err == io.EOF {
					logrus.Infof("read part eof: %s, count: %d", loader.Name(), loader.ReadCount())
					break
				}
				lb.SetStatusError(loadTask.Task.ID, fmt.Sprintf("read edge error: %s", err))
				logrus.Errorf("read name:%v  edge error: %s", loader.Name(), err)
				loader.Close()
				return
			}
			var ok bool
			intEdge.Source, ok = graph.Data.Vertex.GetVertexIndex(edge.Source)
			if !ok {
				continue
			}
			intEdge.Target, ok = graph.Data.Vertex.GetVertexIndex(edge.Target)
			if !ok {
				continue
			}
			workerIdx := common.HashBKDR(edge.Target) % workerCount
			_ = sendBuffers[workerIdx].Marshal(&intEdge)

			if graph.UseProperty {
				_ = sendBuffers[workerIdx].Marshal(&property)
			}
			if sendBuffers[workerIdx].Full() {
				atomic.AddInt32(loadTask.SendCount[peers[workerIdx].Name], 1)
				count := int32(sendBuffers[workerIdx].ObjCount())
				if graph.UseProperty {
					count = int32(sendBuffers[workerIdx].ObjCount() / 2)
				}
				if peers[workerIdx].Self {
					lb.RecvEdge(
						loadTask.Task.ID,
						peers[workerIdx].Name,
						count,
						false,
						0,
						sendBuffers[workerIdx].PayLoad())
				} else {
					peers[workerIdx].LoadActionHandler.LoadAction(
						loadTask.Task.ID,
						pb.LoadAction_LoadEdge,
						count,
						false,
						0,
						sendBuffers[workerIdx].PayLoad())
				}
				sendBuffers[workerIdx].Reset()
			}
			//If the workerId of source and target are the same, sent only once.
			//If they are not the same, send an edge for both workerId's.
			if graph.UseOutEdges || graph.UseOutDegree {
				workerIdxOut := common.HashBKDR(edge.Source) % workerCount
				if workerIdxOut == workerIdx {
					continue
				}
				_ = sendBuffers[workerIdxOut].Marshal(&intEdge)
				if sendBuffers[workerIdxOut].Full() {
					atomic.AddInt32(loadTask.SendCount[peers[workerIdxOut].Name], 1)
					if peers[workerIdxOut].Self {
						lb.RecvEdge(
							loadTask.Task.ID,
							peers[workerIdxOut].Name,
							int32(sendBuffers[workerIdxOut].ObjCount()),
							false,
							0,
							sendBuffers[workerIdxOut].PayLoad())
					} else {
						peers[workerIdxOut].LoadActionHandler.LoadAction(
							loadTask.Task.ID,
							pb.LoadAction_LoadEdge,
							int32(sendBuffers[workerIdxOut].ObjCount()),
							false,
							0,
							sendBuffers[workerIdxOut].PayLoad())
					}
					sendBuffers[workerIdxOut].Reset()
				}
			}
		}
		loader.Close()
	}

	loadTask.Locker.Lock()
	atomic.AddInt32(loadTask.Parallel, -1)
	end := atomic.LoadInt32(loadTask.Parallel) == 0
	for i := range sendBuffers {
		atomic.AddInt32(loadTask.SendCount[peers[i].Name], 1)
	}
	loadTask.Locker.Unlock()

	for i := range sendBuffers {
		count := int32(sendBuffers[i].ObjCount())
		if graph.UseProperty {
			count = int32(sendBuffers[i].ObjCount() / 2)
		}
		if peers[i].Self {
			lb.RecvEdge(
				loadTask.Task.ID,
				peers[i].Name,
				count,
				end,
				atomic.LoadInt32(loadTask.SendCount[peers[i].Name]),
				sendBuffers[i].PayLoad())
		} else {
			peers[i].LoadActionHandler.LoadAction(
				loadTask.Task.ID,
				pb.LoadAction_LoadEdge,
				count,
				end,
				atomic.LoadInt32(loadTask.SendCount[peers[i].Name]),
				sendBuffers[i].PayLoad())
		}
		sendBuffers[i].Reset()
	}
	if end {
		for s := range loadTask.SendCount {
			*loadTask.SendCount[s] = 0
		}
	}
}

func (lb *LoadGraphBl) LoadPartCompleted(taskId int32, partId int32) {
	_ = ServiceWorker.LoadGraphHandler.LoadComplete(taskId, partId)
}

func (lb *LoadGraphBl) RecvVertex(taskId int32, worker string, count int32, end bool, endNum int32, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("RecvVertex panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("RecvVertex panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskId)
	for i := 0; i < 100 && loadTask == nil; i++ {
		//wait 100ms if loadTask not init.
		logrus.Warnf("task id:%v is not available, wait 100ms", taskId)
		time.Sleep(100 * time.Millisecond)
		loadTask = LoadGraphMgr.GetLoadTask(taskId)
	}
	if !lb.CheckAction(loadTask) {
		return
	}
	loadTask.RecvWg.Add(1)
	loadTask.LoadWg.Wait()
	//defer lb.CheckAction(loadTask)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	atomic.AddUint32(&graph.Data.VertexCount, uint32(count))
	logrus.Debugf("recv vertex count: %d, end: %v,endNum:%v, worker: %s", count, end, endNum, worker)

	vertexList := make([]structure.Vertex, count)

	properties := structure.VertexProperties{}
	properties.Init(graph.Data.VertexPropertySchema)
	prop := structure.PropertyValue{}
	prop.Init(graph.Data.VertexPropertySchema)

	c := 0
	for i := 0; i < len(data); {
		n, err := vertexList[c].Unmarshal(data[i:])
		if err != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("load graph read vertex error: %s", err))
			logrus.Errorf("load graph read vertex error: %s", err)
			break
		}
		i += n
		if graph.UseProperty {
			n, err = prop.Unmarshal(data[i:])
			if err != nil {
				lb.SetStatusError(taskId, fmt.Sprintf("load graph read vertex prop error: %s", err))
				logrus.Errorf("load graph read vertex prop error: %s", err)
				break
			}
			properties.AppendProp(prop, graph.Data.VertexPropertySchema)
			i += n
		}
		c += 1
	}
	if c != int(count) {
		lb.SetStatusError(taskId, fmt.Sprintf("RecvVertex count incorrect: %d, %d", c, count))
		logrus.Errorf("RecvVertex count incorrect: %d, %d", c, count)
	}
	graph.Locker.Lock()
	graph.Data.Vertex.AppendVertices(vertexList...)
	if graph.UseProperty {
		graph.Data.VertexProperty.AppendProps(properties)
	}
	graph.Locker.Unlock()

	loadTask.RecvWg.Done()
	atomic.AddInt32(loadTask.RecvCount[worker], 1)

	if end {
		// wait for all messages are processed
		loadTask.RecvWg.Wait()
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(loadTask.RecvCount[worker]) >= endNum {
				break
			}
			logrus.Warnf("There are still buffer left to be processed. From worker:%v", worker)
			logrus.Debugf("recv count:%v ,end num:%v ", *loadTask.RecvCount[worker], endNum)
			time.Sleep(100 * time.Millisecond)
		}
		var allWorkerComplete bool
		loadTask.Locker.Lock()
		loadTask.Task.SetWorkerState(worker, structure.TaskStateLoadVertexOK)
		allWorkerComplete = loadTask.Task.CheckTaskState(structure.TaskStateLoadVertexOK)
		loadTask.Locker.Unlock()
		if allWorkerComplete {
			loadTask.Task.SetState(structure.TaskStateLoadVertexOK)
			loadTask.LoadWg.Add(1)
			lb.OnVertexLoaded(taskId)
			req := pb.LoadTaskStatusReq{
				WorkerName: ServiceWorker.WorkerName,
				TaskId:     taskId,
				State:      string(structure.TaskStateLoadVertexOK),
			}
			ctx := context.Background()
			_, err := ServiceWorker.MasterClient.LoadTaskStatus(ctx, &req)
			if err != nil {
				logrus.Errorf("RecvVertex send load task status error: %s", err)
			}
			for s := range loadTask.RecvCount {
				*loadTask.RecvCount[s] = 0
			}
		}
	}
}

func (lb *LoadGraphBl) RecvEdge(taskId int32, worker string, count int32, end bool, endNum int32, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("RecvEdge panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("RecvEdge panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskId)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	loadTask.LoadWg.Wait()
	loadTask.RecvWg.Add(1)
	logrus.Debugf("recv edge count: %d, end: %v,endNum:%v, worker: %s", count, end, endNum, worker)

	e := structure.IntEdge{}
	prop := structure.PropertyValue{}
	prop.Init(graph.Data.InEdgesPropertySchema)

	//graph.Locker.Lock()
	if graph.UseOutEdges || graph.UseOutDegree {
		//load both inEdges and outEdges
		edgeCount := 0
		rangeStart := graph.Data.VertIDStart
		rangeEnd := graph.Data.VertIDStart + graph.Data.VertexCount
		for i := 0; i < len(data); {
			n, err := e.Unmarshal(data[i:])
			if err != nil {
				lb.SetStatusError(taskId, fmt.Sprintf("load graph read edge error: %s", err))
				logrus.Errorf("load graph read edge error: %s", err)
				break
			}
			i += n
			if rangeStart <= e.Target && e.Target < rangeEnd {
				edgeCount += 1
				inIdx := e.Target - graph.Data.VertIDStart
				graph.Data.Edges.AppendInEdge(inIdx, serialize.SUint32(e.Source))
				//graph.Data.EdgeLocker[inIdx].Lock()
				//graph.Data.InEdges[inIdx] = append(graph.Data.InEdges[inIdx], serialize.SUint32(e.Source))
				//graph.Data.EdgeLocker[inIdx].UnLock()

				if graph.UseProperty {
					n, err = prop.Unmarshal(data[i:])
					if err != nil {
						lb.SetStatusError(taskId, fmt.Sprintf("load graph read edge Property error: %s", err))
						logrus.Errorf("load graph read edge Property error: %s", err)
						break
					}
					graph.Data.Edges.EdgeLockFunc(inIdx, func() {
						graph.Data.InEdgesProperty.AppendProp(prop, inIdx, graph.Data.InEdgesPropertySchema)
					})
					//graph.Data.EdgeLocker[inIdx].Lock()
					//graph.Data.InEdgesProperty.AppendProp(prop, inIdx, graph.Data.InEdgesPropertySchema)
					//graph.Data.EdgeLocker[inIdx].UnLock()
					i += n
				}
			}
			if rangeStart <= e.Source && e.Source < rangeEnd {
				if graph.UseOutDegree {
					graph.Data.Edges.AddOutDegree(e.Source, 1)
					//atomic.AddUint32((*uint32)(&graph.Data.OutDegree[e.Source]), 1)
				}
				if graph.UseOutEdges {
					outIdx := e.Source - graph.Data.VertIDStart
					graph.Data.Edges.AppendOutEdge(outIdx, serialize.SUint32(e.Target))
				}
				//graph.Data.EdgeLocker[outIdx].Lock()
				////if graph.UseUndirected {
				////	graph.Data.BothEdges[outIdx] = append(graph.Data.BothEdges[outIdx], serialize.SUint32(e.Target))
				//if graph.UseOutEdges {
				//	graph.Data.OutEdges[outIdx] = append(graph.Data.OutEdges[outIdx], serialize.SUint32(e.Target))
				//}
				//graph.Data.EdgeLocker[outIdx].UnLock()

			}
		}
		atomic.AddInt64(&graph.EdgeCount, int64(edgeCount))
	} else {
		//load inEdges only
		atomic.AddInt64(&graph.EdgeCount, int64(count))
		for i := 0; i < len(data); {
			n, err := e.Unmarshal(data[i:])
			if err != nil {
				lb.SetStatusError(taskId, fmt.Sprintf("load graph read edge error: %s", err))
				logrus.Errorf("load graph read edge error: %s", err)
				break
			}
			i += n
			eIdx := e.Target - graph.Data.VertIDStart
			if eIdx > graph.Data.VertexCount {
				logrus.Warnf("edge out of range, source:%v ,target:%v", e.Source, e.Target)
			}
			graph.Data.Edges.AppendInEdge(eIdx, serialize.SUint32(e.Source))
			//graph.Data.EdgeLocker[eIdx].Lock()
			//graph.Data.InEdges[eIdx] = append(graph.Data.InEdges[eIdx], serialize.SUint32(e.Source))
			//graph.Data.EdgeLocker[eIdx].UnLock()
			if graph.UseProperty {
				n, err = prop.Unmarshal(data[i:])
				if err != nil {
					lb.SetStatusError(taskId, fmt.Sprintf("load graph read edge Property error: %s", err))
					logrus.Errorf("load graph read edge Property error: %s", err)
					break
				}
				i += n
				graph.Data.Edges.EdgeLockFunc(eIdx, func() {
					graph.Data.InEdgesProperty.AppendProp(prop, eIdx, graph.Data.InEdgesPropertySchema)
				})
				//graph.Data.EdgeLocker[eIdx].Lock()
				//graph.Data.InEdgesProperty.AppendProp(prop, eIdx, graph.Data.InEdgesPropertySchema)
				//graph.Data.EdgeLocker[eIdx].UnLock()
			}
		}
	}
	//graph.Locker.Unlock()
	loadTask.RecvWg.Done()
	atomic.AddInt32(loadTask.RecvCount[worker], 1)

	if end {
		// wait for all messages are processed
		loadTask.RecvWg.Wait()
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(loadTask.RecvCount[worker]) >= endNum {
				break
			}
			logrus.Warnf("There are still buffer left to be processed. From worker:%v", worker)
			logrus.Debugf("recv count:%v ,end num:%v ", *loadTask.RecvCount[worker], endNum)
			time.Sleep(100 * time.Millisecond)
		}

		tarStatus := structure.TaskStateLoaded
		if graph.UseOutDegree {
			tarStatus = structure.TaskStateLoadEdgeOK
		}
		var allWorkerComplete bool
		loadTask.Locker.Lock()
		loadTask.Task.SetWorkerState(worker, tarStatus)
		allWorkerComplete = loadTask.Task.CheckTaskState(tarStatus)
		loadTask.Locker.Unlock()
		if allWorkerComplete {
			graph.OptimizeMemory()
			ctx := context.Background()
			req2 := pb.WorkerEdgeCountReq{
				TaskId:     taskId,
				WorkerName: ServiceWorker.WorkerName,
				Count:      graph.EdgeCount,
			}
			graph.Data.EdgeCount = graph.EdgeCount
			_, err := ServiceWorker.MasterClient.WorkEdgeCount(ctx, &req2)
			if err != nil {
				logrus.Errorf("WorkEdgeCount error: %s", err)
				return
			}

			loadTask.Task.SetState(tarStatus)
			req := pb.LoadTaskStatusReq{
				WorkerName: ServiceWorker.WorkerName,
				TaskId:     taskId,
				State:      string(tarStatus),
			}
			ctx = context.Background()
			_, err = ServiceWorker.MasterClient.LoadTaskStatus(ctx, &req)
			if err != nil {
				logrus.Errorf("RecvEdge send load task status error: %s", err)
			}
			for s := range loadTask.RecvCount {
				*loadTask.RecvCount[s] = 0
			}
		}
	}
}

func (lb *LoadGraphBl) ScatterOutDegree(taskId int32) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("ScatterOutDegree panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("ScatterOutDegree panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	logrus.Infof("ScatterOutDegree start: %d", taskId)
	loadTask := LoadGraphMgr.GetLoadTask(taskId)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)

	workerCount := len(loadTask.Task.Workers)
	peers := make([]*PeerClient, 0, workerCount-1)
	for _, wn := range loadTask.Task.Workers {
		if wn.Name == ServiceWorker.WorkerName {
			lb.GatherOutDegree(taskId, wn.Name, true, []byte{})
			continue
		}
		peers = append(peers, PeerMgr.GetPeer(wn.Name))
	}

	if len(peers) == 0 {
		return
	}

	sendBuffer := buffer.EncodeBuffer{}
	sendBuffer.Init(BufferSize)

	vOffSet := serialize.SUint32(graph.Data.VertIDStart)
	_ = sendBuffer.Marshal(&vOffSet)

	for i := graph.Data.VertIDStart; i < graph.Data.VertIDStart+graph.Data.VertexCount; i++ {
		outDegree := graph.Data.Edges.GetOutDegree(i)
		_ = sendBuffer.Marshal(&outDegree)
		if sendBuffer.Full() {
			for _, peer := range peers {
				peer.LoadActionHandler.LoadAction(
					loadTask.Task.ID,
					pb.LoadAction_LoadOutDegree,
					int32(sendBuffer.ObjCount()),
					false,
					0,
					sendBuffer.PayLoad())
			}
			sendBuffer.Reset()
			vOffSet = serialize.SUint32(i + 1)
			_ = sendBuffer.Marshal(&vOffSet)
		}
	}

	for _, peer := range peers {
		peer.LoadActionHandler.LoadAction(
			loadTask.Task.ID,
			pb.LoadAction_LoadOutDegree,
			int32(sendBuffer.ObjCount()),
			true,
			0,
			sendBuffer.PayLoad())
	}
	sendBuffer.Reset()
}

func (lb *LoadGraphBl) GatherOutDegree(taskId int32, workerName string, end bool, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("GatherOutDegree panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("GatherOutDegree panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	logrus.Debugf("gather out degree worker:%v, end; %v", workerName, end)
	loadTask := LoadGraphMgr.GetLoadTask(taskId)
	if !lb.CheckAction(loadTask) {
		return
	}
	//defer lb.CheckAction(loadTask)
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	loadTask.RecvWg.Add(1)

	i := 0
	vOffSet := serialize.SUint32(0)
	if len(data) >= 4 {
		n, _ := vOffSet.Unmarshal(data)
		i += n
	}
	var outDegree serialize.SUint32
	for i < len(data) {
		n, err := outDegree.Unmarshal(data[i:])
		//n, err := graph.Data.OutDegree[int(vOffSet)].Unmarshal(data[i:])
		if err != nil {
			lb.SetStatusError(taskId, fmt.Sprintf("load graph gather out degree error: %s", err))
			logrus.Errorf("load graph gather out degree error: %s", err)
			break
		}
		graph.Data.Edges.SetOutDegree(uint32(vOffSet), outDegree)
		i += n
		vOffSet += 1
	}

	loadTask.RecvWg.Done()

	if end {
		// wait for all messages are processed
		loadTask.RecvWg.Wait()
		var allWorkerComplete bool
		loadTask.Locker.Lock()
		loadTask.Task.SetWorkerState(workerName, structure.TaskStateLoaded)
		allWorkerComplete = loadTask.Task.CheckTaskState(structure.TaskStateLoaded)
		loadTask.Locker.Unlock()
		if allWorkerComplete {
			//loadTask.Task.SetWorkerState(workerName, structure.TaskStateLoaded)
			//if loadTask.Task.CheckTaskState(structure.TaskStateLoaded) {
			loadTask.Task.SetState(structure.TaskStateLoaded)
			req := pb.LoadTaskStatusReq{
				WorkerName: ServiceWorker.WorkerName,
				TaskId:     taskId,
				State:      string(structure.TaskStateLoaded),
			}
			ctx := context.Background()
			_, err := ServiceWorker.MasterClient.LoadTaskStatus(ctx, &req)
			if err != nil {
				logrus.Errorf("LoadTaskStatus error: %s", err)
			}
		}
	}
}

func (lb *LoadGraphBl) LoadComplete(taskID int32) {
	defer func() {
		if r := recover(); r != nil {
			lb.SetStatusError(taskID, fmt.Sprintf("LoadComplete panic recover panic:%v, stack message: %s", r,
				common.GetCurrentGoroutineStack()))
			logrus.Errorf("LoadComplete panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskID)
	loadTask.Task.State = structure.TaskStateLoaded
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	graph.SetState(structure.GraphStateLoaded)
	logrus.Infof("graph load complete task: %d, graph: %s", taskID, loadTask.Task.GraphName)
	common.PrometheusMetrics.GraphLoadedCnt.WithLabelValues().Inc()
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(loadTask.Task.Type).Dec()
	common.PrometheusMetrics.VertexCnt.WithLabelValues(graph.Name).Set(float64(graph.Data.VertexCount))
	common.PrometheusMetrics.EdgeCnt.WithLabelValues(graph.Name).Set(float64(graph.EdgeCount))
	lb.endTask(taskID)
}

func (lb *LoadGraphBl) GetStatusError(taskID int32) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("GetStatusError panic recover taskID:%v, panic:%v, stack message: %s", taskID, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskID)
	if loadTask != nil {
		loadTask.Task.State = structure.TaskStateError
	}
	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	if graph != nil {
		graph.SetState(structure.GraphStateError)
	}
	time.AfterFunc(1*time.Minute, func() { lb.endTask(taskID) })
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(loadTask.Task.Type).Dec()
}

func (lb *LoadGraphBl) SetStatusError(taskId int32, message string) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("SetStatusError panic recover taskID:%v, panic:%v, stack message: %s", taskId, r,
				common.GetCurrentGoroutineStack())
		}
	}()
	loadTask := LoadGraphMgr.GetLoadTask(taskId)

	loadTask.Task.State = structure.TaskStateError

	graph := GraphMgr.GetGraphByName(loadTask.Task.SpaceName, loadTask.Task.GraphName)
	graph.SetState(structure.GraphStateError)
	logrus.Errorf("graph load task error: taskId:%d, graph: %s", taskId, loadTask.Task.GraphName)
	req := pb.LoadTaskStatusReq{
		WorkerName: ServiceWorker.WorkerName,
		TaskId:     taskId,
		State:      string(structure.TaskStateError),
		ErrorMsg:   message,
	}
	_, err := ServiceWorker.MasterClient.LoadTaskStatus(context.Background(), &req)
	if err != nil {
		logrus.Errorf("LoadTaskStatus error: %s", err)
	}
}

func (lb *LoadGraphBl) CheckAction(loadTask *graphio.LoadGraphTask) (isContinue bool) {
	if loadTask == nil {
		return true
	}
	switch atomic.LoadInt32(&loadTask.Task.Action) {
	case structure.ActionDoNoting:

	case structure.ActionCancelTask:
		lb.cancelAction(loadTask)
		return false
	case structure.ActionPauseTask:
		return lb.pauseAction(loadTask)
	default:
		logrus.Errorf("unknown action %d", loadTask.Task.Action)
	}
	//if computeTask.Task.State == structure.TaskStateCanceled {
	//	return true
	//} else if computeTask.Task.State == structure.TaskStateCanceling {
	//	logrus.Infof("task is canceled, task_id:%v", computeTask.Task.ID)
	//	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(computeTask.Task.Type).Dec()
	//	computeTask.Task.SetState(structure.TaskStateCanceled)
	//	time.AfterFunc(1*time.Minute, func() { cb.endTask(computeTask.Task.ID) })
	//	return true
	//}
	return true
}
func (lb *LoadGraphBl) cancelAction(loadTask *graphio.LoadGraphTask) {
	loadTask.Task.SetState(structure.TaskStateCanceled)
	common.PrometheusMetrics.TaskRunningCnt.WithLabelValues(loadTask.Task.Type).Dec()
	time.AfterFunc(1*time.Minute, func() { lb.endTask(loadTask.Task.ID) })
}

func (lb *LoadGraphBl) pauseAction(loadTask *graphio.LoadGraphTask) bool {
	task := loadTask.Task
	for {
		switch atomic.LoadInt32(&task.Action) {
		case structure.ActionCancelTask:
			lb.cancelAction(loadTask)
			return false
		case structure.ActionResumeTask:
			return true
		default:
			time.Sleep(10 * time.Second)
		}
	}
}
func (lb *LoadGraphBl) endTask(taskID int32) {
	loadGraphTask := LoadGraphMgr.GetLoadTask(taskID)
	if loadGraphTask != nil {
		loadGraphTask.FreeMemory()
		LoadGraphMgr.DeleteTask(loadGraphTask.Task.ID)
	}
}

type EdgesBl struct {
}

func (eb *EdgesBl) GetEdges(spaceName string, graphName string, vertId string, direction string) ([]string, []string, []*pb.EdgeProperty, error) {
	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		return []string{}, []string{}, []*pb.EdgeProperty{}, fmt.Errorf("graph %s not found", graphName)
	}
	shortId, ok := graph.Data.Vertex.GetVertexIndex(vertId)
	if !ok {
		return []string{}, []string{}, []*pb.EdgeProperty{}, fmt.Errorf("vertexId %s not found", vertId)
	}

	if shortId < graph.Data.VertIDStart ||
		shortId >= graph.Data.VertIDStart+graph.Data.VertexCount {
		return []string{}, []string{}, []*pb.EdgeProperty{}, fmt.Errorf("vertexId %s not in range", vertId)
	}

	getInEdges := func() ([]string, error) {
		//if graph.Data.InEdges == nil {
		//	return []string{}, fmt.Errorf("inEdges is nil")
		//}
		inEdges := graph.Data.Edges.GetInEdges(shortId - graph.Data.VertIDStart)
		edges := make([]string, 0, len(inEdges))
		for _, s := range inEdges {
			edges = append(edges, graph.Data.Vertex.GetVertex(uint32(s)).ID)
		}
		return edges, nil
	}

	getOutEdges := func() ([]string, error) {
		if !graph.UseOutEdges {
			return []string{}, fmt.Errorf("outEdges is nil")
		}
		outEdges := graph.Data.Edges.GetOutEdges(shortId - graph.Data.VertIDStart)
		edges := make([]string, 0, len(outEdges))
		for _, s := range outEdges {
			edges = append(edges, graph.Data.Vertex.GetVertex(uint32(s)).ID)
		}
		return edges, nil
	}

	var edgeProperties []*pb.EdgeProperty
	if graph != nil && graph.UseProperty {
		inEdges, err := getInEdges()
		if err != nil {
			return []string{}, []string{}, []*pb.EdgeProperty{}, err
		}
		edgeProperties = make([]*pb.EdgeProperty, 0, len(inEdges))
		for i, edge := range inEdges {
			properties := &pb.EdgeProperty{}
			properties.Edge = edge
			properties.Property = make(map[string]string)
			for _, ps := range graph.Data.InEdgesPropertySchema.Schema {
				prop, err := graph.Data.InEdgesProperty.GetValue(ps.PropKey, shortId-graph.Data.VertIDStart, uint32(i))
				if err != nil {
					return []string{}, []string{}, []*pb.EdgeProperty{}, err
				}
				properties.Property[ps.PropKey] = prop.ToString()
			}
			edgeProperties = append(edgeProperties, properties)
		}
	}

	if direction == "in" {
		inEdges, err := getInEdges()
		return inEdges, []string{}, edgeProperties, err
	} else if direction == "out" {
		outEdges, err := getOutEdges()
		return []string{}, outEdges, []*pb.EdgeProperty{}, err
	} else if direction == "both" {
		inEdges, err := getInEdges()
		outEdges, err := getOutEdges()
		return inEdges, outEdges, []*pb.EdgeProperty{}, err
	}
	return []string{}, []string{}, []*pb.EdgeProperty{}, fmt.Errorf("direction %s not supported", direction)
}

type DegreeBl struct {
}

func (db *DegreeBl) GetDegree(spaceName string, graphName string, vertId string, direction string) uint32 {
	graph := GraphMgr.GetGraphByName(spaceName, graphName)
	if graph == nil {
		return 0
	}
	shortId, ok := graph.Data.Vertex.GetVertexIndex(vertId)
	if !ok {
		return 0
	}

	//if shortId < graph.Data.VertIDStart ||
	//	shortId > graph.Data.VertIDStart+graph.Data.VertexCount {
	//	return 0
	//}

	degree := uint32(0)
	if direction == "in" || direction == "both" {
		degree += uint32(len(graph.Data.Edges.GetInEdges(shortId)))
	}

	if graph.UseOutDegree && (direction == "out" || direction == "both") {
		degree += uint32(graph.Data.Edges.GetOutDegree(shortId))
	}

	return degree
}
