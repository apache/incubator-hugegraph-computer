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

package graphs

import (
	"context"
	"fmt"
	"sync"
	"time"
	pb "vermeer/apps/protos"
	"vermeer/apps/structure"

	. "vermeer/apps/master/workers"

	"github.com/sirupsen/logrus"
)

type GraphPersistenceTaskBl struct {
}

type OperateType int8

const (
	// Save 写盘并从worker内存中删除数据
	Save OperateType = 0
	// Read 从磁盘中读取图数据
	Read OperateType = 1
	// WriteDisk 写盘，但不从worker内存中删除图数据
	WriteDisk OperateType = 2
)

var GraphPersistenceTask GraphPersistenceTaskBl

func (apt *GraphPersistenceTaskBl) Operate(spaceName string, dbName string, pt OperateType) ([]*structure.GraphPersistenceInfo, bool) {

	//workerClients := workerMgr.GetAllWorkers()
	workerReq := new(pb.GraphPersistenceReq)
	workerReq.GraphName = dbName
	workerReq.SpaceName = spaceName
	graph := graphMgr.GetGraphByName(workerReq.SpaceName, workerReq.GraphName)

	//对同一个图的操作，必须串行
	graph.Locker.Lock()
	defer graph.Locker.Unlock()

	workerInfos := make([]*structure.GraphPersistenceInfo, len(graph.Workers))
	success := true
	wg := &sync.WaitGroup{}
	for i, worker := range graph.Workers {
		client := workerMgr.GetWorker(worker.Name)
		workerInfos[i] = new(structure.GraphPersistenceInfo)

		wg.Add(1)
		go func(res *structure.GraphPersistenceInfo, c *WorkerClient, ok *bool, pType OperateType) {
			defer wg.Done()

			t := time.Now()
			var err error
			switch pt {
			case Read:
				_, err = c.Session.ReadGraph(context.Background(), workerReq)
			case Save:
				// TODO: double check within lock
				_, err = c.Session.SaveGraph(context.Background(), workerReq)
			case WriteDisk:
				//if !graph.OnDisk {
				_, err = c.Session.WriteDisk(context.Background(), workerReq)
				//}
			}
			cost := time.Since(t)

			res.WorkerName = c.Name
			res.Cost = fmt.Sprintf("%v", cost)
			if err != nil {
				res.Status = "error"
				res.ErrorInfo = err.Error()
				*ok = false
			} else {
				res.Status = "success"
			}
			logrus.Debugf("GraphPersistenceTask,operate: %v,info:%v", pt, res)

		}(workerInfos[i], client, &success, pt)
	}
	wg.Wait()
	if success {
		switch pt {
		case Read:
			graph.SetState(structure.GraphStateLoaded)
			logrus.Infof("graph: %v/%v read success", graph.SpaceName, graph.Name)
		case Save:
			graph.SetState(structure.GraphStateOnDisk)
			graph.OnDisk = true
			logrus.Infof("graph: %v/%v save success", graph.SpaceName, graph.Name)
		case WriteDisk:
			graph.OnDisk = true
			logrus.Infof("graph: %v/%v write success", graph.SpaceName, graph.Name)
		}

		if err := graphMgr.SaveInfo(graph.SpaceName, graph.Name); err != nil {
			logrus.Errorf("GraphPersistenceTask, saving graph info error: %v", err)
		}
	}
	return workerInfos, success

}

func (apt *GraphPersistenceTaskBl) autoSave() {

	//logrus.Infof("autoSave start time=%v", time.Now())

	workerClients := workerMgr.GetAllWorkers()

	needSave := false
	if len(workerClients) == 0 {
		logrus.Errorf("autoSave no workclient")
		return
	}
	for _, client := range workerClients {

		req := new(pb.WorkerStatInfoReq)
		res, err := client.Session.GetWorkerStatInfo(context.Background(), req)

		if err != nil {
			logrus.Errorf("autoSave get workerStatInfo res err:%v,worker=%v", err, client.Name)
		} else {
			memMachineUsedPercent := res.MemMachineUsedPercent
			memProcessUsedPercent := res.MemProcessUsedPercent

			if memMachineUsedPercent > 80 || memProcessUsedPercent > 50 {
				needSave = true
				logrus.Infof("autoSave workerStatInfo need save,res:%v,worker:%v", res, client.Name)
			}
		}
	}
	if needSave {

		records := accessMgr.LeastRecentRecords()
		if len(records) == 0 {
			//logrus.Errorf("autoSave no garph to save")
			return
		}
		for _, record := range records {
			dbName := record.Name
			graph := graphMgr.GetGraphByName(record.SpaceName, dbName)
			if graph.State != structure.GraphStateLoaded {
				continue
			} else {
				_, success := GraphPersistenceTask.Operate(record.SpaceName, dbName, Save)
				if !success {
					logrus.Errorf("autoSave graph %v failed", dbName)
				}
			}
			break
		}
	}
}

func (apt *GraphPersistenceTaskBl) Run() {
	go func() {
		for {
			time.Sleep(3 * time.Minute)
			GraphPersistenceTask.autoSave()
		}
	}()
}
